package informer

import (
	"context"
	"database/sql"
	"encoding/gob"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	namespaced    bool
	indexedFields []string

	addFieldQuery     string
	deleteFieldQuery  string
	upsertLabelsQuery string
	deleteLabelsQuery string

	addFieldStmt     *sql.Stmt
	deleteFieldStmt  *sql.Stmt
	upsertLabelsStmt *sql.Stmt
	deleteLabelsStmt *sql.Stmt
}

var (
	defaultIndexedFields   = []string{"metadata.name", "metadata.creationTimestamp"}
	defaultIndexNamespaced = "metadata.namespace"
	subfieldRegex          = regexp.MustCompile(`([a-zA-Z]+)|(\[[a-zA-Z./]+])|(\[[0-9]+])`)

	InvalidColumnErr = errors.New("supplied column is invalid")
)

const (
	matchFmt                 = `%%%s%%`
	strictMatchFmt           = `%s`
	escapeBackslashDirective = ` ESCAPE '\'` // The leading space is crucial for unit tests only
	createFieldsTableFmt     = `CREATE TABLE "%s_fields" (
			key TEXT NOT NULL PRIMARY KEY,
            %s
	   )`
	createFieldsIndexFmt = `CREATE INDEX "%s_%s_index" ON "%s_fields"("%s")`

	failedToGetFromSliceFmt = "[listoption indexer] failed to get subfield [%s] from slice items: %w"

	createLabelsTableFmt = `CREATE TABLE IF NOT EXISTS "%s_labels" (
		key TEXT NOT NULL REFERENCES "%s"(key) ON DELETE CASCADE,
		label TEXT NOT NULL,
		value TEXT NOT NULL,
		PRIMARY KEY (key, label)
	)`
	createLabelsTableIndexFmt = `CREATE INDEX IF NOT EXISTS "%s_labels_index" ON "%s_labels"(label, value)`

	upsertLabelsStmtFmt = `REPLACE INTO "%s_labels"(key, label, value) VALUES (?, ?, ?)`
	deleteLabelsStmtFmt = `DELETE FROM "%s_labels" WHERE KEY = ?`
)

//QQQ: Prob not needed
//	UpsertLabels(tx db.TXClient, stmt *sql.Stmt, key string, obj any, shouldEncrypt bool) error

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields
// Fields are specified as slices (eg. "metadata.resourceVersion" is ["metadata", "resourceVersion"])
func NewListOptionIndexer(fields [][]string, s Store, namespaced bool) (*ListOptionIndexer, error) {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	i, err := NewIndexer(cache.Indexers{}, s)
	if err != nil {
		return nil, err
	}

	var indexedFields []string
	for _, f := range defaultIndexedFields {
		indexedFields = append(indexedFields, f)
	}
	if namespaced {
		indexedFields = append(indexedFields, defaultIndexNamespaced)
	}
	for _, f := range fields {
		indexedFields = append(indexedFields, toColumnName(f))
	}

	l := &ListOptionIndexer{
		Indexer:       i,
		namespaced:    namespaced,
		indexedFields: indexedFields,
	}
	l.RegisterAfterUpsert(l.addIndexFields)
	l.RegisterAfterUpsert(l.addLabels)
	l.RegisterAfterDelete(l.deleteIndexFields)
	l.RegisterAfterDelete(l.deleteLabels)
	columnDefs := make([]string, len(indexedFields))
	for index, field := range indexedFields {
		column := fmt.Sprintf(`"%s" TEXT`, field)
		columnDefs[index] = column
	}

	tx, err := l.BeginTx(context.Background(), true)
	if err != nil {
		return nil, err
	}
	dbName := db.Sanitize(i.GetName())
	err = tx.Exec(fmt.Sprintf(createFieldsTableFmt, dbName, strings.Join(columnDefs, ", ")))
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(indexedFields))
	qmarks := make([]string, len(indexedFields))
	setStatements := make([]string, len(indexedFields))

	for index, field := range indexedFields {
		// create index for field
		err = tx.Exec(fmt.Sprintf(createFieldsIndexFmt, dbName, field, dbName, field))
		if err != nil {
			return nil, err
		}

		// format field into column for prepared statement
		column := fmt.Sprintf(`"%s"`, field)
		columns[index] = column

		// add placeholder for column's value in prepared statement
		qmarks[index] = "?"

		// add formatted set statement for prepared statement
		setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, field, field)
		setStatements[index] = setStatement
	}
	createLabelsTableQuery := fmt.Sprintf(createLabelsTableFmt, dbName, dbName)
	err = tx.Exec(createLabelsTableQuery)
	if err != nil {
		return nil, &db.QueryError{QueryString: createLabelsTableQuery, Err: err}
	}

	createLabelsTableIndexQuery := fmt.Sprintf(createLabelsTableIndexFmt, dbName, dbName)
	err = tx.Exec(createLabelsTableIndexQuery)
	if err != nil {
		return nil, &db.QueryError{QueryString: createLabelsTableIndexQuery, Err: err}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	l.addFieldQuery = fmt.Sprintf(
		`INSERT INTO "%s_fields"(key, %s) VALUES (?, %s) ON CONFLICT DO UPDATE SET %s`,
		dbName,
		strings.Join(columns, ", "),
		strings.Join(qmarks, ", "),
		strings.Join(setStatements, ", "),
	)
	l.deleteFieldQuery = fmt.Sprintf(`DELETE FROM "%s_fields" WHERE key = ?`, dbName)

	l.addFieldStmt = l.Prepare(l.addFieldQuery)
	l.deleteFieldStmt = l.Prepare(l.deleteFieldQuery)

	l.upsertLabelsQuery = fmt.Sprintf(upsertLabelsStmtFmt, dbName)
	l.deleteLabelsQuery = fmt.Sprintf(deleteLabelsStmtFmt, dbName)
	l.upsertLabelsStmt = l.Prepare(l.upsertLabelsQuery)
	l.deleteLabelsStmt = l.Prepare(l.deleteLabelsQuery)

	return l, nil
}

/* Core methods */

// addIndexFields saves sortable/filterable fields into tables
func (l *ListOptionIndexer) addIndexFields(key string, obj any, tx db.TXClient) error {
	args := []any{key}
	for _, field := range l.indexedFields {
		value, err := getField(obj, field)
		if err != nil {
			logrus.Errorf("cannot index object of type [%s] with key [%s] for indexer [%s]: %v", l.GetType().String(), key, l.GetName(), err)
			cErr := tx.Cancel()
			if cErr != nil {
				return fmt.Errorf("could not cancel transaction: %s while recovering from error: %w", cErr, err)
			}
			return err
		}
		switch typedValue := value.(type) {
		case nil:
			args = append(args, "")
		case int, bool, string:
			args = append(args, fmt.Sprint(typedValue))
		case []string:
			args = append(args, strings.Join(typedValue, "|"))
		default:
			err2 := fmt.Errorf("field %v has a non-supported type value: %v", field, value)
			cErr := tx.Cancel()
			if cErr != nil {
				return fmt.Errorf("could not cancel transaction: %s while recovering from error: %w", cErr, err2)
			}
			return err2
		}
	}

	err := tx.StmtExec(tx.Stmt(l.addFieldStmt), args...)
	if err != nil {
		return &db.QueryError{QueryString: l.addFieldQuery, Err: err}
	}
	return nil
}

// labels are stored in tables that shadow the underlying object table for each GVK
func (l *ListOptionIndexer) addLabels(key string, obj any, tx db.TXClient) error {
	k8sObj, ok := obj.(*unstructured.Unstructured)
	if !ok {
		logrus.Debugf("UpsertLabels: Error?: Can't convert obj into an unstructured thing.")
		return nil
	}
	incomingLabels := k8sObj.GetLabels()
	for k, v := range incomingLabels {
		err := tx.StmtExec(tx.Stmt(l.upsertLabelsStmt), key, k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *ListOptionIndexer) deleteIndexFields(key string, tx db.TXClient) error {
	args := []any{key}

	err := tx.StmtExec(tx.Stmt(l.deleteFieldStmt), args...)
	if err != nil {
		return &db.QueryError{QueryString: l.deleteFieldQuery, Err: err}
	}
	return nil
}

func (l *ListOptionIndexer) deleteLabels(key string, tx db.TXClient) error {
	err := tx.StmtExec(tx.Stmt(l.deleteLabelsStmt), key)
	if err != nil {
		return &db.QueryError{QueryString: l.deleteLabelsQuery, Err: err}
	}
	return nil
}

// ListByOptions returns objects according to the specified list options and partitions.
// Specifically:
//   - an unstructured list of resources belonging to any of the specified partitions
//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
//   - a continue token, if there are more pages after the returned one
//   - an error instead of all of the above if anything went wrong
func (l *ListOptionIndexer) ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error) {
	// 1- First, what kind of filtering will be doing?
	dbName := db.Sanitize(l.GetName())
	// 1.1- Intro: SELECT and JOIN clauses
	query := fmt.Sprintf(`SELECT o.object, o.objectnonce, o.dekid FROM "%s" o`, dbName)
	query += "\n  "
	query += fmt.Sprintf(`JOIN "%s_fields" f ON o.key = f.key`, dbName)
	if hasLabelFilter(lo.Filters) {
		query += "\n  "
		query += fmt.Sprintf(`JOIN "%s_labels" lt ON o.key = lt.key`, dbName)
	}
	params := []any{}

	// 2- Filtering: WHERE clauses (from lo.Filters)
	whereClauses := []string{}
	for _, orFilters := range lo.Filters {
		orClause, orParams, err := l.buildORClauseFromFilters(orFilters)
		if err != nil {
			return nil, 0, "", err
		}
		if orClause == "" {
			continue
		}
		whereClauses = append(whereClauses, orClause)
		params = append(params, orParams...)
	}

	// WHERE clauses (from namespace)
	if namespace != "" && namespace != "*" {
		whereClauses = append(whereClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
		params = append(params, namespace)
	}

	// WHERE clauses (from partitions and their corresponding parameters)
	partitionClauses := []string{}
	for _, thisPartition := range partitions {
		if thisPartition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := []string{}

			// filter by namespace
			if thisPartition.Namespace != "" && thisPartition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
				params = append(params, thisPartition.Namespace)
			}

			// optionally filter by names
			if !thisPartition.All {
				names := thisPartition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.name" IN (?%s)`, strings.Repeat(", ?", len(thisPartition.Names)-1)))
					// sort for reproducibility
					sortedNames := thisPartition.Names.UnsortedList()
					sort.Strings(sortedNames)
					for _, name := range sortedNames {
						params = append(params, name)
					}
				}
			}

			if len(singlePartitionClauses) > 0 {
				partitionClauses = append(partitionClauses, strings.Join(singlePartitionClauses, " AND "))
			}
		}
	}
	if len(partitions) == 0 {
		// degenerate case, there will be no results
		whereClauses = append(whereClauses, "FALSE")
	}
	if len(partitionClauses) == 1 {
		whereClauses = append(whereClauses, partitionClauses[0])
	}
	if len(partitionClauses) > 1 {
		whereClauses = append(whereClauses, "(\n      ("+strings.Join(partitionClauses, ") OR\n      (")+")\n)")
	}

	if len(whereClauses) > 0 {
		query += "\n  WHERE\n    "
		for index, clause := range whereClauses {
			query += fmt.Sprintf("(%s)", clause)
			if index == len(whereClauses)-1 {
				break
			}
			query += " AND\n    "
		}
	}

	// 2- Sorting: ORDER BY clauses (from lo.Sort)
	orderByClauses := []string{}
	if len(lo.Sort.PrimaryField) > 0 {
		columnName := toColumnName(lo.Sort.PrimaryField)
		if err := l.validateColumn(columnName); err != nil {
			return nil, 0, "", err
		}

		direction := "ASC"
		if lo.Sort.PrimaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}
	if len(lo.Sort.SecondaryField) > 0 {
		columnName := toColumnName(lo.Sort.SecondaryField)
		if err := l.validateColumn(columnName); err != nil {
			return nil, 0, "", err
		}

		direction := "ASC"
		if lo.Sort.SecondaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}

	if len(orderByClauses) > 0 {
		query += "\n  ORDER BY "
		query += strings.Join(orderByClauses, ", ")
	} else {
		// make sure one default order is always picked
		if l.namespaced {
			query += "\n  ORDER BY f.\"metadata.namespace\" ASC, f.\"metadata.name\" ASC "
		} else {
			query += "\n  ORDER BY f.\"metadata.name\" ASC "
		}
	}

	// 4- Pagination: LIMIT clause (from lo.Pagination and/or lo.ChunkSize/lo.Resume)

	// before proceeding, save a copy of the query and params without LIMIT/OFFSET
	// for COUNTing all results later
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s)", query)
	countParams := params[:]

	limitClause := ""
	// take the smallest limit between lo.Pagination and lo.ChunkSize
	limit := lo.Pagination.PageSize
	if limit == 0 || (lo.ChunkSize > 0 && lo.ChunkSize < limit) {
		limit = lo.ChunkSize
	}
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		params = append(params, limit)
	}

	// OFFSET clause (from lo.Pagination and/or lo.Resume)
	offsetClause := ""
	offset := 0
	if lo.Resume != "" {
		offsetInt, err := strconv.Atoi(lo.Resume)
		if err != nil {
			return nil, 0, "", err
		}
		offset = offsetInt
	}
	if lo.Pagination.Page >= 1 {
		offset += lo.Pagination.PageSize * (lo.Pagination.Page - 1)
	}
	if offset > 0 {
		offsetClause = "\n  OFFSET ?"
		params = append(params, offset)
	}

	// assemble and log the final query
	query += limitClause
	query += offsetClause
	logrus.Debugf("ListOptionIndexer prepared statement: %v", query)
	logrus.Debugf("Params: %v", params)

	// execute
	stmt := l.Prepare(query)
	defer l.CloseStmt(stmt)

	tx, err := l.BeginTx(ctx, false)
	if err != nil {
		return nil, 0, "", err
	}

	txStmt := tx.Stmt(stmt)
	rows, err := txStmt.QueryContext(ctx, params...)
	if err != nil {
		if cerr := tx.Cancel(); cerr != nil {
			return nil, 0, "", fmt.Errorf("failed to cancel transaction (%v) after error: %w", cerr, err)
		}
		return nil, 0, "", &db.QueryError{QueryString: query, Err: err}
	}
	items, err := l.ReadObjects(rows, l.GetType(), l.GetShouldEncrypt())
	if err != nil {
		if cerr := tx.Cancel(); cerr != nil {
			return nil, 0, "", fmt.Errorf("failed to cancel transaction (%v) after error: %w", cerr, err)
		}
		return nil, 0, "", err
	}

	total := len(items)
	// if limit or offset were set, execute counting of all rows
	if limit > 0 || offset > 0 {
		countStmt := l.Prepare(countQuery)
		defer l.CloseStmt(countStmt)
		txStmt := tx.Stmt(countStmt)
		rows, err := txStmt.QueryContext(ctx, countParams...)
		if err != nil {
			if cerr := tx.Cancel(); cerr != nil {
				return nil, 0, "", fmt.Errorf("failed to cancel transaction (%v) after error: %w", cerr, err)
			}
			return nil, 0, "", fmt.Errorf("error executing query: %w", err)
		}
		total, err = l.ReadInt(rows)
		if err != nil {
			if cerr := tx.Cancel(); cerr != nil {
				return nil, 0, "", fmt.Errorf("failed to cancel transaction (%v) after error: %w", cerr, err)
			}
			return nil, 0, "", fmt.Errorf("error reading query results: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, "", err
	}

	continueToken := ""
	if limit > 0 && offset+len(items) < total {
		continueToken = fmt.Sprintf("%d", offset+limit)
	}

	return toUnstructuredList(items), total, continueToken, nil
}

func (l *ListOptionIndexer) validateColumn(column string) error {
	for _, v := range l.indexedFields {
		if v == column {
			return nil
		}
	}
	return fmt.Errorf("column is invalid [%s]: %w", column, InvalidColumnErr)
}

// buildORClause creates an SQLite compatible query that ORs conditions built from passed filters
func (l *ListOptionIndexer) buildORClauseFromFilters(orFilters OrFilter) (string, []any, error) {
	var params []any
	clauses := make([]string, 0, len(orFilters.Filters))
	var newParams []any
	var newClause string
	var err error

	for _, filter := range orFilters.Filters {
		if isLabelFilter(&filter) {
			newClause, newParams, err = l.getLabelFilter(filter)
		} else {
			newClause, newParams, err = l.getFieldFilter(filter)
		}
		if err != nil {
			return "", nil, err
		}
		clauses = append(clauses, newClause)
		params = append(params, newParams...)
	}
	return strings.Join(clauses, " OR "), params, nil
}

//type Filter struct {
//	Field   []string
//	Match   []string
//	Op      Op: one of
//	Partial bool
//}

// Possible ops from the k8s parser:
// KEY = and == (same) VALUE
// KEY != VALUE
// KEY exists []  # ,KEY, => this filter
// KEY ! []  # ,!KEY, => assert KEY doesn't exist
// KEY in VALUES
// KEY notin VALUES

func (l *ListOptionIndexer) getFieldFilter(filter Filter) (string, []any, error) {
	opString := ""
	escapeString := ""
	columnName := toColumnName(filter.Field)
	if err := l.validateColumn(columnName); err != nil {
		return "", nil, err
	}
	switch filter.Op {
	case Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`f."%s" %s ?%s`, columnName, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil
	case NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
		} else {
			opString = "!="
		}
		clause := fmt.Sprintf(`f."%s" %s ?%s`, columnName, opString, escapeString)
		return clause, []any{formatMatchTarget(filter)}, nil
	case Exists:
		clause := fmt.Sprintf(`f."%s" IS NOT NULL`, columnName)
		return clause, []any{}, nil
	case In:
		fallthrough
	case NotIn:
		target := "()"
		if len(filter.Matches) > 0 {
			target = fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)-1))
		}
		opString = "IN"
		if filter.Op == "notin" {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf(`f."%s" %s IN %s`, columnName, opString, target)
		matches := make([]any, len(filter.Matches))
		for i, match := range filter.Matches {
			matches[i] = match
		}
		return clause, matches, nil
	}

	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)
}

func (l *ListOptionIndexer) getLabelFilter(filter Filter) (string, []any, error) {
	opString := ""
	escapeString := ""
	if len(filter.Field) < 3 || filter.Field[0] != "metadata" || filter.Field[1] != "labels" {
		return "", nil, fmt.Errorf("expecting a metadata.labels field, got '%s'", strings.Join(filter.Field, "."))
	}
	matchFmtToUse := strictMatchFmt
	labelName := filter.Field[2]
	switch filter.Op {
	case Eq:
		if filter.Partial {
			opString = "LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "="
		}
		clause := fmt.Sprintf(`lt.label = ? AND lt.value %s ?%s`, opString, escapeString)
		return clause, []any{labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case NotEq:
		if filter.Partial {
			opString = "NOT LIKE"
			escapeString = escapeBackslashDirective
			matchFmtToUse = matchFmt
		} else {
			opString = "!="
		}
		clause := fmt.Sprintf(`lt.label = ? AND lt.value %s ?%s`, opString, escapeString)
		return clause, []any{labelName, formatMatchTargetWithFormatter(filter.Matches[0], matchFmtToUse)}, nil

	case Exists:
		clause := fmt.Sprintf(`lt.label = ?`)
		return clause, []any{labelName}, nil

	case In:
		fallthrough
	case NotIn:
		target := fmt.Sprintf("(?%s)", strings.Repeat(", ?", len(filter.Matches)))
		opString = "IN"
		if filter.Op == NotIn {
			opString = "NOT IN"
		}
		clause := fmt.Sprintf(`lt.label = ? AND lt.value %s %s`, opString, target)
		matches := make([]any, len(filter.Matches)+1)
		matches[0] = labelName
		for i, match := range filter.Matches {
			matches[i+1] = match
		}
		return clause, matches, nil
	}

	return "", nil, fmt.Errorf("unrecognized operator: %s", opString)

}

func formatMatchTarget(filter Filter) string {
	format := strictMatchFmt
	if filter.Partial {
		format = matchFmt
	}
	return formatMatchTargetWithFormatter(filter.Matches[0], format)
}

func formatMatchTargetWithFormatter(match string, format string) string {
	// To allow matches on the backslash itself, the character needs to be replaced first.
	// Otherwise, it will undo the following replacements.
	match = strings.ReplaceAll(match, `\`, `\\`)
	match = strings.ReplaceAll(match, `_`, `\_`)
	match = strings.ReplaceAll(match, `%`, `\%`)
	return fmt.Sprintf(format, match)
}

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return db.Sanitize(strings.Join(s, "."))
}

// getField extracts the value of a field expressed as a string path from an unstructured object
func getField(a any, field string) (any, error) {
	subFields := extractSubFields(field)
	o, ok := a.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unexpected object type, expected unstructured.Unstructured: %v", a)
	}

	var obj interface{}
	var found bool
	var err error
	obj = o.Object
	for i, subField := range subFields {
		switch t := obj.(type) {
		case map[string]interface{}:
			subField = strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]")
			obj, found, err = unstructured.NestedFieldNoCopy(t, subField)
			if err != nil {
				return nil, err
			}
			if !found {
				// particularly with labels/annotation indexes, it is totally possible that some objects won't have these,
				// so either we this is not an error state or it could be an error state with a type that callers can check for
				return nil, nil
			}
		case []interface{}:
			if strings.HasPrefix(subField, "[") && strings.HasSuffix(subField, "]") {
				key, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]"))
				if err != nil {
					return nil, fmt.Errorf("[listoption indexer] failed to convert subfield [%s] to int in listoption index: %w", subField, err)
				}
				if key >= len(t) {
					return nil, fmt.Errorf("[listoption indexer] given index is too large for slice of len %d", len(t))
				}
				obj = fmt.Sprintf("%v", t[key])
			} else if i == len(subFields)-1 {
				result := make([]string, len(t))
				for index, v := range t {
					itemVal, ok := v.(map[string]interface{})
					if !ok {
						return nil, fmt.Errorf(failedToGetFromSliceFmt, subField, err)
					}
					itemStr, ok := itemVal[subField].(string)
					if !ok {
						return nil, fmt.Errorf(failedToGetFromSliceFmt, subField, err)
					}
					result[index] = itemStr
				}
				return result, nil
			}
		default:
			return nil, fmt.Errorf("[listoption indexer] failed to parse subfields: %v", subFields)
		}
	}
	return obj, nil
}

func extractSubFields(fields string) []string {
	subfields := make([]string, 0)
	for _, subField := range subfieldRegex.FindAllString(fields, -1) {
		subfields = append(subfields, strings.TrimSuffix(subField, "."))
	}
	return subfields
}

func isLabelFilter(f *Filter) bool {
	return len(f.Field) >= 2 && f.Field[0] == "metadata" && f.Field[1] == "labels"
}

func hasLabelFilter(filters []OrFilter) bool {
	for _, outerFilter := range filters {
		for _, filter := range outerFilter.Filters {
			if isLabelFilter(&filter) {
				return true
			}
		}
	}
	return false
}

// toUnstructuredList turns a slice of unstructured objects into an unstructured.UnstructuredList
func toUnstructuredList(items []any) *unstructured.UnstructuredList {
	objectItems := make([]map[string]any, len(items))
	result := &unstructured.UnstructuredList{
		Items:  make([]unstructured.Unstructured, len(items)),
		Object: map[string]interface{}{"items": objectItems},
	}
	for i, item := range items {
		result.Items[i] = *item.(*unstructured.Unstructured)
		objectItems[i] = item.(*unstructured.Unstructured).Object
	}
	return result
}
