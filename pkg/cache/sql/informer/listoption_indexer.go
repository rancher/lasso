package informer

import (
	"context"
	"database/sql"
	"encoding/gob"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

// ListOptionIndexer extends Indexer by allowing queries based on ListOption
type ListOptionIndexer struct {
	*Indexer

	indexedFields []string
	addField      *sql.Stmt
	deleteField   *sql.Stmt
}

var (
	defaultIndexedFields      = []string{"metadata.name", "metadata.namespace", "metadata.creationTimestamp"}
	typeSpecificIndexedFields = map[string][]string{
		"_v1_Namespace": {`metadata.labels["field.cattle.io/projectId"]`},
	}
	subfieldRegex = regexp.MustCompile(`([a-zA-Z]+)|(\["[a-zA-Z./]+"])|(\[[0-9]+])`)
)

const (
	matchFmt             = `%%%s%%`
	strictMatchFmt       = `%s`
	createFieldsTableFmt = `CREATE TABLE db2."%s_fields" (
			key VARCHAR NOT NULL PRIMARY KEY,
            %s
	   )`
	createFieldsIndexFmt = `CREATE INDEX db2."%s_%s_index" ON "%s_fields"("%s")`
)

// NewListOptionIndexer returns a SQLite-backed cache.Indexer of unstructured.Unstructured Kubernetes resources of a certain GVK
// ListOptionIndexer is also able to satisfy ListOption queries on indexed (sub)fields
// Fields are specified as slices (eg. "metadata.resourceVersion" is ["metadata", "resourceVersion"])
func NewListOptionIndexer(fields [][]string, s Store) (*ListOptionIndexer, error) {
	// necessary in order to gob/ungob unstructured.Unstructured objects
	gob.Register(map[string]interface{}{})

	i, err := NewIndexer(cache.Indexers{}, s)
	if err != nil {
		return nil, err
	}

	var indexedFields []string
	for _, f := range defaultIndexedFields {
		indexedFields = append(indexedFields, f)
	}
	for _, f := range fields {
		indexedFields = append(indexedFields, toColumnName(f))
	}

	if _, ok := typeSpecificIndexedFields[i.GetName()]; ok {
		for _, f := range typeSpecificIndexedFields[i.GetName()] {
			indexedFields = append(indexedFields, f)
		}
	}

	l := &ListOptionIndexer{
		Indexer:       i,
		indexedFields: indexedFields,
	}
	l.RegisterAfterUpsert(l.afterUpsert)
	l.RegisterAfterDelete(l.afterDelete)
	columnDefs := make([]string, len(indexedFields))
	sanitizedIndexFields := make([]string, len(indexedFields))
	for index, field := range indexedFields {
		sanitizedIndexField := db.Sanitize(field)
		column := fmt.Sprintf(`"%s" VARCHAR`, sanitizedIndexField)
		sanitizedIndexFields[index] = sanitizedIndexField
		columnDefs[index] = column
	}

	tx, err := l.Begin()
	if err != nil {
		return nil, err
	}
	err = tx.Exec(fmt.Sprintf(createFieldsTableFmt, i.GetName(), strings.Join(columnDefs, ", ")))
	if err != nil {
		return nil, err
	}

	for _, sanitizedField := range sanitizedIndexFields {
		err = tx.Exec(fmt.Sprintf(createFieldsIndexFmt, i.GetName(), sanitizedField, i.GetName(), sanitizedField))
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	columns := make([]string, len(indexedFields))
	for index, field := range sanitizedIndexFields {
		column := fmt.Sprintf(`"%s"`, field)
		columns[index] = column
	}

	qmarks := make([]string, len(indexedFields))
	for index := range indexedFields {
		qmarks[index] = "?"
	}

	setStatements := make([]string, len(indexedFields))
	for index, sanitizedField := range sanitizedIndexFields {
		setStatement := fmt.Sprintf(`"%s" = excluded."%s"`, sanitizedField, sanitizedField)
		setStatements[index] = setStatement
	}

	l.addField = l.Prepare(fmt.Sprintf(
		`INSERT INTO db2."%s_fields"(key, %s) VALUES (?, %s) ON CONFLICT DO UPDATE SET %s`,
		i.GetName(),
		strings.Join(columns, ", "),
		strings.Join(qmarks, ", "),
		strings.Join(setStatements, ", "),
	))
	l.deleteField = l.Prepare(fmt.Sprintf(`DELETE FROM db2."%s_fields" WHERE key = ?`, s.GetName()))
	return l, nil
}

/* Core methods */

// afterUpsert saves sortable/filterable fields into tables
func (l *ListOptionIndexer) afterUpsert(key string, obj any, tx db.TXClient) error {
	args := []any{key}
	for _, field := range l.indexedFields {
		value, err := getField(obj, field)
		if err != nil {
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
			return errors.Errorf("%v has a non-supported type value: %v", field, value)
		}
		if err != nil {
			return err
		}
	}

	return tx.StmtExec(tx.Stmt(l.addField), args...)
}

func (l *ListOptionIndexer) afterDelete(key string, tx db.TXClient) error {
	args := []any{key}
	return tx.StmtExec(tx.Stmt(l.deleteField), args...)
}

// ListByOptions returns objects according to the specified list options and partitions
// result is an unstructured.UnstructuredList, the continue token for the next page (or an error)
func (l *ListOptionIndexer) ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, string, error) {
	// 1- Intro: SELECT and JOIN clauses
	stmt := fmt.Sprintf(`SELECT o.object, o.objectnonce, o.dek, o.deknonce FROM "%s" o`, l.GetName())
	stmt += "\n  "
	stmt += fmt.Sprintf(`JOIN db2."%s_fields" f ON o.key = f.key`, l.GetName())
	params := []any{}

	// 2- Filtering: WHERE clauses (from lo.Filters)
	whereClauses := []string{}
	for _, orFilters := range lo.Filters {
		orClause, orParams := buildORClause(orFilters)
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
	for _, partition := range partitions {
		if partition.Passthrough {
			// nothing to do, no extra filtering to apply by definition
		} else {
			singlePartitionClauses := []string{}

			// filter by namespace
			if partition.Namespace != "" && partition.Namespace != "*" {
				singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.namespace" = ?`))
				params = append(params, partition.Namespace)
			}

			// optionally filter by names
			if !partition.All {
				names := partition.Names

				if len(names) == 0 {
					// degenerate case, there will be no results
					singlePartitionClauses = append(singlePartitionClauses, "FALSE")
				} else {
					singlePartitionClauses = append(singlePartitionClauses, fmt.Sprintf(`f."metadata.name" IN (?%s)`, strings.Repeat(", ?", len(partition.Names)-1)))
					for name := range partition.Names {
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
		stmt += "\n  WHERE\n    "
		for index, clause := range whereClauses {
			stmt += fmt.Sprintf("(%s)", clause)
			if index == len(whereClauses)-1 {
				break
			}
			stmt += " AND\n    "
		}
		// stmt += strings.Join(whereClauses, " AND\n    ")
	}

	// 2- Sorting: ORDER BY clauses (from lo.Sort)
	orderByClauses := []string{}
	if len(lo.Sort.PrimaryField) > 0 {
		columnName := toColumnName(lo.Sort.PrimaryField)
		direction := "ASC"
		if lo.Sort.PrimaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}
	if len(lo.Sort.SecondaryField) > 0 {
		columnName := toColumnName(lo.Sort.SecondaryField)
		direction := "ASC"
		if lo.Sort.SecondaryOrder == DESC {
			direction = "DESC"
		}
		orderByClauses = append(orderByClauses, fmt.Sprintf(`f."%s" %s`, columnName, direction))
	}

	if len(orderByClauses) > 0 {
		stmt += "\n  ORDER BY "
		stmt += strings.Join(orderByClauses, ", ")
	} else {
		// make sure one default order is always picked
		stmt += "\n  ORDER BY f.\"metadata.namespace\" ASC, f.\"metadata.name\" ASC "
	}

	// 3- Pagination: LIMIT clause (from lo.Pagination and/or lo.ChunkSize/lo.Resume)
	limitClause := ""
	offsetClause := ""
	// take the smallest limit between lo.Pagination and lo.ChunkSize
	limit := lo.Pagination.PageSize
	if limit == 0 || (lo.ChunkSize > 0 && lo.ChunkSize < limit) {
		limit = lo.ChunkSize
	}
	if limit > 0 {
		limitClause = "\n  LIMIT ?"
		// note: retrieve one extra row. If it comes back, then there are more pages and a continueToken should be created
		params = append(params, limit+1)
	}

	// OFFSET clause (from lo.Pagination and/or lo.Resume)
	offset := 0
	if lo.Resume != "" {
		offsetInt, err := strconv.Atoi(lo.Resume)
		if err != nil {
			return nil, "", err
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

	stmt += limitClause
	stmt += offsetClause

	// log the final query
	logrus.Debugf("ListOptionIndexer prepared statement: %v", stmt)
	logrus.Debugf("Params: %v", params...)

	// execute
	prepStmt := l.Prepare(stmt)
	defer l.CloseStmt(prepStmt)
	rows, err := l.QueryForRows(ctx, prepStmt, params...)
	if err != nil {
		return nil, "", err
	}
	items, err := l.ReadObjects(rows, l.GetType(), l.GetShouldEncrypt())
	if err != nil {
		return nil, "", err
	}

	continueToken := ""
	if limit > 0 && len(items) == limit+1 {
		// remove extra row
		items = items[:limit]
		continueToken = fmt.Sprintf("%d", offset+limit)
	}

	return toUnstructuredList(items), continueToken, nil
}

/* Utilities */

// buildORClause creates an SQLite compatible query that ORs conditions built from passed filters
func buildORClause(orFilters OrFilter) (string, []any) {
	var orWhereClause string
	var params []any

	for index, filter := range orFilters.Filters {
		opString := "LIKE"
		if filter.Op == NotEq {
			opString = "NOT LIKE"
		}
		columnName := toColumnName(filter.Field)
		orWhereClause += fmt.Sprintf(`f."%s" %s ?`, columnName, opString)
		format := strictMatchFmt
		if filter.Partial {
			format = matchFmt
		}
		params = append(params, fmt.Sprintf(format, filter.Match))
		if index == len(orFilters.Filters)-1 {
			continue
		}
		orWhereClause += " OR "
	}
	return orWhereClause, params
}

// toColumnName returns the column name corresponding to a field expressed as string slice
func toColumnName(s []string) string {
	return strings.Join(s, ".")
}

// getField extracts the value of a field expressed as a string path from an unstructured object
func getField(a any, field string) (any, error) {
	subFields := extractSubFields(field)
	o, ok := a.(*unstructured.Unstructured)
	if !ok {
		return nil, errors.Errorf("Unexpected object type, expected unstructured.Unstructured: %v", a)
	}

	var obj interface{}
	var found bool
	var err error
	obj = o.Object
	for _, subField := range subFields {
		switch t := obj.(type) {
		case map[string]interface{}:
			subField = strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]")
			obj, found, err = unstructured.NestedFieldNoCopy(t, subField)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, nil // fmt.Errorf("[listoption indexer] did not find key [%s] while indexing object", subField)
			}
		case []interface{}:
			key, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(subField, "["), "]"))
			if err != nil {
				return nil, fmt.Errorf("[listoption indexer] failed to convert subfield [%s] to int in listoption index: %w", subField, err)
			}
			if key >= len(t) {
				return nil, fmt.Errorf("[listoption indexer] given index is too large for slice of len %d", len(t))
			}
			obj = fmt.Sprintf("%v", t[key])
		default:
			return nil, fmt.Errorf("[listoption indexer] failed to parse subfields: %v", subFields)
		}
	}
	return obj, nil
}

func extractSubFields(fields string) []string {
	subfields := make([]string, 0)
	for _, subField := range subfieldRegex.FindAllString(fields, -1) {
		subfields = append(subfields, strings.TrimSuffix(db.Sanitize(subField), "."))
	}
	return subfields
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
