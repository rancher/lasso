package informer

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/db/transaction"
	"k8s.io/client-go/tools/cache"
)

const (
	selectQueryFmt = `
			SELECT object FROM "%[1]s"
				WHERE key IN (
					SELECT key FROM "%[1]s_indices"
						WHERE name = ? AND value IN (?%s)
				)
		`
	createTableFmt = `CREATE TABLE IF NOT EXISTS "%[1]s_indices" (
			name TEXT NOT NULL,
			value TEXT NOT NULL,
			key TEXT NOT NULL REFERENCES "%[1]s"(key) ON DELETE CASCADE,
			PRIMARY KEY (name, value, key)
        )`
	createIndexFmt = `CREATE INDEX IF NOT EXISTS "%[1]s_indices_index" ON "%[1]s_indices"(name, value)`

	deleteIndicesFmt = `DELETE FROM "%s_indices" WHERE key = ?`
	addIndexFmt      = `INSERT INTO "%s_indices"(name, value, key) VALUES (?, ?, ?)`
	listByIndexFmt   = `SELECT object FROM "%[1]s"
			WHERE key IN (
			    SELECT key FROM "%[1]s_indices"
			    	WHERE name = ? AND value = ?
			)`
	listKeyByIndexFmt  = `SELECT DISTINCT key FROM "%s_indices" WHERE name = ? AND value = ?`
	listIndexValuesFmt = `SELECT DISTINCT value FROM "%s_indices" WHERE name = ?`
)

// Indexer is a SQLite-backed cache.Indexer which builds upon Store adding an index table
type Indexer struct {
	Store
	indexers            cache.Indexers
	indexersLock        sync.RWMutex
	deleteIndicesStmt   *sql.Stmt
	addIndexStmt        *sql.Stmt
	listByIndexStmt     *sql.Stmt
	listKeysByIndexStmt *sql.Stmt
	listIndexValuesStmt *sql.Stmt
}

var _ cache.Indexer = (*Indexer)(nil)

type Store interface {
	DBClient
	cache.Store

	GetByKey(key string) (item any, exists bool, err error)
	GetName() string
	RegisterAfterUpsert(f func(key string, obj any, tx db.TXClient) error)
	RegisterAfterDelete(f func(key string, tx db.TXClient) error)
	GetShouldEncrypt() bool
	GetType() reflect.Type
}

type DBClient interface {
	Begin() (db.TXClient, error)
	QueryForRows(ctx context.Context, stmt transaction.Stmt, params ...any) (*sql.Rows, error)
	ReadObjects(rows db.Rows, typ reflect.Type, shouldDecrypt bool) ([]any, error)
	ReadStrings(rows db.Rows) ([]string, error)
	Prepare(stmt string) *sql.Stmt
	CloseStmt(stmt db.Closable) error
}

// NewIndexer returns a cache.Indexer backed by SQLite for objects of the given example type
func NewIndexer(indexers cache.Indexers, s Store) (*Indexer, error) {
	// sanity checks first
	for key := range indexers {
		if strings.Contains(key, `"`) || strings.Contains(key, `''`) {
			panic("Quote characters (\") or (') in indexer names are not supported")
		}
	}

	tx, err := s.Begin()
	if err != nil {
		return nil, err
	}
	err = tx.Exec(fmt.Sprintf(createTableFmt, db.Sanitize(s.GetName())))
	if err != nil {
		return nil, err
	}
	err = tx.Exec(fmt.Sprintf(createIndexFmt, db.Sanitize(s.GetName())))
	if err != nil {
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	i := &Indexer{
		Store:    s,
		indexers: indexers,
	}
	i.RegisterAfterUpsert(i.AfterUpsert)

	i.deleteIndicesStmt = s.Prepare(fmt.Sprintf(deleteIndicesFmt, db.Sanitize(s.GetName())))
	i.addIndexStmt = s.Prepare(fmt.Sprintf(addIndexFmt, db.Sanitize(s.GetName())))
	i.listByIndexStmt = s.Prepare(fmt.Sprintf(listByIndexFmt, db.Sanitize(s.GetName())))
	i.listKeysByIndexStmt = s.Prepare(fmt.Sprintf(listKeyByIndexFmt, db.Sanitize(s.GetName())))
	i.listIndexValuesStmt = s.Prepare(fmt.Sprintf(listIndexValuesFmt, db.Sanitize(s.GetName())))

	return i, nil
}

/* Core methods */

// AfterUpsert updates indices of an object
func (i *Indexer) AfterUpsert(key string, obj any, tx db.TXClient) error {
	// delete all
	err := tx.StmtExec(tx.Stmt(i.deleteIndicesStmt), key)
	if err != nil {
		return err
	}

	// re-insert all
	i.indexersLock.RLock()
	defer i.indexersLock.RUnlock()
	for indexName, indexFunc := range i.indexers {
		values, err := indexFunc(obj)
		if err != nil {
			return err
		}

		for _, value := range values {
			err = tx.StmtExec(tx.Stmt(i.addIndexStmt), indexName, value, key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// add afterDelete
/* Satisfy cache.Indexer */

// Index returns a list of items that match the given object on the index function
func (i *Indexer) Index(indexName string, obj any) ([]any, error) {
	i.indexersLock.RLock()
	defer i.indexersLock.RUnlock()
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("index with name %s does not exist", indexName)
	}

	values, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}

	if len(values) == 0 {
		return nil, nil
	}

	// typical case
	if len(values) == 1 {
		return i.ByIndex(indexName, values[0])
	}

	// atypical case - more than one value to lookup
	// HACK: sql.Statement.Query does not allow to pass slices in as of go 1.19 - create an ad-hoc statement
	query := fmt.Sprintf(fmt.Sprintf(selectQueryFmt, db.Sanitize(i.GetName()), strings.Repeat(", ?", len(values)-1)))
	stmt := i.Prepare(query)
	defer i.CloseStmt(stmt)
	// HACK: Query will accept []any but not []string
	params := []any{indexName}
	for _, value := range values {
		params = append(params, value)
	}

	rows, err := i.QueryForRows(context.TODO(), stmt, params...)
	if err != nil {
		return nil, err
	}
	return i.ReadObjects(rows, i.GetType(), i.GetShouldEncrypt())
}

// ByIndex returns the stored objects whose set of indexed values
// for the named index includes the given indexed value
func (i *Indexer) ByIndex(indexName, indexedValue string) ([]any, error) {
	rows, err := i.QueryForRows(context.TODO(), i.listByIndexStmt, indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return i.ReadObjects(rows, i.GetType(), i.GetShouldEncrypt())
}

// IndexKeys returns a list of the Store keys of the objects whose indexed values in the given index include the given indexed value
func (i *Indexer) IndexKeys(indexName, indexedValue string) ([]string, error) {
	i.indexersLock.RLock()
	defer i.indexersLock.Unlock()
	indexFunc := i.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	rows, err := i.QueryForRows(context.TODO(), i.listKeysByIndexStmt, false, indexName, indexedValue)
	if err != nil {
		return nil, err
	}
	return i.ReadStrings(rows)
}

// ListIndexFuncValues wraps safeListIndexFuncValues and panics in case of I/O errors
func (i *Indexer) ListIndexFuncValues(name string) []string {
	result, err := i.safeListIndexFuncValues(name)
	if err != nil {
		panic(fmt.Errorf("unexpected error in safeListIndexFuncValues: %w", err))
	}
	return result
}

// safeListIndexFuncValues returns all the indexed values of the given index
func (i *Indexer) safeListIndexFuncValues(indexName string) ([]string, error) {
	rows, err := i.QueryForRows(context.TODO(), i.listIndexValuesStmt, false, indexName)
	if err != nil {
		return nil, err
	}
	return i.ReadStrings(rows)
}

// GetIndexers returns the indexers
func (i *Indexer) GetIndexers() cache.Indexers {
	i.indexersLock.RLock()
	defer i.indexersLock.RUnlock()
	return i.indexers
}

// AddIndexers adds more indexers to this Store.  If you call this after you already have data
// in the Store, the results are undefined.
func (i *Indexer) AddIndexers(newIndexers cache.Indexers) error {
	i.indexersLock.Lock()
	defer i.indexersLock.Unlock()
	if i.indexers == nil {
		i.indexers = make(map[string]cache.IndexFunc)
	}
	for k, v := range newIndexers {
		i.indexers[k] = v
	}
	return nil
}
