/*
Package store contains the sql backed store. It persists objects to a sqlite database.
*/
package store

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/db/transaction"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	_ "modernc.org/sqlite"
)

const (
	upsertStmtFmt   = `REPLACE INTO "%s"(key, object, objectnonce, dek, deknonce) VALUES (?, ?, ?, ?, ?)`
	deleteStmtFmt   = `DELETE FROM "%s" WHERE key = ?`
	getStmtFmt      = `SELECT object, objectnonce, dek, deknonce FROM "%s" WHERE key = ?`
	listStmtFmt     = `SELECT object, objectnonce, dek, deknonce FROM "%s"`
	listKeysStmtFmt = `SELECT key FROM "%s"`
)

// Store is a SQLite-backed cache.Store
type Store struct {
	c       cache.Indexer
	name    string
	encrypt bool
	typ     reflect.Type
	keyFunc cache.KeyFunc
	dbLock  *sync.RWMutex

	DBClient

	shouldEncrypt bool

	upsertStmt   *sql.Stmt
	deleteStmt   *sql.Stmt
	getStmt      *sql.Stmt
	listStmt     *sql.Stmt
	listKeysStmt *sql.Stmt

	afterUpsert []func(key string, obj any, tx db.TXClient) error
	afterDelete []func(key string, tx db.TXClient) error
}

// Test that Store implements cache.Indexer
var _ cache.Store = (*Store)(nil)

type DBClient interface {
	Begin() (db.TXClient, error)
	Prepare(stmt string) *sql.Stmt
	QueryForRows(ctx context.Context, stmt transaction.Stmt, params ...any) (*sql.Rows, error)
	ReadObjects(rows db.Rows, typ reflect.Type, shouldDecrypt bool) ([]any, error)
	ReadStrings(rows db.Rows) ([]string, error)
	Upsert(tx db.TXClient, stmt *sql.Stmt, key string, obj any, shouldEncrypt bool) error
	CloseStmt(closable db.Closable) error
	// TxModeClient() DBClient
}

var backoffRetry = wait.Backoff{Duration: 50 * time.Millisecond, Factor: 2, Steps: 10}

// NewStore creates a SQLite-backed cache.Store for objects of the given example type
func NewStore(example any, keyFunc cache.KeyFunc, c DBClient, shouldEncrypt bool, name string) (*Store, error) {
	s := &Store{
		name:          sanitize(name),
		typ:           reflect.TypeOf(example),
		DBClient:      c,
		keyFunc:       keyFunc,
		shouldEncrypt: shouldEncrypt,
		afterUpsert:   []func(key string, obj any, tx db.TXClient) error{},
		afterDelete:   []func(key string, tx db.TXClient) error{},
	}

	// once multiple informerfactories are needed, this can accept the case where table already exists error is received
	txC, err := s.Begin()
	if err != nil {
		return nil, err
	}
	err = txC.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		key TEXT UNIQUE NOT NULL PRIMARY KEY,
		object BLOB, objectnonce TEXT, dek TEXT, deknonce TEXT
	)`, s.name))
	if err != nil {
		return nil, err
	}

	err = txC.Commit()
	if err != nil {
		return nil, err
	}
	s.upsertStmt = s.Prepare(fmt.Sprintf(upsertStmtFmt, s.name))
	s.deleteStmt = s.Prepare(fmt.Sprintf(deleteStmtFmt, s.name))
	s.getStmt = s.Prepare(fmt.Sprintf(getStmtFmt, s.name))
	s.listStmt = s.Prepare(fmt.Sprintf(listStmtFmt, s.name))
	s.listKeysStmt = s.Prepare(fmt.Sprintf(listKeysStmtFmt, s.name))

	return s, nil
}

/* Core methods */
// upsert saves an obj with its key, or updates key with obj if it exists in this Store
func (s *Store) upsert(key string, obj any) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}

	err = s.Upsert(tx, s.upsertStmt, key, obj, s.shouldEncrypt)
	if err != nil {
		return err
	}

	err = s.runAfterUpsert(key, obj, tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// deleteByKey deletes the object associated with key, if it exists in this Store
func (s *Store) deleteByKey(key string) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}

	err = tx.StmtExec(tx.Stmt(s.deleteStmt), key)
	if err != nil {
		return err
	}

	err = s.runAfterDelete(key, tx)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// GetByKey returns the object associated with the given object's key
func (s *Store) GetByKey(key string) (item any, exists bool, err error) {
	rows, err := s.QueryForRows(context.TODO(), s.getStmt, key)
	if err != nil {
		return nil, false, err
	}
	result, err := s.ReadObjects(rows, s.typ, s.shouldEncrypt)
	if err != nil {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, nil
	}

	return result[0], true, nil
}

// replaceByKey will delete the contents of the Store, using instead the given key to obj map
func (s *Store) replaceByKey(objects map[string]any) error {
	rows, err := s.QueryForRows(context.TODO(), s.listKeysStmt)
	if err != nil {
		return err
	}
	keys, err := s.ReadStrings(rows)
	if err != nil {
		return err
	}

	txC, err := s.Begin()
	if err != nil {
		return err
	}

	for _, key := range keys {
		err = txC.StmtExec(txC.Stmt(s.deleteStmt), key)
		if err != nil {
			return err
		}
		err = s.runAfterDelete(key, txC)
		if err != nil {
			return err
		}
	}

	for key, obj := range objects {
		err = s.Upsert(txC, s.upsertStmt, key, obj, s.shouldEncrypt)
		if err != nil {
			return err
		}
		err = s.runAfterUpsert(key, obj, txC)
		if err != nil {
			return err
		}
	}

	return txC.Commit()
}

/* Satisfy cache.Store */

// Add saves an obj, or updates it if it exists in this Store
func (s *Store) Add(obj any) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}

	err = s.upsert(key, obj)
	return err
}

// Update saves an obj, or updates it if it exists in this Store
func (s *Store) Update(obj any) error {
	return s.Add(obj)
}

// Delete deletes the given object, if it exists in this Store
func (s *Store) Delete(obj any) error {
	key, err := s.keyFunc(obj)
	if err != nil {
		return err
	}
	return s.deleteByKey(key)
}

// List returns a list of all the currently known objects
// Note: I/O errors will panic this function, as the interface signature does not allow returning errors
func (s *Store) List() []any {
	rows, err := s.QueryForRows(context.TODO(), s.listStmt)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in Store.List"))
	}
	result, err := s.ReadObjects(rows, s.typ, s.shouldEncrypt)
	if err != nil {
		panic(errors.Wrap(err, "Unexpected error in Store.List"))
	}
	return result
}

// ListKeys returns a list of all the keys currently in this Store
// Note: Atm it doesn't appear returning nil in the case of an error has any detrimental effects. An error is not
// uncommon enough nor does it appear to necessitate a panic.
func (s *Store) ListKeys() []string {
	rows, err := s.QueryForRows(context.TODO(), s.listKeysStmt)
	if err != nil {
		fmt.Printf("Unexpected error in store.ListKeys: %v\n", err)
		return []string{}
	}
	result, err := s.ReadStrings(rows)
	if err != nil {
		fmt.Printf("Unexpected error in store.ListKeys: %v\n", err)
		return []string{}
	}
	return result
}

// Get returns the object with the same key as obj
func (s *Store) Get(obj any) (item any, exists bool, err error) {
	key, err := s.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}

	return s.GetByKey(key)
}

// Replace will delete the contents of the Store, using instead the given list
func (s *Store) Replace(objects []any, _ string) error {
	objectMap := map[string]any{}

	for _, object := range objects {
		key, err := s.keyFunc(object)
		if err != nil {
			return err
		}
		objectMap[key] = object
	}
	return s.replaceByKey(objectMap)
}

// Resync is a no-op and is deprecated
func (s *Store) Resync() error {
	return nil
}

/* Utilities */

// sanitize returns a string  that can be used in SQL as a name
func sanitize(s string) string {
	return strings.ReplaceAll(s, "\"", "")
}

// RegisterAfterUpsert registers a func to be called after each upsert
func (s *Store) RegisterAfterUpsert(f func(key string, obj any, txC db.TXClient) error) {
	s.afterUpsert = append(s.afterUpsert, f)
}

func (s *Store) GetName() string {
	return s.name
}

func (s *Store) GetShouldEncrypt() bool {
	return s.shouldEncrypt
}

func (s *Store) GetType() reflect.Type {
	return s.typ
}

// keep
// runAfterUpsert executes functions registered to run after upsert
func (s *Store) runAfterUpsert(key string, obj any, txC db.TXClient) error {
	for _, f := range s.afterUpsert {
		err := f(key, obj, txC)
		if err != nil {
			return err
		}
	}
	return nil
}

// RegisterAfterDelete registers a func to be called after each deletion
func (s *Store) RegisterAfterDelete(f func(key string, txC db.TXClient) error) {
	s.afterDelete = append(s.afterDelete, f)
}

// keep
// runAfterDelete executes functions registered to run after upsert
func (s *Store) runAfterDelete(key string, txC db.TXClient) error {
	for _, f := range s.afterDelete {
		err := f(key, txC)
		if err != nil {
			return err
		}
	}
	return nil
}
