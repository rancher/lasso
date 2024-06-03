/*
Copyright 2023 SUSE LLC

Adapted from client-go, Copyright 2014 The Kubernetes Authors.
*/

package store

// Mocks for this test are generated with the following command.
//go:generate mockgen --build_flags=--mod=mod -package store -destination ./store_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/store DBClient
//go:generate mockgen --build_flags=--mod=mod -package store -destination ./db_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db TXClient,Rows
//go:generate mockgen --build_flags=--mod=mod -package store -destination ./tx_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db/transaction Stmt

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"github.com/rancher/lasso/pkg/cache/sql/db"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

const ()
const TEST_DB_LOCATION = "./sqlstore.sqlite"

func testStoreKeyFunc(obj interface{}) (string, error) {
	return obj.(testStoreObject).Id, nil
}

type testStoreObject struct {
	Id  string
	Val string
}

func TestAdd(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Add with no DB Client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		txC.EXPECT().Commit().Return(nil)
		err := store.Add(testObject)
		assert.Nil(t, err)
		// dbclient beginerr
	},
	})

	tests = append(tests, testCase{description: "Add with no DB Client errors and an afterUpsert function", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		txC.EXPECT().Commit().Return(nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)

		var count int
		store.afterUpsert = append(store.afterUpsert, func(key string, object any, tx db.TXClient) error {
			count++
			return nil
		})
		err := store.Add(testObject)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	},
	})

	tests = append(tests, testCase{description: "Add with no DB Client errors and an afterUpsert function that returns error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		store.afterUpsert = append(store.afterUpsert, func(key string, object any, txC db.TXClient) error {
			return fmt.Errorf("error")
		})
		err := store.Add(testObject)
		assert.NotNil(t, err)
		// dbclient beginerr
	},
	})

	tests = append(tests, testCase{description: "Add with DB Client Begin() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		c.EXPECT().Begin().Return(nil, fmt.Errorf("failed"))

		store := SetupStore(t, c, shouldEncrypt)
		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Add with DB Client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Add with DB Client Upsert() error with following Rollback() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Add with DB Client Commit() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		txC.EXPECT().Commit().Return(fmt.Errorf("failed"))

		err := store.Add(testObject)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Update updates the given object in the accumulator associated with the given object's key
func TestUpdate(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Update with no DB Client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		txC.EXPECT().Commit().Return(nil)
		err := store.Update(testObject)
		assert.Nil(t, err)
		// dbclient beginerr
	},
	})

	tests = append(tests, testCase{description: "Update with no DB Client errors and an afterUpsert function", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		txC.EXPECT().Commit().Return(nil)

		var count int
		store.afterUpsert = append(store.afterUpsert, func(key string, object any, txC db.TXClient) error {
			count++
			return nil
		})
		err := store.Update(testObject)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	},
	})

	tests = append(tests, testCase{description: "Update with no DB Client errors and an afterUpsert function that returns error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)

		store.afterUpsert = append(store.afterUpsert, func(key string, object any, txC db.TXClient) error {
			return fmt.Errorf("error")
		})
		err := store.Update(testObject)
		assert.NotNil(t, err)
	},
	})

	tests = append(tests, testCase{description: "Update with DB Client Begin() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		c.EXPECT().Begin().Return(nil, fmt.Errorf("failed"))

		store := SetupStore(t, c, shouldEncrypt)
		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Update with DB Client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Update with DB Client Upsert() error with following Rollback() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(fmt.Errorf("failed"))
		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	tests = append(tests, testCase{description: "Update with DB Client Commit() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, "something", testObject, store.shouldEncrypt).Return(nil)
		txC.EXPECT().Commit().Return(fmt.Errorf("failed"))

		err := store.Update(testObject)
		assert.NotNil(t, err)
	}})

	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Delete deletes the given object from the accumulator associated with the given object's key
func TestDelete(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Delete with no DB Client errors", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		// deleteStmt here will be an empty string since Prepare mock returns an empty *sql.Stmt
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id).Return(nil)
		txC.EXPECT().Commit().Return(nil)
		err := store.Delete(testObject)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with DB Client Begin() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(nil, fmt.Errorf("error"))
		// deleteStmt here will be an empty string since Prepare mock returns an empty *sql.Stmt
		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with TX Client StmtExec() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id).Return(fmt.Errorf("error"))
		// deleteStmt here will be an empty string since Prepare mock returns an empty *sql.Stmt
		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Delete with DB Client Commit() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(txC, nil)
		// deleteStmt here will be an empty string since Prepare mock returns an empty *sql.Stmt
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		// tx.EXPECT().
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id).Return(nil)
		txC.EXPECT().Commit().Return(fmt.Errorf("error"))
		err := store.Delete(testObject)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// List returns a list of all the currently non-empty accumulators
func TestList(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	testObject := testStoreObject{Id: "something", Val: "a"}

	var tests []testCase

	tests = append(tests, testCase{description: "List with no DB Client errors and no items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		items := store.List()
		assert.Len(t, items, 0)
	},
	})
	tests = append(tests, testCase{description: "List with no DB Client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		fakeItemsToReturn := []any{"something1", 2, false}
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(fakeItemsToReturn, nil)
		items := store.List()
		assert.Equal(t, fakeItemsToReturn, items)
	},
	})
	tests = append(tests, testCase{description: "List with DB Client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.listStmt).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		defer func() {
			recover()
		}()
		_ = store.List()
		assert.Fail(t, "Store list should panic when ReadObjects returns an error")
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// ListKeys returns a list of all the keys currently associated with non-empty accumulators
func TestListKeys(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "ListKeys with no DB Client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{"a", "b", "c"}, nil)
		keys := store.ListKeys()
		assert.Len(t, keys, 3)
	},
	})

	tests = append(tests, testCase{description: "ListKeys with DB Client ReadStrings() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return(nil, fmt.Errorf("error"))
		keys := store.ListKeys()
		assert.Len(t, keys, 0)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Get returns the accumulator associated with the given object's key
func TestGet(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "Get with no DB Client errors and object exists", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{testObject}, nil)
		item, exists, err := store.Get(testObject)
		assert.Nil(t, err)
		assert.Equal(t, item, testObject)
		assert.True(t, exists)
	},
	})
	tests = append(tests, testCase{description: "Get with no DB Client errors and object does not exist", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		item, exists, err := store.Get(testObject)
		assert.Nil(t, err)
		assert.Equal(t, item, nil)
		assert.False(t, exists)
	},
	})
	tests = append(tests, testCase{description: "Get with DB Client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		_, _, err := store.Get(testObject)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// GetByKey returns the accumulator associated with the given key
func TestGetByKey(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "GetByKey with no DB Client errors and item exists", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{testObject}, nil)
		item, exists, err := store.GetByKey(testObject.Id)
		assert.Nil(t, err)
		assert.Equal(t, item, testObject)
		assert.True(t, exists)
	},
	})
	tests = append(tests, testCase{description: "GetByKey with no DB Client errors and item does not exist", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return([]any{}, nil)
		item, exists, err := store.GetByKey(testObject.Id)
		assert.Nil(t, err)
		assert.Equal(t, nil, item)
		assert.False(t, exists)
	},
	})
	tests = append(tests, testCase{description: "GetByKey with DB Client ReadObjects() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().QueryForRows(context.TODO(), store.getStmt, testObject.Id).Return(r, nil)
		c.EXPECT().ReadObjects(r, reflect.TypeOf(testObject), store.shouldEncrypt).Return(nil, fmt.Errorf("error"))
		_, _, err := store.GetByKey(testObject.Id)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Replace will delete the contents of the store, using instead the
// given list. Store takes ownership of the list, you should not reference
// it after calling this function.
func TestReplace(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	testObject := testStoreObject{Id: "something", Val: "a"}
	tests = append(tests, testCase{description: "Replace with no DB Client errors and some items", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(txC, nil)
		txC.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{testObject.Id}, nil)
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id)
		c.EXPECT().Upsert(txC, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt)
		txC.EXPECT().Commit()
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with no DB Client errors and no items", test: func(t *testing.T, shouldEncrypt bool) {
		c, tx := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{}, nil)
		c.EXPECT().Upsert(tx, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt)
		tx.EXPECT().Commit()
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with DB Client Begin() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		c.EXPECT().Begin().Return(nil, fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with no DB Client ReadStrings() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, tx := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return(nil, fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with ReadStrings() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, tx := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return(nil, fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with TX Client StmtExec() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(txC, nil)
		txC.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{testObject.Id}, nil)
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id).Return(fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Replace with DB Client Upsert() error", test: func(t *testing.T, shouldEncrypt bool) {
		c, txC := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		r := &sql.Rows{}
		c.EXPECT().Begin().Return(txC, nil)
		txC.EXPECT().Stmt(store.listKeysStmt).Return(store.listKeysStmt)
		c.EXPECT().QueryForRows(context.TODO(), store.listKeysStmt).Return(r, nil)
		c.EXPECT().ReadStrings(r).Return([]string{testObject.Id}, nil)
		txC.EXPECT().Stmt(store.deleteStmt).Return(store.deleteStmt)
		txC.EXPECT().StmtExec(store.deleteStmt, testObject.Id).Return(nil)
		c.EXPECT().Upsert(txC, store.upsertStmt, testObject.Id, testObject, store.shouldEncrypt).Return(fmt.Errorf("error"))
		err := store.Replace([]any{testObject}, testObject.Id)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

// Resync is meaningless in the terms appearing here but has
// meaning in some implementations that have non-trivial
// additional behavior (e.g., DeltaFIFO).
func TestResync(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T, shouldEncrypt bool)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "Resync shouldn't call the client, panic, or do anything else", test: func(t *testing.T, shouldEncrypt bool) {
		c, _ := SetupMockDB(t)
		store := SetupStore(t, c, shouldEncrypt)
		err := store.Resync()
		assert.Nil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t, false) })
		t.Run(fmt.Sprintf("%s with encryption", test.description), func(t *testing.T) { test.test(t, true) })
	}
}

func SetupMockDB(t *testing.T) (*MockDBClient, *MockTXClient) {
	dbC := NewMockDBClient(gomock.NewController(t)) // add functionality once store expectation are known
	txC := NewMockTXClient(gomock.NewController(t))
	// stmt := NewMockStmt(gomock.NewController())
	txC.EXPECT().Exec(fmt.Sprintf(createTableFmt, "testStoreObject")).Return(nil)
	txC.EXPECT().Commit().Return(nil)
	dbC.EXPECT().Begin().Return(txC, nil)

	// use stmt mock here
	dbC.EXPECT().Prepare(fmt.Sprintf(upsertStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(deleteStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(getStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(listStmtFmt, "testStoreObject")).Return(&sql.Stmt{})
	dbC.EXPECT().Prepare(fmt.Sprintf(listKeysStmtFmt, "testStoreObject")).Return(&sql.Stmt{})

	return dbC, txC
}
func SetupStore(t *testing.T, client *MockDBClient, shouldEncrypt bool) *Store {
	store, err := NewStore(testStoreObject{}, testStoreKeyFunc, client, shouldEncrypt, "testStoreObject")
	if err != nil {
		t.Error(err)
	}
	return store
}
