package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

// Mocks for this test are generated with the following command.
//go:generate mockgen --build_flags=--mod=mod -package db -destination ./db_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db Rows,Connection,Encryptor,Decryptor,TXClient
//go:generate mockgen --build_flags=--mod=mod -package db -destination ./transaction_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db/transaction Stmt,SQLTx

type testStoreObject struct {
	Id  string
	Val string
}

func TestNewClient(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query rows with no params, no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		expectedClient := Client{
			conn:      c,
			encryptor: e,
			decryptor: d,
		}
		client, err := NewClient(c, e, d)
		assert.Nil(t, err)
		assert.Equal(t, expectedClient, *client)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
func TestQueryForRows(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query rows with no params, no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		client := SetupClient(t, c, nil, nil)
		s := NewMockStmt(gomock.NewController(t))
		ctx := context.TODO()
		r := &sql.Rows{}
		s.EXPECT().QueryContext(ctx).Return(r, nil)
		rows, err := client.QueryForRows(ctx, s)
		assert.Nil(t, err)
		assert.Equal(t, r, rows)
	},
	})
	tests = append(tests, testCase{description: "Query rows with params, QueryContext() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		client := SetupClient(t, c, nil, nil)
		s := NewMockStmt(gomock.NewController(t))
		ctx := context.TODO()
		s.EXPECT().QueryContext(ctx).Return(nil, fmt.Errorf("error"))
		_, err := client.QueryForRows(ctx, s)
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestQueryObjects(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := testStoreObject{Id: "something", Val: "a"}
	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Query objects, with one row, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*sql.RawBytes)
				*vk = toBytes(testObject)
			}
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), toBytes(testObject), toBytes(testObject)).Return(toBytes(testObject), nil)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a decrypt error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*sql.RawBytes)
				*vk = toBytes(testObject)
			}
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), toBytes(testObject), toBytes(testObject)).Return(nil, fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a Scan() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and a Close() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*sql.RawBytes)
				*vk = toBytes(testObject)
			}
		})
		d.EXPECT().Decrypt(toBytes(testObject), toBytes(testObject), toBytes(testObject), toBytes(testObject)).Return(toBytes(testObject), nil)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Query objects, with no rows, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadObjects(r, reflect.TypeOf(testObject), true)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(items))
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestQueryStrings(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := testStoreObject{Id: "something", Val: "a"}
	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadStrings(r)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(items))
	},
	})
	tests = append(tests, testCase{description: "Query objects, with one row, and Scan error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and Err() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(fmt.Errorf("error"))
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with one row, and Close() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(true)
		r.EXPECT().Scan(gomock.Any()).Do(func(a ...any) {
			for _, v := range a {
				vk := v.(*string)
				*vk = string(toBytes(testObject.Id))
			}
		})
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Close().Return(fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.ReadStrings(r)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "ReadStrings(), with no rows, and no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)
		r := SetupMockRows(t)
		r.EXPECT().Next().Return(false)
		r.EXPECT().Err().Return(nil)
		r.EXPECT().Close().Return(nil)
		client := SetupClient(t, c, e, d)
		items, err := client.ReadStrings(r)
		assert.Nil(t, err)
		assert.Equal(t, 0, len(items))
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestBegin(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Begin(), with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		sqlTx := &sql.Tx{}
		c.EXPECT().Begin().Return(sqlTx, nil)
		client := SetupClient(t, c, e, d)
		txC, err := client.Begin()
		assert.Nil(t, err)
		assert.NotNil(t, txC)
	},
	})
	tests = append(tests, testCase{description: "Begin(), with connection Begin() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		c.EXPECT().Begin().Return(nil, fmt.Errorf("error"))
		client := SetupClient(t, c, e, d)
		_, err := client.Begin()
		assert.NotNil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestUpsert(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	testObject := testStoreObject{Id: "something", Val: "a"}
	// Tests with shouldEncryptSet to false
	tests = append(tests, testCase{description: "Upsert() with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTXClient(gomock.NewController(t))
		sqlStmt := &sql.Stmt{}
		stmt := NewMockStmt(gomock.NewController(t))
		testObjBytes := toBytes(testObject)
		testByteValue := []byte("something")
		e.EXPECT().Encrypt(testObjBytes).Return(testByteValue, testByteValue, testByteValue, testByteValue, nil)
		txC.EXPECT().Stmt(sqlStmt).Return(stmt)
		txC.EXPECT().StmtExec(stmt, "somekey", testByteValue, testByteValue, testByteValue, testByteValue).Return(nil)
		err := client.Upsert(txC, sqlStmt, "somekey", testObject, true)
		assert.Nil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with Encrypt() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTXClient(gomock.NewController(t))
		sqlStmt := &sql.Stmt{}
		testObjBytes := toBytes(testObject)
		e.EXPECT().Encrypt(testObjBytes).Return(nil, nil, nil, nil, fmt.Errorf("error"))
		err := client.Upsert(txC, sqlStmt, "somekey", testObject, true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with StmtExec() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTXClient(gomock.NewController(t))
		sqlStmt := &sql.Stmt{}
		stmt := NewMockStmt(gomock.NewController(t))
		testObjBytes := toBytes(testObject)
		testByteValue := []byte("something")
		e.EXPECT().Encrypt(testObjBytes).Return(testByteValue, testByteValue, testByteValue, testByteValue, nil)
		txC.EXPECT().Stmt(sqlStmt).Return(stmt)
		txC.EXPECT().StmtExec(stmt, "somekey", testByteValue, testByteValue, testByteValue, testByteValue).Return(fmt.Errorf("error"))
		err := client.Upsert(txC, sqlStmt, "somekey", testObject, true)
		assert.NotNil(t, err)
	},
	})
	tests = append(tests, testCase{description: "Upsert() with no errors and shouldEncrypt false", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		d := SetupMockDecryptor(t)
		e := SetupMockEncryptor(t)

		client := SetupClient(t, c, e, d)
		txC := NewMockTXClient(gomock.NewController(t))
		sqlStmt := &sql.Stmt{}
		stmt := NewMockStmt(gomock.NewController(t))
		var testByteValue []byte
		testObjBytes := toBytes(testObject)
		txC.EXPECT().Stmt(sqlStmt).Return(stmt)
		txC.EXPECT().StmtExec(stmt, "somekey", testObjBytes, testByteValue, testByteValue, testByteValue).Return(nil)
		err := client.Upsert(txC, sqlStmt, "somekey", testObject, false)
		assert.Nil(t, err)
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestPrepare(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "Prepare() with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		sqlStmt := &sql.Stmt{}
		c.EXPECT().Prepare("something").Return(sqlStmt, nil)

		stmt := client.Prepare("something")
		assert.Equal(t, sqlStmt, stmt)
	},
	})
	tests = append(tests, testCase{description: "Prepare() with Connection Prepare() error", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		c.EXPECT().Prepare("something").Return(nil, fmt.Errorf("error"))

		assert.Panics(t, func() { client.Prepare("something") })
	},
	})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestNewConnection(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase
	tests = append(tests, testCase{description: "Prepare() with no errors", test: func(t *testing.T) {
		c := SetupMockConnection(t)
		e := SetupMockEncryptor(t)
		d := SetupMockDecryptor(t)

		client := SetupClient(t, c, e, d)
		sqlStmt := &sql.Stmt{}
		c.EXPECT().Prepare("something").Return(sqlStmt, nil)

		err := client.NewConnection()
		assert.Nil(t, err)
		assert.FileExists(t, InformerObjectCacheDBPath)
		err = os.Remove(InformerObjectCacheDBPath)
		if err != nil {
			assert.Fail(t, "could not remove object cache path after test")
		}
	},
	})
}

func TestCommit(t *testing.T) {

}

func TestRollback(t *testing.T) {

}

func SetupMockConnection(t *testing.T) *MockConnection {
	mockC := NewMockConnection(gomock.NewController(t))
	return mockC
}

func SetupMockEncryptor(t *testing.T) *MockEncryptor {
	mockE := NewMockEncryptor(gomock.NewController(t))
	return mockE
}

func SetupMockDecryptor(t *testing.T) *MockDecryptor {
	MockD := NewMockDecryptor(gomock.NewController(t))
	return MockD
}

func SetupMockRows(t *testing.T) *MockRows {
	MockR := NewMockRows(gomock.NewController(t))
	return MockR
}

func SetupClient(t *testing.T, connection Connection, encryptor Encryptor, decryptor Decryptor) *Client {
	c, _ := NewClient(connection, encryptor, decryptor)
	return c
}
