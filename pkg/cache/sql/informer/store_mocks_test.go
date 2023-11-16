// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/lasso/pkg/cache/sql/store (interfaces: DBClient)

// Package informer is a generated GoMock package.
package informer

import (
	context "context"
	sql "database/sql"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	db "github.com/rancher/lasso/pkg/cache/sql/db"
	transaction "github.com/rancher/lasso/pkg/cache/sql/db/transaction"
)

// MockDBClient is a mock of DBClient interface.
type MockDBClient struct {
	ctrl     *gomock.Controller
	recorder *MockDBClientMockRecorder
}

// MockDBClientMockRecorder is the mock recorder for MockDBClient.
type MockDBClientMockRecorder struct {
	mock *MockDBClient
}

// NewMockDBClient creates a new mock instance.
func NewMockDBClient(ctrl *gomock.Controller) *MockDBClient {
	mock := &MockDBClient{ctrl: ctrl}
	mock.recorder = &MockDBClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDBClient) EXPECT() *MockDBClientMockRecorder {
	return m.recorder
}

// Begin mocks base method.
func (m *MockDBClient) Begin() (db.TXClient, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Begin")
	ret0, _ := ret[0].(db.TXClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Begin indicates an expected call of Begin.
func (mr *MockDBClientMockRecorder) Begin() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Begin", reflect.TypeOf((*MockDBClient)(nil).Begin))
}

// CloseStmt mocks base method.
func (m *MockDBClient) CloseStmt(arg0 db.Closable) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseStmt", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseStmt indicates an expected call of CloseStmt.
func (mr *MockDBClientMockRecorder) CloseStmt(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseStmt", reflect.TypeOf((*MockDBClient)(nil).CloseStmt), arg0)
}

// Prepare mocks base method.
func (m *MockDBClient) Prepare(arg0 string) *sql.Stmt {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Prepare", arg0)
	ret0, _ := ret[0].(*sql.Stmt)
	return ret0
}

// Prepare indicates an expected call of Prepare.
func (mr *MockDBClientMockRecorder) Prepare(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Prepare", reflect.TypeOf((*MockDBClient)(nil).Prepare), arg0)
}

// QueryForRows mocks base method.
func (m *MockDBClient) QueryForRows(arg0 context.Context, arg1 transaction.Stmt, arg2 ...interface{}) (*sql.Rows, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0, arg1}
	for _, a := range arg2 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "QueryForRows", varargs...)
	ret0, _ := ret[0].(*sql.Rows)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryForRows indicates an expected call of QueryForRows.
func (mr *MockDBClientMockRecorder) QueryForRows(arg0, arg1 interface{}, arg2 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0, arg1}, arg2...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryForRows", reflect.TypeOf((*MockDBClient)(nil).QueryForRows), varargs...)
}

// QueryObjects mocks base method.
func (m *MockDBClient) ReadObjects(arg0 db.Rows, arg1 reflect.Type, arg2 bool) ([]interface{}, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadObjects", arg0, arg1, arg2)
	ret0, _ := ret[0].([]interface{})
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryObjects indicates an expected call of QueryObjects.
func (mr *MockDBClientMockRecorder) QueryObjects(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadObjects", reflect.TypeOf((*MockDBClient)(nil).ReadObjects), arg0, arg1, arg2)
}

// QueryStrings mocks base method.
func (m *MockDBClient) ReadStrings(arg0 db.Rows) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadStrings", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryStrings indicates an expected call of QueryStrings.
func (mr *MockDBClientMockRecorder) QueryStrings(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadStrings", reflect.TypeOf((*MockDBClient)(nil).ReadStrings), arg0)
}

// Upsert mocks base method.
func (m *MockDBClient) Upsert(arg0 db.TXClient, arg1 *sql.Stmt, arg2 string, arg3 interface{}, arg4 bool) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Upsert", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Upsert indicates an expected call of Upsert.
func (mr *MockDBClientMockRecorder) Upsert(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Upsert", reflect.TypeOf((*MockDBClient)(nil).Upsert), arg0, arg1, arg2, arg3, arg4)
}
