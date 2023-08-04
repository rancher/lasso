// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/rancher/lasso/pkg/controller (interfaces: SharedController)

// Package controller is a generated GoMock package.
package controller

import (
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
	client "github.com/rancher/lasso/pkg/client"
	cache "k8s.io/client-go/tools/cache"
)

// MockSharedController is a mock of SharedController interface.
type MockSharedController struct {
	ctrl     *gomock.Controller
	recorder *MockSharedControllerMockRecorder
}

// MockSharedControllerMockRecorder is the mock recorder for MockSharedController.
type MockSharedControllerMockRecorder struct {
	mock *MockSharedController
}

// NewMockSharedController creates a new mock instance.
func NewMockSharedController(ctrl *gomock.Controller) *MockSharedController {
	mock := &MockSharedController{ctrl: ctrl}
	mock.recorder = &MockSharedControllerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSharedController) EXPECT() *MockSharedControllerMockRecorder {
	return m.recorder
}

// Client mocks base method.
func (m *MockSharedController) Client() *client.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Client")
	ret0, _ := ret[0].(*client.Client)
	return ret0
}

// Client indicates an expected call of Client.
func (mr *MockSharedControllerMockRecorder) Client() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Client", reflect.TypeOf((*MockSharedController)(nil).Client))
}

// Enqueue mocks base method.
func (m *MockSharedController) Enqueue(arg0, arg1 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Enqueue", arg0, arg1)
}

// Enqueue indicates an expected call of Enqueue.
func (mr *MockSharedControllerMockRecorder) Enqueue(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Enqueue", reflect.TypeOf((*MockSharedController)(nil).Enqueue), arg0, arg1)
}

// EnqueueAfter mocks base method.
func (m *MockSharedController) EnqueueAfter(arg0, arg1 string, arg2 time.Duration) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "EnqueueAfter", arg0, arg1, arg2)
}

// EnqueueAfter indicates an expected call of EnqueueAfter.
func (mr *MockSharedControllerMockRecorder) EnqueueAfter(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueAfter", reflect.TypeOf((*MockSharedController)(nil).EnqueueAfter), arg0, arg1, arg2)
}

// EnqueueKey mocks base method.
func (m *MockSharedController) EnqueueKey(arg0 string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "EnqueueKey", arg0)
}

// EnqueueKey indicates an expected call of EnqueueKey.
func (mr *MockSharedControllerMockRecorder) EnqueueKey(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnqueueKey", reflect.TypeOf((*MockSharedController)(nil).EnqueueKey), arg0)
}

// Informer mocks base method.
func (m *MockSharedController) Informer() cache.SharedIndexInformer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Informer")
	ret0, _ := ret[0].(cache.SharedIndexInformer)
	return ret0
}

// Informer indicates an expected call of Informer.
func (mr *MockSharedControllerMockRecorder) Informer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Informer", reflect.TypeOf((*MockSharedController)(nil).Informer))
}

// RegisterHandler mocks base method.
func (m *MockSharedController) RegisterHandler(arg0 context.Context, arg1 string, arg2 SharedControllerHandler) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterHandler", arg0, arg1, arg2)
}

// RegisterHandler indicates an expected call of RegisterHandler.
func (mr *MockSharedControllerMockRecorder) RegisterHandler(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterHandler", reflect.TypeOf((*MockSharedController)(nil).RegisterHandler), arg0, arg1, arg2)
}

// Start mocks base method.
func (m *MockSharedController) Start(arg0 context.Context, arg1 int) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockSharedControllerMockRecorder) Start(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockSharedController)(nil).Start), arg0, arg1)
}