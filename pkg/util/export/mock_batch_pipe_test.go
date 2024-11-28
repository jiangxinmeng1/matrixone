// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/util/batchpipe/batch_pipe.go
//
// Generated by this command:
//
//	mockgen -source pkg/util/batchpipe/batch_pipe.go --destination pkg/util/export/mock_batch_pipe_test.go -package=export
//

// Package export is a generated GoMock package.
package export

import (
	bytes "bytes"
	context "context"
	reflect "reflect"
	time "time"

	gomock "github.com/golang/mock/gomock"
)

// MockHasName is a mock of HasName interface.
type MockHasName struct {
	ctrl     *gomock.Controller
	recorder *MockHasNameMockRecorder
	isgomock struct{}
}

// MockHasNameMockRecorder is the mock recorder for MockHasName.
type MockHasNameMockRecorder struct {
	mock *MockHasName
}

// NewMockHasName creates a new mock instance.
func NewMockHasName(ctrl *gomock.Controller) *MockHasName {
	mock := &MockHasName{ctrl: ctrl}
	mock.recorder = &MockHasNameMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockHasName) EXPECT() *MockHasNameMockRecorder {
	return m.recorder
}

// GetName mocks base method.
func (m *MockHasName) GetName() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetName")
	ret0, _ := ret[0].(string)
	return ret0
}

// GetName indicates an expected call of GetName.
func (mr *MockHasNameMockRecorder) GetName() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetName", reflect.TypeOf((*MockHasName)(nil).GetName))
}

// MockReminder is a mock of Reminder interface.
type MockReminder struct {
	ctrl     *gomock.Controller
	recorder *MockReminderMockRecorder
	isgomock struct{}
}

// MockReminderMockRecorder is the mock recorder for MockReminder.
type MockReminderMockRecorder struct {
	mock *MockReminder
}

// NewMockReminder creates a new mock instance.
func NewMockReminder(ctrl *gomock.Controller) *MockReminder {
	mock := &MockReminder{ctrl: ctrl}
	mock.recorder = &MockReminderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReminder) EXPECT() *MockReminderMockRecorder {
	return m.recorder
}

// RemindBackOff mocks base method.
func (m *MockReminder) RemindBackOff() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemindBackOff")
}

// RemindBackOff indicates an expected call of RemindBackOff.
func (mr *MockReminderMockRecorder) RemindBackOff() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindBackOff", reflect.TypeOf((*MockReminder)(nil).RemindBackOff))
}

// RemindBackOffCnt mocks base method.
func (m *MockReminder) RemindBackOffCnt() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemindBackOffCnt")
	ret0, _ := ret[0].(int)
	return ret0
}

// RemindBackOffCnt indicates an expected call of RemindBackOffCnt.
func (mr *MockReminderMockRecorder) RemindBackOffCnt() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindBackOffCnt", reflect.TypeOf((*MockReminder)(nil).RemindBackOffCnt))
}

// RemindNextAfter mocks base method.
func (m *MockReminder) RemindNextAfter() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemindNextAfter")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// RemindNextAfter indicates an expected call of RemindNextAfter.
func (mr *MockReminderMockRecorder) RemindNextAfter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindNextAfter", reflect.TypeOf((*MockReminder)(nil).RemindNextAfter))
}

// RemindReset mocks base method.
func (m *MockReminder) RemindReset() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemindReset")
}

// RemindReset indicates an expected call of RemindReset.
func (mr *MockReminderMockRecorder) RemindReset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindReset", reflect.TypeOf((*MockReminder)(nil).RemindReset))
}

// MockItemBuffer is a mock of ItemBuffer interface.
type MockItemBuffer[T any, B any] struct {
	ctrl     *gomock.Controller
	recorder *MockItemBufferMockRecorder[T, B]
	isgomock struct{}
}

// MockItemBufferMockRecorder is the mock recorder for MockItemBuffer.
type MockItemBufferMockRecorder[T any, B any] struct {
	mock *MockItemBuffer[T, B]
}

// NewMockItemBuffer creates a new mock instance.
func NewMockItemBuffer[T any, B any](ctrl *gomock.Controller) *MockItemBuffer[T, B] {
	mock := &MockItemBuffer[T, B]{ctrl: ctrl}
	mock.recorder = &MockItemBufferMockRecorder[T, B]{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockItemBuffer[T, B]) EXPECT() *MockItemBufferMockRecorder[T, B] {
	return m.recorder
}

// Add mocks base method.
func (m *MockItemBuffer[T, B]) Add(item T) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Add", item)
}

// Add indicates an expected call of Add.
func (mr *MockItemBufferMockRecorder[T, B]) Add(item any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Add", reflect.TypeOf((*MockItemBuffer[T, B])(nil).Add), item)
}

// GetBatch mocks base method.
func (m *MockItemBuffer[T, B]) GetBatch(ctx context.Context, buf *bytes.Buffer) B {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBatch", ctx, buf)
	ret0, _ := ret[0].(B)
	return ret0
}

// GetBatch indicates an expected call of GetBatch.
func (mr *MockItemBufferMockRecorder[T, B]) GetBatch(ctx, buf any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBatch", reflect.TypeOf((*MockItemBuffer[T, B])(nil).GetBatch), ctx, buf)
}

// IsEmpty mocks base method.
func (m *MockItemBuffer[T, B]) IsEmpty() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsEmpty")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsEmpty indicates an expected call of IsEmpty.
func (mr *MockItemBufferMockRecorder[T, B]) IsEmpty() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsEmpty", reflect.TypeOf((*MockItemBuffer[T, B])(nil).IsEmpty))
}

// RemindBackOff mocks base method.
func (m *MockItemBuffer[T, B]) RemindBackOff() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemindBackOff")
}

// RemindBackOff indicates an expected call of RemindBackOff.
func (mr *MockItemBufferMockRecorder[T, B]) RemindBackOff() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindBackOff", reflect.TypeOf((*MockItemBuffer[T, B])(nil).RemindBackOff))
}

// RemindBackOffCnt mocks base method.
func (m *MockItemBuffer[T, B]) RemindBackOffCnt() int {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemindBackOffCnt")
	ret0, _ := ret[0].(int)
	return ret0
}

// RemindBackOffCnt indicates an expected call of RemindBackOffCnt.
func (mr *MockItemBufferMockRecorder[T, B]) RemindBackOffCnt() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindBackOffCnt", reflect.TypeOf((*MockItemBuffer[T, B])(nil).RemindBackOffCnt))
}

// RemindNextAfter mocks base method.
func (m *MockItemBuffer[T, B]) RemindNextAfter() time.Duration {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RemindNextAfter")
	ret0, _ := ret[0].(time.Duration)
	return ret0
}

// RemindNextAfter indicates an expected call of RemindNextAfter.
func (mr *MockItemBufferMockRecorder[T, B]) RemindNextAfter() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindNextAfter", reflect.TypeOf((*MockItemBuffer[T, B])(nil).RemindNextAfter))
}

// RemindReset mocks base method.
func (m *MockItemBuffer[T, B]) RemindReset() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RemindReset")
}

// RemindReset indicates an expected call of RemindReset.
func (mr *MockItemBufferMockRecorder[T, B]) RemindReset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RemindReset", reflect.TypeOf((*MockItemBuffer[T, B])(nil).RemindReset))
}

// Reset mocks base method.
func (m *MockItemBuffer[T, B]) Reset() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Reset")
}

// Reset indicates an expected call of Reset.
func (mr *MockItemBufferMockRecorder[T, B]) Reset() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reset", reflect.TypeOf((*MockItemBuffer[T, B])(nil).Reset))
}

// ShouldFlush mocks base method.
func (m *MockItemBuffer[T, B]) ShouldFlush() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ShouldFlush")
	ret0, _ := ret[0].(bool)
	return ret0
}

// ShouldFlush indicates an expected call of ShouldFlush.
func (mr *MockItemBufferMockRecorder[T, B]) ShouldFlush() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ShouldFlush", reflect.TypeOf((*MockItemBuffer[T, B])(nil).ShouldFlush))
}