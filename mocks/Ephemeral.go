package mocks

import (
	"github.com/csigo/ephemeral"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"
)

// Ephemeral mocks ephemeral interface.
type Ephemeral struct {
	mock.Mock
}

// AddDir mocks AddDir
func (_m *Ephemeral) AddDir(ctx context.Context, path string) error {
	ret := _m.Called(ctx, path)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, path)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// AddKey mocks AddKey
func (_m *Ephemeral) AddKey(ctx context.Context, path, value string) error {
	ret := _m.Called(ctx, path, value)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, path)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// List mocks List
func (_m *Ephemeral) List(ctx context.Context, path string, watch bool) <-chan *ephemeral.ListResponse {
	ret := _m.Called(ctx, path, watch)

	var r0 <-chan *ephemeral.ListResponse
	if rf, ok := ret.Get(0).(func(context.Context, string, bool) <-chan *ephemeral.ListResponse); ok {
		r0 = rf(ctx, path, watch)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan *ephemeral.ListResponse)
		}
	}

	return r0
}

// Get mocks Get
func (_m *Ephemeral) Get(ctx context.Context, path string) (string, error) {
	ret := _m.Called(ctx, path)
	return ret.String(0), ret.Error(1)
}

// Close mocks Close
func (_m *Ephemeral) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
