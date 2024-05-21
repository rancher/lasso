package factory

import (
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

//go:generate mockgen --build_flags=--mod=mod -package factory -destination ./factory_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/informer/factory DBClient
//go:generate mockgen --build_flags=--mod=mod -package factory -destination ./db_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/db TXClient
//go:generate mockgen --build_flags=--mod=mod -package factory -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface
//go:generate mockgen --build_flags=--mod=mod -package factory -destination ./k8s_cache_mocks_test.go k8s.io/client-go/tools/cache SharedIndexInformer

func TestNewCacheFactory(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "NewCacheFactory() with no errors returned, should return no errors", test: func(t *testing.T) {
		f, err := NewCacheFactory()
		assert.Nil(t, err)
		assert.NotNil(t, f.dbClient)
		assert.False(t, f.encryptAll)
	}})
	tests = append(tests, testCase{description: "NewCacheFactory() with no errors returned and EncryptAllEnvVar set to true, should return no errors and have encryptAll set to true", test: func(t *testing.T) {
		err := os.Setenv(EncryptAllEnvVar, "true")
		f, err := NewCacheFactory()
		assert.Nil(t, err)
		assert.Nil(t, err)
		assert.NotNil(t, f.dbClient)
		assert.True(t, f.encryptAll)
	}})
	// cannot run as parallel because tests involve changing env var
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestInformerFor(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and stopCh not closed, should return no error and should cal Informer.Run(). A subsequent call to CacheFor() should return same informer", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1)
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespaced bool) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
		}

		go func() {
			// this function ensures that stopCh is open for the duration of this test but if part of a longer process it will be closed eventually
			time.Sleep(5 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(fields, dynamicClient, expectedGVK, false)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		// this sleep is critical to the test. It ensure there has been enough time for expected function like Run to be invoked in their go routines.
		time.Sleep(1 * time.Second)
		c2, err := f.CacheFor(fields, dynamicClient, expectedGVK, false)
		assert.Nil(t, err)
		assert.Equal(t, c, c2)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning false, and stopCh not closed, should call Run() and return an error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}

		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(false).AnyTimes()
		sii.EXPECT().Run(gomock.Any())
		expectedI := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		testNewInformer := func(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt, namespaced bool) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			return expectedI, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
		}

		go func() {
			time.Sleep(1 * time.Second)
			close(f.stopCh)
		}()
		var err error
		_, err = f.CacheFor(fields, dynamicClient, expectedGVK, false)
		assert.NotNil(t, err)
		time.Sleep(2 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned, HasSync returning true, and stopCh closed, should not call Run() more than once and not return an error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}

		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true).AnyTimes()
		// may or may not call run initially
		sii.EXPECT().Run(gomock.Any()).MaxTimes(1)
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt, namespaced bool) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, false, shouldEncrypt)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
		}

		close(f.stopCh)
		var c Cache
		var err error
		c, err = f.CacheFor(fields, dynamicClient, expectedGVK, false)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	tests = append(tests, testCase{description: "CacheFor() with no errors returned and encryptAll set to true, should return no error and pass shouldEncrypt as true to newInformer func", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))
		fields := [][]string{{"something"}}
		expectedGVK := schema.GroupVersionKind{}
		sii := NewMockSharedIndexInformer(gomock.NewController(t))
		sii.EXPECT().HasSynced().Return(true)
		sii.EXPECT().Run(gomock.Any()).MinTimes(1).AnyTimes()
		i := &informer.Informer{
			// need to set this so Run function is not nil
			SharedIndexInformer: sii,
		}
		expectedC := Cache{
			ByOptionsLister: i,
		}
		testNewInformer := func(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt, namespaced bool) (*informer.Informer, error) {
			assert.Equal(t, client, dynamicClient)
			assert.Equal(t, fields, fields)
			assert.Equal(t, expectedGVK, gvk)
			assert.Equal(t, db, dbClient)
			assert.Equal(t, true, shouldEncrypt)
			return i, nil
		}
		f := &CacheFactory{
			dbClient:    dbClient,
			stopCh:      make(chan struct{}),
			newInformer: testNewInformer,
			encryptAll:  true,
		}

		go func() {
			time.Sleep(10 * time.Second)
			close(f.stopCh)
		}()
		var c Cache
		var err error
		c, err = f.CacheFor(fields, dynamicClient, expectedGVK, false)
		assert.Nil(t, err)
		assert.Equal(t, expectedC, c)
		time.Sleep(1 * time.Second)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}
