package informer

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rancher/lasso/pkg/cache/sql/partition"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./informer_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/informer ByOptionsLister
//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./dynamic_mocks_test.go k8s.io/client-go/dynamic ResourceInterface
//go:generate mockgen --build_flags=--mod=mod -package informer -destination ./store_mocks_test.go github.com/rancher/lasso/pkg/cache/sql/store DBClient

func TestNewInformer(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "NewInformer() with no errors returned, should return no error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)

		// NewListOptionIndexer() logic. This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)

		informer, err := NewInformer(dynamicClient, fields, gvk, dbClient, false, true)
		assert.Nil(t, err)
		assert.NotNil(t, informer.ByOptionsLister)
		assert.NotNil(t, informer.SharedIndexInformer)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewStore(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewInformer(dynamicClient, fields, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewIndexer(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewInformer(dynamicClient, fields, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	tests = append(tests, testCase{description: "NewInformer() with errors returned from NewListOptionIndexer(), should return an error", test: func(t *testing.T) {
		dbClient := NewMockDBClient(gomock.NewController(t))
		txClient := NewMockTXClient(gomock.NewController(t))
		dynamicClient := NewMockResourceInterface(gomock.NewController(t))

		fields := [][]string{{"something"}}
		gvk := schema.GroupVersionKind{}

		// NewStore() from store package logic. This package is only concerned with whether it returns err or not as NewStore
		// is tested in depth in its own package.
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)
		dbClient.EXPECT().Prepare(gomock.Any()).Return(&sql.Stmt{}).AnyTimes()

		// NewIndexer() logic (within NewListOptionIndexer(). This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(nil)

		// NewListOptionIndexer() logic. This test is only concerned with whether it returns err or not as NewIndexer
		// is tested in depth in its own indexer_test.go
		dbClient.EXPECT().Begin().Return(txClient, nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Exec(gomock.Any(), gomock.Any()).Return(nil)
		txClient.EXPECT().Commit().Return(fmt.Errorf("error"))

		_, err := NewInformer(dynamicClient, fields, gvk, dbClient, false, true)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

func TestInformerListByOptions(t *testing.T) {
	type testCase struct {
		description string
		test        func(t *testing.T)
	}

	var tests []testCase

	tests = append(tests, testCase{description: "ListByOptions() with no errors returned, should return no error and return value from indexer's ListByOptions()", test: func(t *testing.T) {
		indexer := NewMockByOptionsLister(gomock.NewController(t))
		informer := &Informer{
			ByOptionsLister: indexer,
		}
		lo := ListOptions{}
		var partitions []partition.Partition
		ns := "somens"
		expectedList := &unstructured.UnstructuredList{
			Object: map[string]interface{}{"s": 2},
			Items: []unstructured.Unstructured{{
				Object: map[string]interface{}{"s": 2},
			}},
		}
		expectedTotal := len(expectedList.Items)
		expectedContinueToken := "123"
		indexer.EXPECT().ListByOptions(context.TODO(), lo, partitions, ns).Return(expectedList, expectedTotal, expectedContinueToken, nil)
		list, total, continueToken, err := informer.ListByOptions(context.TODO(), lo, partitions, ns)
		assert.Nil(t, err)
		assert.Equal(t, expectedList, list)
		assert.Equal(t, len(expectedList.Items), total)
		assert.Equal(t, expectedContinueToken, continueToken)
	}})
	tests = append(tests, testCase{description: "ListByOptions() with indexer ListByOptions error, should return error", test: func(t *testing.T) {
		indexer := NewMockByOptionsLister(gomock.NewController(t))
		informer := &Informer{
			ByOptionsLister: indexer,
		}
		lo := ListOptions{}
		var partitions []partition.Partition
		ns := "somens"
		indexer.EXPECT().ListByOptions(context.TODO(), lo, partitions, ns).Return(nil, 0, "", fmt.Errorf("error"))
		_, _, _, err := informer.ListByOptions(context.TODO(), lo, partitions, ns)
		assert.NotNil(t, err)
	}})
	t.Parallel()
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) { test.test(t) })
	}
}

// Note: SQLite based caching uses an Informer that unsafely sets the Indexer as the ability to set it is not present 
// in client-go at the moment. Long term, we look forward contribute a patch to client-go to make that configurable.
// Until then, we are adding this canary test that will panic in case the indexer cannot be set.
func TestUnsafeSet(t *testing.T) {
	listWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return &unstructured.UnstructuredList{}, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return dummyWatch{}, nil
		},
	}

	sii := cache.NewSharedIndexInformer(listWatcher, &unstructured.Unstructured{}, 0, cache.Indexers{})

	// will panic if SharedIndexInformer stops having a *Indexer field called "indexer"
	UnsafeSet(sii, "indexer", &Indexer{})
}

type dummyWatch struct{}

func (dummyWatch) Stop() {
}

func (dummyWatch) ResultChan() <-chan watch.Event {
	result := make(chan watch.Event)
	defer close(result)
	return result
}
