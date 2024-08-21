/*
package sql provides an Informer and Indexer that uses SQLite as a store, instead of an in-memory store like a map.
*/

package informer

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/partition"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// Informer exposes read methods for a SQLite-backed cache.SharedIndexInformer that can execute queries on ListOptions structs
type Informer interface {
	cache.SharedIndexInformer
	IsWatchable() bool
	ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error)
}

// Informer is a SQLite-backed cache.SharedIndexInformer that can execute queries on ListOptions structs
type informer struct {
	cache.SharedIndexInformer
	ByOptionsLister

	// Indicates the informer failed to open a watch to the resource because it is not watchable
	// (will be retried)
	unwatchable      bool
	unwatchableMutex sync.RWMutex
}

type ByOptionsLister interface {
	ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error)
}

// NewInformer returns a new SQLite-backed Informer for the type specified by schema in unstructured.Unstructured form
// using the specified client
func NewInformer(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespaced bool) (Informer, error) {
	listWatcher := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			a, err := client.List(context.Background(), options)
			return a, err
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return client.Watch(context.Background(), options)
		},
	}

	example := &unstructured.Unstructured{}
	example.SetGroupVersionKind(gvk)

	// avoids the informer to periodically resync (re-list) its resources
	// currently it is a work hypothesis that, when interacting with the UI, this should not be needed
	resyncPeriod := time.Duration(0)

	sii := cache.NewSharedIndexInformer(listWatcher, example, resyncPeriod, cache.Indexers{})

	name := informerNameFromGVK(gvk)

	s, err := sqlStore.NewStore(example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, name)
	if err != nil {
		return nil, err
	}
	loi, err := NewListOptionIndexer(fields, s, namespaced)
	if err != nil {
		return nil, err
	}

	// HACK: replace the default informer's indexer with the SQL based one
	UnsafeSet(sii, "indexer", loi)

	result := &informer{
		SharedIndexInformer: sii,
		ByOptionsLister:     loi,
	}

	err = sii.SetWatchErrorHandler(func(r *cache.Reflector, err error) {
		if errors.IsMethodNotSupported(err) {
			result.setUnwatchable()
			return
		}
		cache.DefaultWatchErrorHandler(r, err)
	})
	if err != nil {
		return nil, err
	}
	_, err = sii.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			result.setWatchable()
		},
		UpdateFunc: func(old, new interface{}) {
			result.setWatchable()
		},
		DeleteFunc: func(obj interface{}) {
			result.setWatchable()
		},
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// NewInformerFromComponents returns an informer instance from its components for testing purposes
func NewInformerFromComponents(sil cache.SharedIndexInformer, bol ByOptionsLister) Informer {
	return &informer{
		SharedIndexInformer: sil,
		ByOptionsLister:     bol,
	}
}

func (i *informer) IsWatchable() bool {
	i.unwatchableMutex.RLock()
	defer i.unwatchableMutex.RUnlock()
	return !i.unwatchable
}

func (i *informer) setWatchable() {
	i.unwatchableMutex.Lock()
	defer i.unwatchableMutex.Unlock()
	i.unwatchable = false
}

func (i *informer) setUnwatchable() {
	i.unwatchableMutex.Lock()
	defer i.unwatchableMutex.Unlock()
	i.unwatchable = true
}

// ListByOptions returns objects according to the specified list options and partitions.
// Specifically:
//   - an unstructured list of resources belonging to any of the specified partitions
//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
//   - a continue token, if there are more pages after the returned one
//   - an error instead of all of the above if anything went wrong
func (i *informer) ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error) {
	return i.ByOptionsLister.ListByOptions(ctx, lo, partitions, namespace)
}

func informerNameFromGVK(gvk schema.GroupVersionKind) string {
	return gvk.Group + "_" + gvk.Version + "_" + gvk.Kind
}
