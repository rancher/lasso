/*
package sql provides an Informer and Indexer that uses SQLite as a store, instead of an in-memory store like a map.
*/

package informer

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/partition"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// Informer is a SQLite-backed cache.SharedIndexInformer that can execute queries on listprocessor structs
type Informer struct {
	cache.SharedIndexInformer
	ByOptionsLister

	listeners *listeners
}

type ByOptionsLister interface {
	ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error)
}

type Watcher interface {
	Watch(ctx context.Context, listener Listener) int
}

// this is set to a var so that it can be overriden by test code for mocking purposes
var newInformer = cache.NewSharedIndexInformer

// NewInformer returns a new SQLite-backed Informer for the type specified by schema in unstructured.Unstructured form
// using the specified client
func NewInformer(client dynamic.ResourceInterface, fields [][]string, transform cache.TransformFunc, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespaced bool) (*Informer, error) {
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

	sii := newInformer(listWatcher, example, resyncPeriod, cache.Indexers{})
	if transform != nil {
		if err := sii.SetTransform(transform); err != nil {
			return nil, err
		}
	}

	name := informerNameFromGVK(gvk)

	s, err := sqlStore.NewStore(example, cache.DeletionHandlingMetaNamespaceKeyFunc, db, shouldEncrypt, name)
	if err != nil {
		return nil, err
	}
	loi, err := NewListOptionIndexer(fields, s, namespaced)
	if err != nil {
		return nil, err
	}

	listeners := newlisteners()
	sii.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			listeners.Notify(obj)
		},
		UpdateFunc: func(obj any, newObj any) {
			listeners.Notify(newObj)
		},
		DeleteFunc: func(obj any) {
			listeners.Notify(obj)
		},
	})

	// HACK: replace the default informer's indexer with the SQL based one
	UnsafeSet(sii, "indexer", loi)

	return &Informer{
		SharedIndexInformer: sii,
		ByOptionsLister:     loi,
		listeners:           listeners,
	}, nil
}

// ListByOptions returns objects according to the specified list options and partitions.
// Specifically:
//   - an unstructured list of resources belonging to any of the specified partitions
//   - the total number of resources (returned list might be a subset depending on pagination options in lo)
//   - a continue token, if there are more pages after the returned one
//   - an error instead of all of the above if anything went wrong
func (i *Informer) ListByOptions(ctx context.Context, lo ListOptions, partitions []partition.Partition, namespace string) (*unstructured.UnstructuredList, int, string, error) {
	list, total, continueToken, err := i.ByOptionsLister.ListByOptions(ctx, lo, partitions, namespace)
	count := i.listeners.Count()
	fmt.Println("HITHERE replacing resourceVersion", list.GetResourceVersion(), count)
	list.SetResourceVersion(strconv.Itoa(count))
	return list, total, continueToken, err
}

func (i *Informer) Watch(ctx context.Context, listener Listener) int {
	revision := i.listeners.AddListener(listener)
	go func() {
		<-ctx.Done()
		i.listeners.RemoveListener(listener)
	}()
	return revision
}

func informerNameFromGVK(gvk schema.GroupVersionKind) string {
	return gvk.Group + "_" + gvk.Version + "_" + gvk.Kind
}
