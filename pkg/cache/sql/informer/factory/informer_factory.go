/*
Package factory provides a cache factory for the sql-based cache.
*/
package factory

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/encryption"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	"github.com/rancher/lasso/pkg/log"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
)

// EncryptAllEnvVar is set to "true" if users want all types' data blobs to be encrypted in SQLite
// otherwise only variables in defaultEncryptedResourceTypes will have their blobs encrypted
const EncryptAllEnvVar = "CATTLE_ENCRYPT_CACHE_ALL"

// CacheFactory builds Informer instances and keeps a cache of instances it created
type CacheFactory struct {
	wg          wait.Group
	dbClient    DBClient
	stopCh      chan struct{}
	stopChMutex sync.RWMutex
	encryptAll  bool

	newInformer newInformer

	informers       sync.Map
	informerMutexes sync.Map
}

type newInformer func(client dynamic.ResourceInterface, fields [][]string, transform cache.TransformFunc, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespace bool) (*informer.Informer, error)

type DBClient interface {
	informer.DBClient
	sqlStore.DBClient
	connector
}

type Cache struct {
	informer.ByOptionsLister
}

type connector interface {
	NewConnection() error
}

var defaultEncryptedResourceTypes = map[schema.GroupVersionKind]struct{}{
	{
		Version: "v1",
		Kind:    "Secret",
	}: {},
}

// NewCacheFactory returns an informer factory instance
func NewCacheFactory() (*CacheFactory, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, err
	}
	dbClient, err := db.NewClient(nil, m, m)
	if err != nil {
		return nil, err
	}
	return &CacheFactory{
		wg:          wait.Group{},
		stopCh:      make(chan struct{}),
		encryptAll:  os.Getenv(EncryptAllEnvVar) == "true",
		dbClient:    dbClient,
		newInformer: informer.NewInformer,
	}, nil
}

// CacheFor returns an informer for given GVK, using sql store indexed with fields, using the specified client. For virtual fields, they must be added by the transform function
// and specified by fields to be used for later fields.
func (f *CacheFactory) CacheFor(fields [][]string, transform cache.TransformFunc, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool) (Cache, error) {
	// first of all block Reset() until we are done
	f.stopChMutex.RLock()
	defer f.stopChMutex.RUnlock()

	// then, block other concurrent calls to CacheFor for the same gvk
	rmutex, _ := f.informerMutexes.LoadOrStore(gvk, &sync.Mutex{})
	mutex := rmutex.(*sync.Mutex)
	mutex.Lock()
	defer mutex.Unlock()

	// then, if we have a cached informer, return it right away
	result, ok := f.informers.Load(gvk)
	if ok {
		return Cache{ByOptionsLister: result.(*informer.Informer)}, nil
	}

	// otherwise, create a new informer...
	start := time.Now()
	log.Infof("CacheFor STARTS creating informer for %v", gvk)
	defer func() {
		log.Infof("CacheFor ISDONE creating informer for %v (took %v)", gvk, time.Now().Sub(start))
	}()

	_, encryptResourceAlways := defaultEncryptedResourceTypes[gvk]
	shouldEncrypt := f.encryptAll || encryptResourceAlways
	i, err := f.newInformer(client, fields, transform, gvk, f.dbClient, shouldEncrypt, namespaced)
	if err != nil {
		return Cache{}, err
	}

	// ...store it in the informers cache...
	f.informers.Store(gvk, i)

	f.wg.StartWithChannel(f.stopCh, i.Run)
	if !cache.WaitForCacheSync(f.stopCh, i.HasSynced) {
		return Cache{}, fmt.Errorf("failed to sync SQLite Informer cache for GVK %v", gvk)
	}

	// ...and return it
	return Cache{ByOptionsLister: i}, nil
}

// Reset closes the stopCh which stops any running informers, assigns a new stopCh, resets the GVK-informer cache, and resets
// the database connection which wipes any current sqlite database at the default location.
func (f *CacheFactory) Reset() error {
	if f.dbClient == nil {
		// nothing to reset
		return nil
	}

	// first of all wait until all CacheFor() calls that create new informers are finished. Also block any new ones
	f.stopChMutex.Lock()
	defer f.stopChMutex.Unlock()

	// now that we are alone, stop all informers created until this point
	close(f.stopCh)
	f.stopCh = make(chan struct{})

	// and get rid of all references to those informers and their mutexes
	f.informers.Range(func(key, value any) bool {
		f.informers.Delete(key)
		return true
	})
	f.informerMutexes.Range(func(key, value any) bool {
		f.informerMutexes.Delete(key)
		return true
	})

	// finally, reset the DB connection
	err := f.dbClient.NewConnection()
	if err != nil {
		return err
	}

	return nil
}
