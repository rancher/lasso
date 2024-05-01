/*
Package factory provides an informer factory for the sql-based informer.
*/
package factory

import (
	"fmt"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/lasso/pkg/cache/sql/attachdriver"
	db2 "github.com/rancher/lasso/pkg/cache/sql/db"
	"github.com/rancher/lasso/pkg/cache/sql/encryption"
	"github.com/rancher/lasso/pkg/cache/sql/informer"
	sqlStore "github.com/rancher/lasso/pkg/cache/sql/store"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/cache"
	"os"
	"sync"
)

// InformerFactory builds Informer instances and keeps a cache of instances it created
type InformerFactory struct {
	informerCreateLock sync.Mutex
	wg                 wait.Group
	dbClient           DBClient
	stopCh             chan struct{}
	encryptAll         bool
	schemas            map[string]*types.APISchema

	newInformer newInformer

	cache map[schema.GroupVersionKind]*informer.Informer
}

type newInformer func(client dynamic.ResourceInterface, fields [][]string, gvk schema.GroupVersionKind, db sqlStore.DBClient, shouldEncrypt bool, namespace bool) (*informer.Informer, error)

type DBClient interface {
	informer.DBClient
	sqlStore.DBClient
	connector
}

type connector interface {
	NewConnection() error
}

var defaultEncryptedResourceTypes = map[schema.GroupVersionKind]struct{}{
	schema.GroupVersionKind{
		Version: "v1",
		Kind:    "Secret",
	}: {},
}

const (
	EncryptAllEnvVar = "CATTLE_ENCRYPT_CACHE_ALL"
)

func init() {
	indexedFieldDBPath := db2.OnDiskInformerIndexedFieldDBPath
	if os.Getenv(EncryptAllEnvVar) == "true" {
		indexedFieldDBPath = ":memory:" // indexedFieldDBPath = "file::memory:?cache=shared"
	}
	attachdriver.Register("file:" + indexedFieldDBPath + "?cache=shared")
}

// NewInformerFactory returns an informer factory instance
func NewInformerFactory() (*InformerFactory, error) {
	m, err := encryption.NewManager()
	if err != nil {
		return nil, err
	}
	dbClient, err := db2.NewClient(nil, m, m)
	if err != nil {
		return nil, err
	}
	return &InformerFactory{
		wg:          wait.Group{},
		stopCh:      make(chan struct{}),
		cache:       map[schema.GroupVersionKind]*informer.Informer{},
		encryptAll:  os.Getenv(EncryptAllEnvVar) == "true",
		dbClient:    dbClient,
		newInformer: informer.NewInformer,
	}, nil
}

// InformerFor returns an informer for given GVK, using sql store indexed with fields, using the specified client
func (f *InformerFactory) InformerFor(fields [][]string, client dynamic.ResourceInterface, gvk schema.GroupVersionKind, namespaced bool) (*informer.Informer, error) {
	f.informerCreateLock.Lock()
	defer f.informerCreateLock.Unlock()

	result, ok := f.cache[gvk]
	if !ok {
		_, encryptResourceAlways := defaultEncryptedResourceTypes[gvk]
		shouldEncrypt := f.encryptAll || encryptResourceAlways
		i, err := f.newInformer(client, fields, gvk, f.dbClient, shouldEncrypt, namespaced)
		if err != nil {
			return nil, err
		}

		if f.cache == nil {
			f.cache = make(map[schema.GroupVersionKind]*informer.Informer)
		}
		f.cache[gvk] = i
		f.wg.StartWithChannel(f.stopCh, i.Run)
		if !cache.WaitForCacheSync(f.stopCh, i.HasSynced) {
			return nil, fmt.Errorf("failed to sync SQLite Informer cache for GVK %v", gvk)
		}

		return i, nil
	}

	return result, nil
}

// Reset closes the stopCh which stops any running informers, assigns a new stopCh, resets the GVK-informer cache, and resets
// the database connection which wipes any current sqlite databse at the default location.
func (f *InformerFactory) Reset() error {
	if f.dbClient == nil {
		// nothing to reset
		return nil
	}

	f.informerCreateLock.Lock()
	defer f.informerCreateLock.Unlock()

	close(f.stopCh)
	f.stopCh = make(chan struct{})
	f.cache = make(map[schema.GroupVersionKind]*informer.Informer)
	err := f.dbClient.NewConnection()
	if err != nil {
		return err
	}

	return nil
}
