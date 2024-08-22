package cache

import (
	"context"
	"time"

	"github.com/rancher/lasso/pkg/metrics"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	cacheMetricsCollectionPeriod = 1 * time.Minute
)

func (f *sharedCacheFactory) collectMetrics() sharedCacheFactoryMetrics {
	f.lock.RLock()
	defer f.lock.RUnlock()

	gvks := make(map[schema.GroupVersionKind]int)
	for gvk, c := range f.caches {
		items := c.GetStore().List()
		gvks[gvk] = len(items)
	}
	return sharedCacheFactoryMetrics{
		gvks: gvks,
	}
}

type sharedCacheFactoryMetrics struct {
	// gvks is the total count of cache items by GroupVersionKind
	gvks map[schema.GroupVersionKind]int
}

func (f *sharedCacheFactory) startMetricsCollection(ctx context.Context) {
	go func() {
		contextID := metrics.ContextID(ctx)
		timer := time.NewTimer(cacheMetricsCollectionPeriod)
		defer timer.Stop()
		for {
			factoryMetrics := f.collectMetrics()
			for gvk, count := range factoryMetrics.gvks {
				metrics.IncTotalCachedObjects(contextID, gvk, count)
			}

			timer.Reset(cacheMetricsCollectionPeriod)
			select {
			case <-ctx.Done():
				for gvk := range factoryMetrics.gvks {
					metrics.DelTotalCachedObjects(contextID, gvk)
				}
				return
			case <-timer.C:
			}
		}
	}()
}
