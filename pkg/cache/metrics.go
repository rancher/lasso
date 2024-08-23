package cache

import (
	"context"
	"time"

	"github.com/rancher/lasso/pkg/metrics"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	defaultCacheMetricsCollectionPeriod = 1 * time.Minute
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
		timer := time.NewTimer(f.metricsCollectionPeriod)
		defer timer.Stop()
		for {
			factoryMetrics := f.collectMetrics()
			f.recordMetricsForContext(factoryMetrics, contextID)
			timer.Reset(f.metricsCollectionPeriod)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
		}
	}()
}

func (f *sharedCacheFactory) recordMetricsForContext(fm sharedCacheFactoryMetrics, contextID string) {
	for gvk, count := range fm.gvks {
		metrics.IncTotalCachedObjects(contextID, gvk.Group, gvk.Version, gvk.Kind, float64(count))
	}
}

func (f *sharedCacheFactory) cleanupMetricsForContext(fm sharedCacheFactoryMetrics, contextID string) {
	for gvk := range fm.gvks {
		metrics.DelTotalCachedObjects(contextID, gvk.Group, gvk.Version, gvk.Kind)
	}
}
