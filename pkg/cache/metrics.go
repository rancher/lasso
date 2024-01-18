package cache

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/rancher/lasso/pkg/metrics"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	cacheMetricsCollectionPeriod = 1 * time.Minute
	cacheContextLabel            = "context"
	cacheKindLabel               = "kind"
)

var (
	cacheStoreCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cache",
			Name:      "store_count",
			Help:      "Number of items in the cache store",
		}, []string{cacheContextLabel, cacheKindLabel},
	)
	cacheStartedCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Subsystem: "cache",
			Name:      "started_count",
			Help:      "Number of started caches per factory",
		}, []string{cacheContextLabel},
	)
)

func init() {
	metrics.Register(cacheStoreCount, cacheStartedCount)
}

func startMetricsCollection(ctx context.Context, s *sharedCacheFactory, contextName string) {
	go func() {
		ticker := time.NewTicker(cacheMetricsCollectionPeriod)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			factoryMetrics := collectMetrics(s)
			for kind, count := range factoryMetrics.numEntries {
				cacheStoreCount.WithLabelValues(contextName, kind).Set(float64(count))
			}
			cacheStartedCount.WithLabelValues(contextName).Set(float64(factoryMetrics.startedCaches))
			ticker.Reset(cacheMetricsCollectionPeriod)
		}
	}()
}

func collectMetrics(s *sharedCacheFactory) sharedCacheFactoryMetrics {
	numEntries := map[string]int{}
	for gvk, c := range s.caches {
		kindKey := objectKindKey(gvk)
		items := c.GetStore().List()
		numEntries[kindKey] = len(items)
	}
	return sharedCacheFactoryMetrics{
		numEntries:    numEntries,
		startedCaches: len(s.startedCaches),
	}
}

type sharedCacheFactoryMetrics struct {
	numEntries    map[string]int
	startedCaches int
}

func objectKindKey(gvk schema.GroupVersionKind) string {
	return gvk.Kind + "." + gvk.Group + "/" + gvk.Version
}
