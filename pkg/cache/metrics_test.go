//go:generate mockgen --build_flags=--mod=mod -package cache -destination ./mocks_test.go github.com/rancher/lasso/pkg/client SharedClientFactory
package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rancher/lasso/pkg/client"
	"github.com/rancher/lasso/pkg/metrics"
	"go.uber.org/mock/gomock"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
)

func setupMockSharedClientFactory(t *testing.T, cf *MockSharedClientFactory, gvr schema.GroupVersionResource, gvk schema.GroupVersionKind) {
	t.Helper()

	testClient := &client.Client{}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	cf.EXPECT().ResourceForGVK(gvk).DoAndReturn(func(gvk schema.GroupVersionKind) (schema.GroupVersionResource, bool, error) {
		return gvr, true, nil
	}).AnyTimes()
	cf.EXPECT().ForKind(gvk).DoAndReturn(func(gvk schema.GroupVersionKind) (*client.Client, error) {
		return testClient, nil
	}).AnyTimes()
	cf.EXPECT().NewObjects(gvk).DoAndReturn(func(gvk schema.GroupVersionKind) (runtime.Object, runtime.Object, error) {
		obj, err := scheme.New(gvk)
		if err != nil {
			return nil, nil, err
		}
		objList, err := scheme.New(schema.GroupVersionKind{
			Group:   gvk.Group,
			Version: gvk.Version,
			Kind:    gvk.Kind + "List",
		})

		return obj, objList, err
	}).AnyTimes()
	cf.EXPECT().ForResourceKind(gvr, gvk.Kind, true).DoAndReturn(func(gvr schema.GroupVersionResource, kind string, namespaced bool) *client.Client {
		return testClient
	})
}

func Test_sharedCacheFactory_metrics_collection(t *testing.T) {
	cf := NewMockSharedClientFactory(gomock.NewController(t))
	setupMockSharedClientFactory(t, cf, corev1.SchemeGroupVersion.WithResource("configmaps"), corev1.SchemeGroupVersion.WithKind("ConfigMap"))
	setupMockSharedClientFactory(t, cf, rbacv1.SchemeGroupVersion.WithResource("roles"), rbacv1.SchemeGroupVersion.WithKind("Role"))

	scf := NewSharedCachedFactory(cf, &SharedCacheFactoryOptions{MetricsCollectionPeriod: time.Millisecond * 200}).(*sharedCacheFactory)
	if _, err := scf.ForKind(corev1.SchemeGroupVersion.WithKind("ConfigMap")); err != nil {
		t.Fatal(err)
	}
	if _, err := scf.ForKind(rbacv1.SchemeGroupVersion.WithKind("Role")); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ctx = metrics.WithContextID(ctx, "test-ctx")

	// Start metrics collection:
	// It will report 0 items for every kind, since indexers is registered but not started, which is enough for testing
	reg := prometheus.NewPedanticRegistry()
	metrics.MustRegister(reg)
	scf.startMetricsCollection(ctx)

	time.Sleep(time.Second)

	if err := testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP lasso_controller_total_cached_object Total count of cached objects
# TYPE lasso_controller_total_cached_object gauge
lasso_controller_total_cached_object{ctx="test-ctx",group="",kind="ConfigMap",version="v1"} 0
lasso_controller_total_cached_object{ctx="test-ctx",group="rbac.authorization.k8s.io",kind="Role",version="v1"} 0
`)); err != nil {
		t.Fatal(err)
	}

	// Cancelling the context should stop the collection and prune metrics
	cancel()
	time.Sleep(time.Second)

	if err := testutil.GatherAndCompare(reg, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
}
