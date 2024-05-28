package cache

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/rancher/lasso/pkg/client"
	"github.com/rancher/lasso/pkg/internal/mocks"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
)

func Test_sharedCacheFactory_IsCached(t *testing.T) {
	// Kinds used for testing
	notCachedGVK, cachedGVK, cachedGVR := corev1.SchemeGroupVersion.WithKind("Service"),
		corev1.SchemeGroupVersion.WithKind("Pod"),
		corev1.SchemeGroupVersion.WithResource("pod")

	// Setup mock
	ctrl := gomock.NewController(t)
	clientFactoryMock := mocks.NewMockSharedClientFactory(ctrl)
	clientFactoryMock.EXPECT().ResourceForGVK(gomock.Eq(cachedGVK)).
		Return(cachedGVR, false, nil)
	clientFactoryMock.EXPECT().ForResourceKind(gomock.Eq(cachedGVR), gomock.Eq(cachedGVK.Kind), gomock.Eq(false)).
		Return(&client.Client{})
	clientFactoryMock.EXPECT().NewObjects(gomock.Eq(cachedGVK)).
		Return(&corev1.Pod{}, &corev1.PodList{}, nil)
	clientFactoryMock.EXPECT().IsHealthy(gomock.Any()).Return(true)

	// Init SharedCacheFactory using mock, and register kind by obtaining a client
	factory := NewSharedCachedFactory(clientFactoryMock, nil)
	_, _ = factory.ForKind(cachedGVK)

	assert.False(t, factory.IsCached(cachedGVK))
	assert.False(t, factory.IsCached(notCachedGVK))

	// Only returns true after it is started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assert.Nil(t, factory.Start(ctx))

	assert.True(t, factory.IsCached(cachedGVK))
	assert.False(t, factory.IsCached(notCachedGVK))
}
