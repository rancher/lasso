package client

import (
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
	"net/http"
	"testing"
)

func TestNewSharedClientFactoryWithAgent(t *testing.T) {
	mockSharedClientFactory := NewMockSharedClientFactory(gomock.NewController(t))

	testUserAgent := "testagent"
	testConfig := rest.Config{
		UserAgent: "defaultuseragent",
		ContentConfig: rest.ContentConfig{
			NegotiatedSerializer: serializer.WithoutConversionCodecFactory{},
		},
		Transport: userAgentRoundTripper{t: t, expectedUserAgent: testUserAgent},
	}

	testRestClient, err := rest.UnversionedRESTClientFor(&testConfig)
	assert.Nil(t, err)
	testClient := &Client{
		RESTClient: testRestClient,
		Config:     testConfig,
	}
	mockSharedClientFactory.EXPECT().ForKind(gomock.Any()).DoAndReturn(func(gvk schema.GroupVersionKind) (*Client, error) {
		return testClient, nil
	}).AnyTimes()
	mockSharedClientFactory.EXPECT().ForResource(gomock.Any(), gomock.Any()).DoAndReturn(func(gvr schema.GroupVersionResource, namespaced bool) (*Client, error) {
		return testClient, nil
	}).AnyTimes()
	mockSharedClientFactory.EXPECT().ForResourceKind(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(gvr schema.GroupVersionResource, kind string, namespaced bool) *Client {
		return testClient
	}).AnyTimes()
	testSharedClientFactoryWithAgent := NewSharedClientFactoryWithAgent("testagent", mockSharedClientFactory)

	// for kind
	clientWithAgent, err := testSharedClientFactoryWithAgent.ForKind(schema.GroupVersionKind{})
	assert.Nil(t, err)

	restClient := clientWithAgent.RESTClient.(*rest.RESTClient)
	restClient.Client.Transport.RoundTrip(&http.Request{})

	// for resource
	clientWithAgent, err = testSharedClientFactoryWithAgent.ForResource(schema.GroupVersionResource{}, false)
	assert.Nil(t, err)

	restClient = clientWithAgent.RESTClient.(*rest.RESTClient)
	restClient.Client.Transport.RoundTrip(&http.Request{})

	// for resource kind
	clientWithAgent = testSharedClientFactoryWithAgent.ForResourceKind(schema.GroupVersionResource{}, "", false)
	assert.Nil(t, err)

	restClient = clientWithAgent.RESTClient.(*rest.RESTClient)
	restClient.Client.Transport.RoundTrip(&http.Request{})

	// test with errors
	mockSharedClientErrFactory := NewMockSharedClientFactory(gomock.NewController(t))
	mockSharedClientErrFactory.EXPECT().ForKind(gomock.Any()).DoAndReturn(func(gvk schema.GroupVersionKind) (*Client, error) {
		return nil, fmt.Errorf("some error")
	}).AnyTimes()
	mockSharedClientErrFactory.EXPECT().ForResource(gomock.Any(), gomock.Any()).DoAndReturn(func(gvr schema.GroupVersionResource, namespaced bool) (*Client, error) {
		return nil, fmt.Errorf("some error")
	}).AnyTimes()
	mockSharedClientErrFactory.EXPECT().ForResourceKind(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(gvr schema.GroupVersionResource, kind string, namespaced bool) *Client {
		return nil
	}).AnyTimes()
	testSharedClientFactoryWithAgent = NewSharedClientFactoryWithAgent("", mockSharedClientErrFactory)

	// for kind
	clientWithAgent, err = testSharedClientFactoryWithAgent.ForKind(schema.GroupVersionKind{})
	assert.NotNil(t, err)
	assert.Nil(t, clientWithAgent)

	// for resource
	clientWithAgent, err = testSharedClientFactoryWithAgent.ForResource(schema.GroupVersionResource{}, false)
	assert.NotNil(t, err)
	assert.Nil(t, clientWithAgent)

	// for resource kind
	clientWithAgent = testSharedClientFactoryWithAgent.ForResourceKind(schema.GroupVersionResource{}, "", false)
	assert.Nil(t, clientWithAgent)
}
