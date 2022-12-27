package client

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"
)

type userAgentRoundTripper struct {
	expectedUserAgent string
	t                 *testing.T
}

func TestWithAgent(t *testing.T) {
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

	testClientWithAgent, err := testClient.WithAgent("testagent")
	assert.Nil(t, err)

	testRestClientWithAgent := testClientWithAgent.RESTClient.(*rest.RESTClient)

	_, err = testRestClientWithAgent.Client.Transport.RoundTrip(&http.Request{})
	assert.Nil(t, err)

	// with invalid config
	testClient = &Client{
		Config: rest.Config{},
	}

	testClientWithAgent, err = testClient.WithAgent("testagent")
	assert.NotNil(t, err)
	assert.Nil(t, testClientWithAgent)
}

func (u userAgentRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	assert.Equal(u.t, u.expectedUserAgent, req.Header.Get("User-Agent"))
	return &http.Response{}, nil
}
