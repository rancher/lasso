package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/rest"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubectl/pkg/scheme"
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

var accessor = meta.NewAccessor()

// Object is generic object to wrap runtime and metav1.
type Object interface {
	metav1.Object
	runtime.Object
}

type testInfo struct {
	name    string
	fields  clientFields
	args    testArgs
	wantErr bool
}
type clientFields struct {
	Namespaced bool
	GVR        schema.GroupVersionResource
	Kind       string
}
type testArgs struct {
	obj       Object
	namespace string
	desired   runtime.Object
	result    runtime.Object
}

func TestClient_Get(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
		{
			name:    "empty name",
			wantErr: true,
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
		{
			name: "impersonation test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, false, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.Get(context.TODO(), test.args.namespace, test.args.obj.GetName(), test.args.result, GetOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)
		})
	}
}

func TestClient_List(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				namespace: "bar",
				result:    &v1.PodList{},
				desired: &v1.PodList{
					Items: []v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name:        "foo",
								Namespace:   "bar",
								Annotations: map[string]string{"superhero": "batman"},
							},
							TypeMeta: metav1.TypeMeta{
								Kind:       "Pod",
								APIVersion: "v1",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, true, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.List(context.TODO(), test.args.namespace, test.args.result, ListOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)
		})
	}
}

func TestClient_Update(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, false, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.Update(context.TODO(), test.args.namespace, test.args.obj, test.args.result, UpdateOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)

			require.True(t, equality.Semantic.DeepEqual(test.args.desired, test.args.result))
		})
	}
}
func TestClient_UpdateStatus(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					Status: v1.PodStatus{
						Message: "Good to Go",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, false, true)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.UpdateStatus(context.TODO(), test.args.namespace, test.args.obj, test.args.result, UpdateOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)

			require.True(t, equality.Semantic.DeepEqual(test.args.desired, test.args.result))
		})
	}
}
func TestClient_Create(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, false, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.Create(context.TODO(), test.args.namespace, test.args.obj, test.args.result, CreateOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)
		})
	}
}

func TestClient_Delete(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
			},
		},
		{
			name:    "test empty resource name",
			wantErr: true,
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "bar",
					},
				},
				namespace: "bar",
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.obj, test.args.namespace, false, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.Delete(context.TODO(), test.args.namespace, test.args.obj.GetName(), DeleteOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClient_DeleteCollection(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				namespace: "bar",
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, true, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := c.DeleteCollection(context.TODO(), test.args.namespace, DeleteOptions{}, ListOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClient_Watch(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				namespace: "bar",
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			c := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(c, test.args.desired, test.args.namespace, true, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			_, err := c.Watch(context.TODO(), test.args.namespace, ListOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestClient_Patch(t *testing.T) {
	t.Parallel()
	tests := []*testInfo{
		{
			name: "base test",
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				namespace: "bar",
				result:    &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
		{
			name:    "missing namespace test",
			wantErr: true,
			fields: clientFields{
				GVR: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				Namespaced: true,
				Kind:       "Pod",
			},
			args: testArgs{
				obj: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "foo",
						Namespace: "bar",
					},
				},
				result: &v1.Pod{},
				desired: &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:        "foo",
						Namespace:   "bar",
						Annotations: map[string]string{"superhero": "batman"},
					},
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         test.fields.GVR.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}
			client := NewClient(test.fields.GVR, test.fields.Kind, test.fields.Namespaced, mockRESTClient, 0)
			handler := newRESTTestRequestHandler(client, test.args.desired, test.args.namespace, false, false)
			mockRESTClient.Client = fake.CreateHTTPClient(handler)
			err := client.Patch(context.TODO(), test.args.namespace, test.args.obj.GetName(), types.JSONPatchType, nil, test.args.result, PatchOptions{})
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.args.desired, test.args.result)
		})
	}
}

func newRESTTestRequestHandler(client *Client, retObj runtime.Object, namespace string, isCollection, isStatus bool) func(req *http.Request) (*http.Response, error) {
	return func(req *http.Request) (*http.Response, error) {
		// dynamically create expected path
		expectedPath := path.Join("/", path.Join(client.prefix...))
		if client.Namespaced {
			expectedPath = path.Join(expectedPath, "namespaces", namespace)
		}
		expectedPath = path.Join(expectedPath, client.resource)
		if req.Method != http.MethodPost && !isCollection {
			name, err := accessor.Name(retObj)
			if err != nil {
				return nil, fmt.Errorf("failed to get object name: %s", err)
			}
			expectedPath = path.Join(expectedPath, name)
		}
		if isStatus {
			expectedPath = path.Join(expectedPath, "status")
		}

		if !strings.EqualFold(req.URL.Path, expectedPath) {
			return nil, fmt.Errorf("unexpected request got: '%s' wanted: '%s'", req.URL.Path, expectedPath)
		}
		retData, err := json.Marshal(retObj)
		if err != nil {
			return nil, fmt.Errorf("failed to marshall desired return Object: %w", err)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(bytes.NewReader(retData)),
		}, nil
	}
}
