package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/rest/fake"
	"k8s.io/kubectl/pkg/scheme"
)

func TestClient_RequestModifiers(t *testing.T) {
	t.Parallel()
	type optTest struct {
		name     string
		wantErr  bool
		response *http.Response
		options  Options

		//  matchFunc handles if the specific requests is valid or not for each table test
		matchFunc func(t *testing.T, req *http.Request)
	}
	retData, _ := json.Marshal(&v1.Pod{})
	var warningBuffer bytes.Buffer
	tests := []*optTest{
		{
			name:      "nil modifier test",
			matchFunc: func(_ *testing.T, _ *http.Request) {},
		},
		{
			name: "impersonation test",
			options: Options{
				Impersonate: &rest.ImpersonationConfig{
					UserName: "batman",
					Groups:   []string{"heros", "knights"},
				},
			},
			matchFunc: func(t *testing.T, req *http.Request) {
				t.Helper()
				require.Equal(t, req.Header.Get("Impersonate-User"), "batman")
				require.Equal(t, req.Header.Values("Impersonate-Group"), []string{"heros", "knights"})
			},
		},
		{
			name: "warning handler",
			options: Options{
				WarningHandler: rest.NewWarningWriter(&warningBuffer, rest.WarningWriterOptions{}),
			},
			response: &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBuffer(retData)),
				Header:     http.Header{"Warning": []string{"299 test-agent \"test-message\""}},
			},
			matchFunc: func(t *testing.T, req *http.Request) {
				t.Helper()
				require.Equal(t, warningBuffer.String(), "Warning: test-message\n")
				warningBuffer.Reset()
			},
		},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			gvr := schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			}
			mockRESTClient := &fake.RESTClient{
				GroupVersion:         gvr.GroupVersion(),
				NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
			}

			if test.response != nil {
				mockRESTClient.Resp = test.response
			} else {
				mockRESTClient.Resp = &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBuffer(retData)),
				}
			}

			client := NewClient(gvr, "Pods", true, mockRESTClient, 0)

			obj := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "bar",
				},
			}
			result := &v1.Pod{}
			opts := UpdateOptions{Options: test.options}
			err := client.Update(context.TODO(), "foo", obj, result, opts)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			test.matchFunc(t, mockRESTClient.Req)
		})
	}
}

func Test_sanitizeHeaderKey(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "alpha",
			key:  "alphabetical",
		},
		{
			name: "alphanumeric",
			key:  "alph4num3r1c",
		},
		{
			name: "percent encoded",
			key:  "percent%20encoded",
		},
		{
			name: "almost percent encoded",
			key:  "almost%zzpercent%xxencoded",
		},
		{
			name: "illegal char & percent encoding",
			key:  "example.com/percent%20encoded",
		},
		{
			name: "weird unicode stuff",
			key:  "example.com/ᛒᚥᛏᛖᚥᚢとロビン",
		},
		{
			name: "header legal chars",
			key:  "abc123!#$+.-_*\\^`~|'",
		},
		{
			name: "legal path, illegal header",
			key:  "@=:",
		},
		{
			name: "upper case key",
			key:  "COdE",
			want: "code",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			escaped := sanitizeHeaderKey(tc.key)
			unescaped, err := url.PathUnescape(escaped)
			if err != nil {
				require.NoErrorf(t, err, "url.PathUnescape(%q) returned an error", escaped)
			}
			if tc.want == "" {
				require.Equal(t, tc.key, unescaped, "url.PathUnescape(sanitizeHeaderKey(%q)) returned %q, wanted %q", tc.key, unescaped, tc.key)
			} else {
				require.Equal(t, escaped, tc.want)
			}
		})
	}
}
