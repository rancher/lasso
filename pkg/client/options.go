package client

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"
)

// RequestModifier is a function used to modify a request before it is sent.
type RequestModifier func(*rest.Request)

// Options holds options specific to lasso Clients.
type Options struct {
	// Impersonate allows a users to set the impersonation config for a specific request
	Impersonate *rest.ImpersonationConfig

	// WarningHandler allows the user to set the warning handler of a specific request
	WarningHandler rest.WarningHandler
}

// GetOptions holds options for performing get operations with a lasso Clients.
type GetOptions struct {
	Options
	metav1.GetOptions
}

// UpdateOptions holds options for performing update operations with a lasso Clients.
type UpdateOptions struct {
	Options
	metav1.UpdateOptions
}

// CreateOptions holds options for performing create operations with a lasso Clients.
type CreateOptions struct {
	Options
	metav1.CreateOptions
}

// ListOptions holds options for performing list operations with a lasso Clients.
type ListOptions struct {
	Options
	metav1.ListOptions
}

// DeleteOptions holds options for performing delete operations with a lasso Clients.
type DeleteOptions struct {
	Options
	metav1.DeleteOptions
}

// PatchOptions holds options for performing patch operations with a lasso Clients.
type PatchOptions struct {
	Options
	metav1.PatchOptions
}

func (o *Options) apply(req *rest.Request) {
	if o.Impersonate != nil {
		applyImpersonation(*o.Impersonate, req)
	}
	if o.WarningHandler != nil {
		req.WarningHandler(o.WarningHandler)
	}
}

// applyImpersonation adds impersonation headers to a request based on the provided impersonation config.
func applyImpersonation(impersonate rest.ImpersonationConfig, req *rest.Request) {
	req.SetHeader(transport.ImpersonateUserHeader, impersonate.UserName)
	if impersonate.UID != "" {
		req.SetHeader(transport.ImpersonateUIDHeader, impersonate.UID)
	}
	req.SetHeader(transport.ImpersonateGroupHeader, impersonate.Groups...)
	for k, v := range impersonate.Extra {
		req.SetHeader(transport.ImpersonateUserExtraHeaderPrefix+sanitizeHeaderKey(k), v...)
	}
}

func sanitizeHeaderKey(s string) string {
	buf := strings.Builder{}
	for _, r := range s {
		switch r {
		case '!', '#', '$', '&', '\'', '*', '+', '-', '.', '^', '_', '`', '|', '~':
			// valid non alpha numeric characters from https://www.rfc-editor.org/rfc/rfc7230#section-3.2.6
			buf.WriteRune(r)
			continue
		}

		if unicode.IsDigit(r) {
			buf.WriteRune(r)
			continue
		}

		// Kubernetes requires lower-case characters
		if unicode.IsLetter(r) {
			buf.WriteRune(unicode.ToLower(r))
			continue
		}

		// %-encode bytes that should be escaped:
		// https://tools.ietf.org/html/rfc3986#section-2.1
		fmt.Fprintf(&buf, "%%%02X", utf8.AppendRune(nil, r))
	}
	return buf.String()
}
