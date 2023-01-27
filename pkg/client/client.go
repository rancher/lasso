/*
Package client provides a client that can be configured to point to a kubernetes cluster's kube-api and creates requests
for a specified kubernetes resource. Package client also contains functions for a sharedclientfactory which manages the
multiple clients needed to interact with multiple kubernetes resource types.
*/
package client

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

// Client performs CRUD like operations on a specific GVR.
type Client struct {
	// Default RESTClient
	RESTClient rest.Interface
	// Config that can be used to build a RESTClient with custom options
	Config     rest.Config
	timeout    time.Duration
	Namespaced bool
	GVR        schema.GroupVersionResource
	resource   string
	prefix     []string
	apiVersion string
	kind       string
}

// IsNamespaced determines if the give GroupVersionResource is namespaced using the given RESTMapper.
// returns true if namespaced and an error if the scope could not be determined.
func IsNamespaced(gvr schema.GroupVersionResource, mapper meta.RESTMapper) (bool, error) {
	kind, err := mapper.KindFor(gvr)
	if err != nil {
		return false, err
	}

	mapping, err := mapper.RESTMapping(kind.GroupKind(), kind.Version)
	if err != nil {
		return false, err
	}

	return mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// WithAgent attempts to return a copy of the Client but
// with a new restClient created with the passed in userAgent.
func (c *Client) WithAgent(userAgent string) (*Client, error) {
	client := *c
	config := c.Config
	config.UserAgent = userAgent
	restClient, err := rest.UnversionedRESTClientFor(&config)
	if err != nil {
		return nil, fmt.Errorf("failed to created restClient with userAgent [%s]: %w", userAgent, err)
	}
	client.RESTClient = restClient
	return &client, nil
}

// NewClient will create a client for the given GroupResourceVersion and Kind.
// If namespaced is set to true all request will be sent with the scoped to a namespace.
// The namespaced option can be changed after creation with the client.Namespace variable.
// defaultTimeout will be used to set the timeout for all request from this client. The value of 0 is used to specify an infinite timeout.
// request will return if the provided context is canceled regardless of the value of defaultTimeout.
// Changing the value of client.GVR after it's creation of NewClient will not affect future request.
func NewClient(gvr schema.GroupVersionResource, kind string, namespaced bool, client rest.Interface, defaultTimeout time.Duration) *Client {
	var (
		prefix []string
	)

	if gvr.Group == "" {
		prefix = []string{
			"api",
			gvr.Version,
		}
	} else {
		prefix = []string{
			"apis",
			gvr.Group,
			gvr.Version,
		}
	}

	c := &Client{
		RESTClient: client,
		timeout:    defaultTimeout,
		Namespaced: namespaced,
		GVR:        gvr,
		prefix:     prefix,
		resource:   gvr.Resource,
	}
	c.apiVersion, c.kind = gvr.GroupVersion().WithKind(kind).ToAPIVersionAndKind()
	return c
}

func noop() {}

// setupCtx wraps the provided context with client.timeout, and returns the new context and it's cancel func.
// If client.timeout is 0 then the provided context is returned with a noop function instead of a cancel function.
func (c *Client) setupCtx(ctx context.Context) (context.Context, func()) {
	if c.timeout == 0 {
		return ctx, noop
	}

	return context.WithTimeout(ctx, c.timeout)
}

// Get will attempt to find the requested resource with the given name in the given namespace (if client.Namespaced is set to true).
// Get will then attempt to unmarshal the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) Get(ctx context.Context, namespace, name string, result runtime.Object, opts GetOptions) error {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	req := c.RESTClient.Get().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(name).
		VersionedParams(&opts.GetOptions, metav1.ParameterCodec)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

// List will attempt to find resources in the given namespace (if client.Namespaced is set to true).
// List will then attempt to unmarshal the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) List(ctx context.Context, namespace string, result runtime.Object, opts ListOptions) error {
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	r := c.RESTClient.Get()
	if namespace != "" {
		r = r.NamespaceIfScoped(namespace, c.Namespaced)
	}
	req := r.Resource(c.resource).
		Prefix(c.prefix...).
		VersionedParams(&opts.ListOptions, metav1.ParameterCodec).
		Timeout(timeout)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

// Watch will attempt to start a watch request with the kube-apiserver for resources in the given namespace (if client.Namespaced is set to true).
// Results will be streamed too the returned watch.Interface.
func (c *Client) Watch(ctx context.Context, namespace string, opts ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	req := c.RESTClient.Get().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&opts.ListOptions, metav1.ParameterCodec).
		Timeout(timeout)

	opts.apply(req)

	return c.injectKind(req.Watch(ctx))
}

// Create will attempt create the provided object in the given namespace (if client.Namespaced is set to true).
// Create will then attempt to unmarshal the created object from the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) Create(ctx context.Context, namespace string, obj, result runtime.Object, opts CreateOptions) error {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	req := c.RESTClient.Post().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&opts.CreateOptions, metav1.ParameterCodec).
		Body(obj)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

// Update will attempt update the provided object in the given namespace (if client.Namespaced is set to true).
// Update will then attempt to unmarshal the updated object from the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) Update(ctx context.Context, namespace string, obj, result runtime.Object, opts UpdateOptions) error {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	req := c.RESTClient.Put().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(m.GetName()).
		VersionedParams(&opts.UpdateOptions, metav1.ParameterCodec).
		Body(obj)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

// UpdateStatus will attempt update the status on the provided object in the given namespace (if client.Namespaced is set to true).
// UpdateStatus will then attempt to unmarshal the updated object from the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) UpdateStatus(ctx context.Context, namespace string, obj, result runtime.Object, opts UpdateOptions) error {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	m, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	req := c.RESTClient.Put().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(m.GetName()).
		SubResource("status").
		VersionedParams(&opts.UpdateOptions, metav1.ParameterCodec).
		Body(obj)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

// Delete will attempt to delete the resource with the matching name in the given namespace (if client.Namespaced is set to true).
func (c *Client) Delete(ctx context.Context, namespace, name string, opts DeleteOptions) error {
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	req := c.RESTClient.Delete().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		Name(name).
		Body(&opts.DeleteOptions)

	opts.apply(req)

	return req.Do(ctx).Error()
}

// DeleteCollection will attempt to delete all resource the given namespace (if client.Namespaced is set to true).
// Delete Options take precedent over list options and will be applied last.
func (c *Client) DeleteCollection(ctx context.Context, namespace string, opts DeleteOptions, listOpts ListOptions) error {
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	req := c.RESTClient.Delete().
		Prefix(c.prefix...).
		NamespaceIfScoped(namespace, c.Namespaced).
		Resource(c.resource).
		VersionedParams(&listOpts.ListOptions, metav1.ParameterCodec).
		Timeout(timeout).
		Body(&opts.DeleteOptions)

	listOpts.apply(req)
	opts.apply(req)

	return req.Do(ctx).Error()
}

// Patch attempts to patch the existing resource with the provided data and patchType that matches the given name in the given namespace (if client.Namespaced is set to true).
// Patch will then attempt to unmarshal the updated object from the response into the provide result object.
// If the returned response object is of type Status and has .Status != StatusSuccess, the
// additional information in Status will be used to enrich the error.
func (c *Client) Patch(ctx context.Context, namespace, name string, pt types.PatchType, data []byte, result runtime.Object, opts PatchOptions, subresources ...string) error {
	defer c.setKind(result)
	ctx, cancel := c.setupCtx(ctx)
	defer cancel()
	req := c.RESTClient.Patch(pt).
		Prefix(c.prefix...).
		Namespace(namespace).
		Resource(c.resource).
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts.PatchOptions, metav1.ParameterCodec).
		Body(data)

	opts.apply(req)

	return req.Do(ctx).Into(result)
}

func (c *Client) setKind(obj runtime.Object) {
	if c.kind == "" {
		return
	}
	if _, ok := obj.(*metav1.Status); !ok {
		if meta, err := meta.TypeAccessor(obj); err == nil {
			meta.SetKind(c.kind)
			meta.SetAPIVersion(c.apiVersion)
		}
	}
}

func (c *Client) injectKind(w watch.Interface, err error) (watch.Interface, error) {
	if c.kind == "" || err != nil {
		return w, err
	}

	eventChan := make(chan watch.Event)

	go func() {
		defer close(eventChan)
		for event := range w.ResultChan() {
			c.setKind(event.Object)
			eventChan <- event
		}
	}()

	return &watcher{
		Interface: w,
		eventChan: eventChan,
	}, nil
}

type watcher struct {
	watch.Interface
	eventChan chan watch.Event
}

// ResultChan returns a receive only channel of watch events.
func (w *watcher) ResultChan() <-chan watch.Event {
	return w.eventChan
}
