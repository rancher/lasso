package cache

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/client"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	defaultTimeout = 15 * time.Second
)

type healthcheck struct {
	lock     sync.RWMutex
	ctx      context.Context
	nsClient *client.Client
	callback func(bool)
}

func (h *healthcheck) ping() error {
	if h.ctx == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(h.ctx, defaultTimeout)
	defer cancel()
	return h.nsClient.Get(ctx, "", "kube-system", &v1.Namespace{}, metav1.GetOptions{})
}

func (h *healthcheck) start(ctx context.Context, cf client.SharedClientFactory) error {
	h.lock.Lock()
	defer h.lock.Unlock()
	if h.ctx != nil {
		return nil
	}

	nsClient, err := cf.ForResource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "namespaces",
	}, false)
	if err != nil {
		return err
	}
	h.ctx = ctx
	h.nsClient = nsClient

	go h.loop()
	return nil
}

func (h *healthcheck) check(stopCh <-chan struct{}) error {
	done := make(chan struct{})
	go func() {
		h.lock.RLock()
		defer h.lock.RUnlock()
		close(done)
	}()
	select {
	case <-done:
	case <-stopCh:
	}
	return nil
}

func (h *healthcheck) report(good bool) {
	if h.callback != nil {
		h.callback(good)
	}
}

func (h *healthcheck) pingUntilGood() bool {
	for {
		if isDone(h.ctx) {
			return true
		}

		if err := h.ping(); err == nil {
			h.report(true)
			return false
		}

		h.report(false)
		time.Sleep(defaultTimeout)
	}
}

func (h *healthcheck) loop() {
	for {
		h.lock.Lock()
		exit := h.pingUntilGood()
		h.lock.Unlock()
		if exit {
			return
		}
		time.Sleep(1 * time.Minute)
	}
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
