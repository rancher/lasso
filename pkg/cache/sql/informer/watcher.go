package informer

import "sync"

type listeners struct {
	lock      sync.RWMutex
	listeners map[Listener]struct{}
}

func newlisteners() *listeners {
	return &listeners{
		listeners: make(map[Listener]struct{}),
	}
}

func (w *listeners) Notify(obj any) {
	w.lock.RLock()
	defer w.lock.RUnlock()

	for listener := range w.listeners {
		listener.Notify()
	}
}

func (w *listeners) AddListener(listener Listener) {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.listeners[listener] = struct{}{}
}

func (w *listeners) RemoveListener(listener Listener) {
	w.lock.Lock()
	defer w.lock.Unlock()

	delete(w.listeners, listener)
}

type Listener interface {
	Notify()
	Close()
}
