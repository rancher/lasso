package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	c "github.com/rancher/lasso/pkg/controller"
)

func TestSharedControllerFactory_PodEvents(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEnv := &envtest.Environment{
		ControlPlaneStopTimeout: time.Minute,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	scf, err := c.NewSharedControllerFactoryFromConfig(cfg, scheme.Scheme)
	if err != nil {
		t.Fatalf("failed to create SharedControllerFactory: %v", err)
	}

	// Create a lasso client for Pods
	podGVK := corev1.SchemeGroupVersion.WithKind("Pod")
	lassoClient, err := scf.SharedCacheFactory().SharedClientFactory().ForKind(podGVK)
	if err != nil {
		t.Fatalf("failed to create lasso client: %v", err)
	}

	eventRecorder := &testEventHandler{
		events: make(chan string, 10),
	}

	controller, err := scf.ForKind(podGVK)
	if err != nil {
		t.Fatalf("failed to create controller for Pod: %v", err)
	}

	controller.RegisterHandler(ctx, "pod-event-handler", c.SharedControllerHandlerFunc(eventRecorder.Handle))

	// Start the controller factory
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := scf.Start(ctx, 2); err != nil {
			t.Errorf("shared controller factory failed to start: %v", err)
		}
	}()

	// Wait for caches to sync. 5 seconds should be plenty in our case.
	time.Sleep(5 * time.Second)

	testNamespace := "default"
	podName := "test-pod"
	t.Run("Create Pod", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: testNamespace,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "nginx",
						Image: "nginx",
					},
				},
			},
		}
		err = lassoClient.Create(ctx, testNamespace, pod, &corev1.Pod{}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create pod: %v", err)
		}
		expectedEvent := fmt.Sprintf("Change:%s/%s", testNamespace, podName)
		assertEvent(t, eventRecorder, expectedEvent)
	})

	t.Run("Update Pod", func(t *testing.T) {
		existingPod := &corev1.Pod{}
		err := lassoClient.Get(ctx, testNamespace, podName, existingPod, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("failed to get existing pod: %v", err)
		}
		updatedPod := existingPod.DeepCopy()
		updatedPod.Labels = map[string]string{"foo": "bar"}
		err = lassoClient.Update(ctx, testNamespace, updatedPod, &corev1.Pod{}, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("failed to update pod: %v", err)
		}
		expectedEvent := fmt.Sprintf("Change:%s/%s", testNamespace, podName)
		assertEvent(t, eventRecorder, expectedEvent)
	})

	t.Run("Delete Pod", func(t *testing.T) {
		err := lassoClient.Delete(ctx, testNamespace, podName, metav1.DeleteOptions{})
		if err != nil {
			t.Fatalf("failed to delete pod: %v", err)
		}

		// Wait for the object to be removed
		err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, false, func(ctx context.Context) (bool, error) {
			_, exists, err := controller.Informer().GetStore().GetByKey(testNamespace + "/" + podName)
			if err != nil {
				return false, err
			}
			return !exists, nil
		})
		if err != nil {
			t.Fatalf("timed out waiting for pod to be deleted from informer's cache: %v", err)
		}
		// Wait for the "Delete" event from the handler
		expectedDeleteEvent := fmt.Sprintf("Delete:%s/%s", testNamespace, podName)
		foundDeleteEvent := false
		for i := 0; i < 20; i++ { // Poll the channel a few times
			select {
			case event := <-eventRecorder.events:
				if event == expectedDeleteEvent {
					foundDeleteEvent = true
					break
				}
				// If it's a change event just continue waiting
				if event == fmt.Sprintf("Change:%s/%s", testNamespace, podName) {
					continue
				}
				t.Errorf("unexpected event %q received before delete event", event)
			case <-time.After(500 * time.Millisecond):
				// No event yet, continue polling
			}
			if foundDeleteEvent {
				break
			}
		}

		if !foundDeleteEvent {
			t.Errorf("timed out waiting for delete event %q", expectedDeleteEvent)
		}
	})

	cancel()
	wg.Wait()
}

func TestSharedControllerFactory_SyncOnlyChangedObjects(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEnv := &envtest.Environment{
		ControlPlaneStopTimeout: time.Minute,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	// Create a SharedControllerFactory with SyncOnlyChangedObjects enabled
	opts := &c.SharedControllerFactoryOptions{
		SyncOnlyChangedObjects: true,
	}
	scf, err := c.NewSharedControllerFactoryFromConfigWithOptions(cfg, scheme.Scheme, opts)
	if err != nil {
		t.Fatalf("failed to create SharedControllerFactory: %v", err)
	}

	podGVK := corev1.SchemeGroupVersion.WithKind("Pod")
	lassoClient, err := scf.SharedCacheFactory().SharedClientFactory().ForKind(podGVK)
	if err != nil {
		t.Fatalf("failed to create lasso client: %v", err)
	}

	eventRecorder := &testEventHandler{
		events: make(chan string, 10),
	}

	controller, err := scf.ForKind(podGVK)
	if err != nil {
		t.Fatalf("failed to create controller for Pod: %v", err)
	}
	controller.RegisterHandler(ctx, "pod-event-handler", c.SharedControllerHandlerFunc(eventRecorder.Handle))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := scf.Start(ctx, 2); err != nil {
			t.Errorf("shared controller factory failed to start: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)

	testNamespace := "default"
	podName := "test-sync-pod"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: testNamespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
				},
			},
		},
	}
	err = lassoClient.Create(ctx, testNamespace, pod, &corev1.Pod{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	assertEvent(t, eventRecorder, fmt.Sprintf("Change:%s/%s", testNamespace, podName))

	existingPod := &corev1.Pod{}
	err = lassoClient.Get(ctx, testNamespace, podName, existingPod, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get existing pod: %v", err)
	}

	// Update pod with resource version change
	updatedPod := existingPod.DeepCopy()
	updatedPod.Labels = map[string]string{"foo": "bar"}
	err = lassoClient.Update(ctx, testNamespace, updatedPod, &corev1.Pod{}, metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("failed to update pod: %v", err)
	}
	assertEvent(t, eventRecorder, fmt.Sprintf("Change:%s/%s", testNamespace, podName))

	cancel()
	wg.Wait()
}

func TestSharedControllerFactory_ForObject(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testEnv := &envtest.Environment{
		ControlPlaneStopTimeout: time.Minute,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("failed to start test environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("failed to stop test environment: %v", err)
		}
	}()

	scf, err := c.NewSharedControllerFactoryFromConfig(cfg, scheme.Scheme)
	if err != nil {
		t.Fatalf("failed to create SharedControllerFactory: %v", err)
	}

	podGVK := corev1.SchemeGroupVersion.WithKind("Pod")
	lassoClient, err := scf.SharedCacheFactory().SharedClientFactory().ForKind(podGVK)
	if err != nil {
		t.Fatalf("failed to create lasso client: %v", err)
	}

	eventRecorder := &testEventHandler{
		events: make(chan string, 10),
	}

	controller, err := scf.ForObject(&corev1.Pod{})
	if err != nil {
		t.Fatalf("failed to create controller for Pod: %v", err)
	}

	controller.RegisterHandler(ctx, "pod-event-handler", c.SharedControllerHandlerFunc(eventRecorder.Handle))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := scf.Start(ctx, 2); err != nil {
			t.Errorf("shared controller factory failed to start: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)

	testNamespace := "default"
	podName := "test-forobject-pod"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: testNamespace,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
				},
			},
		},
	}
	err = lassoClient.Create(ctx, testNamespace, pod, &corev1.Pod{}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create pod: %v", err)
	}
	assertEvent(t, eventRecorder, fmt.Sprintf("Change:%s/%s", testNamespace, podName))

	cancel()
	wg.Wait()
}

// testEventHandler is a simple handler to record events for testing
type testEventHandler struct {
	events chan string
}

func (h *testEventHandler) Handle(key string, obj runtime.Object) (runtime.Object, error) {
	if obj == nil {
		h.events <- fmt.Sprintf("Delete:%s", key)
	} else {
		metaObj, ok := obj.(metav1.Object)
		if !ok {
			return nil, fmt.Errorf("object %s is not metav1.Object", key)
		}
		// When obj is not nil, it's either an Add or an Update.  Just reporting "Change".
		h.events <- fmt.Sprintf("Change:%s/%s", metaObj.GetNamespace(), metaObj.GetName())
	}
	return nil, nil
}

func assertEvent(t *testing.T, recorder *testEventHandler, expected string) {
	t.Helper()
	select {
	case event := <-recorder.events:
		if event != expected {
			t.Errorf("expected event %q, got %q", expected, event)
		}
	case <-time.After(10 * time.Second): // enough time for event to get through system
		t.Errorf("timed out waiting for event %q", expected)
	}
}
