Lasso
=====

Core controller framework used by [Wrangler](http://github.com/rancher/wrangler) and [Norman](http://github.com/rancher/norman).

This framework is a bit raw, but can be used directly. The intention is try to standardize some boilerplate
of building controllers.

### Basic components

#### Package `cache`

This is a generic form of the generated SharedIndexFactory.

#### Package `client`

Generic form of the generated clientsets.

#### Package `controller`

The `Controller` is a simple approach that wraps the standard pattern of worker goroutines and workqueue.  The package
depends on the `cache` and `client` removing the need for generated clientsets, informers, and listers.

The `SharedController` type is an extension of `Controller` that adds the ability to register multiple handlers on a
shared controller instance.

#### Simple Example

```go

import (
	"context"

	"github.com/rancher/lasso/pkg/controller"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
)

func MyController(ctx context.Context, config *rest.Config) error {

	// Setup types you want to work with
	scheme := runtime.NewScheme()
	appsv1.AddToScheme(scheme)

	controllerFactory, err := controller.NewSharedControllerFactoryFromConfig(config, scheme)
	if err != nil {
		return err
	}

	sharedController, err := controllerFactory.ForObject(&appsv1.Deployment{})
	if err != nil {
		return err
	}

	// Register a handler on a shared controller. If you a dedicated queue then use controller.New()
	sharedController.RegisterHandler(ctx, "my-handler", controller.SharedControllerHandlerFunc(func(key string, obj runtime.Object) (runtime.Object, error) {
		// obj is the latest version of this obj in the cache and MAY BE NIL when an object is finally deleted
		dep, ok := obj.(*appsv1.Deployment)
		if !ok {
			return obj, nil
		}

		// Do some stuff ...

		// Get stuff from the cache
		sharedController.Informer().GetStore().Get(key)

		result := &appsv1.Deployment{}
		err := sharedController.Client().Update(ctx, dep.Namespace, dep, result, metav1.UpdateOptions{})

		// return the latest version of the object
		return result, err
	}))

	return controllerFactory.Start(ctx, 5)
}

```
