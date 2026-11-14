/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	experimentalv1 "github.com/badstreff/nsclass-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const fieldManager = "namespace-class-controller"

var (
	NamespaceClassReconciliationLabel = experimentalv1.GroupVersion.Group + "/namespace-class"
)

type ResourceKey struct {
	GVK            schema.GroupVersionKind
	NamespacedName types.NamespacedName
}

type ResourceInventory map[ResourceKey]experimentalv1.ManagedResource

// NamespaceClassReconciler reconciles a NamespaceClass object
type NamespaceClassReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *NamespaceClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("starting reconciliation")

	// Get NamespaceClass
	nsclass := &experimentalv1.NamespaceClass{}
	if err := r.Client.Get(ctx, req.NamespacedName, nsclass); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Build and Apply desired state
	desired, err := r.BuildDesired(ctx, nsclass)
	if err != nil {
		return ctrl.Result{}, err
	}
	applied, err := r.ApplyDesired(ctx, desired)
	if err != nil {
		inventory := ResourceInventoryFromStatus(ctx, &nsclass.Status)
		maps.Copy(inventory, applied)
		statusErr := r.UpdateStatus(ctx, nsclass, inventory)
		return ctrl.Result{}, errors.Join(statusErr, err)
	}

	// Handle status updates and pruning
	// When we prune we return requeue true if any resources were marked as needing to be deleted so we check again pretty quick
	if nsclass.Spec.Prune != nil && *nsclass.Spec.Prune {
		log.Info("pruning")
		previousInventory := ResourceInventoryFromStatus(ctx, &nsclass.Status)
		requeue, inventory, pruneErr := r.Prune(ctx, previousInventory, desired, applied)
		statusErr := r.UpdateStatus(ctx, nsclass, inventory)
		if pruneErr != nil || statusErr != nil {
			log.Info("prune error")
			return ctrl.Result{}, errors.Join(pruneErr, statusErr)
		}
		if requeue {
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		}
	} else {
		inventory := ResourceInventoryFromStatus(ctx, &nsclass.Status)
		maps.Copy(inventory, applied)
		err = r.UpdateStatus(ctx, nsclass, inventory)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// This is the poor mans way of ensuring we update the resources if they are ever manually editted... I'll have to research a better way to make this dynamic
	// TODO: Figure out a way to do this that doesn't require just requeueing every resource every 5 min
	return ctrl.Result{RequeueAfter: time.Minute * 5}, nil
}

// returns the namespaces associated with a namespace class
func (r *NamespaceClassReconciler) listNamespaces(ctx context.Context, nsclass *experimentalv1.NamespaceClass) (*corev1.NamespaceList, error) {
	namespaces := &corev1.NamespaceList{}
	selector, err := metav1.LabelSelectorAsSelector(
		&metav1.LabelSelector{MatchLabels: map[string]string{NamespaceClassReconciliationLabel: nsclass.Name}},
	)
	if err != nil {
		return namespaces, err
	}
	if err = r.List(ctx, namespaces, client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return namespaces, err
	}
	return namespaces, nil
}

func (r *NamespaceClassReconciler) BuildDesired(ctx context.Context, nsclass *experimentalv1.NamespaceClass) (map[ResourceKey]*unstructured.Unstructured, error) {
	namespaces, err := r.listNamespaces(ctx, nsclass)
	if err != nil {
		return nil, err
	}
	desired := make(map[ResourceKey]*unstructured.Unstructured, len(nsclass.Spec.Resources)*len(namespaces.Items))
	for _, namespace := range namespaces.Items {
		for _, rawResource := range nsclass.Spec.Resources {
			obj := &unstructured.Unstructured{}
			if err := json.Unmarshal(rawResource.Raw, &obj.Object); err != nil {
				return nil, err
			}
			obj.SetNamespace(namespace.Name)
			if err := controllerutil.SetControllerReference(nsclass, obj, r.Scheme); err != nil {
				return nil, err
			}
			key := ResourceKey{
				GVK:            obj.GroupVersionKind(),
				NamespacedName: client.ObjectKeyFromObject(obj),
			}
			desired[key] = obj
		}
	}
	return desired, nil
}

func (r *NamespaceClassReconciler) ApplyDesired(ctx context.Context, desired map[ResourceKey]*unstructured.Unstructured) (ResourceInventory, error) {
	applied := make(map[ResourceKey]experimentalv1.ManagedResource)
	var errs error

	for key, obj := range desired {
		// Apply resource using Server-Side Apply
		err := r.Apply(
			ctx,
			client.ApplyConfigurationFromUnstructured(obj),
			client.FieldOwner(fieldManager),
			client.ForceOwnership,
		)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
		applied[key] = experimentalv1.ManagedResource{
			UID:        obj.GetUID(),
			APIVersion: obj.GetAPIVersion(),
			Kind:       obj.GetKind(),
			Namespace:  obj.GetNamespace(),
			Name:       obj.GetName(),
			Phase:      new(experimentalv1.PhaseApplied),
		}
	}

	return applied, errs
}

func (r *NamespaceClassReconciler) Prune(ctx context.Context, previous ResourceInventory, desired map[ResourceKey]*unstructured.Unstructured, applied ResourceInventory) (bool, ResourceInventory, error) {
	inventory := maps.Clone(applied)
	var errs error
	requeue := false

	for key, managed := range previous {
		if _, ok := desired[key]; ok {
			// Retain keys are are desired but we failed to apply
			if _, appliedOK := applied[key]; !appliedOK {
				inventory[key] = managed
			}
			continue
		}
		// Resource no longer desired
		// Attempt deletion with UID precondition
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion(managed.APIVersion)
		obj.SetKind(managed.Kind)
		obj.SetNamespace(managed.Namespace)
		obj.SetName(managed.Name)
		err := r.Delete(
			ctx,
			obj,
			&client.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID: new(managed.UID),
				},
			},
		)
		switch {
		case apierrors.IsNotFound(err):
			// Resource is confirmed absent.
			// Do not include it in remaining.
			continue
		case err != nil:
			managed.Phase = new(experimentalv1.PhaseFailed)
			inventory[key] = managed
			errs = errors.Join(errs, err)
		default:
			managed.Phase = new(experimentalv1.PhaseDeleting)
			inventory[key] = managed
			requeue = true
		}
	}

	return requeue, inventory, errs
}

// ResourceInventoryFromStatus parses the status of the NamespaceClass and returns a ResourceInventory
func ResourceInventoryFromStatus(ctx context.Context, status *experimentalv1.NamespaceClassStatus) ResourceInventory {
	inventory := make(ResourceInventory, len(status.ManagedResources))
	for _, resource := range status.ManagedResources {
		key := ResourceKey{
			GVK: schema.FromAPIVersionAndKind(
				resource.APIVersion,
				resource.Kind,
			),
			NamespacedName: types.NamespacedName{
				Namespace: resource.Namespace,
				Name:      resource.Name,
			},
		}
		inventory[key] = resource
	}
	return inventory
}

func (r *NamespaceClassReconciler) UpdateStatus(
	ctx context.Context,
	nsclass *experimentalv1.NamespaceClass,
	inventory ResourceInventory,
) error {
	original := nsclass.DeepCopy()

	resources := make(
		[]experimentalv1.ManagedResource,
		0,
		len(inventory),
	)

	for _, resource := range inventory {
		resources = append(resources, resource)
	}

	// Ensure deterministic ordering so we can avoid unnecessary status patches.
	slices.SortFunc(resources, func(a, b experimentalv1.ManagedResource) int {
		return cmp.Or(
			cmp.Compare(a.APIVersion, b.APIVersion),
			cmp.Compare(a.Kind, b.Kind),
			cmp.Compare(a.Namespace, b.Namespace),
			cmp.Compare(a.Name, b.Name),
		)
	})
	nsclass.Status.ManagedResources = resources
	if equality.Semantic.DeepEqual(
		original.Status.ManagedResources,
		nsclass.Status.ManagedResources,
	) {
		return nil
	}

	return r.Status().Patch(
		ctx,
		nsclass,
		client.MergeFromWithOptions(
			original,
			client.MergeFromWithOptimisticLock{},
		),
	)

}

func (r *NamespaceClassReconciler) mapNamespaceToNamespaceClass(ctx context.Context, obj client.Object) []reconcile.Request {
	ns, ok := obj.(*corev1.Namespace)
	if !ok {
		panic("mapNamespaceToNamespaceClass received an object that was not a namespace")
	}
	nsclass, ok := ns.Labels[NamespaceClassReconciliationLabel]
	if ok {
		return []reconcile.Request{
			{NamespacedName: types.NamespacedName{Namespace: "", Name: nsclass}},
		}
	}
	return []reconcile.Request{}
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(
			&experimentalv1.NamespaceClass{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("namespaceclass").
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapNamespaceToNamespaceClass),
		).
		Owns(&networkingv1.NetworkPolicy{}).
		Complete(r)
}
