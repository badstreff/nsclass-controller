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
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	experimentalv1 "github.com/badstreff/nsclass-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	fieldManager          = "namespace-class-controller"
	ConditionTypeAccepted = "Accepted"
	ConditionTypeReady    = "Ready"
	ConditionTypePruned   = "Pruned"
)

var (
	NamespaceClassReconciliationLabel = experimentalv1.GroupVersion.Group + "/namespace-class"
	ErrDuplicateResource              = errors.New("duplicate resource")
	ErrClusterScopedResource          = errors.New("cluster-scoped resource not supported")
)

type ResourceKey struct {
	GVK            schema.GroupVersionKind
	NamespacedName types.NamespacedName
}

type ResourceInventory map[ResourceKey]experimentalv1.ManagedResource

// NamespaceClassReconciler reconciles a NamespaceClass object
type NamespaceClassReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	RESTMapper meta.RESTMapper

	DefaultRequeue time.Duration

	// Dynamic resource watches
	DynamicWatch bool
	controller   controller.Controller
	cache        cache.Cache
	watchMu      sync.Mutex
	watched      map[schema.GroupVersionKind]struct{}
}

// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=experimental.badstreff.io,resources=namespaceclasses/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=namespaces,verbs=get;list;watch

// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.25.0/pkg/reconcile
func (r *NamespaceClassReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	log.V(1).Info("starting reconciliation")

	// Get NamespaceClass
	nsclass := &experimentalv1.NamespaceClass{}
	if err := r.Get(ctx, req.NamespacedName, nsclass); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	inventory := ResourceInventoryFromStatus(ctx, &nsclass.Status)

	// Build desired state
	log.V(1).Info("building desired state")
	desired, err := r.BuildDesired(ctx, nsclass)
	if err != nil {
		if errors.Is(err, ErrDuplicateResource) {
			condition := &metav1.Condition{
				Type:               ConditionTypeAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             "DuplicateResource",
				Message:            err.Error(),
				ObservedGeneration: nsclass.Generation,
			}
			err = r.UpdateStatus(ctx, nsclass, inventory, condition)
			return ctrl.Result{}, err
		}
		if errors.Is(err, ErrClusterScopedResource) {
			condition := &metav1.Condition{
				Type:               ConditionTypeAccepted,
				Status:             metav1.ConditionFalse,
				Reason:             "ClusterScopedResource",
				Message:            err.Error(),
				ObservedGeneration: nsclass.Generation,
			}
			err = r.UpdateStatus(ctx, nsclass, inventory, condition)
			return ctrl.Result{}, err
		}
		condition := &metav1.Condition{
			Type:               ConditionTypeAccepted,
			Status:             metav1.ConditionFalse,
			Reason:             "BuildDesiredStateFailed",
			Message:            err.Error(),
			ObservedGeneration: nsclass.Generation,
		}
		err = r.UpdateStatus(ctx, nsclass, inventory, condition)
		return ctrl.Result{}, err
	} else {
		condition := &metav1.Condition{
			Type:               ConditionTypeAccepted,
			Status:             metav1.ConditionTrue,
			Reason:             "ValidSpec",
			Message:            "Resource specifications are valid",
			ObservedGeneration: nsclass.Generation,
		}
		if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
			return ctrl.Result{}, err
		}
	}

	if r.DynamicWatch {
		for key := range desired {
			if err := r.WatchGVK(key.GVK); err != nil {
				return ctrl.Result{}, err
			}
		}
	}

	// Apply desired state
	log.V(1).Info("applying desired state")
	applied, err := r.ApplyDesired(ctx, desired)
	inventory = ResourceInventoryFromStatus(ctx, &nsclass.Status)
	maps.Copy(inventory, applied)
	if err != nil {
		condition := &metav1.Condition{
			Type:               ConditionTypeReady,
			Status:             metav1.ConditionFalse,
			Reason:             "ApplyFailed",
			Message:            err.Error(),
			ObservedGeneration: nsclass.Generation,
		}
		statusErr := r.UpdateStatus(ctx, nsclass, inventory, condition)
		return ctrl.Result{}, errors.Join(statusErr, err)
	} else {
		condition := &metav1.Condition{
			Type:               ConditionTypeReady,
			Status:             metav1.ConditionTrue,
			Reason:             "Applied",
			Message:            "Desired resources applied successfully",
			ObservedGeneration: nsclass.Generation,
		}
		if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Prune resources if pruning is enabled
	// When we prune we return requeue true if any resources were marked as needing to be deleted so we check again pretty quick
	if nsclass.Spec.Prune != nil && *nsclass.Spec.Prune {
		log.V(1).Info("pruning removed resources")
		requeue, pruneErr := r.Prune(ctx, nsclass, inventory, desired)
		statusErr := r.UpdateStatus(ctx, nsclass, inventory, nil)
		if pruneErr != nil || statusErr != nil {
			condition := &metav1.Condition{
				Type:               ConditionTypePruned,
				Status:             metav1.ConditionFalse,
				Reason:             "PruneFailed",
				Message:            "Some resources were not fully pruned",
				ObservedGeneration: nsclass.Generation,
			}
			if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, errors.Join(pruneErr, statusErr)
		}
		if requeue {
			condition := &metav1.Condition{
				Type:               ConditionTypePruned,
				Status:             metav1.ConditionFalse,
				Reason:             "Pruning",
				Message:            "Some resources are pending pruning",
				ObservedGeneration: nsclass.Generation,
			}
			if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Second * 10}, nil
		} else {
			condition := &metav1.Condition{
				Type:               ConditionTypePruned,
				Status:             metav1.ConditionTrue,
				Reason:             "PruneSuccessful",
				Message:            "Stale resources pruned",
				ObservedGeneration: nsclass.Generation,
			}
			if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
				return ctrl.Result{}, err
			}
		}
	} else {
		condition := &metav1.Condition{
			Type:               ConditionTypePruned,
			Status:             metav1.ConditionTrue,
			Reason:             "PruneDisabled",
			Message:            "Pruning is currently disabled",
			ObservedGeneration: nsclass.Generation,
		}
		if err := r.UpdateStatus(ctx, nsclass, inventory, condition); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: r.DefaultRequeue}, nil
}

// ListNamespaces returns the namespaces associated with a namespace class
func (r *NamespaceClassReconciler) ListNamespaces(ctx context.Context, nsclass *experimentalv1.NamespaceClass) (*corev1.NamespaceList, error) {
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

// IsNamespaced is a helper utility used to check if an object is a cluster-scoped resource or not
func (r *NamespaceClassReconciler) IsNamespaced(
	obj *unstructured.Unstructured,
) (bool, error) {
	gvk := obj.GroupVersionKind()

	mapping, err := r.RESTMapper.RESTMapping(
		gvk.GroupKind(),
		gvk.Version,
	)
	if err != nil {
		return false, err
	}

	return mapping.Scope.Name() == meta.RESTScopeNameNamespace, nil
}

// BuildDesired returns the resources that need applied or an Error if we find any duplicated
func (r *NamespaceClassReconciler) BuildDesired(ctx context.Context, nsclass *experimentalv1.NamespaceClass) (map[ResourceKey]*unstructured.Unstructured, error) {
	namespaces, err := r.ListNamespaces(ctx, nsclass)
	if err != nil {
		return nil, err
	}
	desired := make(map[ResourceKey]*unstructured.Unstructured, len(nsclass.Spec.Resources)*len(namespaces.Items))
	duplicate := make(map[ResourceKey]*unstructured.Unstructured, len(nsclass.Spec.Resources)*len(namespaces.Items))
	for _, rawResource := range nsclass.Spec.Resources {
		// First check if we have the any duplicates
		obj := &unstructured.Unstructured{}
		if err := json.Unmarshal(rawResource.Raw, &obj.Object); err != nil {
			return nil, err
		}
		key := ResourceKey{
			GVK:            obj.GroupVersionKind(),
			NamespacedName: client.ObjectKeyFromObject(obj),
		}
		key.NamespacedName.Namespace = "" // ignore any namespaces set in the template since they will be overwritten
		namespaced, err := r.IsNamespaced(obj)
		if err != nil {
			return nil, fmt.Errorf("resolve resource scope: %w", err)
		}
		if !namespaced {
			return nil, fmt.Errorf("%w: %v", ErrClusterScopedResource, key)
		}
		if _, ok := duplicate[key]; ok {
			return nil, fmt.Errorf("%w: %v", ErrDuplicateResource, key)
		}
		duplicate[key] = obj
		// If not go ahead and build the desired list
		for _, namespace := range namespaces.Items {
			resource := obj.DeepCopy()
			resource.SetNamespace(namespace.Name)
			if err := controllerutil.SetControllerReference(
				nsclass, resource, r.Scheme,
			); err != nil {
				return nil, err
			}
			key := ResourceKey{
				GVK:            resource.GroupVersionKind(),
				NamespacedName: client.ObjectKeyFromObject(resource),
			}
			desired[key] = resource
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

// Prune looks at the current inventory or resources and removed and resources that are not in the desired map, the inventory param is modified in place
func (r *NamespaceClassReconciler) Prune(
	ctx context.Context,
	nsclass *experimentalv1.NamespaceClass,
	inventory ResourceInventory,
	desired map[ResourceKey]*unstructured.Unstructured,
) (bool, error) {
	var errs error
	requeue := false

	for key, managed := range inventory {
		if _, exists := desired[key]; exists {
			continue
		}

		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(key.GVK)

		// Resource already deleted
		err := r.Get(ctx, key.NamespacedName, obj)
		if apierrors.IsNotFound(err) {
			delete(inventory, key)
			continue
		}

		// Delete the resource owned by nsclass
		if err == nil {
			owner := metav1.GetControllerOf(obj)
			if obj.GetUID() != managed.UID ||
				owner == nil || owner.UID != nsclass.UID {
				delete(inventory, key)
				continue
			}
			uid := obj.GetUID()
			rv := obj.GetResourceVersion()
			err = r.Delete(ctx, obj, &client.DeleteOptions{
				Preconditions: &metav1.Preconditions{
					UID:             &uid,
					ResourceVersion: &rv,
				},
			})
		}

		switch {
		case apierrors.IsNotFound(err):
			delete(inventory, key)
		case err != nil:
			managed.Phase = new(experimentalv1.PhaseFailed)
			inventory[key] = managed
			errs = errors.Join(errs, fmt.Errorf("prune %v: %w", key, err))
		default:
			managed.Phase = new(experimentalv1.PhaseDeleting)
			inventory[key] = managed
			requeue = true
		}
	}

	return requeue, errs
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
	condition *metav1.Condition,
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
	if condition != nil {
		meta.SetStatusCondition(
			&nsclass.Status.Conditions,
			*condition,
		)
	}
	if equality.Semantic.DeepEqual(
		original.Status,
		nsclass.Status,
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

func (r *NamespaceClassReconciler) WatchGVK(
	gvk schema.GroupVersionKind,
) error {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()

	if _, exists := r.watched[gvk]; exists {
		return nil
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)

	err := r.controller.Watch(source.Kind(
		r.cache,
		obj,
		handler.TypedEnqueueRequestForOwner[*unstructured.Unstructured](
			r.Scheme,
			r.RESTMapper,
			&experimentalv1.NamespaceClass{},
			handler.OnlyControllerOwner(),
		),
		predicate.TypedResourceVersionChangedPredicate[*unstructured.Unstructured]{},
	))
	if err != nil {
		return err
	}

	r.watched[gvk] = struct{}{}
	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *NamespaceClassReconciler) SetupWithManager(mgr ctrl.Manager) error {
	c, err := ctrl.NewControllerManagedBy(mgr).
		For(
			&experimentalv1.NamespaceClass{},
			builder.WithPredicates(predicate.GenerationChangedPredicate{}),
		).
		Named("namespaceclass").
		Watches(
			&corev1.Namespace{},
			handler.EnqueueRequestsFromMapFunc(r.mapNamespaceToNamespaceClass),
		).
		Build(r)

	if err != nil {
		return err
	}

	r.watchMu = sync.Mutex{}
	r.controller = c
	r.cache = mgr.GetCache()
	r.watched = make(map[schema.GroupVersionKind]struct{})
	return nil
}
