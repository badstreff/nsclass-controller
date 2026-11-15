# nsclass-controller

The controller will attempt to create all resources under `spec.resources` in namespaces with the label `experimental.badstreff.io/namespace-class` that matches `metadata.name` for the `NamespaceClass` resource. Resources are not pruned by default, the boolean `spec.prune` can be set to true if you would like to remove resources from the namespace when the `NamespaceClass` is updated. Duplicate resources or cluster-scoped resources will result in an error.

## Local Dev

```bash
# Setup
kind create cluster --name demo
make generate manifests install
make test  # optional
make run

# In a different shell
kubectl apply -f config/samples/hello_v1_namespaceclass.yaml
kubectl create namespace demo
kubectl patch namespace demo -p '{"metadata":{"labels":{"experimental.badstreff.io/namespace-class":"namespaceclass-helloworld"}}}'
kubectl get all -n demo
```

## Installation Instructions

Install the Helm chart from a local checkout of this repository. Create a `values.yaml` file with RBAC rules for the resources your `NamespaceClass` resources will manage:

```yaml
rbac:
  additionalRules:
    - apiGroups: [""]
      resources: ["configmaps", "serviceaccounts"]
      verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
    - apiGroups: ["apps"]
      resources: ["deployments"]
      verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
```

Use `apiGroups: [""]` for core Kubernetes resources. Adjust the API groups and plural resource names to match your templates. These rules are added to the controller's built-in permissions; `rbac.additionalRules` defaults to an empty list.

```bash
helm upgrade --install nsclass-controller ./dist/chart \
  --namespace nsclass-controller-system \
  --create-namespace \
  --values values.yaml
```

The chart creates a ClusterRole by default (`rbac.namespaced: false`) so the controller can manage resources across namespaces.

## Examples

Examples can be found under `config/samples/`

## Known Issues

- Switching a namespace between classes that manage the same resources can cause the old `NamespaceClass` to prune resources before the new class adopts them. Disable pruning on the original `NamespaceClass`, update the namespace label, wait for the new class to reconcile successfully and take ownership, and then re-enable pruning on the original class. Once ownership has transferred, the old class skips those resources during pruning.

## FAQ

### Why can I modify some fields on a resource spec while others are overwritten?

The controller uses Server-Side Apply with forced field ownership. Fields specified in `spec.resources` are reapplied during reconciliation, so manual changes to those fields are overwritten. Fields omitted from the template can remain under another field manager's ownership. Kubernetes tracks field ownership in `metadata.managedFields`; list and map merge behavior depends on the resource's schema.

### Why does the controller report a forbidden error when managing a resource?

The controller needs RBAC permissions for each resource type in your templates. Add the appropriate API groups, plural resource names, and verbs to `rbac.additionalRules` in your Helm values, then upgrade the release using those values. See [Installation Instructions](#installation-instructions) for an example.
