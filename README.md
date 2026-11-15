# nsclass-controller

The controller will attempt to create all resources under `spec.resources` in namespaces with the label `experimental.badstreff.io/namespace-class` that matches `metadata.name` for the `NamespaceClass` resource. Resources are not pruned by default, the boolean `spec.prune` can be set to true if you would like to remove resources from the namespace when the `NamespaceClass` is updated.


# Local Dev

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

# Installation Instructions

Install the Helm chart from a local checkout of this repository. Create a `values.yaml` file with RBAC rules for the resources your `NamespaceClass` templates will manage:

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

# Examples

Examples can be found under `config/samples/`

# TODOs

- [ ] Instead of requeuing every resource, find a way to dynamically watch generated resources
- [ ] Ensure when a namespace switches classes that would create the same resources the old `NamespaceClass` does not delete them. _This can be worked around by an admin by disabling pruning on the original `NamespaceClass` updating the label and then re-enabling pruning_
