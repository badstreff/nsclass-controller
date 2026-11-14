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
	"context"

	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	experimentalv1 "github.com/badstreff/nsclass-controller/api/v1"
)

var _ = Describe("NamespaceClass Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName = "test-namespaceclass"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name: resourceName,
		}
		namespaceclass := &experimentalv1.NamespaceClass{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind NamespaceClass")
			err := k8sClient.Get(ctx, typeNamespacedName, namespaceclass)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &experimentalv1.NamespaceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: resourceName,
					},
					Spec: experimentalv1.NamespaceClassSpec{
						Prune: new(true),
						Resources: []runtime.RawExtension{
							{
								Raw: []byte(`{
    							    "apiVersion": "v1",
                    			    "kind": "ConfigMap",
                    			    "metadata": {
                    			        "name": "test-config"
                    			    },
                    			    "data": {
                    			        "message": "hello"
                    			    }
                    			}`),
							},
							{
								Raw: []byte(`{
    							    "apiVersion": "v1",
    							    "kind": "ServiceAccount",
    							    "metadata": {
    							        "name": "test-sa"
    							    }
    							}`),
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-namespace",
						Labels: map[string]string{
							NamespaceClassReconciliationLabel: resourceName,
						},
					},
				}
				err := k8sClient.Create(ctx, ns)
				Expect(client.IgnoreAlreadyExists(err)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &experimentalv1.NamespaceClass{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance NamespaceClass")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-config", Namespace: "test-namespace"}, cm)
			if err == nil {
				Expect(k8sClient.Delete(ctx, cm)).To(Succeed())
			}
			sa := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-sa", Namespace: "test-namespace"}, sa)
			if err == nil {
				Expect(k8sClient.Delete(ctx, sa)).To(Succeed())
			}
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			sa := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: "test-namespace", Name: "test-sa"}, sa)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: "test-namespace", Name: "test-config"}, cm)
			Expect(err).NotTo(HaveOccurred())
		})
		It("should successfully prune the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: resourceName,
				},
			}
			key := types.NamespacedName{Namespace: "test-namespace", Name: "test-config"}
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())

			// Check the CM is created on first reconciliation
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, key, cm)).To(Succeed())

			// Remove resources form the spec
			nsclass.Spec.Resources = nil
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			// Reconcile again to trigger pruning.
			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify the ConfigMap was deleted
			Eventually(func() bool {
				err := k8sClient.Get(ctx, key, cm)
				return apierrors.IsNotFound(err)
			}).Should(BeTrue())

			// Reconcile again to clear the inventory entry
			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify status no longer tracks the resources
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(BeEmpty())
		})
		It("should leave resources and status when prune is disabled", func() {
			By("Reconciling the created resource")
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: resourceName,
				},
			}
			key := types.NamespacedName{Namespace: "test-namespace", Name: "test-config"}

			// Disable pruning
			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			nsclass.Spec.Prune = new(false)
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Delete resources
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			nsclass.Spec.Resources = nil
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())
			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Check that the configmap still exists
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, key, cm)).To(Succeed())

			// Verify status fields remain
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).ToNot(BeEmpty())
		})
		It("should error on duplicate resources", func() {
			By("Reconciling the created resource")
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: resourceName,
				},
			}
			// Add duplicate resource
			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			nsclass.Spec.Resources = append(nsclass.Spec.Resources, runtime.RawExtension{
				Raw: []byte(`{
    							    "apiVersion": "v1",
    							    "kind": "ServiceAccount",
    							    "metadata": {
    							        "name": "test-sa"
    							    }
    							}`),
			})
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())
			// Reconciliation should fail with ErrDuplicateResource
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(errors.Is(err, ErrDuplicateResource)).To(Equal(true))
		})
	})
})
