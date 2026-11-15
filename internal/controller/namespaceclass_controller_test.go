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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	experimentalv1 "github.com/badstreff/nsclass-controller/api/v1"
)

const (
	TestNamespaceClassName = "test-namespaceclass"
	TestNamespaceName      = "test-namespace"
	TestConfigMapName      = "test-config"
	TestServiceAccountName = "test-sa"
)

var _ = Describe("NamespaceClass Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName = TestNamespaceClassName
		)

		ctx := context.Background()

		nsclassKey := types.NamespacedName{
			Name: resourceName,
		}
		nsclass := &experimentalv1.NamespaceClass{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind NamespaceClass")
			err := k8sClient.Get(ctx, nsclassKey, nsclass)
			if err != nil && apierrors.IsNotFound(err) {
				nsclass := &experimentalv1.NamespaceClass{
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
                    			        "name": "` + TestConfigMapName + `"
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
                    			        "name": "` + TestServiceAccountName + `"
    							    }
    							}`),
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, nsclass)).To(Succeed())
				ns := &corev1.Namespace{
					ObjectMeta: metav1.ObjectMeta{
						Name: TestNamespaceName,
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
			nsclass := &experimentalv1.NamespaceClass{}
			err := k8sClient.Get(ctx, nsclassKey, nsclass)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance NamespaceClass")
			Expect(k8sClient.Delete(ctx, nsclass)).To(Succeed())

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: TestConfigMapName, Namespace: TestNamespaceName}, cm)
			if err == nil {
				Expect(k8sClient.Delete(ctx, cm)).To(Succeed())
			}
			sa := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: TestServiceAccountName, Namespace: TestNamespaceName}, sa)
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
				NamespacedName: nsclassKey,
			})
			Expect(err).NotTo(HaveOccurred())

			sa := &corev1.ServiceAccount{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: TestNamespaceName, Name: TestServiceAccountName}, sa)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			err = k8sClient.Get(ctx, types.NamespacedName{Namespace: TestNamespaceName, Name: TestConfigMapName}, cm)
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
			cmKey := types.NamespacedName{Namespace: TestNamespaceName, Name: TestConfigMapName}
			saKey := types.NamespacedName{Namespace: TestNamespaceName, Name: TestServiceAccountName}
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())

			// Check the resources are created on first reconciliation
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, saKey, sa)).To(Succeed())

			// Remove only CM from the spec
			nsclass.Spec.Resources = nsclass.Spec.Resources[1:]
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			// Reconcile again to trigger pruning.
			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify the ConfigMap was deleted and SA remains
			Eventually(func() bool {
				err := k8sClient.Get(ctx, cmKey, cm)
				return apierrors.IsNotFound(err)
			}).Should(BeTrue())
			Expect(k8sClient.Get(ctx, saKey, sa)).To(Succeed())

			// Reconcile again to clear the inventory entry
			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify status reports the SA but not the CM
			Expect(k8sClient.Get(ctx, req.NamespacedName, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(
				ContainElement(And(
					HaveField("Kind", "ServiceAccount"),
					HaveField("Name", TestServiceAccountName),
					HaveField("Namespace", TestNamespaceName),
				)),
			)
			Expect(nsclass.Status.ManagedResources).ToNot(
				ContainElement(And(
					HaveField("Kind", "ConfigMap"),
					HaveField("Name", TestConfigMapName),
					HaveField("Namespace", TestNamespaceName),
				)),
			)
		})
		It("should prune resources when the namespace label is removed", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			ns := &corev1.Namespace{}
			nsKey := types.NamespacedName{Name: TestNamespaceName}
			Expect(k8sClient.Get(ctx, nsKey, ns)).To(Succeed())

			delete(ns.Labels, NamespaceClassReconciliationLabel)
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			// Restore the shared test namespace after this spec.
			DeferCleanup(func() {
				current := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, nsKey, current)).To(Succeed())
				if current.Labels == nil {
					current.Labels = map[string]string{}
				}
				current.Labels[NamespaceClassReconciliationLabel] = TestNamespaceClassName
				Expect(k8sClient.Update(ctx, current)).To(Succeed())
			})

			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			cmKey := types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}
			saKey := types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestServiceAccountName,
			}

			Expect(apierrors.IsNotFound(
				k8sClient.Get(ctx, cmKey, &corev1.ConfigMap{}),
			)).To(BeTrue())

			Expect(apierrors.IsNotFound(
				k8sClient.Get(ctx, saKey, &corev1.ServiceAccount{}),
			)).To(BeTrue())

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(BeEmpty())
		})
		It("should not prune a resource replaced with a different UID", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			cmKey := types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}

			original := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, cmKey, original)).To(Succeed())
			originalUID := original.UID

			Expect(k8sClient.Delete(ctx, original)).To(Succeed())

			replacement := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      TestConfigMapName,
					Namespace: TestNamespaceName,
				},
				Data: map[string]string{"message": "replacement"},
			}
			Expect(k8sClient.Create(ctx, replacement)).To(Succeed())
			Expect(replacement.UID).NotTo(Equal(originalUID))

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())

			// Remove the ConfigMap from desired state.
			nsclass.Spec.Resources = nsclass.Spec.Resources[1:]
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			actual := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, cmKey, actual)).To(Succeed())
			Expect(actual.UID).To(Equal(replacement.UID))
			Expect(actual.Data["message"]).To(Equal("replacement"))

			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(HaveLen(1))
			Expect(nsclass.Status.ManagedResources[0].Kind).To(Equal("ServiceAccount"))
		})
		It("should preserve successful applies when another resource fails", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())

			// ConfigMap.data requires string values, not numbers.
			nsclass.Spec.Resources = append(
				nsclass.Spec.Resources,
				runtime.RawExtension{
					Raw: []byte(`{
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": "invalid-config"},
                "data": {"bad-value": 123}
            }`),
				},
			)
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).To(HaveOccurred())

			// The two valid manifests should still have applied.
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}, cm)).To(Succeed())

			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestServiceAccountName,
			}, sa)).To(Succeed())

			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(HaveLen(2))

			Expect(nsclass.Status.ManagedResources).To(
				ContainElement(HaveField("UID", cm.UID)),
			)
			Expect(nsclass.Status.ManagedResources).To(
				ContainElement(HaveField("UID", sa.UID)),
			)
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
			cmKey := types.NamespacedName{Namespace: TestNamespaceName, Name: TestConfigMapName}

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
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())

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
                    			        "name": "` + TestServiceAccountName + `"
    							    }
    							}`),
			})
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			// Reconciliation should not fail but set the status condition correctly
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			condition := meta.FindStatusCondition(
				nsclass.Status.Conditions,
				ConditionTypeAccepted,
			)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("DuplicateResource"))
			Expect(condition.ObservedGeneration).To(Equal(nsclass.Generation))
		})
		It("should correct manually modified resource data", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			cmKey := types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())

			originalUID := cm.UID
			cm.Data["message"] = "manually modified"
			Expect(k8sClient.Update(ctx, cm)).To(Succeed())

			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			Expect(cm.Data["message"]).To(Equal("hello"))
			Expect(cm.UID).To(Equal(originalUID))
		})
		It("should not change status when reconciling unchanged resources", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			before := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, before)).To(Succeed())
			Expect(before.Status.ManagedResources).To(HaveLen(2))

			cmKey := types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			originalUID := cm.UID

			_, err = controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			after := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, after)).To(Succeed())

			Expect(after.Status.ManagedResources).To(
				Equal(before.Status.ManagedResources),
			)
			Expect(after.ResourceVersion).To(Equal(before.ResourceVersion))

			Expect(k8sClient.Get(ctx, cmKey, cm)).To(Succeed())
			Expect(cm.UID).To(Equal(originalUID))
		})
		It("should override the namespace specified in a resource template", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())

			nsclass.Spec.Resources[0].Raw = []byte(`{
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": "test-config",
            "namespace": "wrong-namespace"
        },
        "data": {"message": "hello"}
    }`)
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Namespace: TestNamespaceName,
				Name:      TestConfigMapName,
			}, cm)).To(Succeed())

			Expect(cm.Namespace).To(Equal(TestNamespaceName))

			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(
				ContainElement(And(
					HaveField("Kind", "ConfigMap"),
					HaveField("Namespace", TestNamespaceName),
				)),
			)
		})
		It("should apply resources only to matching namespaces", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			matching := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "second-matching-namespace",
					Labels: map[string]string{
						NamespaceClassReconciliationLabel: TestNamespaceClassName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, matching)).To(Succeed())

			DeferCleanup(func() {
				// envtest doesn't run the namespace or garbage-collection controllers.
				for _, obj := range []client.Object{
					&corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name: TestConfigMapName, Namespace: matching.Name,
						},
					},
					&corev1.ServiceAccount{
						ObjectMeta: metav1.ObjectMeta{
							Name: TestServiceAccountName, Namespace: matching.Name,
						},
					},
				} {
					Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, obj))).To(Succeed())
				}

				current := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Name: matching.Name,
				}, current)).To(Succeed())

				delete(current.Labels, NamespaceClassReconciliationLabel)
				Expect(k8sClient.Update(ctx, current)).To(Succeed())
			})

			unrelated := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: "unrelated-namespace",
				},
			}
			Expect(k8sClient.Create(ctx, unrelated)).To(Succeed())

			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			for _, namespace := range []string{TestNamespaceName, matching.Name} {
				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Namespace: namespace,
					Name:      TestConfigMapName,
				}, &corev1.ConfigMap{})).To(Succeed())

				Expect(k8sClient.Get(ctx, types.NamespacedName{
					Namespace: namespace,
					Name:      TestServiceAccountName,
				}, &corev1.ServiceAccount{})).To(Succeed())
			}

			Expect(apierrors.IsNotFound(
				k8sClient.Get(ctx, types.NamespacedName{
					Namespace: unrelated.Name,
					Name:      TestConfigMapName,
				}, &corev1.ConfigMap{}),
			)).To(BeTrue())

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			Expect(nsclass.Status.ManagedResources).To(HaveLen(4))
		})
		It("should succeed when the NamespaceClass no longer exists", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			req := reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: "nonexistent-namespaceclass",
				},
			}

			result, err := controllerReconciler.Reconcile(ctx, req)

			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeZero())
		})
		It("should reject duplicate templates even without matching namespaces", func() {
			controllerReconciler := &NamespaceClassReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}
			req := reconcile.Request{NamespacedName: nsclassKey}

			nsKey := types.NamespacedName{Name: TestNamespaceName}
			ns := &corev1.Namespace{}
			Expect(k8sClient.Get(ctx, nsKey, ns)).To(Succeed())

			delete(ns.Labels, NamespaceClassReconciliationLabel)
			Expect(k8sClient.Update(ctx, ns)).To(Succeed())

			DeferCleanup(func() {
				current := &corev1.Namespace{}
				Expect(k8sClient.Get(ctx, nsKey, current)).To(Succeed())
				if current.Labels == nil {
					current.Labels = map[string]string{}
				}
				current.Labels[NamespaceClassReconciliationLabel] = TestNamespaceClassName
				Expect(k8sClient.Update(ctx, current)).To(Succeed())
			})

			nsclass := &experimentalv1.NamespaceClass{}
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())

			// Duplicate the ServiceAccount template.
			nsclass.Spec.Resources = append(
				nsclass.Spec.Resources,
				nsclass.Spec.Resources[1],
			)
			Expect(k8sClient.Update(ctx, nsclass)).To(Succeed())

			// Reconciliation should not fail but set the status condition correctly
			_, err := controllerReconciler.Reconcile(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nsclassKey, nsclass)).To(Succeed())
			condition := meta.FindStatusCondition(
				nsclass.Status.Conditions,
				ConditionTypeAccepted,
			)
			Expect(condition).NotTo(BeNil())
			Expect(condition.Status).To(Equal(metav1.ConditionFalse))
			Expect(condition.Reason).To(Equal("DuplicateResource"))
			Expect(condition.ObservedGeneration).To(Equal(nsclass.Generation))
		})
	})
})
