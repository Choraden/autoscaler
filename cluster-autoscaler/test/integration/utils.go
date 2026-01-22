/*
Copyright 2025 The Kubernetes Authors.

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

package integration

import (
	"k8s.io/autoscaler/cluster-autoscaler/builder"
	fakecloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	"k8s.io/autoscaler/cluster-autoscaler/debuggingsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/loop"
	fakek8s "k8s.io/autoscaler/cluster-autoscaler/utils/fake"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"testing"
)

// FakeSet encapsulates all the fake clients and providers needed for
// an in-memory integration test.
type FakeSet struct {
	// KubeClient is the underlying Kubernetes fake clientset.
	KubeClient *fake.Clientset
	// InformerFactory is the shared informer factory.
	InformerFactory informers.SharedInformerFactory
	// K8s provides helpers to manipulate fake nodes and pods.
	K8s *fakek8s.Kubernetes
	// CloudProvider is the fake cloud provider implementation.
	CloudProvider *fakecloudprovider.CloudProvider
	// PodObserver tracks unschedulable pods; it is defaulted by the builder.
	PodObserver *loop.UnschedulablePodObserver
}

// NewFakeSet initializes a coordinated set of fakes.
func NewFakeSet() *FakeSet {
	kubeClient := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	fK8s := fakek8s.NewKubernetes(kubeClient, informerFactory)
	fCloud := fakecloudprovider.NewCloudProvider(fK8s)
	po := &loop.UnschedulablePodObserver{}

	return &FakeSet{
		KubeClient:      kubeClient,
		InformerFactory: informerFactory,
		K8s:             fK8s,
		CloudProvider:   fCloud,
		PodObserver:     po,
	}
}

// MustCreateManager creates a controller-runtime manager with metrics and health probes disabled.
func MustCreateManager(t *testing.T) manager.Manager {
	t.Helper()

	mgr, err := manager.New(&rest.Config{}, manager.Options{
		Metrics: metricsserver.Options{
			BindAddress: "0",
		},
		HealthProbeBindAddress: "0",
	})
	if err != nil {
		t.Fatalf("Failed to create controller manager: %v", err)
	}
	return mgr
}

// DefaultAutoscalingBuilder returns a production builder pre-configured with
// standard fakes and defaults for integration testing.
func DefaultAutoscalingBuilder(
	opts config.AutoscalingOptions,
	fakes *FakeSet,
	ds debuggingsnapshot.DebuggingSnapshotter,
	mgr manager.Manager,
) *builder.AutoscalerBuilder {
	return builder.New(opts).
		WithDebuggingSnapshotter(ds).
		WithManager(mgr).
		WithKubeClient(fakes.KubeClient).
		WithInformerFactory(fakes.InformerFactory).
		WithCloudProvider(fakes.CloudProvider).
		WithPodObserver(fakes.PodObserver)
}
