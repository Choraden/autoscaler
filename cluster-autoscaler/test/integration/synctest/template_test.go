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

package synctest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	fakecloudprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/debuggingsnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/test/integration"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

const (
	unneededTime = 1 * time.Minute
)

func TestStaticAutoscaler_Template(t *testing.T) {
	// Initialize fakes and configuration options.
	// This happens outside the synctest bubble to keep the setup clean.
	config := integration.NewConfig().WithOverrides() // override CA options if needed

	options := config.ResolveOptions()

	fakes := integration.NewFakeSet()

	ds := debuggingsnapshot.NewDebuggingSnapshotter(false)
	mgr := integration.MustCreateManager(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		// This ensures all background goroutines wake up and exit when the test finishes.
		defer TearDown(cancel)

		autoscaler, _, err := integration.DefaultAutoscalingBuilder(options, fakes, ds, mgr).Build(ctx)
		assert.NoError(t, err)

		// Setup the state of the world using fakes.
		n := test.BuildTestNode("node", 1000, 1000, test.IsReady(true))
		fakes.CloudProvider.AddNodeGroup("ng", fakecloudprovider.WithNode(n))
		fakes.K8s.AddPod(test.BuildScheduledTestPod("p", 600, 100, n.Name))

		err = RunOnceAfter(t, autoscaler, unneededTime)
		assert.NoError(t, err)
		// Make assertions.
		size, _ := fakes.CloudProvider.GetNodeGroup("ng").TargetSize()
		assert.Equal(t, 1, size)
	})
}
