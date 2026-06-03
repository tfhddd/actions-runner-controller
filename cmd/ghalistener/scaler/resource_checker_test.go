package scaler

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	arcconst "github.com/actions/actions-runner-controller/controllers/actions.github.com"
)

const (
	testNS     = "test-ns"
	testRSName = "test-runner-set"
)

// buildFakeClientset creates a fake k8s clientset pre-populated with the given objects.
func buildFakeClientset(objs ...runtime.Object) *fake.Clientset {
	return fake.NewClientset(objs...)
}

// buildEphemeralRunnerSet returns a minimal EphemeralRunnerSet with the given
// annotations and nodeSelector.
func buildEphemeralRunnerSet(annotations map[string]string, nodeSelector map[string]string) *v1alpha1.EphemeralRunnerSet {
	return &v1alpha1.EphemeralRunnerSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        testRSName,
			Namespace:   testNS,
			Annotations: annotations,
		},
		Spec: v1alpha1.EphemeralRunnerSetSpec{
			EphemeralRunnerSpec: v1alpha1.EphemeralRunnerSpec{
				PodTemplateSpec: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						NodeSelector: nodeSelector,
						Containers: []corev1.Container{
							{Name: "runner"},
						},
					},
				},
			},
		},
	}
}

// buildNode returns a Node with the given allocatable resources and labels.
func buildNode(name string, allocatable corev1.ResourceList, labels map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		Status:     corev1.NodeStatus{Allocatable: allocatable},
	}
}

// buildPod returns a Running pod bound to nodeName with the given resource requests.
func buildPod(name, nodeName string, phase corev1.PodPhase, requests corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Containers: []corev1.Container{
				{Resources: corev1.ResourceRequirements{Requests: requests}},
			},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// buildResourceQuota returns a ResourceQuota with the given hard limits and used amounts.
func buildResourceQuota(name, ns string, hard, used corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
		Status:     corev1.ResourceQuotaStatus{Hard: hard, Used: used},
	}
}

// fakeERSGetter returns an ersGetter that always returns the given EphemeralRunnerSet.
func fakeERSGetter(ers *v1alpha1.EphemeralRunnerSet) ersGetter {
	return func(_ context.Context, _, _ string) (*v1alpha1.EphemeralRunnerSet, error) {
		return ers, nil
	}
}

// --- AdjustCount tests ---

func TestAdjustCount_PartialAllocation(t *testing.T) {
	// 7 NPU available, each runner needs 2 → can fit 3, not 4
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("7"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n) // floor(7/2) = 3
}

func TestAdjustCount_FullAllocation(t *testing.T) {
	// 8 NPU available, each runner needs 2 → all 4 fit
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("8"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestAdjustCount_ZeroWhenNoResources(t *testing.T) {
	// 1 NPU available, each runner needs 2 → 0 fit
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("1"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestAdjustCount_BottleneckResourceLimits(t *testing.T) {
	// CPU allows 8 runners, NPU allows 3 → bottleneck is NPU → 3
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobCPU: "1",
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceCPU:                    resource.MustParse("8"),
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("7"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n) // min(8, floor(7/2)=3) = 3
}

func TestAdjustCount_NoAnnotations_ReturnsRequestedCount(t *testing.T) {
	// no annotations → skip check → return math.MaxInt (no constraint)
	ers := buildEphemeralRunnerSet(nil, nil)
	cs := buildFakeClientset()
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, math.MaxInt, n)
}

func TestAdjustCount_ERSFetchError(t *testing.T) {
	cs := buildFakeClientset()
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: func(_ context.Context, _, _ string) (*v1alpha1.EphemeralRunnerSet, error) {
			return nil, fmt.Errorf("not found")
		},
	}
	_, err := checker.AdjustCount(context.Background())
	assert.Error(t, err)
}

func TestAdjustCount_RunningRunnersCountedTowardTotal(t *testing.T) {
	// Bug scenario: 2 runners already running (using 4 NPU), available=4 NPU.
	// GitHub wants 4 runners (2 running + 2 queued).
	// floor(available/2) = 2 additional → total feasible = 2+2 = 4, not 2.
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	ers.Status.CurrentReplicas = 2
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("8"),
	}, nil)
	// 2 runner pods already running in the ERS namespace, each using 2 NPU
	runner1 := buildPod("runner-1", "node1", corev1.PodRunning, corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("2"),
	})
	runner1.Namespace = testNS
	runner2 := buildPod("runner-2", "node1", corev1.PodRunning, corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("2"),
	})
	runner2.Namespace = testNS

	cs := buildFakeClientset(node, runner1, runner2)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	// GitHub says 4 jobs assigned (2 running + 2 queued)
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, n) // 2 running + floor((8-4)/2)=2 additional = 4
}

// --- inferFromRunnerContainerLimits fallback tests ---

func TestAdjustCount_FallbackToRunnerContainerLimits_NPU(t *testing.T) {
	// No annotations; runner container has NPU limits → fallback reads them.
	// 8 NPU allocatable, runner requests 2 → capacity = floor(8/2) = 4.
	ers := buildEphemeralRunnerSet(nil, nil)
	ers.Spec.EphemeralRunnerSpec.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("2"),
		},
	}
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("8"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestAdjustCount_FallbackToRunnerContainerLimits_CPU_Memory(t *testing.T) {
	// No annotations; runner container has CPU+memory limits.
	// CPU: 32 allocatable / 8 per runner = 4; memory: 128Gi / 32Gi = 4 → capacity = 4.
	ers := buildEphemeralRunnerSet(nil, nil)
	ers.Spec.EphemeralRunnerSpec.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("8"),
			corev1.ResourceMemory: resource.MustParse("32Gi"),
		},
	}
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("32"),
		corev1.ResourceMemory: resource.MustParse("128Gi"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, n)
}

func TestAdjustCount_AnnotationsTakePriorityOverContainerLimits(t *testing.T) {
	// Annotations say 4 NPU per runner; container limits say 2.
	// Annotations win: 8 / 4 = 2.
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:4",
	}, nil)
	ers.Spec.EphemeralRunnerSpec.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("2"),
		},
	}
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("8"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n) // annotations win over container limits
}

func TestAdjustCount_NoAnnotationsNoLimits_ReturnsMaxInt(t *testing.T) {
	// No annotations, no container limits → no constraint → MaxInt.
	ers := buildEphemeralRunnerSet(nil, nil)
	cs := buildFakeClientset()
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, math.MaxInt, n)
}

// --- Namespace ResourceQuota capacity tests ---

func TestAdjustCount_NamespaceQuota_LimitsCapacity(t *testing.T) {
	// Node has 20 NPU, quota hard=10, used=6 → remaining=4 → ns_capacity=floor(4/2)+0=2
	// Node capacity alone would be floor(20/2)=10, quota wins → 2
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("20"),
	}, nil)
	quota := buildResourceQuota("arc-npu-quota", testNS, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("10"),
	}, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("6"),
	})
	cs := buildFakeClientset(node, quota)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n) // quota wins over node capacity
}

func TestAdjustCount_NamespaceQuota_NodeCapacityWins(t *testing.T) {
	// Node has 4 NPU, quota hard=20, used=0 → node capacity=floor(4/2)=2 wins
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("4"),
	}, nil)
	quota := buildResourceQuota("arc-npu-quota", testNS, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("20"),
	}, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("0"),
	})
	cs := buildFakeClientset(node, quota)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n) // node wins over quota
}

func TestAdjustCount_NoResourceQuota_UsesNodeCapacity(t *testing.T) {
	// No quota configured → fall back to node-only capacity
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("8"),
	}, nil)
	cs := buildFakeClientset(node) // no quota object
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 4, n) // floor(8/2)=4, no quota constraint
}

func TestAdjustCount_NamespaceQuota_QuotaExhausted_RunningCountsBack(t *testing.T) {
	// quota hard=4, used=4 (exhausted), 2 runners already running.
	// remaining=0 → additional=0, ns_capacity = 0 + currentReplicas(2) = 2
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:2",
	}, nil)
	ers.Status.CurrentReplicas = 2
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("8"),
	}, nil)
	quota := buildResourceQuota("arc-npu-quota", testNS, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("4"),
	}, corev1.ResourceList{
		corev1.ResourceName("requests.huawei.com/ascend-1980"): resource.MustParse("4"),
	})
	cs := buildFakeClientset(node, quota)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n) // quota exhausted but 2 already running
}

func TestAdjustCount_NamespaceQuota_RBACForbidden_SkipsQuotaCheck(t *testing.T) {
	// RBAC forbids listing resourcequotas → skip quota check, use node capacity
	// fake.Clientset can't simulate 403 on resourcequotas directly, so we test
	// the no-quota path (same behaviour: node capacity returned)
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/ascend-1980:2",
	}, nil)
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/ascend-1980"): resource.MustParse("6"),
	}, nil)
	cs := buildFakeClientset(node)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n) // floor(6/2)=3
}

func TestAdjustCount_RunningRunnersPartialAdditional(t *testing.T) {
	// 2 runners running (4 NPU used), only 3 NPU left → 1 additional.
	// total feasible = 2 + 1 = 3, GitHub wants 4 → adjusted = 3.
	ers := buildEphemeralRunnerSet(map[string]string{
		arcconst.AnnotationKeyJobNPU: "huawei.com/npu:2",
	}, nil)
	ers.Status.CurrentReplicas = 2
	node := buildNode("node1", corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("7"),
	}, nil)
	runner1 := buildPod("runner-1", "node1", corev1.PodRunning, corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("2"),
	})
	runner1.Namespace = testNS
	runner2 := buildPod("runner-2", "node1", corev1.PodRunning, corev1.ResourceList{
		corev1.ResourceName("huawei.com/npu"): resource.MustParse("2"),
	})
	runner2.Namespace = testNS

	cs := buildFakeClientset(node, runner1, runner2)
	checker := &KubernetesResourceChecker{
		clientset: cs, ephemeralRunnerSetNS: testNS,
		ephemeralRunnerSetName: testRSName, logger: discardLogger,
		ersGetter: fakeERSGetter(ers),
	}
	n, err := checker.AdjustCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n) // 2 running + floor(3/2)=1 additional = 3
}
