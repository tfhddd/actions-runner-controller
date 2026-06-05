package scaler

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/actions/actions-runner-controller/apis/actions.github.com/v1alpha1"
	arcconst "github.com/actions/actions-runner-controller/controllers/actions.github.com"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"
)

// ResourceChecker decides whether the cluster has enough resources to run count runners.
type ResourceChecker interface {
	// AdjustCount returns the true cluster capacity (running + additional feasible runners),
	// used to dynamically update maxRunners reported to GitHub.
	AdjustCount(ctx context.Context) (capacity int, err error)
}

// ersGetter abstracts fetching an EphemeralRunnerSet so tests can inject a fake.
type ersGetter func(ctx context.Context, ns, name string) (*v1alpha1.EphemeralRunnerSet, error)

// KubernetesResourceChecker implements ResourceChecker against a live cluster.
type KubernetesResourceChecker struct {
	clientset              kubernetes.Interface
	volcanoClient          volcanoclient.Interface
	restClient             rest.Interface
	ephemeralRunnerSetNS   string
	ephemeralRunnerSetName string
	logger                 *slog.Logger
	// ersGetter is set to a real k8s REST call in production; overridable in tests.
	ersGetter ersGetter
}

// NewKubernetesResourceChecker constructs a checker wired to the live k8s API.
// cfg is the rest config already obtained by the caller; it is reused to build
// the Volcano client without a redundant InClusterConfig call.
func NewKubernetesResourceChecker(
	cs *kubernetes.Clientset,
	cfg *rest.Config,
	ns, name string,
	logger *slog.Logger,
) *KubernetesResourceChecker {
	var vc volcanoclient.Interface
	if cfg != nil {
		vc, _ = volcanoclient.NewForConfig(cfg)
	}
	c := &KubernetesResourceChecker{
		clientset:              cs,
		volcanoClient:          vc,
		restClient:             cs.RESTClient(),
		ephemeralRunnerSetNS:   ns,
		ephemeralRunnerSetName: name,
		logger:                 logger,
	}
	c.ersGetter = c.fetchERS
	return c
}

func (c *KubernetesResourceChecker) fetchERS(ctx context.Context, ns, name string) (*v1alpha1.EphemeralRunnerSet, error) {
	ers := &v1alpha1.EphemeralRunnerSet{}
	err := c.restClient.
		Get().
		Prefix("apis", v1alpha1.GroupVersion.Group, v1alpha1.GroupVersion.Version).
		Namespace(ns).
		Resource("ephemeralrunnersets").
		Name(name).
		Do(ctx).
		Into(ers)
	return ers, err
}

// jobResourceRequirements extracts per-job resource requirements from ERS annotations.
// Only annotations that are present are included; absent annotations are not checked.
//
// Supported annotations (all optional):
//
//	actions.github.com/job-cpu    — CPU cores per job (e.g. "32")
//	actions.github.com/job-memory — Memory per job    (e.g. "128Gi")
//	actions.github.com/job-npu    — NPU resource per job, format "<resource-name>:<count>"
//	                                (e.g. "huawei.com/ascend-1980:4")
func jobResourceRequirements(annotations map[string]string) (corev1.ResourceList, error) {
	reqs := corev1.ResourceList{}

	if v, ok := annotations[arcconst.AnnotationKeyJobCPU]; ok {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s annotation value %q: %w", arcconst.AnnotationKeyJobCPU, v, err)
		}
		reqs[corev1.ResourceCPU] = q
	}

	if v, ok := annotations[arcconst.AnnotationKeyJobMemory]; ok {
		q, err := resource.ParseQuantity(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s annotation value %q: %w", arcconst.AnnotationKeyJobMemory, v, err)
		}
		reqs[corev1.ResourceMemory] = q
	}

	if v, ok := annotations[arcconst.AnnotationKeyJobNPU]; ok {
		resName, count, err := parseNPUAnnotation(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s annotation value %q: %w", arcconst.AnnotationKeyJobNPU, v, err)
		}
		reqs[corev1.ResourceName(resName)] = *resource.NewQuantity(int64(count), resource.DecimalSI)
	}

	return reqs, nil
}

// inferFromRunnerContainerLimits extracts resource limits from the "runner" container
// in the pod spec as a fallback when no job resource annotations are present.
// This covers dind and kubernetes modes where the runner pod spec itself is the
// authoritative source of per-job resource requirements.
func inferFromRunnerContainerLimits(containers []corev1.Container) corev1.ResourceList {
	for _, c := range containers {
		if c.Name != v1alpha1.EphemeralRunnerContainerName {
			continue
		}
		if len(c.Resources.Limits) > 0 {
			return c.Resources.Limits.DeepCopy()
		}
		return nil
	}
	return nil
}

// parseNPUAnnotation parses "<resource-name>:<count>" (e.g. "huawei.com/ascend-1980:4").
func parseNPUAnnotation(v string) (string, int, error) {
	idx := strings.LastIndex(v, ":")
	if idx <= 0 || idx == len(v)-1 {
		return "", 0, fmt.Errorf("must be \"<resource-name>:<count>\"")
	}
	resName := v[:idx]
	n, err := strconv.Atoi(v[idx+1:])
	if err != nil || n < 0 {
		return "", 0, fmt.Errorf("count must be a non-negative integer, got %q", v[idx+1:])
	}
	return resName, n, nil
}

// jobArchNodeSelector returns an extra node selector derived from the
// actions.github.com/job-arch annotation, or nil if the annotation is absent.
func jobArchNodeSelector(annotations map[string]string) map[string]string {
	arch, ok := annotations[arcconst.AnnotationKeyJobArch]
	if !ok {
		return nil
	}
	return map[string]string{"kubernetes.io/arch": arch}
}

// AdjustCount returns the true cluster capacity: currently running runners plus
// however many more can be scheduled given available resources. This value is
// used to update maxRunners reported to GitHub so it reflects real capacity.
func (c *KubernetesResourceChecker) AdjustCount(ctx context.Context) (int, error) {
	ers, err := c.ersGetter(ctx, c.ephemeralRunnerSetNS, c.ephemeralRunnerSetName)
	if err != nil {
		return 0, fmt.Errorf("fetch EphemeralRunnerSet: %w", err)
	}

	jobRequests, err := jobResourceRequirements(ers.Annotations)
	if err != nil {
		return 0, fmt.Errorf("parse job resource annotations: %w", err)
	}
	if len(jobRequests) == 0 {
		jobRequests = inferFromRunnerContainerLimits(ers.Spec.EphemeralRunnerSpec.Spec.Containers)
	}
	if len(jobRequests) == 0 {
		c.logger.Info("No resource constraints found on EphemeralRunnerSet, skipping resource check")
		return math.MaxInt, nil
	}

	nodeSelector := mergeSelectors(
		ers.Spec.EphemeralRunnerSpec.Spec.NodeSelector,
		jobArchNodeSelector(ers.Annotations),
	)

	nodeListOpts := metav1.ListOptions{}
	if len(nodeSelector) > 0 {
		nodeListOpts.LabelSelector = labels.Set(nodeSelector).String()
	}
	nodeList, err := c.clientset.CoreV1().Nodes().List(ctx, nodeListOpts)
	if err != nil {
		if kerrors.IsForbidden(err) {
			c.logger.Warn("Insufficient RBAC permissions to list nodes, skipping resource check — apply arc-controller-clusterrole.yaml to enable it")
			return math.MaxInt, nil
		}
		return 0, fmt.Errorf("list nodes: %w", err)
	}
	targetNodes := nodeList.Items
	targetNodeNames := make(map[string]struct{}, len(targetNodes))
	nodeNames := make([]string, 0, len(targetNodes))
	for _, n := range targetNodes {
		targetNodeNames[n.Name] = struct{}{}
		nodeNames = append(nodeNames, n.Name)
	}
	c.logger.Info("Target nodes for resource check", "count", len(targetNodes), "nodes", nodeNames, "nodeSelector", nodeSelector)

	clusterAllocatable := sumResourceLists(targetNodes)
	logResourceMap(c.logger, "Cluster allocatable", clusterAllocatable)

	podList, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		if kerrors.IsForbidden(err) {
			c.logger.Warn("Insufficient RBAC permissions to list pods, skipping resource check — apply arc-controller-clusterrole.yaml to enable it")
			return math.MaxInt, nil
		}
		return 0, fmt.Errorf("list pods: %w", err)
	}
	usedResources := sumPodRequests(podList.Items, targetNodeNames)
	logResourceMap(c.logger, "Used resources", usedResources)

	available := subtractResources(clusterAllocatable, usedResources)
	logResourceMap(c.logger, "Available resources", available)

	currentRunnerCount := ers.Status.CurrentReplicas
	c.logger.Info("Current runner pods on target nodes", "count", currentRunnerCount)

	c.logger.Info("Job resource requirements per runner", "requestsPerRunner", resourceMapToStrings(jobRequests))

	capacity := math.MaxInt
	for resName, req := range jobRequests {
		avail := available[resName]
		feasibleAdditional := divideQuantity(avail, req)
		// currentRunnerCount (from ERS status) is the authoritative count of runners
		// of this specific type. Each runner consumes exactly req of this resource,
		// and available already has their usage subtracted, so adding currentRunnerCount
		// back gives the true total feasible count.
		totalFeasible := currentRunnerCount + feasibleAdditional
		c.logger.Info("Resource check",
			"resource", resName,
			"available", avail.String(),
			"perRunner", req.String(),
			"currentRunners", currentRunnerCount,
			"feasibleAdditional", feasibleAdditional,
			"totalFeasible", totalFeasible,
		)
		if totalFeasible < capacity {
			capacity = totalFeasible
		}
	}

	c.logger.Info("Capacity calculated", "capacity", capacity)

	// Apply Volcano Queue constraint if the runner pod template carries a queue annotation.
	queueCapacity, err := c.volcanoQueueCapacity(ctx, ers, jobRequests, currentRunnerCount)
	if err != nil {
		c.logger.Warn("Volcano queue check failed, skipping queue constraint", "error", err)
	} else if queueCapacity < capacity {
		c.logger.Info("Volcano queue limits capacity", "queueCapacity", queueCapacity, "nodeCapacity", capacity)
		capacity = queueCapacity
	}

	c.logger.Info("Final capacity after queue check", "capacity", capacity)
	return capacity, nil
}

// volcanoQueueCapacity reads the Volcano Queue referenced by the runner pod
// template annotation "scheduling.volcano.sh/queue-name" and returns the
// capacity constrained by the queue's capability.
//
// The remaining headroom is queue.spec.capability - queue.status.allocated,
// where status.allocated is maintained by the Volcano scheduler and covers
// all pods bound to nodes across every namespace that share the same queue.
//
// If Volcano is not installed or the ERS carries no queue annotation the
// check is skipped (returns math.MaxInt).
func (c *KubernetesResourceChecker) volcanoQueueCapacity(
	ctx context.Context,
	ers *v1alpha1.EphemeralRunnerSet,
	jobRequests corev1.ResourceList,
	currentRunnerCount int,
) (int, error) {
	if c.volcanoClient == nil {
		return math.MaxInt, nil
	}

	queueName, ok := ers.Spec.EphemeralRunnerSpec.ObjectMeta.Annotations[schedulingv1beta1.QueueNameAnnotationKey]
	if !ok || queueName == "" {
		c.logger.Info("No Volcano queue annotation on runner pod template, skipping queue check")
		return math.MaxInt, nil
	}

	queue, err := c.volcanoClient.SchedulingV1beta1().Queues().Get(ctx, queueName, metav1.GetOptions{})
	if err != nil {
		if kerrors.IsNotFound(err) {
			c.logger.Warn("Volcano queue not found, skipping queue check", "queue", queueName)
			return math.MaxInt, nil
		}
		if kerrors.IsForbidden(err) {
			c.logger.Warn("Insufficient RBAC permissions to get Volcano queue, skipping queue check", "queue", queueName)
			return math.MaxInt, nil
		}
		return 0, fmt.Errorf("get Volcano queue %q: %w", queueName, err)
	}

	capacity := math.MaxInt
	for resName, req := range jobRequests {
		cap, hasCap := queue.Spec.Capability[corev1.ResourceName(resName)]
		if !hasCap {
			continue
		}
		remaining := cap.DeepCopy()
		if allocated, ok := queue.Status.Allocated[corev1.ResourceName(resName)]; ok {
			remaining.Sub(allocated)
		}
		feasibleAdditional := divideQuantity(remaining, req)
		totalFeasible := currentRunnerCount + feasibleAdditional
		allocatedQty := queue.Status.Allocated[corev1.ResourceName(resName)]
		c.logger.Info("Volcano queue check",
			"queue", queueName,
			"resource", resName,
			"capability", cap.String(),
			"allocated", allocatedQty.String(),
			"remaining", remaining.String(),
			"feasibleAdditional", feasibleAdditional,
			"totalFeasible", totalFeasible,
		)
		if totalFeasible < capacity {
			capacity = totalFeasible
		}
	}
	return capacity, nil
}

func logResourceMap(logger *slog.Logger, label string, m map[corev1.ResourceName]resource.Quantity) {
	args := []any{"label", label}
	for k, v := range m {
		args = append(args, string(k), v.String())
	}
	logger.Info("Resource summary", args...)
}

func resourceMapToStrings(m corev1.ResourceList) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[string(k)] = v.String()
	}
	return out
}

func sumResourceLists(nodes []corev1.Node) map[corev1.ResourceName]resource.Quantity {
	total := make(map[corev1.ResourceName]resource.Quantity)
	for _, n := range nodes {
		for res, qty := range n.Status.Allocatable {
			sum := total[res].DeepCopy()
			sum.Add(qty)
			total[res] = sum
		}
	}
	return total
}

func sumPodRequests(pods []corev1.Pod, targetNodes map[string]struct{}) map[corev1.ResourceName]resource.Quantity {
	total := make(map[corev1.ResourceName]resource.Quantity)
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodPending {
			continue
		}
		// Only count pods bound to a target node. Unbound Pending pods are
		// excluded because we cannot reliably determine which node pool they
		// will land on; counting them cluster-wide would produce false
		// negatives for runners targeting a specific pool. This is a
		// deliberately conservative choice: we may allow scheduling when an
		// unbound pod will eventually consume target resources, but we avoid
		// blocking scale-up due to pods destined for other pools.
		_, onTarget := targetNodes[pod.Spec.NodeName]
		if !onTarget {
			continue
		}
		for _, c := range pod.Spec.Containers {
			for res, qty := range c.Resources.Requests {
				sum := total[res].DeepCopy()
				sum.Add(qty)
				total[res] = sum
			}
		}
	}
	return total
}

func subtractResources(
	allocatable, used map[corev1.ResourceName]resource.Quantity,
) map[corev1.ResourceName]resource.Quantity {
	result := make(map[corev1.ResourceName]resource.Quantity, len(allocatable))
	for res, qty := range allocatable {
		avail := qty.DeepCopy()
		if u, ok := used[res]; ok {
			avail.Sub(u)
		}
		result[res] = avail
	}
	return result
}

// divideQuantity returns floor(a / b). Returns 0 if b is zero or a is negative.
func divideQuantity(a, b resource.Quantity) int {
	bVal := b.Value()
	if bVal <= 0 {
		return 0
	}
	aVal := a.Value()
	if aVal <= 0 {
		return 0
	}
	return int(aVal / bVal)
}

// mergeSelectors returns a new map that is the union of a and b.
// Keys in b override keys in a on conflict.
func mergeSelectors(a, b map[string]string) map[string]string {
	if len(a) == 0 && len(b) == 0 {
		return nil
	}
	merged := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		merged[k] = v
	}
	for k, v := range b {
		merged[k] = v
	}
	return merged
}
