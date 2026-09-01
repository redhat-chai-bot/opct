package run

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redhat-openshift-ecosystem/opct/pkg"
	kcorev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestValidateClusterAge(t *testing.T) {
	now := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		creationTime   time.Time
		clientError    error
		expectErrors   bool
		errorSubstring string
	}{
		{
			name:         "fresh cluster",
			creationTime: now.Add(-time.Hour),
		},
		{
			name:         "older than twelve hours warns and continues",
			creationTime: now.Add(-clusterAgeWarningThreshold - time.Second),
		},
		{
			name:         "exactly twelve hours is allowed",
			creationTime: now.Add(-clusterAgeWarningThreshold),
		},
		{
			name:         "exactly twenty-four hours is allowed",
			creationTime: now.Add(-clusterAgeBlockingThreshold),
		},
		{
			name:           "older than twenty-four hours is blocked",
			creationTime:   now.Add(-clusterAgeBlockingThreshold - time.Second),
			expectErrors:   true,
			errorSubstring: "exceeds the 24-hour limit",
		},
		{
			name:           "missing install manifests ConfigMap is blocked",
			expectErrors:   true,
			errorSubstring: "error getting install manifests ConfigMap",
		},
		{
			name:           "ConfigMap client error is blocked",
			creationTime:   now.Add(-time.Hour),
			clientError:    errors.New("access denied"),
			expectErrors:   true,
			errorSubstring: "access denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objects := []runtime.Object{}
			if !tt.creationTime.IsZero() {
				objects = append(objects, &kcorev1.ConfigMap{
					ObjectMeta: metav1.ObjectMeta{
						Name:              installManifestsConfigMapName,
						Namespace:         installManifestsConfigMapNamespace,
						CreationTimestamp: metav1.NewTime(tt.creationTime),
					},
				})
			}

			kclient := fake.NewSimpleClientset(objects...)
			if tt.clientError != nil {
				kclient.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tt.clientError
				})
			}

			errs := validateClusterAge(kclient.CoreV1(), now)
			if tt.expectErrors != (len(errs) > 0) {
				t.Fatalf("validateClusterAge() errors = %v, expectErrors = %t", errs, tt.expectErrors)
			}
			if tt.errorSubstring != "" && !strings.Contains(errs[0].Error(), tt.errorSubstring) {
				t.Errorf("validateClusterAge() error = %q, want substring %q", errs[0], tt.errorSubstring)
			}
		})
	}
}

func TestNormalizeTimeToClusterTimezone(t *testing.T) {
	clusterLocation := time.FixedZone("cluster", -7*60*60)
	now := time.Date(2026, time.September, 2, 6, 0, 0, 0, time.UTC)
	clusterTime := time.Date(2026, time.September, 1, 23, 0, 0, 0, clusterLocation)

	normalized := normalizeTimeToClusterTimezone(now, clusterTime)
	if normalized.Location() != clusterLocation {
		t.Errorf("normalizeTimeToClusterTimezone() location = %s, want %s", normalized.Location(), clusterLocation)
	}
	if !normalized.Equal(now) {
		t.Errorf("normalizeTimeToClusterTimezone() = %s, want instant %s", normalized, now)
	}
}

// TestValidateOpctNamespace tests the validateOpctNamespace function
func TestValidateOpctNamespace(t *testing.T) {
	tests := []struct {
		name         string
		existingPods []kcorev1.Namespace
		expectErrors bool
		errorCount   int
	}{
		{
			name:         "namespace does not exist",
			existingPods: []kcorev1.Namespace{},
			expectErrors: false,
			errorCount:   0,
		},
		{
			name: "namespace already exists",
			existingPods: []kcorev1.Namespace{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: pkg.CertificationNamespace,
					},
				},
			},
			expectErrors: true,
			errorCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake clientset with existing namespaces
			objects := make([]runtime.Object, len(tt.existingPods))
			for i := range tt.existingPods {
				objects[i] = &tt.existingPods[i]
			}

			kclient := fake.NewSimpleClientset(objects...)
			opts := newRunOptions()

			errs := validateOpctNamespace(opts, kclient.CoreV1())

			if tt.expectErrors && len(errs) == 0 {
				t.Errorf("validateOpctNamespace() expected errors but got none")
			}

			if !tt.expectErrors && len(errs) > 0 {
				t.Errorf("validateOpctNamespace() unexpected errors: %v", errs)
			}

			if tt.errorCount > 0 && len(errs) != tt.errorCount {
				t.Errorf("validateOpctNamespace() expected %d errors, got %d", tt.errorCount, len(errs))
			}
		})
	}
}

// TestValidateDedicatedNode tests the validateDedicatedNode function
func TestValidateDedicatedNode(t *testing.T) {
	tests := []struct {
		name          string
		dedicated     bool
		existingNodes []kcorev1.Node
		expectErrors  bool
		errorMessage  string
	}{
		{
			name:          "dedicated mode disabled",
			dedicated:     false,
			existingNodes: []kcorev1.Node{},
			expectErrors:  false,
		},
		{
			name:      "dedicated mode enabled - no nodes with label",
			dedicated: true,
			existingNodes: []kcorev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "worker-1",
						Labels: map[string]string{
							"node-role.kubernetes.io/worker": "",
						},
					},
				},
			},
			expectErrors: true,
			errorMessage: "missing dedicated node",
		},
		{
			name:      "dedicated mode enabled - node with label but no taint",
			dedicated: true,
			existingNodes: []kcorev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node",
						Labels: map[string]string{
							pkg.DedicatedNodeRoleLabel: "",
						},
					},
					Spec: kcorev1.NodeSpec{
						Taints: []kcorev1.Taint{},
					},
				},
			},
			expectErrors: true,
			errorMessage: "missing taint",
		},
		{
			name:      "dedicated mode enabled - valid node with label and taint",
			dedicated: true,
			existingNodes: []kcorev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node",
						Labels: map[string]string{
							pkg.DedicatedNodeRoleLabel: "",
						},
					},
					Spec: kcorev1.NodeSpec{
						Taints: []kcorev1.Taint{
							{
								Key:    pkg.DedicatedNodeRoleLabel,
								Effect: kcorev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			expectErrors: false,
		},
		{
			name:      "dedicated mode enabled - too many nodes with label",
			dedicated: true,
			existingNodes: []kcorev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node-1",
						Labels: map[string]string{
							pkg.DedicatedNodeRoleLabel: "",
						},
					},
					Spec: kcorev1.NodeSpec{
						Taints: []kcorev1.Taint{
							{
								Key:    pkg.DedicatedNodeRoleLabel,
								Effect: kcorev1.TaintEffectNoSchedule,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node-2",
						Labels: map[string]string{
							pkg.DedicatedNodeRoleLabel: "",
						},
					},
					Spec: kcorev1.NodeSpec{
						Taints: []kcorev1.Taint{
							{
								Key:    pkg.DedicatedNodeRoleLabel,
								Effect: kcorev1.TaintEffectNoSchedule,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node-3",
						Labels: map[string]string{
							pkg.DedicatedNodeRoleLabel: "",
						},
					},
					Spec: kcorev1.NodeSpec{
						Taints: []kcorev1.Taint{
							{
								Key:    pkg.DedicatedNodeRoleLabel,
								Effect: kcorev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
			expectErrors: true,
			errorMessage: "too many nodes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create fake clientset with existing nodes
			objects := make([]runtime.Object, len(tt.existingNodes))
			for i := range tt.existingNodes {
				objects[i] = &tt.existingNodes[i]
			}

			kclient := fake.NewSimpleClientset(objects...)
			opts := newRunOptions()
			opts.dedicated = tt.dedicated

			errs := validateDedicatedNode(opts, kclient.CoreV1())

			if tt.expectErrors && len(errs) == 0 {
				t.Errorf("validateDedicatedNode() expected errors but got none")
			}

			if !tt.expectErrors && len(errs) > 0 {
				t.Errorf("validateDedicatedNode() unexpected errors: %v", errs)
			}

			if tt.errorMessage != "" && len(errs) > 0 {
				found := false
				for _, err := range errs {
					if err != nil && len(err.Error()) > 0 {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("validateDedicatedNode() expected error containing %q, got %v", tt.errorMessage, errs)
				}
			}
		})
	}
}
