// +groupName=meridian.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPoolSpec defines the desired state of a ClusterPool.
type ClusterPoolSpec struct {
	// Replicas is the desired number of standby (idle) clusters in the pool.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	Replicas int32 `json:"replicas"`

	// Template defines the profile used when creating new clusters.
	Template ClusterTemplate `json:"template"`

	// ReservationTTL is the maximum duration a cluster may remain Reserved before
	// being automatically released back to the pool (e.g. after a client crash).
	// Accepts Go duration strings: "4h", "30m". Defaults to "4h".
	// +optional
	// +kubebuilder:default="4h"
	ReservationTTL string `json:"reservationTtl,omitempty"`

	// Workload tags all clusters in this pool with a workload type (e.g. "analytics", "etl").
	// Use as a routing key in reservations: reserve_cluster(workload="analytics").
	// +optional
	Workload string `json:"workload,omitempty"`

	// Gateway configures optional Trino Gateway integration. When set, clusters are
	// automatically registered on Idle and deregistered on deletion.
	// +optional
	Gateway *GatewayConfig `json:"gateway,omitempty"`
}

// GatewayConfig configures Trino Gateway integration for a ClusterPool.
type GatewayConfig struct {
	// Endpoint is the Trino Gateway base URL (e.g. "http://trino-gateway:8080").
	Endpoint string `json:"endpoint"`

	// RoutingGroup is the routing group clusters are registered under.
	// Defaults to the pool workload value, or "adhoc" if workload is also unset.
	// +optional
	RoutingGroup string `json:"routingGroup,omitempty"`
}

// ClusterTemplate describes how to create clusters in the pool.
type ClusterTemplate struct {
	// Profile is the named configuration profile for new clusters.
	Profile string `json:"profile"`

	// Image is the Trino container image.
	Image string `json:"image"`

	// Workers is the default number of workers per cluster.
	// +kubebuilder:default=2
	Workers int32 `json:"workers,omitempty"`
}

// ClusterPoolStatus defines the observed state of a ClusterPool.
type ClusterPoolStatus struct {
	// ReadyReplicas is the number of clusters currently in Idle phase.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// ReservedReplicas is the number of clusters currently Reserved.
	ReservedReplicas int32 `json:"reservedReplicas,omitempty"`

	// PendingReplicas is the number of clusters being created.
	PendingReplicas int32 `json:"pendingReplicas,omitempty"`

	// FailedReplicas is the number of clusters in Failed phase.
	FailedReplicas int32 `json:"failedReplicas,omitempty"`

	// Conditions holds standard Kubernetes conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Reserved",type=integer,JSONPath=`.status.reservedReplicas`
// +kubebuilder:printcolumn:name="Pending",type=integer,JSONPath=`.status.pendingReplicas`
// +kubebuilder:printcolumn:name="Workload",type=string,JSONPath=`.spec.workload`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ClusterPool manages a hot standby pool of Trino clusters.
type ClusterPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterPoolSpec   `json:"spec,omitempty"`
	Status ClusterPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPoolList contains a list of ClusterPool.
type ClusterPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterPool{}, &ClusterPoolList{})
}
