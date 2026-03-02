// +groupName=meridian.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPhase represents the lifecycle phase of a Trino cluster.
type ClusterPhase string

const (
	ClusterPhaseEmpty    ClusterPhase = ""
	ClusterPhasePending  ClusterPhase = "Pending"
	ClusterPhaseIdle     ClusterPhase = "Idle"
	ClusterPhaseReserved  ClusterPhase = "Reserved"
	ClusterPhaseFailed    ClusterPhase = "Failed"
	ClusterPhaseDegraded  ClusterPhase = "Degraded"
)

// ClusterSpec defines the desired state of a Cluster.
type ClusterSpec struct {
	// Profile is the configuration profile used to create this cluster.
	Profile string `json:"profile"`

	// Image is the Trino container image to use.
	Image string `json:"image"`

	// Workers is the number of Trino worker nodes.
	// +kubebuilder:default=2
	Workers int32 `json:"workers,omitempty"`

	// ClientID is set by the REST API when reserving this cluster.
	// +optional
	ClientID string `json:"clientId,omitempty"`

	// ReservationID is a unique ID for the reservation, used for idempotency.
	// +optional
	ReservationID string `json:"reservationId,omitempty"`
}

// ClusterStatus defines the observed state of a Cluster.
type ClusterStatus struct {
	// Phase is the current lifecycle phase of the cluster.
	Phase ClusterPhase `json:"phase,omitempty"`

	// CoordinatorURL is the connection URL once the cluster is ready.
	// +optional
	CoordinatorURL string `json:"coordinatorUrl,omitempty"`

	// Ready indicates whether the cluster passed its health check.
	Ready bool `json:"ready,omitempty"`

	// ReservedAt is when the cluster was last reserved.
	// +optional
	ReservedAt *metav1.Time `json:"reservedAt,omitempty"`

	// IdleAt is when the cluster last became idle.
	// +optional
	IdleAt *metav1.Time `json:"idleAt,omitempty"`

	// LastRotatedAt is when the most recent successful credential rotation completed.
	// +optional
	LastRotatedAt *metav1.Time `json:"lastRotatedAt,omitempty"`

	// RotationFailures is the count of consecutive credential rotation failures.
	// Used to compute exponential backoff. Reset to 0 on the next success.
	// +optional
	RotationFailures int32 `json:"rotationFailures,omitempty"`

	// RotatingCatalog is the catalog currently being rotated, if any.
	// Cleared after rotation completes (success or non-retriable failure).
	// +optional
	RotatingCatalog string `json:"rotatingCatalog,omitempty"`

	// Conditions holds standard Kubernetes conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Profile",type=string,JSONPath=`.spec.profile`
// +kubebuilder:printcolumn:name="ClientID",type=string,JSONPath=`.spec.clientId`
// +kubebuilder:printcolumn:name="Ready",type=boolean,JSONPath=`.status.ready`
// +kubebuilder:printcolumn:name="LastRotated",type=date,JSONPath=`.status.lastRotatedAt`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Cluster represents a single managed Trino cluster instance.
type Cluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterSpec   `json:"spec,omitempty"`
	Status ClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterList contains a list of Cluster.
type ClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Cluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Cluster{}, &ClusterList{})
}
