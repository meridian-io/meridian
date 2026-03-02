package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ClusterPoolAutoscalerSpec defines the autoscaling policy for a ClusterPool.
type ClusterPoolAutoscalerSpec struct {
	// TargetRef references the ClusterPool to scale.
	TargetRef TargetRef `json:"targetRef"`

	// MinReplicas is the minimum pool size the autoscaler will scale down to.
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	MinReplicas int32 `json:"minReplicas"`

	// MaxReplicas is the maximum pool size the autoscaler will scale up to.
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`

	// UtilizationThreshold is the ratio of reserved/total above which the pool scales up.
	// +kubebuilder:default="0.70"
	UtilizationThreshold string `json:"utilizationThreshold,omitempty"`
}

// TargetRef identifies the ClusterPool to autoscale.
type TargetRef struct {
	Name string `json:"name"`
}

// ClusterPoolAutoscalerStatus reports the autoscaler's observed state.
type ClusterPoolAutoscalerStatus struct {
	// CurrentReplicas is the current ClusterPool replica count.
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// DesiredReplicas is what the autoscaler last computed.
	DesiredReplicas int32 `json:"desiredReplicas,omitempty"`

	// CurrentUtilization is the last measured utilization ratio.
	CurrentUtilization string `json:"currentUtilization,omitempty"`

	// LastScaleTime is when the autoscaler last changed the replica count.
	// +optional
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// Conditions holds standard Kubernetes conditions.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetRef.name`
// +kubebuilder:printcolumn:name="Min",type=integer,JSONPath=`.spec.minReplicas`
// +kubebuilder:printcolumn:name="Max",type=integer,JSONPath=`.spec.maxReplicas`
// +kubebuilder:printcolumn:name="Current",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Utilization",type=string,JSONPath=`.status.currentUtilization`

// ClusterPoolAutoscaler automatically adjusts a ClusterPool's replica count
// based on the ratio of reserved to total clusters.
type ClusterPoolAutoscaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ClusterPoolAutoscalerSpec   `json:"spec,omitempty"`
	Status ClusterPoolAutoscalerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ClusterPoolAutoscalerList contains a list of ClusterPoolAutoscaler.
type ClusterPoolAutoscalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterPoolAutoscaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterPoolAutoscaler{}, &ClusterPoolAutoscalerList{})
}
