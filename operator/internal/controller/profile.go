package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type profileConfig struct {
	JVMHeap               string
	QueryMaxMemory        string
	QueryMaxMemoryPerNode string
}

var builtinProfiles = map[string]profileConfig{
	"default": {"4G", "4GB", "1GB"},
	"medium":  {"8G", "8GB", "2GB"},
	"large":   {"16G", "16GB", "4GB"},
}

// lookupProfile reads ConfigMap meridian-profile-{name} from namespace.
// Falls back to built-in tiers; falls back to "default" if name is unknown.
func lookupProfile(ctx context.Context, c client.Client, namespace, name string) profileConfig {
	cm := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("meridian-profile-%s", name),
		Namespace: namespace,
	}, cm)
	if err == nil {
		p := builtinProfiles["default"]
		if v := cm.Data["jvmHeap"]; v != "" {
			p.JVMHeap = v
		}
		if v := cm.Data["queryMaxMemory"]; v != "" {
			p.QueryMaxMemory = v
		}
		if v := cm.Data["queryMaxMemoryPerNode"]; v != "" {
			p.QueryMaxMemoryPerNode = v
		}
		return p
	}
	if !apierrors.IsNotFound(err) {
		// non-fatal lookup error — fall through to built-in
	}
	if p, ok := builtinProfiles[name]; ok {
		return p
	}
	return builtinProfiles["default"]
}

func coordinatorConfigProperties(clusterName, namespace string, p profileConfig) string {
	return fmt.Sprintf(`coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
discovery-server.enabled=true
discovery.uri=http://localhost:8080
query.max-memory=%s
query.max-memory-per-node=%s
catalog.management=dynamic
`, p.QueryMaxMemory, p.QueryMaxMemoryPerNode)
}

func workerConfigProperties(clusterName, namespace string, p profileConfig) string {
	return fmt.Sprintf(`coordinator=false
http-server.http.port=8080
discovery.uri=http://%s-coordinator.%s.svc.cluster.local:8080
query.max-memory=%s
query.max-memory-per-node=%s
catalog.management=dynamic
`, clusterName, namespace, p.QueryMaxMemory, p.QueryMaxMemoryPerNode)
}

func jvmConfig(p profileConfig) string {
	return fmt.Sprintf(`-server
-Xmx%s
-XX:InitialRAMPercentage=80
-XX:MaxRAMPercentage=80
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-XX:-OmitStackTraceInFastThrow
-XX:ReservedCodeCacheSize=512M
-XX:PerMethodRecompilationCutoff=10000
-XX:PerBytecodeRecompilationCutoff=10000
-Djdk.attach.allowAttachSelf=true
-Djdk.nio.maxCachedBufferSize=2000000
-Dfile.encoding=UTF-8
`, p.JVMHeap)
}

const logProperties = "io.trino=INFO\n"
