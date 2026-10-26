package controller

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// podResources is the CPU request and memory request/limit for one Trino role.
type podResources struct {
	Memory resource.Quantity
	CPU    resource.Quantity
}

type profileConfig struct {
	JVMHeap               string
	QueryMaxMemory        string
	QueryMaxMemoryPerNode string
	Coordinator           podResources
	Worker                podResources
	// SampleCatalogs are read-only demo catalogs (tpch, tpcds) that ship
	// with Trino, written into each pod's catalog directory at startup.
	SampleCatalogs []string
}

// sampleConnectors are the generated-data connectors bundled with Trino.
// They need no credentials or external systems and are read-only.
var sampleConnectors = map[string]bool{"tpch": true, "tpcds": true}

// Built-in tiers. Memory is ~125% of the JVM heap, leaving room for
// off-heap memory (code cache, thread stacks, direct buffers) so the pod is
// not OOM-killed at full heap.
var builtinProfiles = map[string]profileConfig{
	// default includes the tpch sample catalog so a first query works.
	"default": withSampleCatalogs(newProfile("4G", "4GB", "1GB", "5Gi", "1"), "tpch"),
	"medium":  newProfile("8G", "8GB", "2GB", "10Gi", "2"),
	"large":   newProfile("16G", "16GB", "4GB", "20Gi", "4"),
}

func withSampleCatalogs(p profileConfig, names ...string) profileConfig {
	p.SampleCatalogs = names
	return p
}

func newProfile(heap, maxMem, maxMemPerNode, memory, cpu string) profileConfig {
	r := podResources{Memory: resource.MustParse(memory), CPU: resource.MustParse(cpu)}
	return profileConfig{
		JVMHeap:               heap,
		QueryMaxMemory:        maxMem,
		QueryMaxMemoryPerNode: maxMemPerNode,
		Coordinator:           r,
		Worker:                r,
	}
}

// lookupProfile reads ConfigMap meridian-profile-{name} from namespace.
// Falls back to built-in tiers; falls back to "default" if name is unknown.
//
// Custom profile keys (all optional):
//
//	jvmHeap, queryMaxMemory, queryMaxMemoryPerNode   Trino settings
//	memory, cpu                                      pod resources for both roles
//	coordinatorMemory, coordinatorCPU                coordinator-only override
//	workerMemory, workerCPU                          worker-only override
//	sampleCatalogs                                   comma-separated: tpch, tpcds; "" for none
//
// If jvmHeap is set but memory is not, memory is derived as 125% of the heap.
// Invalid quantities are logged and ignored.
func lookupProfile(ctx context.Context, c client.Client, namespace, name string) profileConfig {
	logger := log.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	err := c.Get(ctx, types.NamespacedName{
		Name:      fmt.Sprintf("meridian-profile-%s", name),
		Namespace: namespace,
	}, cm)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logger.Error(err, "failed to read profile ConfigMap, using built-in profile", "profile", name)
		}
		if p, ok := builtinProfiles[name]; ok {
			return p
		}
		return builtinProfiles["default"]
	}

	// A ConfigMap overrides a built-in tier of the same name (e.g.
	// meridian-profile-large starts from "large"), otherwise "default".
	p, ok := builtinProfiles[name]
	if !ok {
		p = builtinProfiles["default"]
	}
	if v := cm.Data["jvmHeap"]; v != "" {
		p.JVMHeap = v
		if mem, ok := memoryForHeap(v); ok {
			p.Coordinator.Memory, p.Worker.Memory = mem, mem
		} else {
			logger.Info("could not parse jvmHeap to size pod memory; using default memory", "profile", name, "jvmHeap", v)
		}
	}
	if v := cm.Data["queryMaxMemory"]; v != "" {
		p.QueryMaxMemory = v
	}
	if v := cm.Data["queryMaxMemoryPerNode"]; v != "" {
		p.QueryMaxMemoryPerNode = v
	}

	set := func(key string, targets ...*resource.Quantity) {
		v := cm.Data[key]
		if v == "" {
			return
		}
		q, err := resource.ParseQuantity(v)
		if err != nil || q.Sign() <= 0 {
			logger.Info("ignoring invalid resource quantity in profile", "profile", name, "key", key, "value", v)
			return
		}
		for _, t := range targets {
			*t = q
		}
	}
	// Present but empty ("") turns sample catalogs off.
	if v, ok := cm.Data["sampleCatalogs"]; ok {
		p.SampleCatalogs = nil
		for _, catalog := range strings.Split(v, ",") {
			catalog = strings.TrimSpace(catalog)
			switch {
			case catalog == "":
			case sampleConnectors[catalog]:
				p.SampleCatalogs = append(p.SampleCatalogs, catalog)
			default:
				logger.Info("ignoring unknown sample catalog in profile; use tpch or tpcds", "profile", name, "catalog", catalog)
			}
		}
	}

	set("memory", &p.Coordinator.Memory, &p.Worker.Memory)
	set("cpu", &p.Coordinator.CPU, &p.Worker.CPU)
	set("coordinatorMemory", &p.Coordinator.Memory)
	set("coordinatorCPU", &p.Coordinator.CPU)
	set("workerMemory", &p.Worker.Memory)
	set("workerCPU", &p.Worker.CPU)

	if heap, ok := parseJavaSize(p.JVMHeap); ok {
		for role, m := range map[string]resource.Quantity{"coordinator": p.Coordinator.Memory, "worker": p.Worker.Memory} {
			if m.Value() <= heap {
				logger.Info("pod memory is not larger than the JVM heap; Trino will be OOM-killed under load",
					"profile", name, "role", role, "memory", m.String(), "jvmHeap", p.JVMHeap)
			}
		}
	}
	return p
}

var javaSizeRE = regexp.MustCompile(`^(\d+)([kKmMgGtT]?)$`)

// parseJavaSize parses a JVM size like "4G", "4096m" or "1073741824" into bytes.
func parseJavaSize(s string) (int64, bool) {
	m := javaSizeRE.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	shift := map[string]uint{"": 0, "k": 10, "m": 20, "g": 30, "t": 40}[strings.ToLower(m[2])]
	return n << shift, true
}

// memoryForHeap returns a pod memory size of 125% of the heap, rounded up to a whole Mi.
func memoryForHeap(heap string) (resource.Quantity, bool) {
	b, ok := parseJavaSize(heap)
	if !ok || b <= 0 {
		return resource.Quantity{}, false
	}
	mi := (b*5/4 + (1<<20 - 1)) >> 20
	return resource.MustParse(fmt.Sprintf("%dMi", mi)), true
}

// resourceRequirements turns a role's sizing into container resources.
// Memory request equals the limit (Guaranteed memory, no surprise OOM from
// overcommit). CPU gets a request but no limit: CPU limits throttle the JVM
// (GC and JIT threads) and hurt query latency.
func (r podResources) resourceRequirements() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    r.CPU,
			corev1.ResourceMemory: r.Memory,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: r.Memory,
		},
	}
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
