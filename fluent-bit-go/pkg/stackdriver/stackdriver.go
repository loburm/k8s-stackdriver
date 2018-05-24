package main

import (
	"C"
	"fmt"
	"github.com/fluent/fluent-bit-go/output"
	"unsafe"

	"golang.org/x/net/context"

	"cloud.google.com/go/logging"
	gce "cloud.google.com/go/compute/metadata"
	mrpb "google.golang.org/genproto/googleapis/api/monitoredres"
	"reflect"
	"strings"
)

var client *logging.Client
var projectID string
var instanceID string
var clusterName string
var zone string
var logger *logging.Logger

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "stackdriver", "Output plugin for Stackdriver")
}

//export FLBPluginInit
// (fluevintbit will call this)
// ctx (context) pointer to fluentbit context (state/ c code)
func FLBPluginInit(ctx unsafe.Pointer) int {
	var err error
	projectID, err = gce.ProjectID()
	if err != nil {
		fmt.Printf("error while getting project id: %v\n", err)
		return output.FLB_ERROR
	}
	instanceID, err = gce.InstanceID()
	if err != nil {
		fmt.Printf("error while getting instance id: %v\n", err)
		return output.FLB_ERROR
	}
	clusterName, err = gce.InstanceAttributeValue("cluster-name")
	if err != nil {
		fmt.Printf("error while getting cluster name: %v\n", err)
		return output.FLB_ERROR
	}
	clusterName = strings.TrimSpace(clusterName)
	zone, err = gce.Zone()
	if err != nil {
		fmt.Printf("error while getting cluster zone: %v\n", err)
		return output.FLB_ERROR
  }

	client, err = logging.NewClient(context.Background(), projectID)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return output.FLB_ERROR
	}
	fmt.Printf("Stackdriver plugin is configured for project: %s, cluster-name: %s, instance: %s\n", projectID, clusterName, instanceID)
	logger = client.Logger("k8s")
	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {

	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	var containerName string
	var podId string
	var namespaceId string

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	for {
		// Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}
		timestamp := ts.(output.FLBTime)
		var payload interface{}
		var monRes *mrpb.MonitoredResource

		// Search for kubernetes related information and log payload.
		for k, v := range record {
			if k == "kubernetes" && monRes == nil {
				asMap, ok := v.(map[interface{}]interface{})
				if !ok {
					fmt.Printf("was not able to convert kubernetes entry to map\n")
					return output.FLB_ERROR
				}
				for k, v := range asMap {
					switch k {
					case "container_name":
						containerName = convertToString(v)
					case "pod_name":
						podId = convertToString(v)
					case "namespace_name":
						namespaceId = convertToString(v)
					}
				}
				monRes = &mrpb.MonitoredResource{
					Type: "container",
					Labels: map[string]string{
						"project_id":     projectID,
						"cluster_name":   clusterName,
						"namespace_id":   namespaceId,
						"instance_id":    instanceID,
						"pod_id":         podId,
						"container_name": containerName,
						"zone":           zone,
					},
				}
			}	else if k == "log" {
				payload = convertToString(v)
			}
		}

		if payload == nil {
			payload = convertToStringMap(record)
		}
		entry := logging.Entry{
			Timestamp: timestamp.Time,
			Resource:  monRes,
			Payload:   payload,
		}
		logger.Log(entry)
	}
	err := logger.Flush()
	if err != nil {
		fmt.Printf("Error during log sending: %v\n", err)
		return output.FLB_ERROR
	}
	// Return options:
	//
	// output.FLB_OK    = data have been processed.
	// output.FLB_ERROR = unrecoverable error, do not try this again.
	// output.FLB_RETRY = retry to flush later.
	return output.FLB_OK
}

func convertToStringMap(record map[interface{}]interface{}) map[string]interface{} {
	res := make(map[string]interface{})
	for k,v := range record {
		res[convertToString(k)] = convertToString(v)
	}
	return res
}

func convertToString(i interface{}) string {
	switch reflect.TypeOf(i) {
	case reflect.TypeOf(([]uint8)(nil)):
		return B2S(i.([]uint8))
	case reflect.TypeOf((string)("")):
		return i.(string)
	default:
		return fmt.Sprintf("Unknown type: %s\n", reflect.TypeOf(i))
	}
}

func B2S(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}

//export FLBPluginExit
func FLBPluginExit() int {
	if err := client.Close(); err != nil {
		fmt.Printf("Failed to close client: %v\n", err)
	}
	return output.FLB_OK
}

func main() {
}
