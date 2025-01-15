package main

import (
	"time"

	"github.com/nats-io/nats-streaming-server/cluster"
)

func main() {
	cfg := cluster.ClusterConfig{
		ClusterIO:            "no-nats-streaming",
		NodePrefix:           "node",
		SD:                   true,
		SV:                   true,
		DataPath:             "nats-cluster",
		NodesPorts:           []uint16{14222, 24222, 34222},
		NodesMonitoringPorts: []uint16{18222, 28222, 38222},
		NodesClusterPorts:    []uint16{16222, 26222, 36222},
	}
	cl, err := cluster.NewConfiguredCluster(cfg, true)

	if err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 20)
	cl.Shutdown()
}
