package cluster

import (
	"fmt"
	"os"
	"text/template"
)

type ClusterConfig struct {
	ClusterIO            string
	NodePrefix           string
	SD                   bool
	SV                   bool
	LogTime              bool
	DataPath             string
	NodesPorts           []uint16
	NodesMonitoringPorts []uint16
	NodesClusterPorts    []uint16
}

// GenerateConfigFiles генерирует и сохраняет конфиги для натсов, возвращает пути до файлов
func (c *ClusterConfig) GenerateConfigFiles() ([]string, error) {
	cfgPath := fmt.Sprintf("./%s/cfg", c.DataPath)
	err := os.MkdirAll(cfgPath, 0744)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(fmt.Sprintf("./%s/logs", c.DataPath), 0744)
	tpl, err := template.New("node-config").Parse(cfgTpl)
	if err != nil {
		return nil, err
	}
	files := []string{}
	type CurNodeConfig struct {
		ClusterConfig
		NodeChar           string
		NodePort           uint16
		NodeMonitoringPort uint16
		NodeClusterPort    uint16
		Bootstrap          bool
	}
	for i := 0; i < len(c.NodesPorts); i++ {
		nodeChar := string(rune('A' + i))
		cfg := CurNodeConfig{
			ClusterConfig:      *c,
			NodeChar:           nodeChar,
			NodePort:           c.NodesPorts[i],
			NodeMonitoringPort: c.NodesMonitoringPorts[i],
			NodeClusterPort:    c.NodesClusterPorts[i],
		}
		if i == 0 {
			cfg.Bootstrap = true
		}
		fileName := cfgPath + "/" + cfg.NodePrefix + "-" + cfg.NodeChar + ".conf"
		f, err := os.Create(fileName)
		if err != nil {
			return nil, fmt.Errorf("openning file: %v", err)
		}
		defer f.Close()
		err = tpl.Execute(f, cfg)
		if err != nil {
			return nil, fmt.Errorf("executing tpl: %v", err)
		}
		files = append(files, fileName)
	}
	return files, nil
}

var cfgTpl = `# Cluster Server {{.NodeChar}}

listen: 0.0.0.0:{{.NodePort}}
monitor_port: {{.NodeMonitoringPort}}

// логи
sd: {{.SD}}
sv: {{.SV}}
logtime: {{.LogTime}}
logfile_size_limit: 100MB
logfile_max_num: 100
log_file: "{{.DataPath}}/logs/nats-{{.NodePrefix}}-{{.NodeChar}}.log"

authorization {
  users = [
    {user: ruser, password: T0pS3cr3t, permissions: { publish = ">", subscribe = ">" }}
  ]
}

cluster {
  listen: 0.0.0.0:{{.NodeClusterPort}}
  name: {{.ClusterIO}}
    routes: [ 
	{{ range .NodesClusterPorts }}nats://localhost:{{.}}
	{{ end }}]
}

streaming: {
  cluster_id: {{.ClusterIO}}
  store: "file"
  dir: "{{.DataPath}}/data/{{.NodePrefix}}-{{.NodeChar}}"
  cluster: {
	bootstrap: {{.Bootstrap}}
	node_id: {{.NodePrefix}}-{{.NodeChar}}
    raft_logging: true
	log_path: "{{.DataPath}}/raft/{{.NodePrefix}}-{{.NodeChar}}"
    raft_heartbeat_timeout: "1s"
    raft_election_timeout: "5s"
    raft_lease_timeout: "500ms"
    raft_commit_timeout: "50ms"
  }
  sd: {{.SD}}
  sv: {{.SV}}
  hb_interval: "2s"
  hb_timeout: "5s"
  hb_fail_count: 1
    user: ruser
    password: T0pS3cr3t
}`
