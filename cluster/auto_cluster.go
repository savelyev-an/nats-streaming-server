package cluster

import (
	"fmt"
	"os"
)

type ConfiguredCluster struct {
	StanCluster
	cfg       ClusterConfig
	autoClean bool
}

func (c *ConfiguredCluster) Shutdown() {
	c.StanCluster.Shutdown()
	if c.autoClean {
		os.RemoveAll(c.cfg.DataPath)
	}
}

func NewConfiguredCluster(cfg ClusterConfig, autoClean bool) (*ConfiguredCluster, error) {
	files, err := cfg.GenerateConfigFiles()
	if err != nil {
		return nil, fmt.Errorf("generating configs: %w", err)
	}
	cl := NewCluster(files)
	return &ConfiguredCluster{
		StanCluster: cl,
		cfg:         cfg,
		autoClean:   autoClean,
	}, nil
}
