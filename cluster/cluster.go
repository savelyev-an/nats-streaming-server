package cluster

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	natsd "github.com/nats-io/nats-server/v2/server"

	stand "github.com/savelyev-an/nats-streaming-server/server"
)

type StanServer struct {
	*stand.StanServer
	nOtps      *natsd.Options
	sOpts      *stand.Options
	configPath string
}

type StanCluster struct {
	serverIDs []raft.ServerID
	srvrs     map[raft.ServerID]*StanServer
}

func (c *StanCluster) Shutdown() {
	for _, srv := range c.srvrs {
		srv.Shutdown()
	}
}

func (c *StanCluster) RaftStepDown() error {
	s := c.srvrs[c.serverIDs[0]]
	srvrs := s.GetConfiguration()
	_, leaderID := s.RaftLeader()
	var newLeader raft.Server
	if leaderID != srvrs[0].ID {
		newLeader = srvrs[0]
	} else {
		newLeader = srvrs[1]
	}
	leaderSRV := c.srvrs[leaderID]
	if leaderSRV == nil {
		return fmt.Errorf("not found leader by id: %q", leaderID)
	}
	fmt.Printf("raft.step_down: oldLeader: %s, new pretendent: %v\n", leaderID, newLeader)
	return leaderSRV.RaftStepDownToServer(newLeader)
}

func (c *StanCluster) GetLeaderID() raft.ServerID {
	s := c.srvrs[c.serverIDs[0]]
	_, leaderID := s.RaftLeader()
	return leaderID
}

func (c *StanCluster) GetLeaderNatsUrl() string {
	leaderID := c.GetLeaderID()
	return fmt.Sprintf("nats://127.0.0.1:%d", c.srvrs[leaderID].nOtps.Port)
}

func NewCluster(configPaths []string) StanCluster {
	wg := sync.WaitGroup{}
	wg.Add(len(configPaths))
	c := StanCluster{
		srvrs: make(map[raft.ServerID]*StanServer),
	}
	mu := sync.Mutex{}
	for _, configPath := range configPaths {
		go func(path string) {
			srv, nOpts, sOpts, err := runStan(path)
			if err != nil {
				fmt.Println(err.Error())
			}
			mu.Lock()
			serverID := raft.ServerID(sOpts.Clustering.NodeID)
			c.srvrs[serverID] = &StanServer{
				StanServer: srv,
				nOtps:      nOpts,
				sOpts:      sOpts,
				configPath: path,
			}
			c.serverIDs = append(c.serverIDs, serverID)
			mu.Unlock()
			wg.Done()
		}(configPath)
	}
	wg.Wait()
	time.Sleep(time.Second)
	return c
}

func runStan(configPath string) (*stand.StanServer, *natsd.Options, *stand.Options, error) {
	fs := flag.NewFlagSet("fake", flag.ExitOnError)

	stanOpts, natsOpts, err := stand.ConfigureOptions(fs, []string{"-c", configPath, "--clustered"},
		func() {
			fmt.Printf("nats-streaming-server version %s, ", stand.VERSION)
			natsd.PrintServerAndExit()
		},
		fs.Usage,
		natsd.PrintTLSHelpAndDie)
	if err != nil {
		natsd.PrintAndDie(err.Error())
	}

	// Force the streaming server to setup its own signal handler
	stanOpts.HandleSignals = true
	// override the NoSigs for NATS since Streaming has its own signal handler
	natsOpts.NoSigs = true
	// Without this option set to true, the logger is not configured.
	stanOpts.EnableLogging = true
	// This will invoke RunServerWithOpts but on Windows, may run it as a service.
	srv, err := stand.Run(stanOpts, natsOpts)
	if err != nil {
		return nil, nil, nil, err
	}

	return srv, natsOpts, stanOpts, nil
}
