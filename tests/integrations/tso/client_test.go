// Copyright 2023 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tso

import (
	"context"
	"math"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/testutil"
	bs "github.com/tikv/pd/pkg/basicserver"
	mcsutils "github.com/tikv/pd/pkg/mcs/utils"
	"github.com/tikv/pd/pkg/storage/endpoint"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"github.com/tikv/pd/pkg/utils/tsoutil"
	"github.com/tikv/pd/server/apiv2/handlers"
	"github.com/tikv/pd/tests"
	"github.com/tikv/pd/tests/integrations/mcs"
	handlersutil "github.com/tikv/pd/tests/server/apiv2/handlers"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

type tsoClientTestSuite struct {
	suite.Suite
	legacy bool

	ctx    context.Context
	cancel context.CancelFunc
	// The PD cluster.
	cluster *tests.TestCluster
	// pdLeaderServer is the leader server of the PD cluster.
	pdLeaderServer *tests.TestServer
	// The TSO service in microservice mode.
	tsoCluster *mcs.TestTSOCluster

	keyspaceGroups []struct {
		keyspaceGroupID uint32
		keyspaceIDs     []uint32
	}

	backendEndpoints string
	keyspaceIDs      []uint32
	clients          []pd.Client
}

func TestLegacyTSOClient(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: true,
	})
}

func TestMicroserviceTSOClient(t *testing.T) {
	suite.Run(t, &tsoClientTestSuite{
		legacy: false,
	})
}

func (suite *tsoClientTestSuite) SetupSuite() {
	re := suite.Require()

	var err error
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	if suite.legacy {
		suite.cluster, err = tests.NewTestCluster(suite.ctx, serverCount)
	} else {
		suite.cluster, err = tests.NewTestAPICluster(suite.ctx, serverCount)
	}
	re.NoError(err)
	err = suite.cluster.RunInitialServers()
	re.NoError(err)
	leaderName := suite.cluster.WaitLeader()
	suite.pdLeaderServer = suite.cluster.GetServer(leaderName)
	re.NoError(suite.pdLeaderServer.BootstrapCluster())
	suite.backendEndpoints = suite.pdLeaderServer.GetAddr()
	suite.keyspaceIDs = make([]uint32, 0)

	if suite.legacy {
		client, err := pd.NewClientWithContext(suite.ctx, strings.Split(suite.backendEndpoints, ","), pd.SecurityOption{})
		re.NoError(err)
		suite.keyspaceIDs = append(suite.keyspaceIDs, 0)
		suite.clients = make([]pd.Client, 0)
		suite.clients = append(suite.clients, client)
	} else {
		suite.tsoCluster, err = mcs.NewTestTSOCluster(suite.ctx, 3, suite.backendEndpoints)
		re.NoError(err)

		suite.keyspaceGroups = []struct {
			keyspaceGroupID uint32
			keyspaceIDs     []uint32
		}{
			{0, []uint32{0, 10}},
			{1, []uint32{1, 11}},
			{2, []uint32{2}},
		}

		for _, param := range suite.keyspaceGroups {
			if param.keyspaceGroupID == 0 {
				// we have already created default keyspace group, so we can skip it.
				// keyspace 10 isn't assigned to any keyspace group, so they will be
				// served by default keyspace group.
				continue
			}
			handlersutil.MustCreateKeyspaceGroup(re, suite.pdLeaderServer, &handlers.CreateKeyspaceGroupParams{
				KeyspaceGroups: []*endpoint.KeyspaceGroup{
					{
						ID:        param.keyspaceGroupID,
						UserKind:  endpoint.Standard.String(),
						Members:   suite.tsoCluster.GetKeyspaceGroupMember(),
						Keyspaces: param.keyspaceIDs,
					},
				},
			})
		}

		for _, keyspaceGroup := range suite.keyspaceGroups {
			suite.keyspaceIDs = append(suite.keyspaceIDs, keyspaceGroup.keyspaceIDs...)
		}

		// Make sure all keyspace groups are available.
		testutil.Eventually(re, func() bool {
			for _, keyspaceID := range suite.keyspaceIDs {
				served := false
				for _, server := range suite.tsoCluster.GetServers() {
					if server.IsKeyspaceServing(keyspaceID, mcsutils.DefaultKeyspaceGroupID) {
						served = true
						break
					}
				}
				if !served {
					return false
				}
			}
			return true
		}, testutil.WithWaitFor(5*time.Second), testutil.WithTickInterval(50*time.Millisecond))

		// Create clients and make sure they all have discovered the tso service.
		suite.clients = mcs.WaitForMultiKeyspacesTSOAvailable(
			suite.ctx, re, suite.keyspaceIDs, strings.Split(suite.backendEndpoints, ","))
		re.Equal(len(suite.keyspaceIDs), len(suite.clients))
	}
}

func (suite *tsoClientTestSuite) TearDownSuite() {
	suite.cancel()
	if !suite.legacy {
		suite.tsoCluster.Destroy()
	}
	suite.cluster.Destroy()
}

func (suite *tsoClientTestSuite) TestGetTS() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastTS uint64
				for j := 0; j < tsoRequestRound; j++ {
					physical, logical, err := client.GetTS(suite.ctx)
					suite.NoError(err)
					ts := tsoutil.ComposeTS(physical, logical)
					suite.Less(lastTS, ts)
					lastTS = ts
				}
			}(client)
		}
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestGetTSAsync() {
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				tsFutures := make([]pd.TSFuture, tsoRequestRound)
				for j := range tsFutures {
					tsFutures[j] = client.GetTSAsync(suite.ctx)
				}
				var lastTS uint64 = math.MaxUint64
				for j := len(tsFutures) - 1; j >= 0; j-- {
					physical, logical, err := tsFutures[j].Wait()
					suite.NoError(err)
					ts := tsoutil.ComposeTS(physical, logical)
					suite.Greater(lastTS, ts)
					lastTS = ts
				}
			}(client)
		}
	}
	wg.Wait()
}

func (suite *tsoClientTestSuite) TestDiscoverTSOServiceWithLegacyPath() {
	re := suite.Require()
	// Simulate the case that the server has lower version than the client and returns no tso addrs
	// in the GetClusterInfo RPC.
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/serverReturnsNoTSOAddrs", `return(true)`))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/serverReturnsNoTSOAddrs"))
	}()
	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			client := mcs.SetupClientWithDefaultKeyspaceName(
				suite.ctx, re, strings.Split(suite.backendEndpoints, ","))
			var lastTS uint64
			for j := 0; j < tsoRequestRound; j++ {
				physical, logical, err := client.GetTS(suite.ctx)
				suite.NoError(err)
				ts := tsoutil.ComposeTS(physical, logical)
				suite.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
	wg.Wait()
}

// TestGetMinTS tests the correctness of GetMinTS.
func (suite *tsoClientTestSuite) TestGetMinTS() {
	// Skip this test for the time being due to https://github.com/tikv/pd/issues/6453
	// TODO: fix it #6453
	suite.T().SkipNow()

	var wg sync.WaitGroup
	wg.Add(tsoRequestConcurrencyNumber * len(suite.clients))
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		for _, client := range suite.clients {
			go func(client pd.Client) {
				defer wg.Done()
				var lastMinTS uint64
				for j := 0; j < tsoRequestRound; j++ {
					physical, logical, err := client.GetMinTS(suite.ctx)
					suite.NoError(err)
					minTS := tsoutil.ComposeTS(physical, logical)
					suite.Less(lastMinTS, minTS)
					lastMinTS = minTS

					// Now we check whether the returned ts is the minimum one
					// among all keyspace groups, i.e., the returned ts is
					// less than the new timestamps of all keyspace groups.
					for _, client := range suite.clients {
						physical, logical, err := client.GetTS(suite.ctx)
						suite.NoError(err)
						ts := tsoutil.ComposeTS(physical, logical)
						suite.Less(minTS, ts)
					}
				}
			}(client)
		}
	}
	wg.Wait()
}

// More details can be found in this issue: https://github.com/tikv/pd/issues/4884
func (suite *tsoClientTestSuite) TestUpdateAfterResetTSO() {
	re := suite.Require()
	ctx, cancel := context.WithCancel(suite.ctx)
	defer cancel()

	for i := 0; i < len(suite.clients); i++ {
		client := suite.clients[i]
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		// Resign leader to trigger the TSO resetting.
		re.NoError(failpoint.Enable("github.com/tikv/pd/server/updateAfterResetTSO", "return(true)"))
		oldLeaderName := suite.cluster.WaitLeader()
		err := suite.cluster.GetServer(oldLeaderName).ResignLeader()
		re.NoError(err)
		re.NoError(failpoint.Disable("github.com/tikv/pd/server/updateAfterResetTSO"))
		newLeaderName := suite.cluster.WaitLeader()
		re.NotEqual(oldLeaderName, newLeaderName)
		// Request a new TSO.
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		// Transfer leader back.
		re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp", `return(true)`))
		err = suite.cluster.GetServer(newLeaderName).ResignLeader()
		re.NoError(err)
		// Should NOT panic here.
		testutil.Eventually(re, func() bool {
			_, _, err := client.GetTS(ctx)
			return err == nil
		})
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/delaySyncTimestamp"))
	}
}

func (suite *tsoClientTestSuite) TestRandomResignLeader() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	parallelAct := func() {
		// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
		// currently, the time to discover tso service is usually a little longer than 1s, compared
		// to the previous time taken < 1s.
		n := r.Intn(2) + 3
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			wg := sync.WaitGroup{}
			// Select the first keyspace from all keyspace groups. We need to make sure the selected
			// keyspaces are from different keyspace groups, otherwise multiple goroutines below could
			// try to resign the primary of the same keyspace group and cause race condition.
			keyspaceIDs := make([]uint32, 0)
			for _, keyspaceGroup := range suite.keyspaceGroups {
				if len(keyspaceGroup.keyspaceIDs) > 0 {
					keyspaceIDs = append(keyspaceIDs, keyspaceGroup.keyspaceIDs[0])
				}
			}
			wg.Add(len(keyspaceIDs))
			for _, keyspaceID := range keyspaceIDs {
				go func(keyspaceID uint32) {
					defer wg.Done()
					err := suite.tsoCluster.ResignPrimary(keyspaceID, mcsutils.DefaultKeyspaceGroupID)
					re.NoError(err)
					suite.tsoCluster.WaitForPrimaryServing(re, keyspaceID, 0)
				}(keyspaceID)
			}
			wg.Wait()
		} else {
			err := suite.cluster.ResignLeader()
			re.NoError(err)
			suite.cluster.WaitLeader()
		}
		time.Sleep(time.Duration(n) * time.Second)
	}

	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
}

func (suite *tsoClientTestSuite) TestRandomShutdown() {
	re := suite.Require()
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))

	parallelAct := func() {
		// After https://github.com/tikv/pd/issues/6376 is fixed, we can use a smaller number here.
		// currently, the time to discover tso service is usually a little longer than 1s, compared
		// to the previous time taken < 1s.
		n := r.Intn(2) + 3
		time.Sleep(time.Duration(n) * time.Second)
		if !suite.legacy {
			suite.tsoCluster.WaitForDefaultPrimaryServing(re).Close()
		} else {
			suite.cluster.GetServer(suite.cluster.GetLeader()).GetServer().Close()
		}
		time.Sleep(time.Duration(n) * time.Second)
	}

	mcs.CheckMultiKeyspacesTSO(suite.ctx, re, suite.clients, parallelAct)
	suite.TearDownSuite()
	suite.SetupSuite()
	re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
}

// When we upgrade the PD cluster, there may be a period of time that the old and new PDs are running at the same time.
func TestMixedTSODeployment(t *testing.T) {
	re := require.New(t)

	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/client/skipUpdateServiceMode", "return(true)"))
	re.NoError(failpoint.Enable("github.com/tikv/pd/pkg/mcs/tso/server/skipWaitAPIServiceReady", "return(true)"))
	defer func() {
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/tso/fastUpdatePhysicalInterval"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/client/skipUpdateServiceMode"))
		re.NoError(failpoint.Disable("github.com/tikv/pd/pkg/mcs/tso/server/skipWaitAPIServiceReady"))
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cluster, err := tests.NewTestCluster(ctx, 1)
	re.NoError(err)
	defer cancel()
	defer cluster.Destroy()

	err = cluster.RunInitialServers()
	re.NoError(err)

	leaderServer := cluster.GetServer(cluster.WaitLeader())
	backendEndpoints := leaderServer.GetAddr()

	apiSvr, err := cluster.JoinAPIServer(ctx)
	re.NoError(err)
	err = apiSvr.Run()
	re.NoError(err)

	s, cleanup := mcs.StartSingleTSOTestServer(ctx, re, backendEndpoints, tempurl.Alloc())
	defer cleanup()
	mcs.WaitForPrimaryServing(re, map[string]bs.Server{s.GetAddr(): s})

	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	checkTSO(ctx1, re, &wg, backendEndpoints)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 2; i++ {
			n := r.Intn(2) + 1
			time.Sleep(time.Duration(n) * time.Second)
			leaderServer.ResignLeader()
			leaderServer = cluster.GetServer(cluster.WaitLeader())
		}
		cancel1()
	}()
	wg.Wait()
}

func checkTSO(ctx context.Context, re *require.Assertions, wg *sync.WaitGroup, backendEndpoints string) {
	wg.Add(tsoRequestConcurrencyNumber)
	for i := 0; i < tsoRequestConcurrencyNumber; i++ {
		go func() {
			defer wg.Done()
			cli := mcs.SetupClientWithDefaultKeyspaceName(ctx, re, strings.Split(backendEndpoints, ","))
			var ts, lastTS uint64
			for {
				select {
				case <-ctx.Done():
					// Make sure the lastTS is not empty
					re.NotEmpty(lastTS)
					return
				default:
				}
				physical, logical, err := cli.GetTS(ctx)
				// omit the error check since there are many kinds of errors
				if err != nil {
					continue
				}
				ts = tsoutil.ComposeTS(physical, logical)
				re.Less(lastTS, ts)
				lastTS = ts
			}
		}()
	}
}
