// Copyright 2018 TiKV Project Authors.
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

package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	tu "github.com/tikv/pd/pkg/testutil"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/server/core"
)

var _ = Suite(&testAdminSuite{})

type testAdminSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func (s *testAdminSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
}

func (s *testAdminSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testAdminSuite) TestDropRegion(c *C) {
	cluster := s.svr.GetRaftCluster()

	// Update region's epoch to (100, 100).
	region := cluster.GetRegionByKey([]byte("foo")).Clone(
		core.SetRegionConfVer(100),
		core.SetRegionVersion(100),
	)
	region = region.Clone(core.WithLeader(&metapb.Peer{Id: 109, StoreId: 2}), core.SetPeers([]*metapb.Peer{
		{
			Id: 109, StoreId: 2,
		},
	}))
	err := cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	// Region epoch cannot decrease.
	region = region.Clone(
		core.SetRegionConfVer(50),
		core.SetRegionVersion(50),
	)
	err = cluster.HandleRegionHeartbeat(region)
	c.Assert(err, NotNil)

	// After drop region from cache, lower version is accepted.
	url := fmt.Sprintf("%s/admin/cache/region/%d", s.urlPrefix, region.GetID())
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	c.Assert(err, IsNil)
	res, err := testDialClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(res.StatusCode, Equals, http.StatusOK)
	res.Body.Close()
	err = cluster.HandleRegionHeartbeat(region)
	c.Assert(err, IsNil)

	region = cluster.GetRegionByKey([]byte("foo"))
	c.Assert(region.GetRegionEpoch().ConfVer, Equals, uint64(50))
	c.Assert(region.GetRegionEpoch().Version, Equals, uint64(50))
}

func (s *testAdminSuite) TestPersistFile(c *C) {
	data := []byte("#!/bin/sh\nrm -rf /")
	err := tu.CheckPostJSON(testDialClient, s.urlPrefix+"/admin/persist-file/fun.sh", data, tu.StatusNotOK(c))
	c.Assert(err, IsNil)
	data = []byte(`{"foo":"bar"}`)
	err = tu.CheckPostJSON(testDialClient, s.urlPrefix+"/admin/persist-file/good.json", data, tu.StatusOK(c))
	c.Assert(err, IsNil)
}

var _ = Suite(&testTSOSuite{})

type testTSOSuite struct {
	svr       *server.Server
	cleanup   cleanUpFunc
	urlPrefix string
}

func makeTS(offset time.Duration) uint64 {
	physical := time.Now().Add(offset).UnixNano() / int64(time.Millisecond)
	return uint64(physical << 18)
}

func (s *testTSOSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})
	addr := s.svr.GetAddr()
	s.urlPrefix = fmt.Sprintf("%s%s/api/v1/admin/reset-ts", addr, apiPrefix)

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (s *testTSOSuite) TearDownSuite(c *C) {
	s.cleanup()
}

func (s *testTSOSuite) TestResetTS(c *C) {
	args := make(map[string]interface{})
	t1 := makeTS(time.Hour)
	url := s.urlPrefix
	args["tso"] = fmt.Sprintf("%d", t1)
	values, err := json.Marshal(args)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.StatusOK(c),
		tu.StringEqual(c, "\"Reset ts successfully.\"\n"))
	c.Assert(err, IsNil)
	t2 := makeTS(32 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t2)
	values, err = json.Marshal(args)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(c, http.StatusForbidden),
		tu.StringContain(c, "too large"))
	c.Assert(err, IsNil)

	t3 := makeTS(-2 * time.Hour)
	args["tso"] = fmt.Sprintf("%d", t3)
	values, err = json.Marshal(args)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(c, http.StatusForbidden),
		tu.StringContain(c, "small"))
	c.Assert(err, IsNil)

	args["tso"] = ""
	values, err = json.Marshal(args)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(c, http.StatusBadRequest),
		tu.StringEqual(c, "\"invalid tso value\"\n"))
	c.Assert(err, IsNil)

	args["tso"] = "test"
	values, err = json.Marshal(args)
	c.Assert(err, IsNil)
	err = tu.CheckPostJSON(testDialClient, url, values,
		tu.Status(c, http.StatusBadRequest),
		tu.StringEqual(c, "\"invalid tso value\"\n"))
	c.Assert(err, IsNil)
}

var _ = Suite(&testServiceSuite{})

type testServiceSuite struct {
	svr     *server.Server
	cleanup cleanUpFunc
}

func (s *testServiceSuite) SetUpSuite(c *C) {
	s.svr, s.cleanup = mustNewServer(c)
	mustWaitLeader(c, []*server.Server{s.svr})

	mustBootstrapCluster(c, s.svr)
	mustPutStore(c, s.svr, 1, metapb.StoreState_Up, metapb.NodeState_Serving, nil)
}

func (s *testServiceSuite) TearDownSuite(c *C) {
	s.cleanup()
}
