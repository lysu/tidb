// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"context"
	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testDistSQlSuite{})

type testDistSQlSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore mocktikv.MVCCStore
	store     kv.Storage
	dom       *domain.Domain
}

func (s *testDistSQlSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.cluster = mocktikv.NewCluster()
	mocktikv.BootstrapWithMultiStores(s.cluster, 2)
	s.mvccStore = mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(s.cluster),
		mockstore.WithMVCCStore(s.mvccStore),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testDistSQlSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testDistSQlSuite) TestSelectTwoStoreButFailOne(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists two_row")
	tk.MustExec("create table two_row (id int primary key, v int)")
	tk.MustExec("insert into two_row values (1, 1), (2, 1), (3, 1), (4, 1)")
	tblID := tk.GetTableID("two_row")
	s.cluster.SplitTable(s.mvccStore, tblID, 2)
	failpoint.Enable("github.com/pingcap/tidb/store/tikv/copHandleFail", "1*return(false)->return(true)")
	defer func() {
		failpoint.Disable("github.com/pingcap/tidb/store/tikv/copHandleFail")
	}()
	rs, err := tk.Exec("select * from two_row where id in (1, 4)")
	b := rs.NewRecordBatch()
	b.Reset()
	err = rs.Next(context.TODO(), b)
	c.Assert(err, IsNil)
	//c.Assert(err, NotNil)
}
