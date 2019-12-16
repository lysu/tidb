// Copyright 2016 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite3) TestGrantGlobal(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testGlobal'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the global privs for new user is "N".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\";", mysql.Priv2UserCol[v])
		r := tk.MustQuery(sql)
		r.Check(testkit.Rows("N"))
	}

	// Grant each priv to the user.
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("GRANT %s ON *.* TO 'testGlobal'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testGlobal1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("GRANT ALL ON *.* TO 'testGlobal1'@'localhost';")
	// Make sure all the global privs for granted user is "Y".
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}
}

func (s *testSuite3) TestGrantDBScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testDB'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the db privs for new user is empty.
	sql := fmt.Sprintf("SELECT * FROM mysql.db WHERE User=\"testDB\" and host=\"localhost\"")
	tk.MustQuery(sql).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("GRANT %s ON test.* TO 'testDB'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB\" and host=\"localhost\" and db=\"test\"", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testDB1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	tk.MustExec("GRANT ALL ON * TO 'testDB1'@'localhost';")
	// Make sure all the db privs for granted user is "Y".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB1\" and host=\"localhost\" and db=\"test\";", mysql.Priv2UserCol[v])
		tk.MustQuery(sql).Check(testkit.Rows("Y"))
	}
}

func (s *testSuite3) TestWithGrantOption(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testWithGrant'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	// Make sure all the db privs for new user is empty.
	sql := fmt.Sprintf("SELECT * FROM mysql.db WHERE User=\"testWithGrant\" and host=\"localhost\"")
	tk.MustQuery(sql).Check(testkit.Rows())

	// Grant select priv to the user, with grant option.
	tk.MustExec("GRANT select ON test.* TO 'testWithGrant'@'localhost' WITH GRANT OPTION;")
	tk.MustQuery("SELECT grant_priv FROM mysql.DB WHERE User=\"testWithGrant\" and host=\"localhost\" and db=\"test\"").Check(testkit.Rows("Y"))
}

func (s *testSuite3) TestTableScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testTbl'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec(`CREATE TABLE test.test1(c1 int);`)
	// Make sure all the table privs for new user is empty.
	tk.MustQuery(`SELECT * FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1"`).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllTablePrivs {
		sql := fmt.Sprintf("GRANT %s ON test.test1 TO 'testTbl'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		rows := tk.MustQuery(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
	}
	// Create a new user.
	createUserSQL = `CREATE USER 'testTbl1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	tk.MustExec(`CREATE TABLE test2(c1 int);`)
	// Grant all table scope privs.
	tk.MustExec("GRANT ALL ON test2 TO 'testTbl1'@'localhost';")
	// Make sure all the table privs for granted user are in the Table_priv set.
	for _, v := range mysql.AllTablePrivs {
		rows := tk.MustQuery(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl1" and host="localhost" and db="test" and Table_name="test2";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
	}
}

func (s *testSuite3) TestColumnScope(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Create a new user.
	createUserSQL := `CREATE USER 'testCol'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec(`CREATE TABLE test.test3(c1 int, c2 int);`)

	// Make sure all the column privs for new user is empty.
	tk.MustQuery(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1"`).Check(testkit.Rows())
	tk.MustQuery(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2"`).Check(testkit.Rows())

	// Grant each priv to the user.
	for _, v := range mysql.AllColumnPrivs {
		sql := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testCol'@'localhost';", mysql.Priv2Str[v])
		tk.MustExec(sql)
		rows := tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
	}

	// Create a new user.
	createUserSQL = `CREATE USER 'testCol1'@'localhost' IDENTIFIED BY '123';`
	tk.MustExec(createUserSQL)
	tk.MustExec("USE test;")
	// Grant all column scope privs.
	tk.MustExec("GRANT ALL(c2) ON test3 TO 'testCol1'@'localhost';")
	// Make sure all the column privs for granted user are in the Column_priv set.
	for _, v := range mysql.AllColumnPrivs {
		rows := tk.MustQuery(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol1" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2";`).Rows()
		c.Assert(rows, HasLen, 1)
		row := rows[0]
		c.Assert(row, HasLen, 1)
		p := fmt.Sprintf("%v", row[0])
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
	}
}

func (s *testSuite3) TestIssue2456(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE USER 'dduser'@'%' IDENTIFIED by '123456';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `dddb_%`.* TO 'dduser'@'%';")
	tk.MustExec("GRANT ALL PRIVILEGES ON `dddb_%`.`te%` to 'dduser'@'%';")
}

func (s *testSuite3) TestNoAutoCreateUser(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustExec(`SET sql_mode='NO_AUTO_CREATE_USER'`)
	_, err := tk.Exec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	c.Check(err, NotNil)
	c.Assert(terror.ErrorEqual(err, executor.ErrCantCreateUserWithGrant), IsTrue)
}

func (s *testSuite3) TestCreateUserWhenGrant(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	// This only applies to sql_mode:NO_AUTO_CREATE_USER off
	tk.MustExec(`SET SQL_MODE=''`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON *.* to 'test'@'%' IDENTIFIED BY 'xxx'`)
	// Make sure user is created automatically when grant to a non-exists one.
	tk.MustQuery(`SELECT user FROM mysql.user WHERE user='test' and host='%'`).Check(
		testkit.Rows("test"),
	)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
}

func (s *testSuite3) TestIssue2654(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`DROP USER IF EXISTS 'test'@'%'`)
	tk.MustExec(`CREATE USER 'test'@'%' IDENTIFIED BY 'test'`)
	tk.MustExec("GRANT SELECT ON test.* to 'test'")
	rows := tk.MustQuery(`SELECT user,host FROM mysql.user WHERE user='test' and host='%'`)
	rows.Check(testkit.Rows(`test %`))
}

func (s *testSuite3) TestGrantUnderANSIQuotes(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	// Fix a bug that the GrantExec fails in ANSI_QUOTES sql mode
	// The bug is caused by the improper usage of double quotes like:
	// INSERT INTO mysql.user ... VALUES ("..", "..", "..")
	tk.MustExec(`SET SQL_MODE='ANSI_QUOTES'`)
	tk.MustExec(`GRANT ALL PRIVILEGES ON video_ulimit.* TO web@'%' IDENTIFIED BY 'eDrkrhZ>l2sV'`)
	tk.MustExec(`REVOKE ALL PRIVILEGES ON video_ulimit.* FROM web@'%';`)
	tk.MustExec(`DROP USER IF EXISTS 'web'@'%'`)
}

func (s *testSuite3) TestMaintainRequire(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test create with require
	tk.MustExec(`CREATE USER 'ssl_auser'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_buser'@'%' require subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_cuser'@'%' require cipher 'AES128-GCM-SHA256'`)
	tk.MustExec(`CREATE USER 'ssl_duser'@'%'`)
	tk.MustExec(`CREATE USER 'ssl_euser'@'%' require none`)
	tk.MustExec(`CREATE USER 'ssl_fuser'@'%' require ssl`)
	tk.MustExec(`CREATE USER 'ssl_guser'@'%' require x509`)
	tk.MustQuery("select * from mysql.global_priv where `user` like 'ssl_%'").Check(testkit.Rows(
		"% ssl_auser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_buser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}",
		"% ssl_cuser {\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}",
		"% ssl_duser {}",
		"% ssl_euser {}",
		"% ssl_fuser {\"ssl_type\":1}",
		"% ssl_guser {\"ssl_type\":2}",
	))

	// test grant with require
	tk.MustExec("CREATE USER 'u1'@'%'")
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' and subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH'") // add new require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require cipher 'AES128-GCM-SHA256'") // modify always overwrite.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustExec("GRANT select ON *.* TO 'u1'@'%'") // modify without require should not modify old require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\"}"))
	tk.MustExec("GRANT ALL ON *.* TO 'u1'@'%' require none") // use require none to clean up require.
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u1'").Check(testkit.Rows("{}"))

	// test alter with require
	tk.MustExec("CREATE USER 'u2'@'%'")
	tk.MustExec("alter user 'u2'@'%' require ssl")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":1}"))
	tk.MustExec("alter user 'u2'@'%' require x509")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":2}"))
	tk.MustExec("alter user 'u2'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{\"ssl_type\":3,\"ssl_cipher\":\"AES128-GCM-SHA256\",\"x509_issuer\":\"/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US\",\"x509_subject\":\"/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH\"}"))
	tk.MustExec("alter user 'u2'@'%' require none")
	tk.MustQuery("select priv from mysql.global_priv where `Host` = '%' and `User` = 'u2'").Check(testkit.Rows("{}"))

	// test show create user
	tk.MustExec(`CREATE USER 'u3'@'%' require issuer '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' subject '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' cipher 'AES128-GCM-SHA256'`)
	tk.MustQuery("show create user 'u3'").Check(testkit.Rows("CREATE USER 'u3'@'%' IDENTIFIED WITH 'mysql_native_password' AS '' REQUIRE CIPHER 'AES128-GCM-SHA256' ISSUER '/CN=TiDB admin/OU=TiDB/O=PingCAP/L=San Francisco/ST=California/C=US' SUBJECT '/CN=tester1/OU=TiDB/O=PingCAP.Inc/L=Haidian/ST=Beijing/C=ZH' PASSWORD EXPIRE DEFAULT ACCOUNT UNLOCK"))

	// check issuer/subject/cipher value
	_, err := tk.Exec(`CREATE USER 'u4'@'%' require issuer 'CN=TiDB,OU=PingCAP'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`CREATE USER 'u5'@'%' require subject '/CN=TiDB\OU=PingCAP'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`CREATE USER 'u6'@'%' require subject '/CN=TiDB\NC=PingCAP'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`CREATE USER 'u7'@'%' require cipher 'AES128-GCM-SHA1'`)
	c.Assert(err, NotNil)
	_, err = tk.Exec(`CREATE USER 'u8'@'%' require subject '/CN'`)
	c.Assert(err, NotNil)
}
