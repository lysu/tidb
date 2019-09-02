// Copyright 2015 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

var (
	_ Executor = &baseExecutor{}
	_ Executor = &CheckTableExec{}
	_ Executor = &HashAggExec{}
	_ Executor = &LimitExec{}
	_ Executor = &MaxOneRowExec{}
	_ Executor = &ProjectionExec{}
	_ Executor = &SelectionExec{}
	_ Executor = &SelectLockExec{}
	_ Executor = &ShowNextRowIDExec{}
	_ Executor = &ShowDDLExec{}
	_ Executor = &ShowDDLJobsExec{}
	_ Executor = &ShowDDLJobQueriesExec{}
	_ Executor = &SortExec{}
	_ Executor = &StreamAggExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &TableScanExec{}
	_ Executor = &TopNExec{}
	_ Executor = &UnionExec{}
	_ Executor = &CheckIndexExec{}
	_ Executor = &HashJoinExec{}
	_ Executor = &IndexLookUpExecutor{}
	_ Executor = &MergeJoinExec{}
)

type baseExecutor struct {
	ctx           sessionctx.Context
	id            string
	schema        *expression.Schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
	runtimeStats  *execdetails.RuntimeStats
}

func (e *baseExecutor) base() *baseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	var firstErr error
	for _, child := range e.children {
		if err := child.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// newFirstChunk creates a new chunk to buffer current executor's result.
func (e *baseExecutor) newFirstChunk() *chunk.Chunk {
	return chunk.New(e.retTypes(), e.initCap, e.maxChunkSize)
}

// retTypes returns all output column types.
func (e *baseExecutor) retTypes() []*types.FieldType {
	return e.retFieldTypes
}

// Next fills mutiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, chk *chunk.Chunk) error {
	return nil
}

func newBaseExecutor(ctx sessionctx.Context, schema *expression.Schema, id string, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().MaxChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}
	if ctx.GetSessionVars().StmtCtx.RuntimeStatsColl != nil {
		e.runtimeStats = e.ctx.GetSessionVars().StmtCtx.RuntimeStatsColl.Get(e.id)
	}
	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, chk *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema

	retTypes() []*types.FieldType
	newFirstChunk() *chunk.Chunk
}

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, chk *chunk.Chunk) error {
	sessVars := e.base().ctx.GetSessionVars()
	if atomic.CompareAndSwapUint32(&sessVars.Killed, 1, 0) {
		return ErrQueryInterrupted
	}

	return e.Next(ctx, chk)
}

// CancelDDLJobsExec represents a cancel DDL jobs executor.
type CancelDDLJobsExec struct {
	baseExecutor

	cursor int
	jobIDs []int64
	errs   []error
}

// Next implements the Executor Next interface.
func (e *CancelDDLJobsExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobIDs) {
		return nil
	}
	numCurBatch := mathutil.Min(chk.Capacity(), len(e.jobIDs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		chk.AppendString(0, fmt.Sprintf("%d", e.jobIDs[i]))
		if e.errs[i] != nil {
			chk.AppendString(1, fmt.Sprintf("error: %v", e.errs[i]))
		} else {
			chk.AppendString(1, "successful")
		}
	}
	e.cursor += numCurBatch
	return nil
}

// ShowNextRowIDExec represents a show the next row ID executor.
type ShowNextRowIDExec struct {
	baseExecutor
	tblName *ast.TableName
	done    bool
}

// Next implements the Executor Next interface.
func (e *ShowNextRowIDExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	is := domain.GetDomain(e.ctx).InfoSchema()
	tbl, err := is.TableByName(e.tblName.Schema, e.tblName.Name)
	if err != nil {
		return errors.Trace(err)
	}
	colName := model.ExtraHandleName
	for _, col := range tbl.Meta().Columns {
		if mysql.HasAutoIncrementFlag(col.Flag) {
			colName = col.Name
			break
		}
	}
	nextGlobalID, err := tbl.Allocator(e.ctx).NextGlobalAutoID(tbl.Meta().ID)
	if err != nil {
		return errors.Trace(err)
	}
	chk.AppendString(0, e.tblName.Schema.O)
	chk.AppendString(1, e.tblName.Name.O)
	chk.AppendString(2, colName.O)
	chk.AppendInt64(3, nextGlobalID)
	e.done = true
	return nil
}

// ShowDDLExec represents a show DDL executor.
type ShowDDLExec struct {
	baseExecutor

	ddlOwnerID string
	selfID     string
	ddlInfo    *admin.DDLInfo
	done       bool
}

// Next implements the Executor Next interface.
func (e *ShowDDLExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}

	ddlJobs := ""
	l := len(e.ddlInfo.Jobs)
	for i, job := range e.ddlInfo.Jobs {
		ddlJobs += job.String()
		if i != l-1 {
			ddlJobs += "\n"
		}
	}
	chk.AppendInt64(0, e.ddlInfo.SchemaVer)
	chk.AppendString(1, e.ddlOwnerID)
	chk.AppendString(2, ddlJobs)
	chk.AppendString(3, e.selfID)
	e.done = true
	return nil
}

// ShowDDLJobsExec represent a show DDL jobs executor.
type ShowDDLJobsExec struct {
	baseExecutor

	cursor    int
	jobs      []*model.Job
	jobNumber int64
	is        infoschema.InfoSchema
}

// ShowDDLJobQueriesExec represents a show DDL job queries executor.
// The jobs id that is given by 'admin show ddl job queries' statement,
// only be searched in the latest 10 history jobs
type ShowDDLJobQueriesExec struct {
	baseExecutor

	cursor int
	jobs   []*model.Job
	jobIDs []int64
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobQueriesExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		return errors.Trace(err)
	}
	historyJobs, err := admin.GetHistoryDDLJobs(txn, admin.DefNumHistoryJobs)
	if err != nil {
		return errors.Trace(err)
	}

	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)

	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobQueriesExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		return nil
	}
	if len(e.jobIDs) >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(chk.Capacity(), len(e.jobs)-e.cursor)
	for _, id := range e.jobIDs {
		for i := e.cursor; i < e.cursor+numCurBatch; i++ {
			if id == e.jobs[i].ID {
				chk.AppendString(0, e.jobs[i].Query)
			}
		}
	}
	e.cursor += numCurBatch
	return nil
}

// Open implements the Executor Open interface.
func (e *ShowDDLJobsExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	jobs, err := admin.GetDDLJobs(txn)
	if err != nil {
		return errors.Trace(err)
	}
	if e.jobNumber == 0 {
		e.jobNumber = admin.DefNumHistoryJobs
	}
	historyJobs, err := admin.GetHistoryDDLJobs(txn, int(e.jobNumber))
	if err != nil {
		return errors.Trace(err)
	}
	e.jobs = append(e.jobs, jobs...)
	e.jobs = append(e.jobs, historyJobs...)
	e.cursor = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowDDLJobsExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.jobs) {
		return nil
	}
	numCurBatch := mathutil.Min(chk.Capacity(), len(e.jobs)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		chk.AppendInt64(0, e.jobs[i].ID)
		chk.AppendString(1, getSchemaName(e.is, e.jobs[i].SchemaID))
		chk.AppendString(2, getTableName(e.is, e.jobs[i].TableID))
		chk.AppendString(3, e.jobs[i].Type.String())
		chk.AppendString(4, e.jobs[i].SchemaState.String())
		chk.AppendInt64(5, e.jobs[i].SchemaID)
		chk.AppendInt64(6, e.jobs[i].TableID)
		chk.AppendInt64(7, e.jobs[i].RowCount)
		chk.AppendString(8, model.TSConvert2Time(e.jobs[i].StartTS).String())
		chk.AppendString(9, e.jobs[i].State.String())
	}
	e.cursor += numCurBatch
	return nil
}

func getSchemaName(is infoschema.InfoSchema, id int64) string {
	var schemaName string
	DBInfo, ok := is.SchemaByID(id)
	if ok {
		schemaName = DBInfo.Name.O
		return schemaName
	}

	return schemaName
}

func getTableName(is infoschema.InfoSchema, id int64) string {
	var tableName string
	table, ok := is.TableByID(id)
	if ok {
		tableName = table.Meta().Name.O
		return tableName
	}

	return tableName
}

// CheckTableExec represents a check table executor.
// It is built from the "admin check table" statement, and it checks if the
// index matches the records in the table.
type CheckTableExec struct {
	baseExecutor

	dbName  string
	tblInfo *model.TableInfo
	indices []table.Index
	srcs    []*IndexLookUpExecutor
	done    bool
	is      infoschema.InfoSchema
	exitCh  chan struct{}
	retCh   chan error
}

// Open implements the Executor Open interface.
func (e *CheckTableExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	for _, src := range e.srcs {
		if err := src.Open(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckTableExec) Close() error {
	var firstErr error
	for _, src := range e.srcs {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (e *CheckTableExec) checkIndexHandle(ctx context.Context, num int, src *IndexLookUpExecutor) error {
	cols := src.schema.Columns
	retFieldTypes := make([]*types.FieldType, len(cols))
	for i := range cols {
		retFieldTypes[i] = cols[i].RetType
	}
	chk := chunk.New(retFieldTypes, e.initCap, e.maxChunkSize)

	var err error
	for {
		err = src.Next(ctx, chk)
		if err != nil {
			break
		}
		if chk.NumRows() == 0 {
			break
		}

		select {
		case <-e.exitCh:
			return nil
		default:
		}
	}
	e.retCh <- errors.Trace(err)
	return errors.Trace(err)
}

func (e *CheckTableExec) handlePanic(r interface{}) {
	if r != nil {
		e.retCh <- errors.Errorf("%v", r)
	}
}

// Next implements the Executor Next interface.
func (e *CheckTableExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.done || len(e.srcs) == 0 {
		return nil
	}
	defer func() { e.done = true }()

	idxNames := make([]string, 0, len(e.indices))
	for _, idx := range e.indices {
		idxNames = append(idxNames, idx.Meta().Name.O)
	}
	greater, idxOffset, err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tblInfo.Name.O, idxNames)
	if err != nil {
		tbl := e.srcs[idxOffset].table
		if greater == admin.IdxCntGreater {
			err = e.checkIndexHandle(ctx, idxOffset, e.srcs[idxOffset])
		} else if greater == admin.TblCntGreater {
			err = e.checkTableRecord(tbl, idxOffset)
		}
		if err != nil && admin.ErrDataInConsistent.Equal(err) {
			return ErrAdminCheckTable.GenWithStack("%v err:%v", tbl.Meta().Name, err)
		}
		return errors.Trace(err)
	}

	// The number of table rows is equal to the number of index rows.
	// TODO: Make the value of concurrency adjustable. And we can consider the number of records.
	concurrency := 3
	wg := sync.WaitGroup{}
	for i := range e.srcs {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			util.WithRecovery(func() {
				err1 := e.checkIndexHandle(ctx, num, e.srcs[num])
				if err1 != nil {
					logutil.Logger(ctx).Info("check index handle failed", zap.Error(err))
				}
			}, e.handlePanic)
		}(i)

		if (i+1)%concurrency == 0 {
			wg.Wait()
		}
	}

	for i := 0; i < len(e.srcs); i++ {
		err = <-e.retCh
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (e *CheckTableExec) checkTableRecord(tbl table.Table, idxOffset int) error {
	idx := e.indices[idxOffset]
	genExprs := e.srcs[idxOffset].genExprs
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	if tbl.Meta().GetPartitionInfo() == nil {
		return admin.CheckRecordAndIndex(e.ctx, txn, tbl, idx, genExprs)
	}

	info := tbl.Meta().GetPartitionInfo()
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.(table.PartitionedTable).GetPartition(pid)
		if err := admin.CheckRecordAndIndex(e.ctx, txn, partition, idx, genExprs); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CheckIndexExec represents the executor of checking an index.
// It is built from the "admin check index" statement, and it checks
// the consistency of the index data with the records of the table.
type CheckIndexExec struct {
	baseExecutor

	dbName    string
	tableName string
	idxName   string
	src       *IndexLookUpExecutor
	done      bool
	is        infoschema.InfoSchema
}

// Open implements the Executor Open interface.
func (e *CheckIndexExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	if err := e.src.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.done = false
	return nil
}

// Close implements the Executor Close interface.
func (e *CheckIndexExec) Close() error {
	return errors.Trace(e.src.Close())
}

// Next implements the Executor Next interface.
func (e *CheckIndexExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.done {
		return nil
	}
	defer func() { e.done = true }()

	_, _, err := admin.CheckIndicesCount(e.ctx, e.dbName, e.tableName, []string{e.idxName})
	if err != nil {
		return errors.Trace(err)
	}
	chk = e.src.newFirstChunk()
	for {
		err := Next(ctx, e.src, chk)
		if err != nil {
			return errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}
	}
	return nil
}

// ShowSlowExec represents the executor of showing the slow queries.
// It is build from the "admin show slow" statement:
//	admin show slow top [internal | all] N
//	admin show slow recent N
type ShowSlowExec struct {
	baseExecutor

	ShowSlow *ast.ShowSlow
	result   []*domain.SlowQueryInfo
	cursor   int
}

// Open implements the Executor Open interface.
func (e *ShowSlowExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	dom := domain.GetDomain(e.ctx)
	e.result = dom.ShowSlowQuery(e.ShowSlow)
	return nil
}

// Next implements the Executor Next interface.
func (e *ShowSlowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.cursor >= len(e.result) {
		return nil
	}

	for e.cursor < len(e.result) && chk.NumRows() < e.maxChunkSize {
		slow := e.result[e.cursor]
		chk.AppendString(0, slow.SQL)
		chk.AppendTime(1, types.Time{
			Time: types.FromGoTime(slow.Start),
			Type: mysql.TypeTimestamp,
			Fsp:  types.MaxFsp,
		})
		chk.AppendDuration(2, types.Duration{Duration: slow.Duration, Fsp: types.MaxFsp})
		chk.AppendString(3, slow.Detail.String())
		if slow.Succ {
			chk.AppendInt64(4, 1)
		} else {
			chk.AppendInt64(4, 0)
		}
		chk.AppendUint64(5, slow.ConnID)
		chk.AppendUint64(6, slow.TxnTS)
		chk.AppendString(7, slow.User)
		chk.AppendString(8, slow.DB)
		chk.AppendString(9, slow.TableIDs)
		chk.AppendString(10, slow.IndexIDs)
		if slow.Internal {
			chk.AppendInt64(11, 0)
		} else {
			chk.AppendInt64(11, 1)
		}
		chk.AppendString(12, slow.Digest)
		e.cursor++
	}
	return nil
}

// SelectLockExec represents a select lock executor.
// It is built from the "SELECT .. FOR UPDATE" or the "SELECT .. LOCK IN SHARE MODE" statement.
// For "SELECT .. FOR UPDATE" statement, it locks every row key from source Executor.
// After the execution, the keys are buffered in transaction, and will be sent to KV
// when doing commit. If there is any key already locked by another transaction,
// the transaction will rollback and retry.
type SelectLockExec struct {
	baseExecutor

	Lock ast.SelectLockType
}

// Open implements the Executor Open interface.
func (e *SelectLockExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}

	txnCtx := e.ctx.GetSessionVars().TxnCtx
	txnCtx.ForUpdate = true
	for id := range e.Schema().TblID2Handle {
		// This operation is only for schema validator check.
		txnCtx.UpdateDeltaForTable(id, 0, 0, map[int64]int64{})
	}
	return nil
}

// Next implements the Executor Next interface.
func (e *SelectLockExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	err := Next(ctx, e.children[0], chk)
	if err != nil {
		return errors.Trace(err)
	}
	// If there's no handle or it's not a `SELECT FOR UPDATE` statement.
	if len(e.Schema().TblID2Handle) == 0 || e.Lock != ast.SelectLockForUpdate {
		return nil
	}
	txn, err := e.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	keys := make([]kv.Key, 0, chk.NumRows())
	iter := chunk.NewIterator4Chunk(chk)
	for id, cols := range e.Schema().TblID2Handle {
		for _, col := range cols {
			keys = keys[:0]
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				keys = append(keys, tablecodec.EncodeRowKeyWithHandle(id, row.GetInt64(col.Index)))
			}
			err = txn.LockKeys(keys...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(chk.RequiredRows(), e.maxChunkSize)
		err := Next(ctx, e.children[0], e.adjustRequiredRows(e.childResult))
		if err != nil {
			return errors.Trace(err)
		}
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			chk.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	e.adjustRequiredRows(chk)
	err := Next(ctx, e.children[0], chk)
	if err != nil {
		return errors.Trace(err)
	}
	batchSize := uint64(chk.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		chk.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.childResult = e.children[0].newFirstChunk()
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	e.childResult = nil
	return errors.Trace(e.baseExecutor.Close())
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
		limitRequired = chk.RequiredRows()
	}

	return chk.SetRequiredRows(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

func init() {
	// While doing optimization in the plan package, we need to execute uncorrelated subquery,
	// but the plan package cannot import the executor package because of the dependency cycle.
	// So we assign a function implemented in the executor package to the plan package to avoid the dependency cycle.
	plannercore.EvalSubquery = func(p plannercore.PhysicalPlan, is infoschema.InfoSchema, sctx sessionctx.Context) (rows [][]types.Datum, err error) {
		e := &executorBuilder{is: is, ctx: sctx}
		exec := e.build(p)
		if e.err != nil {
			return rows, errors.Trace(e.err)
		}
		ctx := context.TODO()
		err = exec.Open(ctx)
		defer terror.Call(exec.Close)
		if err != nil {
			return rows, errors.Trace(err)
		}
		chk := exec.newFirstChunk()
		for {
			err = Next(ctx, exec, chk)
			if err != nil {
				return rows, errors.Trace(err)
			}
			if chk.NumRows() == 0 {
				return rows, nil
			}
			iter := chunk.NewIterator4Chunk(chk)
			for r := iter.Begin(); r != iter.End(); r = iter.Next() {
				row := r.GetDatumRow(exec.retTypes())
				rows = append(rows, row)
			}
			chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
		}
	}
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		chk.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			chk.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

// SelectionExec represents a filter executor.
type SelectionExec struct {
	baseExecutor

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputIter   *chunk.Iterator4Chunk
	inputRow    chunk.Row
	childResult *chunk.Chunk
}

// Open implements the Executor Open interface.
func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.childResult = e.children[0].newFirstChunk()
	e.batched = expression.Vectorizable(e.filters)
	if e.batched {
		e.selected = make([]bool, 0, chunk.InitialCapacity)
	}
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

// Close implements plannercore.Plan Close interface.
func (e *SelectionExec) Close() error {
	e.childResult = nil
	e.selected = nil
	return errors.Trace(e.baseExecutor.Close())
}

// Next implements the Executor Next interface.
func (e *SelectionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.GrowAndReset(e.maxChunkSize)

	if !e.batched {
		return errors.Trace(e.unBatchedNext(ctx, chk))
	}

	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			if !e.selected[e.inputRow.Idx()] {
				continue
			}
			if chk.IsFull() {
				return nil
			}
			chk.AppendRow(e.inputRow)
		}
		err := Next(ctx, e.children[0], e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
		e.selected, err = expression.VectorizedFilter(e.ctx, e.filters, e.inputIter, e.selected)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
	}
}

// unBatchedNext filters input rows one by one and returns once an input row is selected.
// For sql with "SETVAR" in filter and "GETVAR" in projection, for example: "SELECT @a FROM t WHERE (@a := 2) > 0",
// we have to set batch size to 1 to do the evaluation of filter and projection.
func (e *SelectionExec) unBatchedNext(ctx context.Context, chk *chunk.Chunk) error {
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				return errors.Trace(err)
			}
			if selected {
				chk.AppendRow(e.inputRow)
				e.inputRow = e.inputIter.Next()
				return nil
			}
		}
		err := Next(ctx, e.children[0], e.childResult)
		if err != nil {
			return errors.Trace(err)
		}
		e.inputRow = e.inputIter.Begin()
		// no more data.
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

// TableScanExec is a table scan executor without result fields.
type TableScanExec struct {
	baseExecutor

	t                     table.Table
	seekHandle            int64
	iter                  kv.Iterator
	columns               []*model.ColumnInfo
	isVirtualTable        bool
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

// Next implements the Executor Next interface.
func (e *TableScanExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.GrowAndReset(e.maxChunkSize)
	if e.isVirtualTable {
		return errors.Trace(e.nextChunk4InfoSchema(ctx, chk))
	}
	handle, found, err := e.nextHandle()
	if err != nil || !found {
		return errors.Trace(err)
	}

	mutableRow := chunk.MutRowFromTypes(e.retTypes())
	for chk.NumRows() < chk.Capacity() {
		row, err := e.getRow(handle)
		if err != nil {
			return errors.Trace(err)
		}
		e.seekHandle = handle + 1
		mutableRow.SetDatums(row...)
		chk.AppendRow(mutableRow.ToRow())
	}
	return nil
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	chk.GrowAndReset(e.maxChunkSize)
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(e.retTypes(), e.initCap, e.maxChunkSize)
		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}
		mutableRow := chunk.MutRowFromTypes(e.retTypes())
		err := e.t.IterRecords(e.ctx, nil, columns, func(h int64, rec []types.Datum, cols []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

// nextHandle gets the unique handle for next row.
func (e *TableScanExec) nextHandle() (handle int64, found bool, err error) {
	for {
		handle, found, err = e.t.Seek(e.ctx, e.seekHandle)
		if err != nil || !found {
			return 0, false, errors.Trace(err)
		}
		return handle, true, nil
	}
}

func (e *TableScanExec) getRow(handle int64) ([]types.Datum, error) {
	columns := make([]*table.Column, e.schema.Len())
	for i, v := range e.columns {
		columns[i] = table.ToColumn(v)
	}
	row, err := e.t.RowWithCols(e.ctx, handle, columns)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return row, nil
}

// Open implements the Executor Open interface.
func (e *TableScanExec) Open(ctx context.Context) error {
	e.iter = nil
	e.virtualTableChunkList = nil
	return nil
}

// MaxOneRowExec checks if the number of rows that a query returns is at maximum one.
// It's built from subquery expression.
type MaxOneRowExec struct {
	baseExecutor

	evaluated bool
}

// Open implements the Executor Open interface.
func (e *MaxOneRowExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	e.evaluated = false
	return nil
}

// Next implements the Executor Next interface.
func (e *MaxOneRowExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.Reset()
	if e.evaluated {
		return nil
	}
	e.evaluated = true
	err := Next(ctx, e.children[0], chk)
	if err != nil {
		return errors.Trace(err)
	}

	if num := chk.NumRows(); num == 0 {
		for i := range e.schema.Columns {
			chk.AppendNull(i)
		}
		return nil
	} else if num != 1 {
		return errors.New("subquery returns more than 1 row")
	}

	childChunk := e.children[0].newFirstChunk()
	err = e.children[0].Next(ctx, childChunk)
	if err != nil {
		return errors.Trace(err)
	}
	if childChunk.NumRows() != 0 {
		return errors.New("subquery returns more than 1 row")
	}

	return nil
}

// UnionExec pulls all it's children's result and returns to its parent directly.
// A "resultPuller" is started for every child to pull result from that child and push it to the "resultPool", the used
// "Chunk" is obtained from the corresponding "resourcePool". All resultPullers are running concurrently.
//                             +----------------+
//   +---> resourcePool 1 ---> | resultPuller 1 |-----+
//   |                         +----------------+     |
//   |                                                |
//   |                         +----------------+     v
//   +---> resourcePool 2 ---> | resultPuller 2 |-----> resultPool ---+
//   |                         +----------------+     ^               |
//   |                               ......           |               |
//   |                         +----------------+     |               |
//   +---> resourcePool n ---> | resultPuller n |-----+               |
//   |                         +----------------+                     |
//   |                                                                |
//   |                          +-------------+                       |
//   |--------------------------| main thread | <---------------------+
//                              +-------------+
type UnionExec struct {
	baseExecutor

	stopFetchData atomic.Value
	wg            sync.WaitGroup

	finished      chan struct{}
	resourcePools []chan *chunk.Chunk
	resultPool    chan *unionWorkerResult
	initialized   bool

	childrenResults []*chunk.Chunk
}

// unionWorkerResult stores the result for a union worker.
// A "resultPuller" is started for every child to pull result from that child, unionWorkerResult is used to store that pulled result.
// "src" is used for Chunk reuse: after pulling result from "resultPool", main-thread must push a valid unused Chunk to "src" to
// enable the corresponding "resultPuller" continue to work.
type unionWorkerResult struct {
	chk *chunk.Chunk
	err error
	src chan<- *chunk.Chunk
}

func (e *UnionExec) waitAllFinished() {
	e.wg.Wait()
	close(e.resultPool)
}

// Open implements the Executor Open interface.
func (e *UnionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return errors.Trace(err)
	}
	for _, child := range e.children {
		e.childrenResults = append(e.childrenResults, child.newFirstChunk())
	}
	e.stopFetchData.Store(false)
	e.initialized = false
	e.finished = make(chan struct{})
	return nil
}

func (e *UnionExec) initialize(ctx context.Context) {
	e.resultPool = make(chan *unionWorkerResult, len(e.children))
	e.resourcePools = make([]chan *chunk.Chunk, len(e.children))
	for i := range e.children {
		e.resourcePools[i] = make(chan *chunk.Chunk, 1)
		e.resourcePools[i] <- e.childrenResults[i]
		e.wg.Add(1)
		go e.resultPuller(ctx, i)
	}
	go e.waitAllFinished()
}

func (e *UnionExec) resultPuller(ctx context.Context, childID int) {
	result := &unionWorkerResult{
		err: nil,
		chk: nil,
		src: e.resourcePools[childID],
	}
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			logutil.Logger(ctx).Error("resultPuller panicked", zap.String("stack", string(buf)))
			result.err = errors.Errorf("%v", r)
			e.resultPool <- result
			e.stopFetchData.Store(true)
		}
		e.wg.Done()
	}()
	for {
		if e.stopFetchData.Load().(bool) {
			return
		}
		select {
		case <-e.finished:
			return
		case result.chk = <-e.resourcePools[childID]:
		}
		result.err = errors.Trace(e.children[childID].Next(ctx, result.chk))
		if result.err == nil && result.chk.NumRows() == 0 {
			return
		}
		e.resultPool <- result
		if result.err != nil {
			e.stopFetchData.Store(true)
			return
		}
	}
}

// Next implements the Executor Next interface.
func (e *UnionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.runtimeStats != nil {
		start := time.Now()
		defer func() { e.runtimeStats.Record(time.Now().Sub(start), chk.NumRows()) }()
	}
	chk.GrowAndReset(e.maxChunkSize)
	if !e.initialized {
		e.initialize(ctx)
		e.initialized = true
	}
	result, ok := <-e.resultPool
	if !ok {
		return nil
	}
	if result.err != nil {
		return errors.Trace(result.err)
	}

	chk.SwapColumns(result.chk)
	result.src <- result.chk
	return nil
}

// Close implements the Executor Close interface.
func (e *UnionExec) Close() error {
	close(e.finished)
	e.childrenResults = nil
	if e.resultPool != nil {
		for range e.resultPool {
		}
	}
	e.resourcePools = nil
	return errors.Trace(e.baseExecutor.Close())
}

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func ResetContextOfStmt(ctx sessionctx.Context, s ast.StmtNode) (err error) {
	vars := ctx.GetSessionVars()
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = vars.Location()
	sc.MemTracker = memory.NewTracker(s.Text(), vars.MemQuotaQuery)
	switch config.GetGlobalConfig().OOMAction {
	case config.OOMActionCancel:
		sc.MemTracker.SetActionOnExceed(&memory.PanicOnExceed{})
	case config.OOMActionLog:
		sc.MemTracker.SetActionOnExceed(&memory.LogOnExceed{})
	default:
		sc.MemTracker.SetActionOnExceed(&memory.LogOnExceed{})
	}
	if execStmt, ok := s.(*ast.ExecuteStmt); ok {
		s, err = getPreparedStmt(execStmt, vars)
		if err != nil {
			return
		}
	}
	// execute missed stmtID uses empty sql
	sc.OriginalSQL = s.Text()
	if explainStmt, ok := s.(*ast.ExplainStmt); ok {
		sc.CastStrToIntStrict = true
		s = explainStmt.Stmt
	}
	// TODO: Many same bool variables here.
	// We should set only two variables (
	// IgnoreErr and StrictSQLMode) to avoid setting the same bool variables and
	// pushing them down to TiKV as flags.
	switch stmt := s.(type) {
	case *ast.UpdateStmt:
		sc.InUpdateStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.DeleteStmt:
		sc.InDeleteStmt = true
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.InsertStmt:
		sc.InInsertStmt = true
		// For insert statement (not for update statement), disabling the StrictSQLMode
		// should make TruncateAsWarning and DividedByZeroAsWarning,
		// but should not make DupKeyAsWarning or BadNullAsWarning,
		sc.DupKeyAsWarning = stmt.IgnoreErr
		sc.BadNullAsWarning = stmt.IgnoreErr
		sc.TruncateAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.DividedByZeroAsWarning = !vars.StrictSQLMode || stmt.IgnoreErr
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		sc.IgnoreZeroInDate = !vars.StrictSQLMode || stmt.IgnoreErr || sc.AllowInvalidDate
		sc.Priority = stmt.Priority
	case *ast.CreateTableStmt, *ast.AlterTableStmt:
		// Make sure the sql_mode is strict when checking column default value.
	case *ast.LoadDataStmt:
		sc.DupKeyAsWarning = true
		sc.BadNullAsWarning = true
		sc.TruncateAsWarning = !vars.StrictSQLMode
		sc.InLoadDataStmt = true
	case *ast.SelectStmt:
		sc.InSelectStmt = true

		// see https://dev.mysql.com/doc/refman/5.7/en/sql-mode.html#sql-mode-strict
		// said "For statements such as SELECT that do not change data, invalid values
		// generate a warning in strict mode, not an error."
		// and https://dev.mysql.com/doc/refman/5.7/en/out-of-range-and-overflow.html
		sc.OverflowAsWarning = true

		// Return warning for truncate error in selection.
		sc.TruncateAsWarning = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if opts := stmt.SelectStmtOpts; opts != nil {
			sc.Priority = opts.Priority
			sc.NotFillCache = !opts.SQLCache
		}
		sc.PadCharToFullLength = ctx.GetSessionVars().SQLMode.HasPadCharToFullLengthMode()
		sc.CastStrToIntStrict = true
	case *ast.ShowStmt:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
		if stmt.Tp == ast.ShowWarnings || stmt.Tp == ast.ShowErrors {
			sc.InShowWarning = true
			sc.SetWarnings(vars.StmtCtx.GetWarnings())
		}
	case *ast.SplitRegionStmt:
		sc.IgnoreTruncate = false
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	default:
		sc.IgnoreTruncate = true
		sc.IgnoreZeroInDate = true
		sc.AllowInvalidDate = vars.SQLMode.HasAllowInvalidDatesMode()
	}
	vars.PreparedParams = vars.PreparedParams[:0]
	if !vars.InRestrictedSQL {
		if priority := mysql.PriorityEnum(atomic.LoadInt32(&variable.ForcePriority)); priority != mysql.NoPriority {
			sc.Priority = priority
		}
	}
	if vars.StmtCtx.LastInsertID > 0 {
		sc.PrevLastInsertID = vars.StmtCtx.LastInsertID
	} else {
		sc.PrevLastInsertID = vars.StmtCtx.PrevLastInsertID
	}
	sc.PrevAffectedRows = 0
	if vars.StmtCtx.InUpdateStmt || vars.StmtCtx.InDeleteStmt || vars.StmtCtx.InInsertStmt {
		sc.PrevAffectedRows = int64(vars.StmtCtx.AffectedRows())
	} else if vars.StmtCtx.InSelectStmt {
		sc.PrevAffectedRows = -1
	}
	err = vars.SetSystemVar("warning_count", fmt.Sprintf("%d", vars.StmtCtx.NumWarnings(false)))
	if err != nil {
		return errors.Trace(err)
	}
	err = vars.SetSystemVar("error_count", fmt.Sprintf("%d", vars.StmtCtx.NumWarnings(true)))
	if err != nil {
		return errors.Trace(err)
	}
	if s != nil {
		// execute missed stmtID uses empty sql
		sc.OriginalSQL = s.Text()
	}
	vars.StmtCtx = sc
	return
}
