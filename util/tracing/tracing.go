// Copyright 2018 PingCAP, Inc.
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

package tracing

import (
	"context"
	"fmt"
	"github.com/opentracing/basictracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
)

// TiDBTrace is set as Baggage on traces which are used for tidb tracing.
const TiDBTrace = "tr"

// A CallbackRecorder immediately invokes itself on received trace spans.
type CallbackRecorder func(sp basictracer.RawSpan)

// RecordSpan implements basictracer.SpanRecorder.
func (cr CallbackRecorder) RecordSpan(sp basictracer.RawSpan) {
	cr(sp)
}

// NewRecordedTrace returns a Span which records directly via the specified
// callback.
func NewRecordedTrace(opName string, callback func(sp basictracer.RawSpan)) opentracing.Span {
	tr := basictracer.New(CallbackRecorder(callback))
	opentracing.SetGlobalTracer(tr)
	sp := tr.StartSpan(opName)
	sp.SetBaggageItem(TiDBTrace, "1")
	return sp
}

// noopSpan returns a Span which discards all operations.
func noopSpan() opentracing.Span {
	return (opentracing.NoopTracer{}).StartSpan("DefaultSpan")
}

// SpanFromContext returns the span obtained from the context or, if none is found, a new one started through tracer.
func SpanFromContext(ctx context.Context) (sp opentracing.Span) {
	if sp = opentracing.SpanFromContext(ctx); sp == nil {
		return noopSpan()
	}
	return sp
}

// ChildSpanFromContxt return a non-nil span. If span can be got from ctx, then returned span is
// a child of such span. Otherwise, returned span is a noop span.
func ChildSpanFromContxt(ctx context.Context, opName string) (opentracing.Span, context.Context) {
	if sp := opentracing.SpanFromContext(ctx); sp != nil {
		if _, ok := sp.Tracer().(opentracing.NoopTracer); !ok {
			child := opentracing.StartSpan(opName, opentracing.ChildOf(sp.Context()))
			return child, opentracing.ContextWithSpan(ctx, child)
		}
	}
	return noopSpan(), ctx
}

type SessionTracing struct {
	Enable   bool
	RootCtx  context.Context
	span     opentracing.Span
	recorder *basictracer.InMemorySpanRecorder
}

func (t *SessionTracing) Start() {
	if t.Enable {
		return
	}
	t.Enable = true
	t.recorder = basictracer.NewInMemoryRecorder()
	t.span = basictracer.New(t.recorder).StartSpan("start session tracing")
	t.RootCtx = opentracing.ContextWithSpan(context.TODO(), t.span)
}

func (t *SessionTracing) Stop() {
	t.Enable = false
	t.RootCtx = nil
	if t.span != nil {
		t.span.Finish()
	}
}

func (t *SessionTracing) Close() {
	t.Enable = false
	t.RootCtx = nil
	t.span = nil
	t.recorder = nil
}

func (t *SessionTracing) GetResultRows() [][]types.Datum {
	const timeFormat = "15:04:05.000000"
	var rows [][]types.Datum
	if t.span != nil {
		allSpans := t.recorder.GetSpans()
		for rIdx := range allSpans {
			span := &allSpans[rIdx]
			record := types.MakeDatums(
				span.Start.Format(timeFormat),            // TIME
				"--- start span "+span.Operation+" ----", // EVENT
				"",             // TAGS
				span.Operation, // SPAN
			)
			rows = append(rows, record)

			var tags string
			if len(span.Tags) > 0 {
				tags = fmt.Sprintf("%v", span.Tags)
			}
			for _, l := range span.Logs {
				for _, field := range l.Fields {
					if field.Key() == logutil.TraceEventKey {
						record := types.MakeDatums(
							l.Timestamp.Format(timeFormat), // TIME
							field.Value().(string),         // EVENT
							tags,                           // TAGS
							span.Operation,                 // SPAN
						)
						rows = append(rows, record)
					}
				}
			}
		}
	}
	return rows
}
