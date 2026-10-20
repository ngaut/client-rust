// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Client-side tracing hooks and TiKV trace-control flags.

use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::ops::{BitOr, BitOrAssign};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use lazy_static::lazy_static;

use crate::proto::kvrpcpb;

/// One source `spanInfo` node reconstructed from TiKV `ExecDetailsV2`.
///
/// `duration` is the duration explicitly reported for this node. A zero value
/// is intentionally retained for formatting; timeline construction derives it
/// from synchronous children exactly as client-go's `calcDur` does.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDetailSpan {
    pub name: &'static str,
    pub duration: Duration,
    pub asynchronous: bool,
    pub children: Vec<ExecutionDetailSpan>,
}

impl ExecutionDetailSpan {
    fn leaf(name: &'static str, nanos: u64) -> Self {
        Self {
            name,
            duration: Duration::from_nanos(nanos),
            asynchronous: false,
            children: Vec::new(),
        }
    }

    fn calculate_duration(&mut self) -> Duration {
        if self.duration.is_zero() {
            self.duration = self
                .children
                .iter_mut()
                .filter_map(|child| {
                    let duration = child.calculate_duration();
                    (!child.asynchronous).then_some(duration)
                })
                .sum();
        }
        self.duration
    }

    fn calculate_timeline_durations(&mut self) {
        self.calculate_duration();
        for child in &mut self.children {
            child.calculate_timeline_durations();
        }
    }

    fn append_timeline(
        &self,
        start_offset: Duration,
        output: &mut Vec<ExecutionDetailTiming>,
    ) -> Duration {
        if self.duration.is_zero() {
            return start_offset;
        }
        let mut child_offset = start_offset;
        for child in &self.children {
            child_offset = child.append_timeline(child_offset, output);
        }
        output.push(ExecutionDetailTiming {
            name: self.name,
            start_offset,
            duration: self.duration,
            asynchronous: self.asynchronous,
        });
        if self.asynchronous {
            start_offset
        } else {
            start_offset + self.duration
        }
    }

    /// client-go `spanInfo.addTo`: report this normalized tree as historical
    /// client spans starting at `start`, returning where a following sibling
    /// starts.
    fn add_client_spans(&self, parent: &ClientSpanContext, start: Instant) -> Instant {
        if self.duration.is_zero() {
            return start;
        }
        let end = start + self.duration;
        let id = NEXT_CLIENT_SPAN_ID.fetch_add(1, AtomicOrdering::Relaxed);
        (parent.handler)(ClientSpanEvent::Start {
            id,
            parent: parent.parent,
            name: Cow::Borrowed(self.name),
            at: start,
        });
        let context = ClientSpanContext {
            handler: parent.handler.clone(),
            parent: Some(id),
        };
        let mut child_start = start;
        for child in &self.children {
            child_start = child.add_client_spans(&context, child_start);
        }
        (parent.handler)(ClientSpanEvent::Finish { id, at: end });
        if self.asynchronous {
            start
        } else {
            end
        }
    }

    /// Return child-before-parent span timings, matching the finish order of
    /// client-go's recursive `spanInfo.addTo` implementation.
    pub fn timeline(&self) -> Vec<ExecutionDetailTiming> {
        let mut normalized = self.clone();
        normalized.calculate_timeline_durations();
        let mut output = Vec::new();
        normalized.append_timeline(Duration::ZERO, &mut output);
        output
    }
}

impl fmt::Display for ExecutionDetailSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name)?;
        if self.asynchronous {
            formatter.write_str("'")?;
        }
        if !self.duration.is_zero() {
            write!(formatter, "[{}]", format_go_duration(self.duration))?;
        }
        if !self.children.is_empty() {
            formatter.write_str("{")?;
            for child in &self.children {
                write!(formatter, " {child}")?;
            }
            formatter.write_str(" }")?;
        }
        Ok(())
    }
}

/// Historical timing for one emitted execution-detail span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDetailTiming {
    pub name: &'static str,
    pub start_offset: Duration,
    pub duration: Duration,
    pub asynchronous: bool,
}

/// Per-task sink used by [`with_trace_exec_details`]. The first argument is
/// the physical RPC start instant; the tree supplies source-exact offsets.
pub type ExecutionDetailsTraceHandler = Arc<dyn Fn(Instant, &ExecutionDetailSpan) + Send + Sync>;

tokio::task_local! {
    static EXECUTION_DETAILS_TRACE_HANDLER: ExecutionDetailsTraceHandler;
}

/// Enable TiKV execution-detail tracing for one asynchronous operation.
///
/// This is the native async counterpart of client-go's
/// `ContextWithTraceExecDetails`: every physical RPC awaited by `future`
/// reports its reconstructed historical span tree to `handler`.
pub async fn with_trace_exec_details<F>(
    handler: ExecutionDetailsTraceHandler,
    future: F,
) -> F::Output
where
    F: Future,
{
    EXECUTION_DETAILS_TRACE_HANDLER.scope(handler, future).await
}

pub(crate) fn current_execution_details_trace_handler() -> Option<ExecutionDetailsTraceHandler> {
    EXECUTION_DETAILS_TRACE_HANDLER
        .try_with(ExecutionDetailsTraceHandler::clone)
        .ok()
}

/// Return whether the current asynchronous scope requests TiKV execution details.
pub fn trace_exec_details_enabled() -> bool {
    EXECUTION_DETAILS_TRACE_HANDLER.try_with(|_| ()).is_ok()
}

/// One event of a client-go opentracing span, reported to the handler of
/// [`with_client_spans`].
///
/// client-go opens its spans as children of the span in the request's
/// `context.Context` (`opentracing.SpanFromContext(ctx)`), which is how
/// TiDB's `TRACE` statement shows them. The Rust counterpart of that ctx span
/// is the innermost client span open in the current asynchronous task:
/// `parent` names it, and `None` means the caller's own span, the one that
/// was current when [`with_client_spans`] was entered. `Log` events are
/// client-go's `logutil.Event`/`Eventf` on the ctx span, with the same `id`
/// convention.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientSpanEvent {
    Start {
        id: u64,
        parent: Option<u64>,
        name: Cow<'static, str>,
        at: Instant,
    },
    Finish {
        id: u64,
        at: Instant,
    },
    Log {
        id: Option<u64>,
        at: Instant,
        event: String,
    },
}

/// Sink for [`ClientSpanEvent`]s. It is called synchronously on the task
/// that produced the event, so it should only record.
pub type ClientSpanHandler = Arc<dyn Fn(ClientSpanEvent) + Send + Sync>;

/// The client span scope of one asynchronous task: the handler and the
/// innermost open client span, the counterpart of client-go's ctx span.
///
/// Tokio task locals do not follow `tokio::spawn`; capture this with
/// [`current_client_span_context`] before spawning and re-enter it with
/// [`with_client_span_context`] inside the spawned task.
#[derive(Clone)]
pub struct ClientSpanContext {
    handler: ClientSpanHandler,
    parent: Option<u64>,
}

impl fmt::Debug for ClientSpanContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientSpanContext")
            .field("parent", &self.parent)
            .finish_non_exhaustive()
    }
}

impl ClientSpanContext {
    /// client-go `logutil.Event` on this context's span.
    pub(crate) fn log(&self, event: String) {
        (self.handler)(ClientSpanEvent::Log {
            id: self.parent,
            at: Instant::now(),
            event,
        });
    }
}

tokio::task_local! {
    static CLIENT_SPAN_CONTEXT: ClientSpanContext;
}

/// Span ids are unique per process; zero is never allocated.
static NEXT_CLIENT_SPAN_ID: AtomicU64 = AtomicU64::new(1);

/// Report client-go's opentracing spans for one asynchronous operation.
///
/// Every client span opened while `future` runs (`regionRequest.SendReqCtx`,
/// `tikvSnapshot.get`, `twoPhaseCommitter.prewriteMutations`, ...) and every
/// span log event reaches `handler`. Spans the client opens in tasks it
/// spawns for this operation inherit the scope. Outside such a scope nothing
/// is recorded; each would-be span costs one task-local lookup.
pub async fn with_client_spans<F>(handler: ClientSpanHandler, future: F) -> F::Output
where
    F: Future,
{
    CLIENT_SPAN_CONTEXT
        .scope(
            ClientSpanContext {
                handler,
                parent: None,
            },
            future,
        )
        .await
}

/// The client span scope of the current task, if any.
pub fn current_client_span_context() -> Option<ClientSpanContext> {
    CLIENT_SPAN_CONTEXT.try_with(ClientSpanContext::clone).ok()
}

/// Run `future` in a scope captured by [`current_client_span_context`],
/// typically in a task spawned for the operation that owned it. `None` runs
/// `future` unchanged.
pub async fn with_client_span_context<F>(context: Option<ClientSpanContext>, future: F) -> F::Output
where
    F: Future,
{
    match context {
        Some(context) => CLIENT_SPAN_CONTEXT.scope(context, future).await,
        None => future.await,
    }
}

/// Whether the current task reports client spans.
pub fn client_spans_enabled() -> bool {
    CLIENT_SPAN_CONTEXT.try_with(|_| ()).is_ok()
}

/// Capture the current client span scope for a future about to be spawned.
pub(crate) fn inherit_client_spans<F>(future: F) -> impl Future<Output = F::Output>
where
    F: Future,
{
    with_client_span_context(current_client_span_context(), future)
}

/// One open client span. Dropping it finishes the span, as client-go's
/// `defer span1.Finish()`, including when the owning future is cancelled.
pub(crate) struct ClientSpan {
    handler: ClientSpanHandler,
    id: u64,
}

impl ClientSpan {
    /// client-go `span.Tracer().StartSpan(name, opentracing.ChildOf(...))`
    /// under `context`'s span.
    pub(crate) fn start(context: &ClientSpanContext, name: Cow<'static, str>) -> Self {
        let id = NEXT_CLIENT_SPAN_ID.fetch_add(1, AtomicOrdering::Relaxed);
        (context.handler)(ClientSpanEvent::Start {
            id,
            parent: context.parent,
            name,
            at: Instant::now(),
        });
        Self {
            handler: context.handler.clone(),
            id,
        }
    }

    /// The scope whose ctx span is this span
    /// (`opentracing.ContextWithSpan(ctx, span1)`).
    pub(crate) fn context(&self) -> ClientSpanContext {
        ClientSpanContext {
            handler: self.handler.clone(),
            parent: Some(self.id),
        }
    }

    /// Run `future` with this span as its ctx span.
    pub(crate) async fn scope<F: Future>(&self, future: F) -> F::Output {
        CLIENT_SPAN_CONTEXT.scope(self.context(), future).await
    }
}

impl Drop for ClientSpan {
    fn drop(&mut self) {
        (self.handler)(ClientSpanEvent::Finish {
            id: self.id,
            at: Instant::now(),
        });
    }
}

/// Run `future` with `span`, when open, as its ctx span.
pub(crate) async fn in_client_span<F: Future>(span: Option<&ClientSpan>, future: F) -> F::Output {
    match span {
        Some(span) => span.scope(future).await,
        None => future.await,
    }
}

/// Open `name` under the current task's ctx span, if any.
pub(crate) fn start_client_span(name: &'static str) -> Option<ClientSpan> {
    CLIENT_SPAN_CONTEXT
        .try_with(|context| ClientSpan::start(context, Cow::Borrowed(name)))
        .ok()
}

/// A client-go span that becomes the ctx span of the work it encloses:
/// `span1 := StartSpan(name, ChildOf(ctx span)); ctx = ContextWithSpan(ctx, span1)`.
pub(crate) async fn client_span<F: Future>(name: &'static str, future: F) -> F::Output {
    client_span_in(current_client_span_context(), name, future).await
}

/// [`client_span`] under an explicitly captured scope.
pub(crate) async fn client_span_in<F: Future>(
    context: Option<ClientSpanContext>,
    name: &'static str,
    future: F,
) -> F::Output {
    let Some(context) = context else {
        return future.await;
    };
    let span = ClientSpan::start(&context, Cow::Borrowed(name));
    let output = span.scope(future).await;
    drop(span);
    output
}

/// [`client_span`] with a name built only when spans are reported.
pub(crate) async fn client_span_named<F: Future>(
    name: impl FnOnce() -> Cow<'static, str>,
    future: F,
) -> F::Output {
    let Some(span) = CLIENT_SPAN_CONTEXT
        .try_with(|context| ClientSpan::start(context, name()))
        .ok()
    else {
        return future.await;
    };
    let output = span.scope(future).await;
    drop(span);
    output
}

/// A client-go span whose derived ctx is discarded, so work it encloses still
/// reports to the enclosing ctx span (`tikvSnapshot.get`, `tikv.backoff.*`,
/// and the region-cache loads whose PD calls open no spans).
pub(crate) async fn detached_client_span<F: Future>(name: &'static str, future: F) -> F::Output {
    detached_client_span_named(|| Cow::Borrowed(name), future).await
}

/// [`detached_client_span`] with a name built only when spans are reported.
pub(crate) async fn detached_client_span_named<F: Future>(
    name: impl FnOnce() -> Cow<'static, str>,
    future: F,
) -> F::Output {
    let span = CLIENT_SPAN_CONTEXT
        .try_with(|context| ClientSpan::start(context, name()))
        .ok();
    let output = future.await;
    drop(span);
    output
}

/// client-go `logutil.Eventf(ctx, ...)`: a log event on the current ctx span.
/// `event` runs only when the task reports client spans.
pub(crate) fn client_span_event(event: impl FnOnce() -> String) {
    let _ = CLIENT_SPAN_CONTEXT.try_with(|context| context.log(event()));
}

/// Build client-go's exact execution-detail span tree.
pub fn build_execution_detail_span(
    details: &kvrpcpb::ExecDetailsV2,
) -> Option<ExecutionDetailSpan> {
    let (rpc_duration, wait_duration, process_duration, suspend_duration, has_v2) =
        if let Some(time) = details.time_detail_v2.as_ref() {
            (
                time.total_rpc_wall_time_ns,
                time.wait_wall_time_ns,
                time.process_wall_time_ns,
                time.process_suspend_wall_time_ns,
                true,
            )
        } else {
            let time = details.time_detail.as_ref()?;
            (
                time.total_rpc_wall_time_ns,
                time.wait_wall_time_ms.saturating_mul(1_000_000),
                time.process_wall_time_ms.saturating_mul(1_000_000),
                0,
                false,
            )
        };

    let mut wait = ExecutionDetailSpan::leaf("tikv.Wait", wait_duration);
    let mut process = ExecutionDetailSpan::leaf("tikv.Process", process_duration);
    if let Some(scan) = details.scan_detail_v2.as_ref() {
        wait.children.push(ExecutionDetailSpan::leaf(
            "tikv.GetSnapshot",
            scan.get_snapshot_nanos,
        ));
        if details.write_detail.is_none() {
            process.children.push(ExecutionDetailSpan::leaf(
                "tikv.RocksDBBlockRead",
                scan.rocksdb_block_read_nanos,
            ));
        }
    }

    let mut root = ExecutionDetailSpan::leaf("tikv.RPC", rpc_duration);
    root.children.push(wait);
    root.children.push(process);
    if has_v2 {
        root.children
            .push(ExecutionDetailSpan::leaf("tikv.Suspend", suspend_duration));
    }

    if let Some(write) = details.write_detail.as_ref() {
        let mut persist_log = ExecutionDetailSpan::leaf("tikv.PersistLog", write.persist_log_nanos);
        persist_log.asynchronous = true;
        persist_log.children = vec![
            ExecutionDetailSpan::leaf(
                "tikv.RaftDBWriteWait",
                write.raft_db_write_leader_wait_nanos,
            ),
            ExecutionDetailSpan::leaf("tikv.RaftDBWriteWAL", write.raft_db_sync_log_nanos),
            ExecutionDetailSpan::leaf(
                "tikv.RaftDBWriteMemtable",
                write.raft_db_write_memtable_nanos,
            ),
        ];
        let mut apply_log = ExecutionDetailSpan::leaf("tikv.ApplyLog", write.apply_log_nanos);
        apply_log.children = vec![
            ExecutionDetailSpan::leaf("tikv.ApplyMutexLock", write.apply_mutex_lock_nanos),
            ExecutionDetailSpan::leaf(
                "tikv.ApplyWriteLeaderWait",
                write.apply_write_leader_wait_nanos,
            ),
            ExecutionDetailSpan::leaf("tikv.ApplyWriteWAL", write.apply_write_wal_nanos),
            ExecutionDetailSpan::leaf("tikv.ApplyWriteMemtable", write.apply_write_memtable_nanos),
        ];
        root.children.push(ExecutionDetailSpan {
            name: "tikv.AsyncWrite",
            duration: Duration::ZERO,
            asynchronous: false,
            children: vec![
                ExecutionDetailSpan::leaf("tikv.StoreBatchWait", write.store_batch_wait_nanos),
                ExecutionDetailSpan::leaf("tikv.ProposeSendWait", write.propose_send_wait_nanos),
                persist_log,
                ExecutionDetailSpan::leaf("tikv.CommitLog", write.commit_log_nanos),
                ExecutionDetailSpan::leaf("tikv.ApplyBatchWait", write.apply_batch_wait_nanos),
                apply_log,
            ],
        });
    }
    Some(root)
}

pub(crate) fn trace_exec_details_response(started_at: Instant, response: &dyn Any) {
    let Ok(handler) = EXECUTION_DETAILS_TRACE_HANDLER.try_with(ExecutionDetailsTraceHandler::clone)
    else {
        return;
    };
    let Some(details) = crate::store::exec_details_v2(response) else {
        return;
    };
    let Some(span) = build_execution_detail_span(details) else {
        return;
    };
    handler(started_at, &span);
    // client-go `spanInfo.addTo(spanRPC, start)`: with execution details
    // enabled, the reconstructed tree also hangs under the RPC's span.
    if let Ok(context) = CLIENT_SPAN_CONTEXT.try_with(ClientSpanContext::clone) {
        let mut normalized = span;
        normalized.calculate_timeline_durations();
        normalized.add_client_spans(&context, started_at);
    }
}

fn format_go_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    if nanos == 0 {
        return "0s".to_owned();
    }
    fn decimal(whole: u128, remainder: u128, width: usize, suffix: &str) -> String {
        if remainder == 0 {
            return format!("{whole}{suffix}");
        }
        let fraction = format!("{remainder:0width$}")
            .trim_end_matches('0')
            .to_owned();
        format!("{whole}.{fraction}{suffix}")
    }
    if nanos < 1_000 {
        return format!("{nanos}ns");
    }
    if nanos < 1_000_000 {
        return decimal(nanos / 1_000, nanos % 1_000, 3, "µs");
    }
    if nanos < 1_000_000_000 {
        return decimal(nanos / 1_000_000, nanos % 1_000_000, 6, "ms");
    }

    let seconds = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let hours = seconds / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let seconds = seconds % 60;
    let seconds = decimal(seconds, fraction, 9, "s");
    if hours > 0 {
        format!("{hours}h{minutes}m{seconds}")
    } else if minutes > 0 {
        format!("{minutes}m{seconds}")
    } else {
        seconds
    }
}

/// Trace logging control bits sent to TiKV.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct TraceControlFlags(pub u64);

impl TraceControlFlags {
    pub const IMMEDIATE_LOG: Self = Self(1 << 0);
    pub const TIKV_CATEGORY_REQUEST: Self = Self(1 << 1);
    pub const TIKV_CATEGORY_WRITE_DETAILS: Self = Self(1 << 2);
    pub const TIKV_CATEGORY_READ_DETAILS: Self = Self(1 << 3);

    pub const fn has(self, flag: Self) -> bool {
        self.0 & flag.0 != 0
    }

    pub const fn with(self, flag: Self) -> Self {
        Self(self.0 | flag.0)
    }
}

impl BitOr for TraceControlFlags {
    type Output = Self;

    fn bitor(self, right: Self) -> Self::Output {
        Self(self.0 | right.0)
    }
}

impl BitOrAssign for TraceControlFlags {
    fn bitor_assign(&mut self, right: Self) {
        self.0 |= right.0;
    }
}

/// Client trace-event family. Known values match client-go, while the public
/// numeric representation retains unknown values introduced by consumers or
/// future client-go versions.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct Category(pub u32);

#[allow(non_upper_case_globals)]
impl Category {
    pub const TransactionTwoPhaseCommit: Self = Self(0);
    pub const TransactionLockResolve: Self = Self(1);
    pub const KvRequest: Self = Self(2);
    pub const RegionCache: Self = Self(3);

    pub const fn from_raw(value: u32) -> Self {
        Self(value)
    }

    pub const fn as_raw(self) -> u32 {
        self.0
    }
}

/// Native structured field passed to a trace event handler.
#[derive(Clone)]
pub struct TraceField {
    pub name: String,
    value: Arc<dyn Any + Send + Sync>,
}

impl TraceField {
    pub fn new<V>(name: impl Into<String>, value: V) -> Self
    where
        V: Any + Send + Sync,
    {
        Self {
            name: name.into(),
            value: Arc::new(value),
        }
    }

    pub fn value<V: Any + Send + Sync>(&self) -> Option<&V> {
        self.value.downcast_ref()
    }
}

impl std::fmt::Debug for TraceField {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TraceField")
            .field("name", &self.name)
            .field("value_type", &self.value.type_id())
            .finish()
    }
}

/// Immutable context supporting type-keyed values and trace-ID propagation.
#[derive(Clone, Default)]
pub struct TraceContext {
    values: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
    trace_id: Option<Arc<[u8]>>,
}

impl TraceContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a derived context with a value keyed by the marker type `K`.
    pub fn with_value<K, V>(&self, value: V) -> Self
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        let mut derived = self.clone();
        derived.values.insert(TypeId::of::<K>(), Arc::new(value));
        derived
    }

    pub fn value<K, V>(&self) -> Option<&V>
    where
        K: 'static,
        V: Any + Send + Sync,
    {
        self.values
            .get(&TypeId::of::<K>())
            .and_then(|value| value.downcast_ref())
    }

    pub fn with_trace_id(&self, trace_id: impl Into<Vec<u8>>) -> Self {
        let mut derived = self.clone();
        derived.trace_id = Some(Arc::from(trace_id.into()));
        derived
    }

    pub fn trace_id(&self) -> Option<&[u8]> {
        self.trace_id.as_deref()
    }
}

pub type TraceEventHandler =
    Arc<dyn Fn(&TraceContext, Category, &str, &[TraceField]) + Send + Sync>;
pub type CategoryEnabledHandler = Arc<dyn Fn(Category) -> bool + Send + Sync>;
pub type TraceControlExtractor = Arc<dyn Fn(&TraceContext) -> TraceControlFlags + Send + Sync>;
/// Native counterpart of grpc-opentracing's global text-map injector.
/// Implementations add the active trace carrier to outgoing gRPC metadata.
pub type GrpcTraceMetadataInjector = Arc<dyn Fn(&mut tonic::metadata::MetadataMap) + Send + Sync>;

fn no_op_event(_: &TraceContext, _: Category, _: &str, _: &[TraceField]) {}
fn no_categories(_: Category) -> bool {
    false
}
fn default_trace_control(_: &TraceContext) -> TraceControlFlags {
    TraceControlFlags::TIKV_CATEGORY_REQUEST
}
fn no_op_grpc_trace_injector(_: &mut tonic::metadata::MetadataMap) {}

lazy_static! {
    static ref TRACE_EVENT_HANDLER: RwLock<TraceEventHandler> = RwLock::new(Arc::new(no_op_event));
    static ref CATEGORY_ENABLED_HANDLER: RwLock<CategoryEnabledHandler> =
        RwLock::new(Arc::new(no_categories));
    static ref TRACE_CONTROL_EXTRACTOR: RwLock<TraceControlExtractor> =
        RwLock::new(Arc::new(default_trace_control));
    static ref GRPC_TRACE_METADATA_INJECTOR: RwLock<GrpcTraceMetadataInjector> =
        RwLock::new(Arc::new(no_op_grpc_trace_injector));
}

tokio::task_local! {
    static GRPC_OPEN_TRACING_ENABLED: bool;
    static ACTIVE_TRACE_CONTEXT: TraceContext;
}

/// Run an asynchronous client operation with an active trace context.
///
/// This is Rust's task-local counterpart to passing client-go's
/// `context.Context` through a request. Request plans capture the context so
/// it survives region fan-out into independently spawned tasks.
pub async fn with_trace_context<F>(context: TraceContext, future: F) -> F::Output
where
    F: Future,
{
    ACTIVE_TRACE_CONTEXT.scope(context, future).await
}

pub(crate) fn current_trace_context() -> TraceContext {
    ACTIVE_TRACE_CONTEXT
        .try_with(TraceContext::clone)
        .unwrap_or_default()
}

/// Replace the event handler; `None` restores the no-op implementation.
pub fn set_trace_event_handler(handler: Option<TraceEventHandler>) {
    *TRACE_EVENT_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_op_event));
}

/// Replace the category predicate; `None` restores the always-disabled implementation.
pub fn set_category_enabled_handler(handler: Option<CategoryEnabledHandler>) {
    *CATEGORY_ENABLED_HANDLER.write().unwrap() = handler.unwrap_or_else(|| Arc::new(no_categories));
}

/// Emit an event through the currently registered handler.
pub fn trace_event(context: &TraceContext, category: Category, name: &str, fields: &[TraceField]) {
    let handler = TRACE_EVENT_HANDLER.read().unwrap().clone();
    handler(context, category, name, fields);
}

pub fn is_category_enabled(category: Category) -> bool {
    let handler = CATEGORY_ENABLED_HANDLER.read().unwrap().clone();
    handler(category)
}

/// Replace the control extractor; `None` restores request-category tracing.
pub fn set_trace_control_extractor(extractor: Option<TraceControlExtractor>) {
    *TRACE_CONTROL_EXTRACTOR.write().unwrap() =
        extractor.unwrap_or_else(|| Arc::new(default_trace_control));
}

/// Installs the process-wide carrier injector used when
/// [`crate::Config::open_tracing_enable`] is true. `None` restores the no-op
/// global tracer behavior.
pub fn set_grpc_trace_metadata_injector(injector: Option<GrpcTraceMetadataInjector>) {
    *GRPC_TRACE_METADATA_INJECTOR.write().unwrap() =
        injector.unwrap_or_else(|| Arc::new(no_op_grpc_trace_injector));
}

pub(crate) async fn with_grpc_open_tracing<F>(enabled: bool, future: F) -> F::Output
where
    F: Future,
{
    GRPC_OPEN_TRACING_ENABLED.scope(enabled, future).await
}

pub(crate) fn inject_current_grpc_trace_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    if GRPC_OPEN_TRACING_ENABLED.try_with(|enabled| *enabled) != Ok(true) {
        return;
    }
    let injector = GRPC_TRACE_METADATA_INJECTOR.read().unwrap().clone();
    injector(metadata);
}

pub(crate) fn inject_grpc_trace_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    enabled: bool,
) {
    if !enabled {
        return;
    }
    let injector = GRPC_TRACE_METADATA_INJECTOR.read().unwrap().clone();
    injector(metadata);
}

pub fn trace_control_flags(context: &TraceContext) -> TraceControlFlags {
    let extractor = TRACE_CONTROL_EXTRACTOR.read().unwrap().clone();
    extractor(context)
}

pub fn immediate_logging_enabled(context: &TraceContext) -> bool {
    trace_control_flags(context).has(TraceControlFlags::IMMEDIATE_LOG)
}

#[derive(Clone, Copy)]
pub(crate) enum TransactionBatchKind {
    Prewrite,
    Commit,
}

#[doc(hidden)]
#[derive(Clone)]
pub struct TransactionBatchTrace {
    pub(crate) context: TraceContext,
    pub(crate) kind: TransactionBatchKind,
    pub(crate) start_ts: u64,
    pub(crate) commit_ts: u64,
    pub(crate) region_id: u64,
    pub(crate) is_primary: bool,
    pub(crate) key_count: usize,
}

impl TransactionBatchTrace {
    pub(crate) fn emit_start(&self) {
        if !is_category_enabled(Category::TransactionTwoPhaseCommit) {
            return;
        }
        let mut fields = vec![TraceField::new("startTS", self.start_ts)];
        if matches!(self.kind, TransactionBatchKind::Commit) {
            fields.push(TraceField::new("commitTS", self.commit_ts));
        }
        fields.extend([
            TraceField::new("regionID", self.region_id),
            TraceField::new("isPrimary", self.is_primary),
            TraceField::new("keyCount", self.key_count),
        ]);
        trace_event(
            &self.context,
            Category::TransactionTwoPhaseCommit,
            match self.kind {
                TransactionBatchKind::Prewrite => "prewrite.batch.start",
                TransactionBatchKind::Commit => "commit.batch.start",
            },
            &fields,
        );
    }

    pub(crate) fn emit_result(&self, success: bool, response_error: bool) {
        // client-go's commit path emits a false result only when SendReq
        // itself fails. A key error carried by CommitResponse returns without
        // a commit.batch.result event. Prewrite emits its terminal result for
        // both request and response errors.
        if response_error && matches!(self.kind, TransactionBatchKind::Commit) {
            return;
        }
        if !is_category_enabled(Category::TransactionTwoPhaseCommit) {
            return;
        }
        trace_event(
            &self.context,
            Category::TransactionTwoPhaseCommit,
            match self.kind {
                TransactionBatchKind::Prewrite => "prewrite.batch.result",
                TransactionBatchKind::Commit => "commit.batch.result",
            },
            &[
                TraceField::new("regionID", self.region_id),
                TraceField::new("success", success),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(non_snake_case)]

    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    fn reset_handlers() {
        set_trace_event_handler(None);
        set_category_enabled_handler(None);
        set_trace_control_extractor(None);
        set_grpc_trace_metadata_injector(None);
    }

    #[test]
    fn source_go_trace_flags_test_TestTraceControlFlags_Has() {
        let flags = TraceControlFlags::IMMEDIATE_LOG | TraceControlFlags::TIKV_CATEGORY_REQUEST;
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        assert!(!flags.has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(!flags.has(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));
        assert!(!TraceControlFlags::default().has(TraceControlFlags::IMMEDIATE_LOG));
    }

    #[test]
    fn source_go_trace_flags_test_TestTraceControlFlags_With() {
        let mut flags = TraceControlFlags::default();
        flags = flags.with(TraceControlFlags::IMMEDIATE_LOG);
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(!flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        flags = flags.with(TraceControlFlags::TIKV_CATEGORY_REQUEST);
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        flags = flags.with(TraceControlFlags::IMMEDIATE_LOG);
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
    }

    #[test]
    fn source_go_trace_flags_test_TestTraceControlFlags_CombinedOperations() {
        let flags = TraceControlFlags::default()
            .with(TraceControlFlags::IMMEDIATE_LOG)
            .with(TraceControlFlags::TIKV_CATEGORY_REQUEST)
            .with(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS)
            .with(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS);
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));
    }

    #[test]
    fn source_go_trace_flags_test_TestTraceControlFlags_BitValues() {
        assert_eq!(TraceControlFlags::IMMEDIATE_LOG, TraceControlFlags(1 << 0));
        assert_eq!(
            TraceControlFlags::TIKV_CATEGORY_REQUEST,
            TraceControlFlags(1 << 1)
        );
        assert_eq!(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS,
            TraceControlFlags(1 << 2)
        );
        assert_eq!(
            TraceControlFlags::TIKV_CATEGORY_READ_DETAILS,
            TraceControlFlags(1 << 3)
        );
        assert_ne!(
            TraceControlFlags::IMMEDIATE_LOG,
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        assert_ne!(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS,
            TraceControlFlags::TIKV_CATEGORY_READ_DETAILS
        );
        assert_eq!(
            (TraceControlFlags::IMMEDIATE_LOG
                | TraceControlFlags::TIKV_CATEGORY_REQUEST
                | TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS
                | TraceControlFlags::TIKV_CATEGORY_READ_DETAILS)
                .0,
            0b1111
        );
    }

    #[test]
    #[serial]
    fn source_go_trace_trace_test_TestTraceControlExtractor() {
        reset_handlers();
        let context = TraceContext::new();
        let default_flags = trace_control_flags(&context);
        assert_eq!(default_flags, TraceControlFlags::TIKV_CATEGORY_REQUEST);
        assert!(default_flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        assert!(!default_flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(!immediate_logging_enabled(&context));

        set_trace_control_extractor(Some(Arc::new(|_| {
            TraceControlFlags::IMMEDIATE_LOG | TraceControlFlags::TIKV_CATEGORY_REQUEST
        })));
        let flags = trace_control_flags(&context);
        assert!(flags.has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(flags.has(TraceControlFlags::TIKV_CATEGORY_REQUEST));
        assert!(!flags.has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(immediate_logging_enabled(&context));

        struct FlagsKey;
        set_trace_control_extractor(Some(Arc::new(|context| {
            context
                .value::<FlagsKey, TraceControlFlags>()
                .copied()
                .unwrap_or_default()
        })));
        assert_eq!(trace_control_flags(&context), TraceControlFlags::default());
        assert!(!immediate_logging_enabled(&context));
        let detailed = context.with_value::<FlagsKey, _>(
            TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS
                | TraceControlFlags::TIKV_CATEGORY_READ_DETAILS,
        );
        assert!(trace_control_flags(&detailed).has(TraceControlFlags::TIKV_CATEGORY_WRITE_DETAILS));
        assert!(trace_control_flags(&detailed).has(TraceControlFlags::TIKV_CATEGORY_READ_DETAILS));
        assert!(!trace_control_flags(&detailed).has(TraceControlFlags::IMMEDIATE_LOG));
        assert!(!immediate_logging_enabled(&detailed));

        let immediate = context.with_value::<FlagsKey, _>(TraceControlFlags::IMMEDIATE_LOG);
        assert!(immediate_logging_enabled(&immediate));

        set_trace_control_extractor(None);
        assert_eq!(
            trace_control_flags(&detailed),
            TraceControlFlags::TIKV_CATEGORY_REQUEST
        );
        assert!(!immediate_logging_enabled(&detailed));
        reset_handlers();
    }

    #[test]
    #[serial]
    fn source_go_trace_trace_test_TestTraceEventFunc() {
        reset_handlers();
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        set_trace_event_handler(Some(Arc::new(move |_, category, name, fields| {
            assert_eq!(category, Category::TransactionTwoPhaseCommit);
            assert_eq!(name, "test");
            assert_eq!(fields.len(), 1);
            assert_eq!(fields[0].name, "key");
            assert_eq!(fields[0].value::<&str>(), Some(&"value"));
            observed.store(true, Ordering::SeqCst);
        })));
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[TraceField::new("key", "value")],
        );
        assert!(called.load(Ordering::SeqCst));

        called.store(false, Ordering::SeqCst);
        set_trace_event_handler(None);
        trace_event(
            &TraceContext::new(),
            Category::TransactionTwoPhaseCommit,
            "test",
            &[TraceField::new("key", "value")],
        );
        assert!(!called.load(Ordering::SeqCst));
        reset_handlers();
    }

    #[test]
    #[serial]
    fn source_go_trace_trace_test_TestIsCategoryEnabledFunc() {
        reset_handlers();
        assert!(!is_category_enabled(Category::TransactionTwoPhaseCommit));
        set_category_enabled_handler(Some(Arc::new(|category| {
            category == Category::TransactionTwoPhaseCommit
        })));
        assert!(is_category_enabled(Category::TransactionTwoPhaseCommit));
        assert!(!is_category_enabled(Category::TransactionLockResolve));
        set_category_enabled_handler(None);
        assert!(!is_category_enabled(Category::TransactionTwoPhaseCommit));
        reset_handlers();
    }

    #[test]
    #[serial]
    fn source_uncovered_unknown_category_values_reach_registered_handlers() {
        reset_handlers();
        assert_eq!(Category::default(), Category::TransactionTwoPhaseCommit);
        let unknown = Category(99);
        set_category_enabled_handler(Some(Arc::new(move |category| category == unknown)));
        assert!(is_category_enabled(unknown));

        let observed = Arc::new(AtomicU32::new(0));
        let event_category = observed.clone();
        set_trace_event_handler(Some(Arc::new(move |_, category, _, _| {
            event_category.store(category.0, Ordering::SeqCst);
        })));
        trace_event(&TraceContext::new(), unknown, "future.category", &[]);
        assert_eq!(observed.load(Ordering::SeqCst), 99);
        reset_handlers();
    }

    #[test]
    fn source_go_trace_trace_test_TestTraceIDContext() {
        let context = TraceContext::new();
        assert_eq!(context.trace_id(), None);
        let first = context.with_trace_id(vec![1, 2, 3, 4, 5]);
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
        let second = first.with_trace_id(vec![6, 7, 8, 9, 10]);
        assert_eq!(second.trace_id(), Some(&[6, 7, 8, 9, 10][..]));
        assert_eq!(first.trace_id(), Some(&[1, 2, 3, 4, 5][..]));
    }

    #[tokio::test]
    async fn task_local_trace_context_is_nested_and_restored() {
        struct Marker;

        assert_eq!(current_trace_context().trace_id(), None);
        let outer = TraceContext::new()
            .with_trace_id(vec![1])
            .with_value::<Marker, _>("outer");
        with_trace_context(outer, async {
            assert_eq!(current_trace_context().trace_id(), Some(&[1][..]));
            assert_eq!(
                current_trace_context().value::<Marker, &str>(),
                Some(&"outer")
            );
            with_trace_context(TraceContext::new().with_trace_id(vec![2]), async {
                assert_eq!(current_trace_context().trace_id(), Some(&[2][..]));
            })
            .await;
            assert_eq!(current_trace_context().trace_id(), Some(&[1][..]));
        })
        .await;
        assert_eq!(current_trace_context().trace_id(), None);
    }

    fn timing_millis(span: &ExecutionDetailSpan) -> Vec<(&'static str, u128, u128, bool)> {
        span.timeline()
            .into_iter()
            .map(|timing| {
                (
                    timing.name,
                    timing.start_offset.as_millis(),
                    timing.duration.as_millis(),
                    timing.asynchronous,
                )
            })
            .collect()
    }

    #[test]
    fn execution_detail_tree_and_historical_timeline_match_client_go() {
        assert_eq!(
            build_execution_detail_span(&kvrpcpb::ExecDetailsV2::default()),
            None
        );

        let rpc_only = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: 1_000_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            rpc_only.to_string(),
            "tikv.RPC[1s]{ tikv.Wait tikv.Process tikv.Suspend }"
        );
        assert_eq!(
            timing_millis(&rpc_only),
            vec![("tikv.RPC", 0, 1_000, false)]
        );

        let time = kvrpcpb::TimeDetailV2 {
            total_rpc_wall_time_ns: 1_000_000_000,
            wait_wall_time_ns: 100_000_000,
            process_wall_time_ns: 500_000_000,
            process_suspend_wall_time_ns: 50_000_000,
            ..Default::default()
        };
        let timed = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time.clone()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            timed.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms] tikv.Process[500ms] tikv.Suspend[50ms] }"
        );
        assert_eq!(
            timing_millis(&timed),
            vec![
                ("tikv.Wait", 0, 100, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let scan = kvrpcpb::ScanDetailV2 {
            get_snapshot_nanos: 80_000_000,
            rocksdb_block_read_nanos: 200_000_000,
            ..Default::default()
        };
        let read = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time.clone()),
            scan_detail_v2: Some(scan.clone()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            read.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms]{ tikv.RocksDBBlockRead[200ms] } tikv.Suspend[50ms] }"
        );
        assert_eq!(
            timing_millis(&read),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 100, false),
                ("tikv.RocksDBBlockRead", 100, 200, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let empty_write = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(time),
            scan_detail_v2: Some(scan),
            write_detail: Some(kvrpcpb::WriteDetail::default()),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            empty_write.to_string(),
            "tikv.RPC[1s]{ tikv.Wait[100ms]{ tikv.GetSnapshot[80ms] } tikv.Process[500ms] tikv.Suspend[50ms] tikv.AsyncWrite{ tikv.StoreBatchWait tikv.ProposeSendWait tikv.PersistLog'{ tikv.RaftDBWriteWait tikv.RaftDBWriteWAL tikv.RaftDBWriteMemtable } tikv.CommitLog tikv.ApplyBatchWait tikv.ApplyLog{ tikv.ApplyMutexLock tikv.ApplyWriteLeaderWait tikv.ApplyWriteWAL tikv.ApplyWriteMemtable } } }"
        );
        assert_eq!(
            timing_millis(&empty_write),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 100, false),
                ("tikv.Process", 100, 500, false),
                ("tikv.Suspend", 600, 50, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let write = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                total_rpc_wall_time_ns: 1_000_000_000,
                ..Default::default()
            }),
            scan_detail_v2: Some(kvrpcpb::ScanDetailV2 {
                get_snapshot_nanos: 80_000_000,
                ..Default::default()
            }),
            write_detail: Some(kvrpcpb::WriteDetail {
                store_batch_wait_nanos: 10_000_000,
                propose_send_wait_nanos: 10_000_000,
                persist_log_nanos: 100_000_000,
                raft_db_write_leader_wait_nanos: 20_000_000,
                raft_db_sync_log_nanos: 30_000_000,
                raft_db_write_memtable_nanos: 30_000_000,
                commit_log_nanos: 200_000_000,
                apply_batch_wait_nanos: 20_000_000,
                apply_log_nanos: 300_000_000,
                apply_mutex_lock_nanos: 10_000_000,
                apply_write_leader_wait_nanos: 10_000_000,
                apply_write_wal_nanos: 80_000_000,
                apply_write_memtable_nanos: 50_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            write.to_string(),
            "tikv.RPC[1s]{ tikv.Wait{ tikv.GetSnapshot[80ms] } tikv.Process tikv.Suspend tikv.AsyncWrite{ tikv.StoreBatchWait[10ms] tikv.ProposeSendWait[10ms] tikv.PersistLog'[100ms]{ tikv.RaftDBWriteWait[20ms] tikv.RaftDBWriteWAL[30ms] tikv.RaftDBWriteMemtable[30ms] } tikv.CommitLog[200ms] tikv.ApplyBatchWait[20ms] tikv.ApplyLog[300ms]{ tikv.ApplyMutexLock[10ms] tikv.ApplyWriteLeaderWait[10ms] tikv.ApplyWriteWAL[80ms] tikv.ApplyWriteMemtable[50ms] } } }"
        );
        assert_eq!(
            timing_millis(&write),
            vec![
                ("tikv.GetSnapshot", 0, 80, false),
                ("tikv.Wait", 0, 80, false),
                ("tikv.StoreBatchWait", 80, 10, false),
                ("tikv.ProposeSendWait", 90, 10, false),
                ("tikv.RaftDBWriteWait", 100, 20, false),
                ("tikv.RaftDBWriteWAL", 120, 30, false),
                ("tikv.RaftDBWriteMemtable", 150, 30, false),
                ("tikv.PersistLog", 100, 100, true),
                ("tikv.CommitLog", 100, 200, false),
                ("tikv.ApplyBatchWait", 300, 20, false),
                ("tikv.ApplyMutexLock", 320, 10, false),
                ("tikv.ApplyWriteLeaderWait", 330, 10, false),
                ("tikv.ApplyWriteWAL", 340, 80, false),
                ("tikv.ApplyWriteMemtable", 420, 50, false),
                ("tikv.ApplyLog", 320, 300, false),
                ("tikv.AsyncWrite", 80, 540, false),
                ("tikv.RPC", 0, 1_000, false),
            ]
        );

        let legacy = build_execution_detail_span(&kvrpcpb::ExecDetailsV2 {
            time_detail: Some(kvrpcpb::TimeDetail {
                wait_wall_time_ms: 2,
                process_wall_time_ms: 3,
                total_rpc_wall_time_ns: 6_000_000,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            legacy.to_string(),
            "tikv.RPC[6ms]{ tikv.Wait[2ms] tikv.Process[3ms] }"
        );
    }

    #[tokio::test]
    async fn execution_detail_scope_is_task_local_and_opt_in() {
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = observed.clone();
        let response = kvrpcpb::GetResponse {
            exec_details_v2: Some(kvrpcpb::ExecDetailsV2 {
                time_detail_v2: Some(kvrpcpb::TimeDetailV2 {
                    total_rpc_wall_time_ns: 1_000_000,
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        let start = Instant::now();
        trace_exec_details_response(start, &response);
        assert!(observed.lock().unwrap().is_empty());

        with_trace_exec_details(
            Arc::new(move |started_at, span| {
                sink.lock().unwrap().push((started_at, span.to_string()));
            }),
            async {
                trace_exec_details_response(start, &response);
            },
        )
        .await;
        assert_eq!(
            *observed.lock().unwrap(),
            vec![(
                start,
                "tikv.RPC[1ms]{ tikv.Wait tikv.Process tikv.Suspend }".to_owned()
            )]
        );
    }
}

/// Recording and rendering of [`ClientSpanEvent`]s for tests across the crate.
#[cfg(test)]
pub(crate) mod client_span_testing {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use super::{ClientSpanEvent, ClientSpanHandler};

    pub(crate) type Recorded = Arc<Mutex<Vec<ClientSpanEvent>>>;

    pub(crate) fn recorder() -> (ClientSpanHandler, Recorded) {
        let recorded = Recorded::default();
        let sink = recorded.clone();
        (
            Arc::new(move |event| sink.lock().unwrap().push(event)),
            recorded,
        )
    }

    /// One line per span start (indented two spaces per ancestor) and per
    /// log event (`- event`, one level below its span), in event order.
    /// Panics when a span is not finished or finishes before a child does.
    pub(crate) fn render(recorded: &Recorded) -> Vec<String> {
        let events = recorded.lock().unwrap().clone();
        let mut depth = HashMap::new();
        let mut open = HashMap::new();
        let mut lines = Vec::new();
        for event in events {
            match event {
                ClientSpanEvent::Start {
                    id, parent, name, ..
                } => {
                    let level = parent.map_or(0, |parent| {
                        assert!(
                            open.contains_key(&parent),
                            "{name} starts under a closed span"
                        );
                        depth[&parent] + 1
                    });
                    depth.insert(id, level);
                    open.insert(id, parent);
                    lines.push(format!("{}{name}", "  ".repeat(level)));
                }
                ClientSpanEvent::Finish { id, .. } => {
                    assert!(open.remove(&id).is_some(), "span {id} finished twice");
                    assert!(
                        !open.values().any(|parent| *parent == Some(id)),
                        "span {id} finished before its children"
                    );
                }
                ClientSpanEvent::Log { id, event, .. } => {
                    let level = id.map_or(0, |id| {
                        assert!(open.contains_key(&id), "{event} logged on a closed span");
                        depth[&id] + 1
                    });
                    lines.push(format!("{}- {event}", "  ".repeat(level)));
                }
            }
        }
        assert!(open.is_empty(), "unfinished spans: {open:?}");
        lines
    }
}

#[cfg(test)]
mod client_span_tests {
    use std::borrow::Cow;

    use super::client_span_testing::{recorder, render};
    use super::*;
    use crate::TimestampExt;

    #[tokio::test]
    async fn nothing_is_built_or_recorded_outside_a_scope() {
        assert!(!client_spans_enabled());
        assert!(current_client_span_context().is_none());
        assert!(start_client_span("unused").is_none());
        detached_client_span_named(
            || -> Cow<'static, str> { panic!("a span name was built without a scope") },
            async {},
        )
        .await;
        client_span_event(|| panic!("an event was formatted without a scope"));
        assert_eq!(client_span("unused", async { 7 }).await, 7);

        // A scope in one task does not reach another task.
        let (handler, recorded) = recorder();
        with_client_spans(handler, async {
            tokio::spawn(client_span("detached task", async {}))
                .await
                .unwrap();
        })
        .await;
        assert!(recorded.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn spans_nest_like_go_ctx_spans_and_follow_inherited_spawns() {
        let (handler, recorded) = recorder();
        with_client_spans(handler, async {
            client_span("outer", async {
                detached_client_span("leaf", async {
                    client_span_event(|| "inside leaf".to_owned());
                })
                .await;
                tokio::spawn(inherit_client_spans(client_span("spawned", async {
                    client_span_event(|| "inside spawned".to_owned());
                })))
                .await
                .unwrap();
            })
            .await;
            client_span_event(|| "at root".to_owned());
        })
        .await;
        assert_eq!(
            render(&recorded),
            vec![
                "outer",
                "  leaf",
                "  - inside leaf",
                "  spawned",
                "    - inside spawned",
                "- at root",
            ]
        );
    }

    fn mock_transaction(
        rpc: Arc<crate::mock::MockPdClient>,
    ) -> crate::Transaction<crate::mock::MockPdClient> {
        crate::Transaction::new(
            crate::Timestamp::from_version(1),
            rpc,
            crate::TransactionOptions::new_optimistic()
                .heartbeat_option(crate::transaction::HeartbeatOption::NoHeartbeat)
                .drop_check(crate::CheckLevel::None),
            crate::request::Keyspace::Disable,
        )
    }

    fn mock_pd(
        answer: impl Fn(&dyn Any) -> Box<dyn Any> + Send + Sync + 'static,
    ) -> Arc<crate::mock::MockPdClient> {
        let rpc = Arc::new(crate::mock::MockPdClient::new(
            crate::mock::MockKvClient::with_dispatch_hook(move |request: &dyn Any| {
                Ok(answer(request))
            }),
        ));
        rpc.set_timestamp(crate::Timestamp::from_version(10));
        rpc
    }

    fn default_response(request: &dyn Any) -> Box<dyn Any> {
        if request.downcast_ref::<kvrpcpb::GetRequest>().is_some() {
            return Box::new(kvrpcpb::GetResponse {
                value: b"v".to_vec(),
                ..Default::default()
            });
        }
        if request.downcast_ref::<kvrpcpb::BatchGetRequest>().is_some() {
            return Box::<kvrpcpb::BatchGetResponse>::default();
        }
        if request.downcast_ref::<kvrpcpb::PrewriteRequest>().is_some() {
            return Box::<kvrpcpb::PrewriteResponse>::default();
        }
        if request.downcast_ref::<kvrpcpb::CommitRequest>().is_some() {
            return Box::<kvrpcpb::CommitResponse>::default();
        }
        panic!("unexpected request");
    }

    #[tokio::test]
    async fn point_get_reports_a_snapshot_get_beside_its_region_request() {
        let mut transaction = mock_transaction(mock_pd(default_response));
        let (handler, recorded) = recorder();
        let value = with_client_spans(handler, transaction.get(vec![5_u8])).await;
        assert_eq!(value.unwrap(), Some(b"v".to_vec()));
        // client-go `KVSnapshot.get` discards the ctx carrying its span, so
        // the region request is its sibling.
        let lines = render(&recorded);
        assert_eq!(lines[..2], ["tikvSnapshot.get", "regionRequest.SendReqCtx"]);
        assert_eq!(lines.len(), 3, "{lines:?}");
        assert!(
            lines[2].starts_with("  - send Get request to region 1 at "),
            "{lines:?}"
        );

        // A cached read opens no span.
        let (handler, recorded) = recorder();
        with_client_spans(handler, transaction.get(vec![5_u8]))
            .await
            .unwrap();
        assert!(recorded.lock().unwrap().is_empty());

        // Without a scope nothing reaches a handler.
        let (_handler, recorded) = recorder();
        transaction.get(vec![6_u8]).await.unwrap();
        assert!(recorded.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn batch_get_reports_one_region_request_per_region_task() {
        let mut transaction = mock_transaction(mock_pd(default_response));
        let (handler, recorded) = recorder();
        let pairs = with_client_spans(handler, transaction.batch_get(vec![vec![5_u8], vec![11]]))
            .await
            .unwrap();
        assert_eq!(pairs.count(), 0);
        let mut lines = render(&recorded);
        lines.sort();
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(lines[0].starts_with("  - send BatchGet request to region 1 at "));
        assert!(lines[1].starts_with("  - send BatchGet request to region 2 at "));
        assert_eq!(
            lines[2..],
            ["regionRequest.SendReqCtx", "regionRequest.SendReqCtx"]
        );
    }

    #[tokio::test]
    async fn two_phase_commit_reports_client_go_committer_spans() {
        let mut transaction = mock_transaction(mock_pd(default_response));
        transaction.put(vec![5_u8], b"a".to_vec()).await.unwrap();
        transaction.put(vec![11_u8], b"b".to_vec()).await.unwrap();
        let (handler, recorded) = recorder();
        let commit_ts = with_client_spans(handler, transaction.commit())
            .await
            .unwrap();
        assert_eq!(commit_ts.unwrap().version(), 10);
        let lines = render(&recorded)
            .into_iter()
            .map(|line| match line.find(" request to region ") {
                Some(at) => line[..at + " request to region N".len()].to_owned(),
                None => line,
            })
            .collect::<Vec<_>>();
        assert_eq!(lines.len(), 12, "{lines:#?}");
        assert_eq!(
            lines[..2],
            ["tikvTxn.Commit", "  twoPhaseCommitter.prewriteMutations"]
        );
        // The two prewrite batches run concurrently.
        let mut batches = lines[2..6].to_vec();
        batches.sort();
        assert_eq!(
            batches,
            [
                "      - send Prewrite request to region 1",
                "      - send Prewrite request to region 2",
                "    regionRequest.SendReqCtx",
                "    regionRequest.SendReqCtx",
            ]
        );
        // Secondaries commit in the background on the store's context, outside
        // the caller's trace, as in client-go.
        assert_eq!(
            lines[6..],
            [
                "  - start get commit ts",
                "  TiKVStore.getTimestampWithRetry",
                "  - finish get commit ts",
                "  twoPhaseCommitter.commitMutations",
                "    regionRequest.SendReqCtx",
                "      - send Commit request to region 1",
            ]
        );
    }

    #[tokio::test]
    async fn a_cancelled_span_still_finishes() {
        let (handler, recorded) = recorder();
        with_client_spans(handler, async {
            let pending = client_span("cancelled", std::future::pending::<()>());
            tokio::select! {
                biased;
                _ = pending => unreachable!(),
                _ = async {} => {}
            }
        })
        .await;
        assert_eq!(render(&recorded), vec!["cancelled"]);
    }
}
