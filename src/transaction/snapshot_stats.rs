// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Runtime statistics collected for snapshot reads.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;

use crate::interceptor::{RpcDispatchResult, RpcInterceptor, RpcNext};
use crate::proto::kvrpcpb;
use crate::store::Request;

/// Snapshot RPC commands that contribute to [`SnapshotRuntimeStats`].
///
/// This is the native counterpart of client-go's `tikvrpc.CmdType` values
/// observed by `SnapshotRuntimeStats.GetCmdRPCCount`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SnapshotRpcCommand {
    Get,
    BatchGet,
    BufferBatchGet,
    Scan,
}

impl fmt::Display for SnapshotRpcCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Get => "Get",
            Self::BatchGet => "BatchGet",
            Self::BufferBatchGet => "BufferBatchGet",
            Self::Scan => "Scan",
        })
    }
}

#[derive(Clone, Default)]
struct RpcRuntimeStat {
    count: u64,
    duration: Duration,
}

#[derive(Clone, Default)]
struct SnapshotRuntimeStatsInner {
    rpc: BTreeMap<SnapshotRpcCommand, RpcRuntimeStat>,
}

/// Runtime statistics collected for a snapshot's physical TiKV read RPCs.
///
/// Attach this to a [`crate::Snapshot`] with
/// [`crate::Snapshot::set_runtime_stats`]. The collector is shared so callers
/// can inspect it while a snapshot is active; [`Self::clone`] creates an
/// independent point-in-time copy, matching client-go's runtime-stats clone
/// contract.
#[derive(Default)]
pub struct SnapshotRuntimeStats {
    inner: Mutex<SnapshotRuntimeStatsInner>,
}

impl Clone for SnapshotRuntimeStats {
    fn clone(&self) -> Self {
        Self {
            inner: Mutex::new(
                self.inner
                    .lock()
                    .expect("snapshot stats lock poisoned")
                    .clone(),
            ),
        }
    }
}

impl SnapshotRuntimeStats {
    /// Create an empty snapshot runtime-stat collector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of completed physical RPCs for `command`.
    pub fn rpc_count(&self, command: SnapshotRpcCommand) -> u64 {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .rpc
            .get(&command)
            .map_or(0, |stat| stat.count)
    }

    /// Return the cumulative transport duration for completed physical RPCs
    /// for `command`.
    pub fn rpc_duration(&self, command: SnapshotRpcCommand) -> Duration {
        self.inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .rpc
            .get(&command)
            .map_or(Duration::ZERO, |stat| stat.duration)
    }

    /// Merge another collector into this one, matching client-go's
    /// `SnapshotRuntimeStats.Merge` ownership model.
    pub fn merge(&self, other: &Self) {
        let other = other
            .inner
            .lock()
            .expect("snapshot stats lock poisoned")
            .clone();
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        for (command, stat) in other.rpc {
            let merged = inner.rpc.entry(command).or_default();
            merged.count += stat.count;
            merged.duration += stat.duration;
        }
    }

    pub(crate) fn interceptor(self: &Arc<Self>) -> Arc<dyn RpcInterceptor> {
        Arc::new(SnapshotRuntimeStatsInterceptor {
            stats: Arc::clone(self),
        })
    }

    fn record_rpc(&self, command: SnapshotRpcCommand, duration: Duration) {
        let mut inner = self.inner.lock().expect("snapshot stats lock poisoned");
        let stat = inner.rpc.entry(command).or_default();
        stat.count += 1;
        stat.duration += duration;
    }
}

impl fmt::Display for SnapshotRuntimeStats {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.lock().expect("snapshot stats lock poisoned");
        for (index, (command, stat)) in inner.rpc.iter().enumerate() {
            if index > 0 {
                formatter.write_str(",")?;
            }
            write!(
                formatter,
                "{command}:{{num_rpc:{}, total_time:{:?}}}",
                stat.count, stat.duration
            )?;
        }
        Ok(())
    }
}

struct SnapshotRuntimeStatsInterceptor {
    stats: Arc<SnapshotRuntimeStats>,
}

impl RpcInterceptor for SnapshotRuntimeStatsInterceptor {
    fn name(&self) -> &str {
        "snapshot-runtime-stats"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn wrap<'a>(
        &'a self,
        _: &'a str,
        request: &'a dyn Request,
        next: RpcNext<'a>,
    ) -> BoxFuture<'a, RpcDispatchResult> {
        let command = snapshot_rpc_command(request);
        let stats = Arc::clone(&self.stats);
        Box::pin(async move {
            let started = Instant::now();
            let result = next().await;
            if let Some(command) = command {
                stats.record_rpc(command, started.elapsed());
            }
            result
        })
    }
}

fn snapshot_rpc_command(request: &dyn Request) -> Option<SnapshotRpcCommand> {
    let request = request.as_any();
    if request.is::<kvrpcpb::GetRequest>() {
        Some(SnapshotRpcCommand::Get)
    } else if request.is::<kvrpcpb::BatchGetRequest>() {
        Some(SnapshotRpcCommand::BatchGet)
    } else if request.is::<kvrpcpb::BufferBatchGetRequest>() {
        Some(SnapshotRpcCommand::BufferBatchGet)
    } else if request.is::<kvrpcpb::ScanRequest>() {
        Some(SnapshotRpcCommand::Scan)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_and_merge_preserve_independent_rpc_totals() {
        let stats = SnapshotRuntimeStats::new();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_millis(3));
        let cloned = stats.clone();
        stats.record_rpc(SnapshotRpcCommand::Get, Duration::from_millis(2));

        assert_eq!(cloned.rpc_count(SnapshotRpcCommand::Get), 1);
        assert_eq!(
            cloned.rpc_duration(SnapshotRpcCommand::Get),
            Duration::from_millis(3)
        );

        cloned.merge(&stats);
        assert_eq!(cloned.rpc_count(SnapshotRpcCommand::Get), 3);
        assert_eq!(
            cloned.rpc_duration(SnapshotRpcCommand::Get),
            Duration::from_millis(8)
        );
    }
}
