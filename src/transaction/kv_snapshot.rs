// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `txnkv/txnsnapshot.KVSnapshot` as a point read uses it.

use std::sync::Arc;
use std::time::Duration;

use crate::backoff::Backoff;
use crate::interceptor::{RpcInterceptorChain, RpcInterceptorHandle};
use crate::kv::{ReplicaReadAdjuster, ReplicaReadConfig, Variables, DEFAULT_VARIABLES};
use crate::oracle::ReadTimestampValidator;
use crate::pd::{PdClient, PdRpcClient};
use crate::proto::kvrpcpb;
use crate::proto::pdpb::Timestamp;
use crate::request::{EncodeKeyspace, KeyMode, Keyspace};
use crate::resource_control::ResourceGroupControllerHandle;
use crate::timestamp::TimestampExt;
use crate::transaction::transaction::{
    snapshot_read_trace_context_of, snapshot_runtime_interceptor, PointGet,
};
use crate::transaction::{
    Priority, ReadLockContext, RequestSource, ResolveLocksContext, SnapshotResourceGroupTagger,
    SnapshotRuntimeStats, SnapshotVisibilityValidator, TransactionResourceGroupTagger,
};
use crate::{Key, Result, Value};

/// client-go `txnsnapshot.KVSnapshot`: reads at one version through the
/// store, with the settings a statement gives it and no transaction around
/// it. [`crate::TransactionClient::kv_snapshot`] (`KVStore.GetSnapshot`)
/// makes one.
///
/// A [`crate::Transaction`] also carries a write buffer, its commit state
/// and its own lock and heartbeat bookkeeping. A statement that only reads
/// at one version, as Go's point read at `MaxUint64` does, reads through
/// this instead. Its settings and their setters are the snapshot half of
/// [`crate::Transaction`]'s, and its [`Self::get`] sends the same request.
pub struct KvSnapshot<PdC: PdClient = PdRpcClient> {
    timestamp: Timestamp,
    rpc: Arc<PdC>,
    keyspace: Keyspace,
    keyspace_name: Option<String>,
    lock_backoff: Backoff,
    priority: Priority,
    rpc_interceptor: Option<RpcInterceptorChain>,
    runtime_stats: Option<Arc<SnapshotRuntimeStats>>,
    variables: Arc<Variables>,
    resource_group_name: Option<String>,
    snapshot_resource_group_name: Option<String>,
    resource_control: Option<ResourceGroupControllerHandle>,
    ru_details: Option<Arc<crate::RuDetails>>,
    exec_details: Option<Arc<crate::util::ExecDetails>>,
    replica_read_config: ReplicaReadConfig,
    replica_read_adjuster: Option<ReplicaReadAdjuster>,
    not_fill_cache: bool,
    isolation_level: kvrpcpb::IsolationLevel,
    task_id: u64,
    resource_group_tag: Option<Vec<u8>>,
    resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    transaction_resource_group_tagger: Option<TransactionResourceGroupTagger>,
    read_timeout: Option<Duration>,
    read_timestamp_validator: Option<Arc<dyn ReadTimestampValidator>>,
    visibility_validator: Option<Arc<dyn SnapshotVisibilityValidator>>,
    read_replica_scope: String,
    read_lock_context: ReadLockContext,
    lock_resolver_context: ResolveLocksContext,
    request_source: RequestSource,
}

impl<PdC: PdClient> KvSnapshot<PdC> {
    /// A snapshot at `timestamp` over an injected PD/KV client, with the
    /// store's lock resolver and read validation, and every other setting at
    /// the default an optimistic [`crate::Transaction`] starts with.
    ///
    /// [`crate::TransactionClient::kv_snapshot`] builds one for its store;
    /// like [`crate::Transaction::new`], this serves embedded and
    /// in-process stores that hold their own PD/KV client.
    #[doc(hidden)]
    pub fn new(
        timestamp: Timestamp,
        rpc: Arc<PdC>,
        keyspace: Keyspace,
        keyspace_name: Option<String>,
        lock_resolver_context: ResolveLocksContext,
        read_timestamp_validator: Option<Arc<dyn ReadTimestampValidator>>,
        visibility_validator: Option<Arc<dyn SnapshotVisibilityValidator>>,
    ) -> Self {
        KvSnapshot {
            timestamp,
            rpc,
            keyspace,
            keyspace_name,
            // An optimistic transaction's (`TransactionOptions::new_optimistic`).
            lock_backoff: crate::request::RetryOptions::default_optimistic().lock_backoff,
            priority: Priority::Normal,
            rpc_interceptor: None,
            runtime_stats: None,
            variables: DEFAULT_VARIABLES.clone(),
            resource_group_name: None,
            snapshot_resource_group_name: None,
            resource_control: None,
            ru_details: None,
            exec_details: None,
            replica_read_config: ReplicaReadConfig::default(),
            replica_read_adjuster: None,
            not_fill_cache: false,
            isolation_level: kvrpcpb::IsolationLevel::Si,
            task_id: 0,
            resource_group_tag: None,
            resource_group_tagger: None,
            transaction_resource_group_tagger: None,
            read_timeout: None,
            read_timestamp_validator,
            visibility_validator,
            read_replica_scope: String::new(),
            read_lock_context: ReadLockContext::default(),
            lock_resolver_context,
            request_source: RequestSource::default(),
        }
    }

    /// client-go `KVSnapshot.Get`: the value of `key` at the snapshot's
    /// version, resolving the locks the read meets.
    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>> {
        let key = key.into().encode_keyspace(self.keyspace, KeyMode::Txn);
        let internal = self.request_source.is_internal();
        let point_get = self.point_get();
        let read = async move {
            let _timer = crate::stats::snapshot_command_timer("get", internal);
            // client-go `KVSnapshot.get` opens its span and discards the
            // derived ctx.
            crate::trace::detached_client_span("tikvSnapshot.get", point_get.run(key)).await
        };
        match snapshot_read_trace_context_of(self.exec_details.as_ref(), self.timestamp.version()) {
            Some(statement) => crate::trace::with_trace_context(statement, Box::pin(read)).await,
            None => read.await,
        }
    }

    fn point_get(&self) -> PointGet<PdC> {
        let mut replica_read_config = self.replica_read_config.clone();
        if replica_read_config.read_type.is_follower_read() {
            if let Some(adjuster) = &self.replica_read_adjuster {
                replica_read_config.apply_adjustment(adjuster(1));
            }
        }
        PointGet {
            timestamp: self.timestamp.clone(),
            rpc: self.rpc.clone(),
            keyspace: self.keyspace,
            keyspace_name: self.keyspace_name.clone(),
            lock_backoff: self.lock_backoff.clone(),
            rpc_interceptor: snapshot_runtime_interceptor(
                self.rpc_interceptor.clone(),
                self.runtime_stats.clone(),
            ),
            runtime_stats: self.runtime_stats.clone(),
            variables: self.variables.clone(),
            resource_group_name: self
                .snapshot_resource_group_name
                .as_deref()
                .or(self.resource_group_name.as_deref())
                .map(str::to_owned),
            resource_control: self.resource_control.clone(),
            ru_details: self.ru_details.clone(),
            priority: self.priority,
            not_fill_cache: self.not_fill_cache,
            isolation_level: self.isolation_level,
            task_id: self.task_id,
            resource_group_tag: self.resource_group_tag.clone(),
            resource_group_tagger: self.resource_group_tagger.clone(),
            transaction_resource_group_tagger: self.transaction_resource_group_tagger.clone(),
            read_timeout: self.read_timeout,
            read_timestamp_validator: self.read_timestamp_validator.clone(),
            visibility_validator: self.visibility_validator.clone(),
            read_replica_scope: self.read_replica_scope.clone(),
            replica_read_config,
            read_lock_context: self.read_lock_context.clone(),
            lock_resolver_context: self.lock_resolver_context.clone(),
            request_source: self.request_source.context_value(),
        }
    }

    /// The snapshot's version.
    pub fn timestamp(&self) -> &Timestamp {
        &self.timestamp
    }

    /// See [`crate::Transaction::set_replica_read`].
    pub fn set_replica_read(&mut self, read_type: crate::ReplicaReadType) {
        self.replica_read_config = ReplicaReadConfig {
            read_type,
            ..Default::default()
        };
    }

    /// See [`crate::Transaction::set_stale_read`].
    pub fn set_stale_read(&mut self, stale_read: bool) {
        self.replica_read_config.stale_read = stale_read;
    }

    /// See [`crate::Transaction::set_match_store_labels`].
    pub fn set_match_store_labels(&mut self, labels: Vec<crate::proto::metapb::StoreLabel>) {
        self.replica_read_config.labels = labels;
    }

    /// See [`crate::Transaction::set_load_based_replica_read_threshold`].
    pub fn set_load_based_replica_read_threshold(&mut self, busy_threshold: Duration) {
        self.replica_read_config.busy_threshold_ms = u32::try_from(busy_threshold.as_millis())
            .ok()
            .filter(|threshold| *threshold != 0)
            .unwrap_or_default();
    }

    /// See [`crate::Transaction::set_replica_read_adjuster`].
    pub fn set_replica_read_adjuster(&mut self, adjuster: ReplicaReadAdjuster) {
        self.replica_read_adjuster = Some(adjuster);
    }

    /// See [`crate::Transaction::set_isolation_level`].
    pub fn set_isolation_level(&mut self, isolation_level: kvrpcpb::IsolationLevel) {
        self.isolation_level = isolation_level;
    }

    /// See [`crate::Transaction::set_read_replica_scope`].
    pub fn set_read_replica_scope(&mut self, scope: impl Into<String>) {
        self.read_replica_scope = scope.into();
    }

    /// See [`crate::Transaction::set_snapshot_read_timeout`].
    pub fn set_snapshot_read_timeout(&mut self, timeout: Duration) {
        self.read_timeout = (!timeout.is_zero()).then_some(timeout);
    }

    /// See [`crate::Transaction::set_snapshot_resource_group_name`].
    pub fn set_snapshot_resource_group_name(&mut self, name: Option<String>) {
        self.snapshot_resource_group_name = name;
    }

    /// See [`crate::Transaction::set_resource_group_name`].
    pub fn set_resource_group_name(&mut self, resource_group_name: impl Into<String>) {
        self.resource_group_name = Some(resource_group_name.into());
        self.snapshot_resource_group_name = None;
    }

    /// See [`crate::Transaction::set_resource_control`].
    pub fn set_resource_control(&mut self, controller: ResourceGroupControllerHandle) {
        self.resource_control = Some(controller);
    }

    /// See [`crate::Transaction::set_ru_details`].
    pub fn set_ru_details(&mut self, ru_details: Arc<crate::RuDetails>) {
        self.ru_details = Some(ru_details);
    }

    /// See [`crate::Transaction::set_exec_details`].
    pub fn set_exec_details(&mut self, exec_details: Arc<crate::util::ExecDetails>) {
        self.exec_details = Some(exec_details);
    }

    /// See [`crate::Transaction::set_resource_group_tagger`]: the tagger a
    /// transaction's requests share, which a snapshot read applies after its
    /// own tag and tagger.
    pub fn set_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<TransactionResourceGroupTagger>,
    ) {
        self.transaction_resource_group_tagger = resource_group_tagger;
    }

    /// See [`crate::Transaction::set_snapshot_resource_group_tagger`].
    pub fn set_snapshot_resource_group_tagger(
        &mut self,
        resource_group_tagger: Option<SnapshotResourceGroupTagger>,
    ) {
        self.resource_group_tagger = resource_group_tagger;
    }

    /// See [`crate::Transaction::set_resource_group_tag`].
    pub fn set_resource_group_tag(&mut self, resource_group_tag: Option<Vec<u8>>) {
        self.resource_group_tag = resource_group_tag;
    }

    /// See [`crate::Transaction::set_request_source_type`].
    pub fn set_request_source_type(&mut self, source_type: impl Into<String>) {
        self.request_source.source_type = source_type.into();
    }

    /// See [`crate::Transaction::set_request_source_internal`].
    pub fn set_request_source_internal(&mut self, internal: bool) {
        self.request_source.internal = internal;
    }

    /// See [`crate::Transaction::add_rpc_interceptor`].
    pub fn add_rpc_interceptor(&mut self, interceptor: RpcInterceptorHandle) {
        match &mut self.rpc_interceptor {
            Some(chain) => {
                chain.link(interceptor);
            }
            None => {
                let mut chain = RpcInterceptorChain::new();
                chain.link(interceptor);
                self.rpc_interceptor = Some(chain);
            }
        }
    }

    /// See [`crate::Transaction::set_snapshot_runtime_stats`].
    pub fn set_snapshot_runtime_stats(&mut self, stats: Option<Arc<SnapshotRuntimeStats>>) {
        self.runtime_stats = stats;
    }

    /// See [`crate::Transaction::set_snapshot_variables`].
    pub fn set_snapshot_variables(&mut self, variables: Arc<Variables>) {
        self.variables = variables;
    }

    /// See [`crate::Transaction::set_priority`].
    pub fn set_priority(&mut self, priority: Priority) {
        self.priority = priority;
    }

    /// See [`crate::Transaction::set_not_fill_cache`].
    pub fn set_not_fill_cache(&mut self, not_fill_cache: bool) {
        self.not_fill_cache = not_fill_cache;
    }

    /// See [`crate::Transaction::set_task_id`].
    pub fn set_task_id(&mut self, task_id: u64) {
        self.task_id = task_id;
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Mutex;

    use super::*;
    use crate::mock::{MockKvClient, MockPdClient};
    use crate::timestamp::TimestampExt;
    use crate::{CheckLevel, Transaction, TransactionOptions};

    /// client-go's `KVTxn.Get` reads through its `KVSnapshot`: a snapshot
    /// read sends the request a transaction's read at the same version and
    /// settings sends.
    #[tokio::test]
    async fn a_snapshot_get_sends_the_request_a_transaction_get_sends() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = Arc::clone(&requests);
        let pd_client = Arc::new(MockPdClient::new(MockKvClient::with_dispatch_hook(
            move |request: &dyn Any| {
                let request = request.downcast_ref::<kvrpcpb::GetRequest>().unwrap();
                captured.lock().unwrap().push(request.clone());
                Ok(Box::new(kvrpcpb::GetResponse {
                    value: b"value".to_vec(),
                    ..Default::default()
                }) as Box<dyn Any>)
            },
        )));
        let timestamp = Timestamp::from_version(u64::MAX);
        let mut transaction = Transaction::new(
            timestamp.clone(),
            Arc::clone(&pd_client),
            TransactionOptions::new_optimistic()
                .read_only()
                .drop_check(CheckLevel::None),
            Keyspace::Disable,
        );
        let mut snapshot = KvSnapshot::new(
            timestamp,
            pd_client,
            Keyspace::Disable,
            None,
            ResolveLocksContext::default(),
            None,
            None,
        );
        transaction.set_priority(Priority::High);
        snapshot.set_priority(Priority::High);
        transaction.set_not_fill_cache(true);
        snapshot.set_not_fill_cache(true);
        transaction.set_task_id(7);
        snapshot.set_task_id(7);
        transaction.set_isolation_level(kvrpcpb::IsolationLevel::Rc);
        snapshot.set_isolation_level(kvrpcpb::IsolationLevel::Rc);
        transaction.set_resource_group_name("rg");
        snapshot.set_resource_group_name("rg");
        transaction.set_resource_group_tag(Some(b"tag".to_vec()));
        snapshot.set_resource_group_tag(Some(b"tag".to_vec()));
        transaction.set_request_source_internal(true);
        snapshot.set_request_source_internal(true);
        transaction.set_request_source_type("lightning");
        snapshot.set_request_source_type("lightning");
        transaction.set_snapshot_read_timeout(Duration::from_millis(250));
        snapshot.set_snapshot_read_timeout(Duration::from_millis(250));

        assert_eq!(
            transaction.get(b"key".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(
            snapshot.get(b"key".to_vec()).await.unwrap(),
            Some(b"value".to_vec())
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
}
