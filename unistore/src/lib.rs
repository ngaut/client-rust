//! Reusable, in-process UniStore substrate.
//!
//! This crate is intentionally independent of `tikv-client`: client modules,
//! integration tests, and future mock-server adapters can all depend on the
//! same MVCC model without introducing a dependency cycle. Its source mapping
//! is TiDB's `pkg/store/mockstore/unistore`; protocol/RPC adapters are added
//! here as their source packages are ported.

mod mock;
mod mvcc;

pub use mock::{
    Action, Assertion, AssertionLevel, IsolationLevel, LockInfo, LockRecord, MockEngine, MockError,
    MvccInfo, MvccValue, MvccWrite, Op, Pair, PessimisticAction, PessimisticLockKeyResult,
    PessimisticLockKeyResultType, PessimisticLockRequest, PessimisticWakeUpMode, PrewriteRequest,
    TxnMutation, WriteRecord, WriteType,
};
pub use mvcc::{Mutation, MvccError, MvccStore, Timestamp, VersionedValue};
