# `tikvrpc/interceptor` source-artifact audit

Source of truth: `tikv/client-go@52c1e76cec993571493c81de442bcbef90cdc106`.

Rust toolchain: `nightly-2026-08-22`.

## Atomic inventory

The pinned package contains exactly three artifacts and 335 lines:

| Artifact | Role | Lines | SHA-256 |
| --- | --- | ---: | --- |
| `tikvrpc/interceptor/interceptor.go` | complete production and public test-support API | 271 | `95644dd740dce11f21aab96e051e451b7c60c7654c05369c0916f8dd78ee202c` |
| `tikvrpc/interceptor/interceptor_test.go` | behavioral unit test | 39 | `c121c40c3e81e57b5c99abc7627741ce3dad9d27a67b288f85dd8f80e3657f5a` |
| `tikvrpc/interceptor/main_test.go` | goleak harness | 25 | `3801183b5ba28354baf2e37a66f4f181b38ded53e5a3fcad86936b27a8fea3f0` |

There is no package-local `doc.go`, build-tag/platform variant, generated input
or output, fixture, benchmark, example, metadata, `OWNERS`, or build file.

## Production surface and Rust integration

`src/interceptor.rs` owns the public decorator contract. `src/lib.rs` exposes
the source-equivalent constructors, traits, handles, chain, continuation/result
types, and mock manager at the crate root. The concrete low-level request trait
is nameable at `tikv_client::tikv::Request`.

| client-go surface | Rust owner and decision |
| --- | --- |
| `RPCInterceptor.Name`, `Wrap` | `RpcInterceptor::name` and async `wrap`; the resolved target, immutable typed request, and one-shot continuation permit before/after logic, replacement, and suppression. |
| `rpcInterceptorWrapper`, `NewRPCInterceptor` | `FunctionRpcInterceptor` and `new_rpc_interceptor`; an owned `Arc` handle replaces Go interface-pointer ownership. |
| `RPCInterceptorFunc` | lifetime-bound `RpcNext` and `RpcDispatchResult`; an awaited future replaces synchronous call/return without changing onion ownership. |
| `RPCInterceptorChain.Len`, constructor | `len`, `is_empty`, `new`, and `Default`. |
| `Link` | `link` flattens concrete chains, removes the first prior matching name, and appends the replacement, preserving source order. |
| chain `Name`, `Wrap` | exact `interceptor-chain` name and recursive outer-to-inner entry/inner-to-outer return. |
| `ChainRPCInterceptors` | `chain_rpc_interceptors` always constructs a new chain and applies the same flatten/duplicate rule. |
| `WithRPCInterceptor`, `GetRPCInterceptorFromCtx` | Rust has no general Go context value map. `Transaction` and `Snapshot` own an optional chain; set/add APIs on async and sync façades propagate it through every shard, retry, commit, heartbeat, rollback, scanner, and lock-resolution dispatch. |
| `MockInterceptorManager` and constructor | Public cloneable `MockInterceptorManager::new/default`; all clones share synchronized counters and entry log. |
| manager create/reset/count/log methods | `create_mock_interceptor`, `reset`, `begin_count`, `end_count`, and `exec_log`; a drop guard preserves Go's deferred end increment if the continuation errors, panics, is replaced, or its future is cancelled. |

The prior Rust trait required every downstream interceptor to implement a
Rust-only `as_any` method solely for chain flattening. A blanket hidden
downcast helper now supplies its default, so external implementations need only
client-go's public name/wrap contract. Existing implementations that override
the hook remain compatible. `MockInterceptorManager` was previously absent
from the public Rust API despite being production source used by client-go's
integration suite; it is now public and root-exported.

## Unit and consumer test mapping

The sole original test is ported independently as
`interceptor::tests::source_test_interceptor`. It uses the public manager,
links `INTERCEPTOR-1` then `INTERCEPTOR-2`, executes one continuation, and
asserts two entries, two returns, and exact entry order. `TestMain` owns only
goleak verification; Rust interceptors spawn no detached work and every
continuation is ownership-bound and awaited.

Source-uncovered package tests exercise duplicate-name replacement and onion
return order, nested-chain flattening, the standalone chain constructor and
fixed chain name, empty/non-empty state, shared manager clones, and reset.
`tests/public_interceptor_tests.rs` compiles in an ordinary downstream crate:
its custom interceptor implements only `name` and `wrap`, then links alongside
the public manager without private features or hooks.

The client-go integration test is ported at
`source_integration_test_interceptor_transaction_commit_and_get`. A transaction
commit records exactly Prewrite and Commit under `INTERCEPTOR-1`; after reset,
a new transaction Get records exactly one request under `INTERCEPTOR-2` and
returns the expected value. Rust explicitly rolls back the read transaction to
satisfy its stronger active-transaction lifecycle guard.

## Direct client-go consumers

Mechanical exact-import search finds seven files:

- Production: `internal/client/client_interceptor.go`,
  `txnkv/transaction/txn.go`, `txnkv/txnsnapshot/snapshot.go`, and
  `txnkv/txnsnapshot/scan.go`.
- Tests: `internal/client/client_interceptor_test.go`,
  `integration_tests/interceptor_test.go`, and
  `integration_tests/snapshot_test.go`.

No other pinned Go file imports
`github.com/tikv/client-go/v2/tikvrpc/interceptor`. The completed owning
receipts for `internal/client`, `txnkv/transaction`, and `txnkv/txnsnapshot`
retain their transport/retry/lifecycle behavior; this package receipt owns the
decorator contract and its direct propagation edges only.

## Validation

Exact pinned Go package and race suites:

```text
env GOCACHE=/private/tmp/client-go-interceptor-build-cache \
    GOMODCACHE=/private/tmp/client-go-interceptor-module-cache \
    /private/tmp/go1.25.12/bin/go test ./tikvrpc/interceptor -count=1
# passed, including TestMain goleak

env GOCACHE=/private/tmp/client-go-interceptor-race-build-cache \
    GOMODCACHE=/private/tmp/client-go-interceptor-module-cache \
    /private/tmp/go1.25.12/bin/go test -race ./tikvrpc/interceptor -count=1
# passed
```

Focused and downstream gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client \
    interceptor::tests::source_ --lib --all-features
# 4 passed

cargo +nightly-2026-08-22 test -p tikv-client \
    source_integration_test_interceptor_transaction_commit_and_get \
    --lib --all-features
# 1 passed

cargo +nightly-2026-08-22 test -p tikv-client \
    --test public_interceptor_tests --no-default-features
cargo +nightly-2026-08-22 test -p tikv-client \
    --test public_interceptor_tests --all-features
# 1 passed in each configuration
```

Complete matrices and strict gates:

```text
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --no-default-features --quiet
# 590 passed
cargo +nightly-2026-08-22 test -p tikv-client source_ --lib \
    --all-features --quiet
# 587 passed

cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --no-default-features --quiet
# 895 passed; 1 unrelated intentional ignore
cargo +nightly-2026-08-22 test -p tikv-client --lib \
    --all-features --quiet
# 892 passed; 1 unrelated intentional ignore

cargo +nightly-2026-08-22 test --workspace --no-default-features --quiet
cargo +nightly-2026-08-22 check --workspace --all-targets --all-features
cargo +nightly-2026-08-22 clippy --workspace --all-targets \
    --all-features -- -D warnings
env RUSTDOCFLAGS='-Dwarnings --document-private-items' \
    cargo +nightly-2026-08-22 doc --workspace --all-features --no-deps
cargo +nightly-2026-08-22 test --workspace --doc --all-features --quiet
# all passed; 51 doctests

cargo +nightly-2026-08-22 fmt --all -- --check
git diff --check
# passed
```

No live TiKV/PD cluster is required for this deterministic decorator package;
the exact physical commit/read integration is executed against the mock
transport, and the repository live matrix remains owned by its completed
repository receipt.
