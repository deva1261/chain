//! Execution layer of the chain (design doc D1, D3, section 6).
//!
//! Consensus hands this crate a committed, ordered list of transactions; a backend runs
//! them through Aptos Block-STM against a read-only [`StateView`] and returns one output
//! per transaction. Applying the resulting write sets to storage is the caller's job, so
//! storage (in-memory now, RocksDB later) stays swappable.
//!
//! [`StateView`]: aptos_types::state_store::StateView

pub mod backend;
pub mod native;
pub mod query;
pub mod state;

pub use backend::{BlockId, ExecutionBackend, ExecutorConfig, MoveVmBackend, verify_signatures};
pub use native::NativeBackend;
pub use state::{InMemoryState, StateStore, block_changes};
