//! Consensus (design doc section 5): stake-weighted HRW leader election and 2-chain HotStuff
//! (Jolteon-style) with Aptos' safety rules, as an I/O-free state machine.

pub mod block_tree;
mod core;
pub mod hrw;
pub mod safety;

pub use core::{
    Action, BlockArchive, Consensus, ConsensusConfig, ConsensusMessage, InMemoryBlockArchive,
    PayloadSource, Stats,
};
pub use hrw::LeaderSchedule;
pub use safety::{FileSafetyStorage, InMemorySafetyStorage, SafetyData, SafetyRules, SafetyStorage};
