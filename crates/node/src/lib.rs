//! A validator node (design doc sections 2, 4): consensus orders blocks, the mempool feeds them,
//! and a pipelined executor runs committed blocks behind consensus and reports state roots back.

pub mod cluster;
pub mod genesis_file;
pub mod network;
pub mod pipeline;
pub mod rpc;
pub mod run;
pub mod validator;

pub use cluster::{Cluster, ClusterConfig};
pub use validator::NodeEvent;
