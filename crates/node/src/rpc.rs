//! Minimal HTTP API (design doc section 2: RPC): submit transactions, read status, and list
//! committed blocks with their transaction hashes so clients can measure finality themselves.
//!
//! - `POST /v1/transactions`: body is BCS `Vec<SignedTransaction>`; returns `{"accepted": n}`.
//! - `GET /v1/status`
//! - `GET /v1/commits?from=H`: committed blocks with height >= H (recent ones only).

use crate::validator::{Input, NodeEvent};
use anyhow::Result;
use aptos_types::transaction::SignedTransaction;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex, mpsc},
    time::{SystemTime, UNIX_EPOCH},
};

/// Committed blocks kept for `/v1/commits`.
const COMMIT_HISTORY: usize = 20_000;
const MAX_COMMITS_PER_RESPONSE: usize = 1_000;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Status {
    pub validator: u16,
    pub round: u64,
    pub committed_height: u64,
    pub executed_height: u64,
    pub executed_txns: u64,
    pub pool_txns: usize,
    pub block_limit: usize,
    pub timeouts: u64,
    pub peers: Vec<u16>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitInfo {
    pub height: u64,
    pub committed_unix_ms: u64,
    /// Hex transaction hashes, in block order.
    pub txn_hashes: Vec<String>,
}

pub struct RpcState {
    status: Mutex<Status>,
    commits: Mutex<VecDeque<CommitInfo>>,
    inbox: Mutex<mpsc::Sender<Input>>,
}

impl RpcState {
    pub fn new(validator: u16, inbox: mpsc::Sender<Input>) -> Arc<Self> {
        Arc::new(Self {
            status: Mutex::new(Status {
                validator,
                ..Status::default()
            }),
            commits: Mutex::new(VecDeque::new()),
            inbox: Mutex::new(inbox),
        })
    }

    pub fn status(&self) -> Status {
        self.status.lock().unwrap().clone()
    }

    pub fn set_peers(&self, peers: Vec<u16>) {
        self.status.lock().unwrap().peers = peers;
    }

    /// Folds a validator event into the published state.
    pub fn observe(&self, event: &NodeEvent) {
        match event {
            NodeEvent::Committed {
                height, txn_hashes, ..
            } => {
                let mut commits = self.commits.lock().unwrap();
                commits.push_back(CommitInfo {
                    height: *height,
                    committed_unix_ms: unix_ms(),
                    txn_hashes: txn_hashes.iter().map(|h| h.to_hex()).collect(),
                });
                while commits.len() > COMMIT_HISTORY {
                    commits.pop_front();
                }
                self.status.lock().unwrap().committed_height = *height;
            },
            NodeEvent::Executed { height, txns, .. } => {
                let mut status = self.status.lock().unwrap();
                status.executed_height = *height;
                status.executed_txns += *txns as u64;
            },
            NodeEvent::Status {
                round,
                consensus,
                pool_txns,
                block_limit,
                ..
            } => {
                let mut status = self.status.lock().unwrap();
                status.round = *round;
                status.pool_txns = *pool_txns;
                status.block_limit = *block_limit;
                status.timeouts = consensus.tcs_formed;
            },
            NodeEvent::StateRoot { .. } | NodeEvent::Failed { .. } => {},
        }
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Serves the API on `addr` until the process exits.
pub fn serve(addr: SocketAddr, state: Arc<RpcState>) -> Result<std::thread::JoinHandle<()>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("rpc")
        .enable_all()
        .build()?;
    let listener = runtime.block_on(tokio::net::TcpListener::bind(addr))?;
    let app = Router::new()
        .route("/v1/transactions", post(submit))
        .route("/v1/status", get(status))
        .route("/v1/commits", get(commits))
        .with_state(state);
    Ok(std::thread::Builder::new().name("rpc".into()).spawn(move || {
        runtime.block_on(async move {
            axum::serve(listener, app).await.ok();
        });
    })?)
}

async fn submit(
    State(state): State<Arc<RpcState>>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let txns: Vec<SignedTransaction> =
        bcs::from_bytes(&body).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let accepted = txns.len();
    let inbox = state.inbox.lock().unwrap();
    for txn in txns {
        inbox
            .send(Input::Submit(txn))
            .map_err(|_| (StatusCode::SERVICE_UNAVAILABLE, "validator stopped".to_string()))?;
    }
    Ok(Json(serde_json::json!({ "accepted": accepted })))
}

async fn status(State(state): State<Arc<RpcState>>) -> Json<Status> {
    Json(state.status())
}

#[derive(Deserialize)]
struct CommitsQuery {
    from: u64,
}

async fn commits(
    State(state): State<Arc<RpcState>>,
    Query(query): Query<CommitsQuery>,
) -> Json<Vec<CommitInfo>> {
    let commits = state.commits.lock().unwrap();
    Json(
        commits
            .iter()
            .filter(|c| c.height >= query.from)
            .take(MAX_COMMITS_PER_RESPONSE)
            .cloned()
            .collect(),
    )
}
