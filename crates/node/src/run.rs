//! Starting one validator process (`chain run`).

use crate::{
    genesis_file::GenesisFile,
    network::{Delivery, NetMessage, priority},
    pipeline::ExecutionPipeline,
    rpc::{self, RpcState},
    validator::{self, Input, NodeEvent, ValidatorParts},
};
use anyhow::{Context, Result, bail};
use aptos_crypto::{HashValue, ed25519::Ed25519PrivateKey};
use chain_consensus::{ConsensusConfig, FileSafetyStorage, LeaderSchedule, SafetyRules};
use chain_execution::{ExecutorConfig, MoveVmBackend, StateStore};
use chain_mempool::{Mempool, MempoolConfig, VerifiedBatches};
use chain_network::QuicNetwork;
use chain_storage::ChainStore;
use chain_types::{Block, ValidatorSigner};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

/// Batch digests remembered as verified (see `VerifiedBatches`); older ones are re-verified.
pub const VERIFIED_BATCHES: usize = 100_000;

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub genesis: PathBuf,
    pub key: PathBuf,
    pub data_dir: PathBuf,
    pub rpc: SocketAddr,
    pub exec_threads: usize,
    pub base_timeout_ms: u64,
    pub exec_lag: u64,
    pub max_block_txns: usize,
    pub block_interval_ms: u64,
}

pub fn load_key(path: &Path) -> Result<Ed25519PrivateKey> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Ed25519PrivateKey::try_from(hex::decode(text.trim())?.as_slice())?)
}

/// Runs a validator until it fails. Only fresh data directories are supported for now:
/// consensus does not yet persist blocks and certificates, so a node cannot resume.
pub fn run_node(options: NodeOptions) -> Result<()> {
    let genesis = GenesisFile::load(&options.genesis)?;
    let committee = genesis.committee()?;
    let signer = ValidatorSigner::for_committee(load_key(&options.key)?, &committee)
        .context("this key is not a validator in genesis")?;
    let me = signer.index();
    let address: SocketAddr = committee
        .validator(me)
        .expect("member")
        .network_address
        .parse()?;
    let listen = SocketAddr::new([0, 0, 0, 0].into(), address.port());
    log(me, &format!("starting; genesis {}", &genesis.hash().to_hex()[..16]));

    // Storage with the genesis state.
    std::fs::create_dir_all(&options.data_dir)?;
    let mut store = ChainStore::open(options.data_dir.join("db"))?;
    if store.committed_height() != 0 {
        bail!(
            "{} already holds a chain; restarting a validator is not supported yet",
            options.data_dir.display()
        );
    }
    let backend_config = ExecutorConfig::new(options.exec_threads);
    let start = Instant::now();
    let state = genesis.genesis()?.build(&MoveVmBackend::new(backend_config))?;
    let block = store.next_block(HashValue::sha3_256_of(b"genesis state"));
    store.apply_block(block, &[state.snapshot_output()?])?;
    log(me, &format!("genesis state installed in {:.1?}", start.elapsed()));

    let reader = store.reader();
    let (inbox_tx, inbox) = mpsc::channel::<Input>();
    let verified = VerifiedBatches::new(VERIFIED_BATCHES);
    let pipeline = ExecutionPipeline::start(
        store,
        Arc::new(MoveVmBackend::new(backend_config)),
        verified.clone(),
        inbox_tx.clone(),
        Input::Pipeline,
    )?;

    // Validator mesh; wait for a quorum before starting consensus (design doc 7.2).
    let deliver_tx = inbox_tx.clone();
    let network = Arc::new(QuicNetwork::<NetMessage>::start(
        ValidatorSigner::new(me, load_key(&options.key)?),
        committee.clone(),
        listen,
        move |from, msg| {
            deliver_tx.send(Input::Net(Delivery { from, msg })).ok();
        },
        priority,
    )?);
    let mut next_warning = Instant::now() + Duration::from_secs(30);
    loop {
        let peers = network.connected_peers();
        let stake = committee.stake_of(peers.iter().copied().chain([me]))?;
        if committee.is_quorum(stake) {
            log(me, &format!("connected to validators {peers:?}"));
            break;
        }
        if Instant::now() >= next_warning {
            next_warning += Duration::from_secs(30);
            log(me, &format!("waiting for a quorum; connected to {peers:?} (is UDP open?)"));
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // RPC, fed by validator events.
    let rpc_state = RpcState::new(me, inbox_tx, reader);
    rpc::serve(options.rpc, rpc_state.clone())?;
    log(me, &format!("RPC on http://{}", options.rpc));
    let (observer, events) = mpsc::channel::<NodeEvent>();
    let watcher_network = network.clone();
    std::thread::Builder::new().name("rpc-state".into()).spawn(move || {
        let mut next_log = Instant::now();
        for event in events {
            if let NodeEvent::Failed { node, error } = &event {
                log(*node, &format!("FAILED: {error}"));
            }
            rpc_state.observe(&event);
            if Instant::now() >= next_log {
                next_log = Instant::now() + Duration::from_secs(10);
                rpc_state.set_peers(watcher_network.connected_peers());
                let s = rpc_state.status();
                log(
                    me,
                    &format!(
                        "round {} committed {} executed {} ({} txns) pool {} timeouts {} peers {:?}",
                        s.round, s.committed_height, s.executed_height, s.executed_txns,
                        s.pool_txns, s.timeouts, s.peers
                    ),
                );
            }
        }
    })?;

    let mut mempool_config = MempoolConfig::new(genesis.chain_id());
    mempool_config.max_block_txns = options.max_block_txns;
    let parts = ValidatorParts {
        consensus_config: ConsensusConfig {
            chain_id: genesis.chain_id(),
            base_timeout_ms: options.base_timeout_ms,
            max_timeout_ms: 30_000,
            exec_lag: Some(options.exec_lag),
            min_block_interval_ms: options.block_interval_ms,
        },
        schedule: LeaderSchedule::new(committee.clone(), genesis.epoch_seed()?, 1),
        safety: SafetyRules::new(
            signer,
            FileSafetyStorage::new(options.data_dir.join("safety.bcs")),
            committee.epoch(),
        )?,
        mempool: Mempool::new(me, mempool_config).with_verified(verified),
        genesis: Block::genesis(genesis.chain_id(), committee.epoch()),
        pipeline,
        network: Box::new(network.sender()),
        batch_interval: Duration::from_millis(5),
        observer: Some(observer),
        report_txn_hashes: true,
    };
    validator::run(parts, inbox);
    bail!("validator {me} stopped")
}

fn log(me: u16, message: &str) {
    eprintln!("[validator {me}] {message}");
}
