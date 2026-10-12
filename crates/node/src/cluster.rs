//! N validators in one process over the in-process network: the local harness for end-to-end
//! tests and benchmarks before the QUIC transport exists. Each validator has its own RocksDB
//! store, safety-rules file, mempool, execution pipeline and thread.

use crate::{
    network::LocalNetwork,
    pipeline::ExecutionPipeline,
    validator::{self, Input, NodeEvent, ValidatorParts},
};
use anyhow::{Context, Result};
use aptos_crypto::{HashValue, PrivateKey};
use aptos_types::{chain_id::ChainId, transaction::SignedTransaction};
use chain_consensus::{ConsensusConfig, FileSafetyStorage, LeaderSchedule, SafetyRules};
use chain_execution::{ExecutorConfig, MoveVmBackend, StateStore};
use chain_genesis::{Genesis, GenesisAccount};
use chain_mempool::{Mempool, MempoolConfig, VerifiedBatches};
use chain_storage::ChainStore;
use chain_types::{Block, Committee, ValidatorIndex, ValidatorInfo, ValidatorSigner};
use std::{
    sync::{Arc, mpsc},
    thread::JoinHandle,
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub validators: usize,
    pub chain_id: ChainId,
    pub accounts: Vec<GenesisAccount>,
    /// One-way network latency between validators.
    pub latency: Duration,
    /// Block-STM threads per validator.
    pub exec_threads: usize,
    pub base_timeout_ms: u64,
    /// k: committed blocks a validator may leave unexecuted before it stops voting.
    pub exec_lag: u64,
    pub batch_interval: Duration,
    pub max_block_txns: usize,
    pub max_batch_txns: usize,
    /// Block pacing (0 = propose as soon as possible).
    pub block_interval_ms: u64,
}

impl ClusterConfig {
    pub fn new(validators: usize, accounts: Vec<GenesisAccount>) -> Self {
        Self {
            validators,
            chain_id: ChainId::new(42),
            accounts,
            latency: Duration::from_millis(5),
            exec_threads: 2,
            base_timeout_ms: 1_000,
            exec_lag: 4,
            batch_interval: Duration::from_millis(5),
            max_block_txns: 10_000,
            max_batch_txns: 500,
            block_interval_ms: 0,
        }
    }
}

pub struct Cluster {
    inboxes: Vec<mpsc::Sender<Input>>,
    threads: Vec<JoinHandle<()>>,
    network: Option<LocalNetwork>,
    _dir: tempfile::TempDir,
    pub committee: Committee,
    /// Events from every validator. Validator 0 also reports committed transaction hashes.
    pub events: mpsc::Receiver<NodeEvent>,
}

impl Cluster {
    pub fn start(config: ClusterConfig) -> Result<Self> {
        let mut keygen = aptos_keygen::KeyGen::from_seed([11; 32]);
        let mut keys: Vec<_> = (0..config.validators)
            .map(|_| keygen.generate_ed25519_private_key())
            .collect();
        let committee = Committee::new(
            1,
            keys.iter()
                .map(|k| ValidatorInfo {
                    public_key: k.public_key(),
                    network_address: "in-process".into(),
                    stake: 1,
                })
                .collect(),
        )?;
        // Inboxes are addressed by committee index, which orders validators by public key.
        keys.sort_by_key(|k| committee.index_of(&k.public_key()));

        // One genesis for everyone: built once in memory, installed as the same single block.
        let backend_config = ExecutorConfig::new(config.exec_threads);
        let genesis_state = Genesis {
            chain_id: config.chain_id,
            accounts: config.accounts.clone(),
        }
        .build(&MoveVmBackend::new(backend_config))?;
        let genesis_output = genesis_state.snapshot_output()?;
        let genesis_block = Block::genesis(config.chain_id, committee.epoch());
        let seed = HashValue::sha3_256_of(b"cluster epoch seed");

        let dir = tempfile::tempdir()?;
        let (event_tx, events) = mpsc::channel();
        let (inboxes, receivers): (Vec<_>, Vec<_>) =
            (0..config.validators).map(|_| mpsc::channel::<Input>()).unzip();
        let network = LocalNetwork::start(inboxes.clone(), config.latency, Input::Net);

        let mut threads = vec![];
        for (key, inbox) in keys.into_iter().zip(receivers) {
            let signer = ValidatorSigner::for_committee(key, &committee).context("not a member")?;
            let me = signer.index();
            debug_assert_eq!(me as usize, threads.len(), "inbox order follows committee order");
            let node_dir = dir.path().join(format!("validator-{me}"));
            std::fs::create_dir_all(&node_dir)?;

            let mut store = ChainStore::open(node_dir.join("db"))?;
            let block = store.next_block(HashValue::sha3_256_of(b"genesis state"));
            store.apply_block(block, std::slice::from_ref(&genesis_output))?;
            let verified = VerifiedBatches::new(crate::run::VERIFIED_BATCHES);
            let pipeline = ExecutionPipeline::start(
                store,
                Arc::new(MoveVmBackend::new(backend_config)),
                verified.clone(),
                inboxes[me as usize].clone(),
                Input::Pipeline,
            )?;

            let mut mempool_config = MempoolConfig::new(config.chain_id);
            mempool_config.max_block_txns = config.max_block_txns;
            mempool_config.max_batch_txns = config.max_batch_txns;
            let parts = ValidatorParts {
                consensus_config: ConsensusConfig {
                    chain_id: config.chain_id,
                    base_timeout_ms: config.base_timeout_ms,
                    max_timeout_ms: 30_000,
                    exec_lag: Some(config.exec_lag),
                    min_block_interval_ms: config.block_interval_ms,
                },
                schedule: LeaderSchedule::new(committee.clone(), seed, 1),
                safety: SafetyRules::new(
                    signer,
                    FileSafetyStorage::new(node_dir.join("safety.bcs")),
                    committee.epoch(),
                )?,
                mempool: Mempool::new(me, mempool_config).with_verified(verified),
                genesis: genesis_block.clone(),
                pipeline,
                network: Box::new(network.handle(me)),
                batch_interval: config.batch_interval,
                observer: Some(event_tx.clone()),
                report_txn_hashes: me == 0,
            };
            threads.push(
                std::thread::Builder::new()
                    .name(format!("validator-{me}"))
                    .spawn(move || validator::run(parts, inbox))?,
            );
        }
        Ok(Self {
            inboxes,
            threads,
            network: Some(network),
            _dir: dir,
            committee,
            events,
        })
    }

    pub fn submit(&self, node: ValidatorIndex, txn: SignedTransaction) {
        self.inboxes[node as usize].send(Input::Submit(txn)).ok();
    }

    /// A handle that client threads can own to submit transactions.
    pub fn submitter(&self) -> Submitter {
        Submitter {
            inboxes: self.inboxes.clone(),
        }
    }

    pub fn validators(&self) -> usize {
        self.inboxes.len()
    }
}

#[derive(Clone)]
pub struct Submitter {
    inboxes: Vec<mpsc::Sender<Input>>,
}

impl Submitter {
    pub fn submit(&self, node: ValidatorIndex, txn: SignedTransaction) {
        self.inboxes[node as usize].send(Input::Submit(txn)).ok();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for inbox in &self.inboxes {
            inbox.send(Input::Stop).ok();
        }
        for thread in self.threads.drain(..) {
            thread.join().ok();
        }
        drop(self.network.take());
    }
}
