//! `chain-bench exec`: execution-layer throughput, isolated from consensus and networking.
//!
//! Signs all blocks up front, then for each block measures parallel signature verification
//! (intake cost) and Block-STM execution separately. Block-STM's thread count is fixed per
//! process by Aptos, so compare thread counts by running the binary once per count.

use anyhow::{Result, ensure};
use aptos_crypto::HashValue;
use aptos_types::{
    chain_id::ChainId,
    transaction::{ExecutionStatus, Transaction, TransactionStatus},
};
use chain_execution::{
    ExecutionBackend, ExecutorConfig, InMemoryState, MoveVmBackend, NativeBackend, StateStore,
    verify_signatures,
};
use chain_genesis::{Genesis, GenesisAccount, LocalAccount};
use chain_storage::ChainStore;
use clap::{Parser, Subcommand, ValueEnum};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

mod cluster;
mod remote;

const APT: u64 = 100_000_000;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Execution-only throughput of APT transfers.
    Exec(ExecArgs),
    /// End to end: N validators in one process under open-loop load (design doc B2).
    Cluster(cluster::ClusterArgs),
    /// End to end against running validators (e.g. `chain localnet`) through their RPC.
    Remote(remote::RemoteArgs),
}

#[derive(Clone, Copy, ValueEnum)]
enum Backend {
    /// Full Aptos VM.
    MoveVm,
    /// Native APT transfers without MoveVM (execution ceiling).
    Native,
}

#[derive(Clone, Copy, ValueEnum)]
enum Store {
    /// In-memory hash map (no persistence, no state root).
    Memory,
    /// RocksDB flat state with the asynchronous Merkle root.
    Rocksdb,
}

#[derive(clap::Args)]
struct ExecArgs {
    #[arg(long, value_enum, default_value_t = Backend::MoveVm)]
    backend: Backend,
    #[arg(long, value_enum, default_value_t = Store::Memory)]
    store: Store,
    /// RocksDB directory; must not exist yet. Defaults to a temporary directory.
    #[arg(long)]
    db_path: Option<PathBuf>,
    /// Block-STM worker threads (1 = sequential). Defaults to all cores.
    #[arg(long)]
    threads: Option<usize>,
    #[arg(long, default_value_t = 10_000)]
    accounts: usize,
    #[arg(long, default_value_t = 5_000)]
    block_size: usize,
    /// Measured blocks; one extra warm-up block runs first and is not counted.
    #[arg(long, default_value_t = 10)]
    blocks: usize,
    /// Percent of transfers sent to one hot account (design doc 12.2 contention workload).
    #[arg(long, default_value_t = 0)]
    hot_pct: u32,
    /// Use the Block-STM v2 scheduler.
    #[arg(long)]
    blockstm_v2: bool,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Exec(args) => exec(args),
        Command::Cluster(args) => cluster::run(args),
        Command::Remote(args) => remote::run(args),
    }
}

fn exec(args: ExecArgs) -> Result<()> {
    let threads = args.threads.unwrap_or_else(num_cpus::get);
    let chain_id = ChainId::new(42);
    // Genesis mints through MoveVM whichever backend is measured.
    let move_vm = MoveVmBackend::new(ExecutorConfig {
        concurrency_level: threads,
        blockstm_v2: args.blockstm_v2,
    });

    let start = Instant::now();
    let mut accounts = LocalAccount::generate([7; 32], args.accounts);
    let hot = LocalAccount::generate([9; 32], 1)[0].address();
    let genesis = Genesis {
        chain_id,
        accounts: accounts
            .iter()
            .map(|a| GenesisAccount {
                address: a.address(),
                balance: 1_000_000 * APT,
            })
            .collect(),
    };
    let signed = |accounts: &mut [LocalAccount]| {
        let start = Instant::now();
        let blocks = sign_blocks(accounts, hot, chain_id, &args);
        eprintln!(
            "signed {} blocks x {} txns in {:.1?}",
            blocks.len(),
            args.block_size,
            start.elapsed()
        );
        blocks
    };
    let native = NativeBackend::new(threads);
    let native_config = ExecutorConfig {
        concurrency_level: native.concurrency_level(),
        blockstm_v2: false,
    };
    match args.store {
        Store::Memory => {
            let state = genesis.build(&move_vm)?;
            eprintln!("genesis: {} accounts in {:.1?}", args.accounts, start.elapsed());
            let blocks = signed(&mut accounts);
            let no_roots = |_: &InMemoryState| None;
            match args.backend {
                Backend::MoveVm => {
                    run_blocks(&move_vm, ExecutorConfig::effective(), state, blocks, &args, no_roots)?
                },
                Backend::Native => run_blocks(&native, native_config, state, blocks, &args, no_roots)?,
            };
            Ok(())
        },
        Store::Rocksdb => {
            let temp;
            let path = match &args.db_path {
                Some(path) => path.clone(),
                None => {
                    temp = tempfile::tempdir()?;
                    temp.path().to_path_buf()
                },
            };
            let mut store = ChainStore::open(&path)?;
            ensure!(store.committed_height() == 0, "{} is not empty", path.display());
            genesis.build_into(&move_vm, &mut store)?;
            eprintln!(
                "genesis: {} accounts in {:.1?} into {}",
                args.accounts,
                start.elapsed(),
                path.display()
            );
            let blocks = signed(&mut accounts);
            let root_lag = |s: &ChainStore| Some(s.committed_height() - s.hashed_height());
            let store = match args.backend {
                Backend::MoveVm => {
                    run_blocks(&move_vm, ExecutorConfig::effective(), store, blocks, &args, root_lag)?
                },
                Backend::Native => run_blocks(&native, native_config, store, blocks, &args, root_lag)?,
            };
            let start = Instant::now();
            let root = store.wait_for_root(store.committed_height())?;
            println!(
                "state root {} for height {} ready {:.1?} after the last commit",
                root,
                store.committed_height(),
                start.elapsed()
            );
            Ok(())
        },
    }
}

/// Verifies, executes and commits each block in turn. `root_lag` reports how many committed
/// blocks still await their state root (stores without roots return `None`).
fn run_blocks<S: StateStore>(
    backend: &impl ExecutionBackend,
    config: ExecutorConfig,
    mut state: S,
    blocks: Vec<Vec<Transaction>>,
    args: &ExecArgs,
    root_lag: impl Fn(&S) -> Option<u64>,
) -> Result<S> {
    println!(
        "backend={} threads={} blockstm_v2={} accounts={} block_size={} hot_pct={}",
        backend.name(),
        config.concurrency_level,
        config.blockstm_v2,
        args.accounts,
        args.block_size,
        args.hot_pct
    );
    println!("block   verify_tps   exec_tps   exec_ms   commit_ms   root_lag");
    let (mut verify_total, mut exec_total, mut commit_total, mut txns_total) =
        (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0);
    for (height, txns) in blocks.into_iter().enumerate() {
        let n = txns.len();
        let t0 = Instant::now();
        let verified = verify_signatures(txns);
        let t1 = Instant::now();
        let block = state.next_block(HashValue::sha3_256_of(format!("block {height}").as_bytes()));
        let outputs = backend.execute_block(block, verified, &state)?;
        let t2 = Instant::now();
        for output in &outputs[..n] {
            ensure!(
                *output.status() == TransactionStatus::Keep(ExecutionStatus::Success),
                "transaction failed: {:?}",
                output.status()
            );
        }
        state.apply_block(block, &outputs)?;
        let t3 = Instant::now();

        let (verify, exec) = (t1 - t0, t2 - t1);
        let label = if height == 0 {
            "warmup".to_string()
        } else {
            height.to_string()
        };
        let lag = root_lag(&state).map_or("-".to_string(), |l| l.to_string());
        println!(
            "{label:>6} {:>12.0} {:>10.0} {:>9.1} {:>11.1} {lag:>10}",
            tps(n, verify),
            tps(n, exec),
            ms(exec),
            ms(t3 - t2)
        );
        if height > 0 {
            verify_total += verify;
            exec_total += exec;
            commit_total += t3 - t2;
            txns_total += n;
        }
    }
    println!(
        "summary: verify {:.0} tps, execute {:.0} tps, execute+commit {:.0} tps over {} txns",
        tps(txns_total, verify_total),
        tps(txns_total, exec_total),
        tps(txns_total, exec_total + commit_total),
        txns_total
    );
    Ok(state)
}

/// One warm-up block plus `args.blocks` measured ones. Senders rotate through all accounts, so
/// with accounts >= block_size nobody sends twice in a block; receivers are random, or the hot
/// account for `hot_pct` percent of transfers.
fn sign_blocks(
    accounts: &mut [LocalAccount],
    hot: aptos_types::account_address::AccountAddress,
    chain_id: ChainId,
    args: &ExecArgs,
) -> Vec<Vec<Transaction>> {
    let mut rng = StdRng::seed_from_u64(0);
    let addresses: Vec<_> = accounts.iter().map(|a| a.address()).collect();
    let mut next_sender = 0;
    (0..=args.blocks)
        .map(|_| {
            (0..args.block_size)
                .map(|_| {
                    let to = if rng.gen_range(0, 100) < args.hot_pct {
                        hot
                    } else {
                        addresses[rng.gen_range(0, addresses.len())]
                    };
                    let sender = next_sender;
                    next_sender = (next_sender + 1) % accounts.len();
                    accounts[sender].transfer(to, 1, chain_id)
                })
                .collect()
        })
        .collect()
}

fn tps(txns: usize, elapsed: Duration) -> f64 {
    txns as f64 / elapsed.as_secs_f64()
}

fn ms(elapsed: Duration) -> f64 {
    elapsed.as_secs_f64() * 1e3
}
