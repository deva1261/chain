//! `chain-bench cluster`: end-to-end throughput and finality of N in-process validators under
//! open-loop load (design doc 12.4): transactions are pre-signed and submitted on a fixed
//! schedule whatever the chain does; latency runs from the scheduled submit time to commit at
//! validator 0, so client lag counts against the chain, never for it.

use crate::APT;
use anyhow::{Result, bail};
use aptos_crypto::HashValue;
use chain_genesis::{GenesisAccount, LocalAccount};
use chain_node::{Cluster, ClusterConfig, NodeEvent};
use rand::{Rng, SeedableRng, rngs::StdRng};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

#[derive(clap::Args)]
pub struct ClusterArgs {
    #[arg(long, default_value_t = 4)]
    validators: usize,
    /// Offered load, transactions per second.
    #[arg(long, default_value_t = 1_000)]
    tps: u64,
    /// Seconds of load.
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// Seconds at the start excluded from the results.
    #[arg(long, default_value_t = 5)]
    warmup: u64,
    /// Funded accounts; each sends at most once per accounts/tps seconds.
    #[arg(long, default_value_t = 20_000)]
    accounts: usize,
    /// One-way latency between validators, milliseconds.
    #[arg(long, default_value_t = 5)]
    latency_ms: u64,
    /// Block-STM threads per validator.
    #[arg(long, default_value_t = 2)]
    exec_threads: usize,
    #[arg(long, default_value_t = 5_000)]
    max_block_txns: usize,
    /// Block pacing in milliseconds (0 = propose as soon as possible).
    #[arg(long, default_value_t = 0)]
    block_interval_ms: u64,
}

pub fn run(args: ClusterArgs) -> Result<()> {
    let total = (args.tps * args.duration) as usize;
    if args.accounts as u64 / args.tps < 3 {
        bail!("need at least 3 s between an account's transactions: raise --accounts");
    }

    let start = Instant::now();
    let mut accounts = LocalAccount::generate([31; 32], args.accounts);
    let mut config = ClusterConfig::new(
        args.validators,
        accounts
            .iter()
            .map(|a| GenesisAccount {
                address: a.address(),
                balance: 1_000_000 * APT,
            })
            .collect(),
    );
    config.latency = Duration::from_millis(args.latency_ms);
    config.exec_threads = args.exec_threads;
    config.max_block_txns = args.max_block_txns;
    config.block_interval_ms = args.block_interval_ms;
    let chain_id = config.chain_id;

    // Transaction k: account k % A, so each account's transactions are A/tps seconds apart and
    // each account always goes to the same validator.
    let mut rng = StdRng::seed_from_u64(1);
    let addresses: Vec<_> = accounts.iter().map(|a| a.address()).collect();
    let mut txns = Vec::with_capacity(total);
    let mut index_of: HashMap<HashValue, usize> = HashMap::with_capacity(total);
    for k in 0..total {
        let to = addresses[rng.gen_range(0, addresses.len())];
        let txn = accounts[k % args.accounts].transfer(to, 1, chain_id);
        let txn = txn.try_as_signed_user_txn().expect("user txn").clone();
        index_of.insert(txn.committed_hash(), k);
        txns.push(txn);
    }
    eprintln!("signed {total} transactions in {:.1?}", start.elapsed());

    let start = Instant::now();
    let cluster = Cluster::start(config)?;
    eprintln!(
        "{} validators up (genesis {} accounts) in {:.1?}",
        args.validators,
        args.accounts,
        start.elapsed()
    );

    // Open-loop submitter.
    let load_start = Instant::now() + Duration::from_millis(500);
    let submitter = {
        let cluster = cluster.submitter();
        let (tps, validators, accounts) = (args.tps, args.validators, args.accounts);
        std::thread::spawn(move || {
            for (k, txn) in txns.into_iter().enumerate() {
                let due = load_start + Duration::from_secs_f64(k as f64 / tps as f64);
                if let Some(wait) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
                cluster.submit(((k % accounts) % validators) as u16, txn);
            }
        })
    };
    let scheduled = |k: usize| load_start + Duration::from_secs_f64(k as f64 / args.tps as f64);

    let window_start = load_start + Duration::from_secs(args.warmup);
    let load_end = load_start + Duration::from_secs(args.duration);
    let drain_end = load_end + Duration::from_secs(15);
    let mut latencies_ms = vec![];
    let (mut committed, mut committed_in_window) = (0usize, 0usize);
    let (mut executed, mut succeeded, mut executed_in_window) = (0usize, 0usize, 0usize);
    let (mut blocks, mut nonempty_blocks) = (0usize, 0usize);
    let mut last_status: HashMap<u16, (u64, chain_consensus::Stats)> = HashMap::new();
    let mut last_pool: HashMap<u16, String> = HashMap::new();
    let mut next_report = load_start + Duration::from_secs(5);

    while Instant::now() < drain_end && executed < total {
        let event = match cluster.events.recv_timeout(Duration::from_millis(200)) {
            Ok(event) => event,
            Err(_) => continue,
        };
        match event {
            NodeEvent::Committed {
                node: 0,
                txns,
                txn_hashes,
                at,
                ..
            } => {
                blocks += 1;
                nonempty_blocks += usize::from(txns > 0);
                committed += txns;
                let in_window = at >= window_start && at <= load_end;
                if in_window {
                    committed_in_window += txns;
                }
                for hash in txn_hashes {
                    if let Some(&k) = index_of.get(&hash)
                        && scheduled(k) >= window_start
                    {
                        latencies_ms.push((at - scheduled(k)).as_secs_f64() * 1e3);
                    }
                }
            },
            NodeEvent::Executed {
                node: 0,
                txns,
                succeeded: s,
                at,
                ..
            } => {
                executed += txns;
                succeeded += s;
                if at >= window_start && at <= load_end {
                    executed_in_window += txns;
                }
            },
            NodeEvent::Status {
                node,
                committed_height,
                consensus,
                mempool,
                pool_txns,
                in_flight_batches,
                block_limit,
                ..
            } => {
                last_status.insert(node, (committed_height, consensus));
                last_pool.insert(
                    node,
                    format!("pool={pool_txns} in_flight={in_flight_batches} block_limit={block_limit} {mempool:?}"),
                );
            },
            NodeEvent::Failed { node, error } => bail!("validator {node} failed: {error}"),
            _ => {},
        }
        if Instant::now() >= next_report {
            next_report += Duration::from_secs(5);
            let t = load_start.elapsed().as_secs_f64();
            eprintln!(
                "t={t:>5.1}s committed {committed:>8} executed {executed:>8} blocks {blocks:>6}"
            );
        }
    }
    submitter.join().ok();
    let mut nodes: Vec<_> = last_pool.into_iter().collect();
    nodes.sort();
    for (node, pool) in nodes {
        eprintln!("validator {node} mempool: {pool}");
    }

    let window = (load_end - window_start).as_secs_f64();
    latencies_ms.sort_by(f64::total_cmp);
    let pct = |p: f64| {
        latencies_ms
            .get(((latencies_ms.len() as f64 * p) as usize).min(latencies_ms.len().saturating_sub(1)))
            .copied()
            .unwrap_or(f64::NAN)
    };
    let timeouts: u64 = last_status.values().map(|(_, s)| s.tcs_formed).max().unwrap_or(0);
    println!(
        "validators={} offered_tps={} duration={}s latency={}ms exec_threads={} block_interval={}ms",
        args.validators, args.tps, args.duration, args.latency_ms, args.exec_threads,
        args.block_interval_ms
    );
    println!(
        "committed_tps={:.0} executed_tps={:.0} (steady window {:.0}s)",
        committed_in_window as f64 / window,
        executed_in_window as f64 / window,
        window
    );
    println!(
        "finality_ms p50={:.0} p95={:.0} p99={:.0} (n={})",
        pct(0.50),
        pct(0.95),
        pct(0.99),
        latencies_ms.len()
    );
    println!(
        "totals: submitted={total} committed={committed} executed={executed} succeeded={succeeded} \
         blocks={blocks} nonempty={nonempty_blocks} avg_txns_per_block={:.0} timeouts={timeouts}",
        committed as f64 / nonempty_blocks.max(1) as f64
    );
    Ok(())
}
