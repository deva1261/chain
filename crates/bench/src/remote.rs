//! `chain-bench remote`: open-loop load against running validators over their RPC. Accounts are
//! derived from the same seed the network's genesis used. Latency is measured on this client's
//! clock only, from each transaction's scheduled submit time to when the client sees it committed
//! at the first validator (polled every 50 ms), so clock differences between machines do not
//! matter.

use anyhow::{Context, Result, bail};
use aptos_types::{chain_id::ChainId, transaction::SignedTransaction};
use chain_genesis::LocalAccount;
use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::Value;
use std::{
    collections::HashMap,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(clap::Args)]
pub struct RemoteArgs {
    /// Comma-separated RPC URLs, in validator order.
    #[arg(long, default_value = "http://127.0.0.1:8080,http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083")]
    rpc: String,
    #[arg(long, default_value_t = 20_000)]
    accounts: usize,
    #[arg(long, default_value_t = 31)]
    account_seed: u8,
    #[arg(long, default_value_t = 42)]
    chain_id: u8,
    #[arg(long, default_value_t = 1_000)]
    tps: u64,
    #[arg(long, default_value_t = 30)]
    duration: u64,
    #[arg(long, default_value_t = 5)]
    warmup: u64,
}

fn unix_ms() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0.0, |d| d.as_secs_f64() * 1e3)
}

pub fn run(args: RemoteArgs) -> Result<()> {
    let urls: Vec<String> = args.rpc.split(',').map(|s| s.trim_end_matches('/').to_string()).collect();
    let client = reqwest::blocking::Client::builder().timeout(Duration::from_secs(10)).build()?;
    let status = |url: &str| -> Result<Value> {
        Ok(serde_json::from_str(&client.get(format!("{url}/v1/status")).send()?.text()?)?)
    };
    for url in &urls {
        status(url).with_context(|| format!("{url} is not reachable"))?;
    }
    if args.accounts as u64 / args.tps < 3 {
        bail!("need at least 3 s between an account's transactions: raise --accounts");
    }

    // Sequence numbers start at 0, so the network must not have run these accounts before.
    let total = (args.tps * args.duration) as usize;
    let start = Instant::now();
    let mut accounts = LocalAccount::generate([args.account_seed; 32], args.accounts);
    let first = &status(&urls[0])?;
    if first["executed_txns"].as_u64().unwrap_or(0) > 0 {
        bail!("this network already ran transactions; start a fresh localnet (sequence numbers)");
    }
    let chain_id = ChainId::new(args.chain_id);
    let mut rng = StdRng::seed_from_u64(7);
    let addresses: Vec<_> = accounts.iter().map(|a| a.address()).collect();
    let mut txns: Vec<SignedTransaction> = Vec::with_capacity(total);
    let mut index_of: HashMap<String, usize> = HashMap::with_capacity(total);
    for k in 0..total {
        let to = addresses[rng.gen_range(0, addresses.len())];
        let txn = accounts[k % args.accounts].transfer(to, 1, chain_id);
        let txn = txn.try_as_signed_user_txn().expect("user txn").clone();
        index_of.insert(txn.committed_hash().to_hex(), k);
        txns.push(txn);
    }
    eprintln!("signed {total} transactions in {:.1?}", start.elapsed());

    // Commit watcher on validator 0.
    let commits_url = format!("{}/v1/commits", urls[0]);
    let from_height = first["committed_height"].as_u64().unwrap_or(0) + 1;
    let load_start_unix = unix_ms() + 500.0;
    let scheduled_ms = move |k: usize| load_start_unix + k as f64 * 1e3 / args.tps as f64;
    let watch_until = Instant::now() + Duration::from_secs(args.duration + 20);
    let watcher = {
        let client = client.clone();
        std::thread::spawn(move || -> Result<Vec<(usize, f64)>> {
            let mut next = from_height;
            let mut seen = vec![];
            while Instant::now() < watch_until && seen.len() < total {
                let commits: Value =
                    serde_json::from_str(&client.get(format!("{commits_url}?from={next}")).send()?.text()?)?;
                let at = unix_ms();
                for c in commits.as_array().into_iter().flatten() {
                    next = next.max(c["height"].as_u64().unwrap_or(0) + 1);
                    for h in c["txn_hashes"].as_array().into_iter().flatten() {
                        if let Some(&k) = h.as_str().and_then(|h| index_of.get(h)) {
                            seen.push((k, at));
                        }
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(seen)
        })
    };

    // Open-loop submission in 10 ms slices, one request per validator per slice.
    let slice = Duration::from_millis(10);
    let load_start = Instant::now() + Duration::from_millis(500);
    let mut sent = 0;
    let mut late_slices = 0;
    let mut txns = txns.into_iter().enumerate().peekable();
    let mut tick = load_start;
    while txns.peek().is_some() {
        if let Some(wait) = tick.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        let slice_end = tick + slice;
        tick = slice_end;
        let due = |k: usize| load_start + Duration::from_secs_f64(k as f64 / args.tps as f64);
        let mut per_node: Vec<Vec<SignedTransaction>> = vec![vec![]; urls.len()];
        while let Some((k, _)) = txns.peek() {
            if due(*k) > slice_end {
                break;
            }
            let (k, txn) = txns.next().expect("peeked");
            per_node[(k % args.accounts) % urls.len()].push(txn);
        }
        for (url, batch) in urls.iter().zip(per_node) {
            if batch.is_empty() {
                continue;
            }
            sent += batch.len();
            client
                .post(format!("{url}/v1/transactions"))
                .body(bcs::to_bytes(&batch)?)
                .send()?
                .error_for_status()?;
        }
        if Instant::now() > slice_end {
            late_slices += 1;
        }
    }
    eprintln!("submitted {sent} transactions ({late_slices} late slices)");

    let seen = watcher.join().expect("watcher thread")?;
    let window_start = load_start_unix + args.warmup as f64 * 1e3;
    let window_end = load_start_unix + args.duration as f64 * 1e3;
    let committed_in_window = seen
        .iter()
        .filter(|(_, at)| *at >= window_start && *at <= window_end)
        .count();
    let mut latencies: Vec<f64> = seen
        .iter()
        .filter(|(k, _)| scheduled_ms(*k) >= window_start)
        .map(|(k, at)| at - scheduled_ms(*k))
        .collect();
    latencies.sort_by(f64::total_cmp);
    let pct = |p: f64| {
        latencies
            .get(((latencies.len() as f64 * p) as usize).min(latencies.len().saturating_sub(1)))
            .copied()
            .unwrap_or(f64::NAN)
    };
    let last = status(&urls[0])?;
    println!(
        "validators={} offered_tps={} duration={}s",
        urls.len(),
        args.tps,
        args.duration
    );
    println!(
        "committed_tps={:.0} (steady window {}s)",
        committed_in_window as f64 / (args.duration - args.warmup) as f64,
        args.duration - args.warmup
    );
    println!(
        "finality_ms p50={:.0} p95={:.0} p99={:.0} (n={})",
        pct(0.5),
        pct(0.95),
        pct(0.99),
        latencies.len()
    );
    println!(
        "totals: submitted={sent} committed={} executed_txns={} timeouts={}",
        seen.len(),
        last["executed_txns"].as_u64().unwrap_or(0),
        last["timeouts"].as_u64().unwrap_or(0)
    );
    Ok(())
}
