//! `chain-bench transfer`: one APT transfer between two accounts over RPC, with the balances of
//! both accounts read from every validator before and after. Accounts are the genesis test
//! accounts, picked by index or by address (same seed as the network's genesis); the receiver
//! can also be any other address.
//!
//! Steps: read both balances on every validator, sign with the sender's on-chain sequence
//! number, submit to the first validator, wait until it commits the transaction and every
//! validator has executed that block, then read both balances again and check that the
//! receiver gained exactly `amount`, the sender lost `amount` plus the gas fee, and all
//! validators agree.

use anyhow::{Context, Result, bail, ensure};
use aptos_types::{account_address::AccountAddress, chain_id::ChainId};
use chain_genesis::LocalAccount;
use serde_json::Value;
use std::time::{Duration, Instant};

const APT: f64 = 100_000_000.0;

#[derive(clap::Args)]
pub struct TransferArgs {
    /// Comma-separated RPC URLs. The transfer is submitted to the first; balances are read from
    /// all of them.
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    rpc: String,
    #[arg(long, default_value_t = 31)]
    account_seed: u8,
    #[arg(long, default_value_t = 42)]
    chain_id: u8,
    /// Sender: a genesis test account, by index (0-based) or 0x address.
    #[arg(long)]
    from: String,
    /// Receiver: index of a genesis test account, or any 0x address (created if new).
    #[arg(long)]
    to: String,
    /// Amount in octas (1 APT = 100000000 octas).
    #[arg(long)]
    amount: u64,
    /// Genesis --accounts: how many test accounts to search when --from is an address.
    #[arg(long, default_value_t = 60_000)]
    accounts: usize,
    /// Seconds to wait for the transfer to commit and execute.
    #[arg(long, default_value_t = 60)]
    timeout: u64,
}

struct Rpc {
    client: reqwest::blocking::Client,
    urls: Vec<String>,
}

impl Rpc {
    fn get(&self, url: &str, path: &str) -> Result<Value> {
        let response = self.client.get(format!("{url}{path}")).send()?;
        let status = response.status();
        let text = response.text()?;
        ensure!(status.is_success(), "{url}{path}: {status} {text}");
        Ok(serde_json::from_str(&text)?)
    }

    /// (balance, sequence number) of `address` at each validator.
    fn accounts(&self, address: AccountAddress) -> Result<Vec<(u64, u64)>> {
        self.urls
            .iter()
            .map(|url| {
                let a = self
                    .get(url, &format!("/v1/accounts/{}", address.to_hex_literal()))
                    .with_context(|| {
                        format!("{url} has no /v1/accounts; is it running the latest chain binary?")
                    })?;
                Ok((
                    a["balance"].as_u64().context("balance")?,
                    a["sequence_number"].as_u64().context("sequence_number")?,
                ))
            })
            .collect()
    }
}

pub fn run(args: TransferArgs) -> Result<()> {
    let rpc = Rpc {
        client: reqwest::blocking::Client::builder().timeout(Duration::from_secs(10)).build()?,
        urls: args.rpc.split(',').map(|s| s.trim_end_matches('/').to_string()).collect(),
    };
    let from_index = args.from.parse::<usize>().ok();
    let to_index = args.to.parse::<usize>().ok();
    let count = match from_index {
        Some(i) => i.max(to_index.unwrap_or(0)) + 1,
        None => args.accounts.max(to_index.map_or(0, |i| i + 1)),
    };
    let mut accounts = LocalAccount::generate([args.account_seed; 32], count);
    let from_index = match from_index {
        Some(i) => i,
        None => {
            let from = parse_address(&args.from, "--from")?;
            accounts.iter().position(|a| a.address() == from).with_context(|| {
                format!(
                    "{} is not among the first {} test accounts of seed {}; only genesis test \
                     accounts can send (their keys come from the seed)",
                    args.from, args.accounts, args.account_seed
                )
            })?
        },
    };
    let to = match to_index {
        Some(i) => accounts[i].address(),
        None => parse_address(&args.to, "--to")?,
    };
    let from = accounts[from_index].address();
    ensure!(from != to, "sender and receiver are the same account");
    println!("from    account #{from_index} {}", from.to_hex_literal());
    let to_label = match to_index.or_else(|| accounts.iter().position(|a| a.address() == to)) {
        Some(i) => format!("account #{i} "),
        None => String::new(),
    };
    println!("to      {to_label}{}", to.to_hex_literal());
    let sender = &mut accounts[from_index];
    println!("amount  {} octas ({} APT)\n", args.amount, args.amount as f64 / APT);

    // Before.
    let from_before = rpc.accounts(from)?;
    let to_before = rpc.accounts(to)?;
    print_balances("before", &rpc.urls, &from_before, &to_before);
    let (balance, sequence_number) = from_before[0];
    ensure!(
        balance > args.amount,
        "account #{from_index} has {balance} octas, not enough to send {} plus gas; is \
         --account-seed the one genesis used, and the index below genesis --accounts?",
        args.amount
    );

    // Submit to the first validator.
    let first = &rpc.urls[0];
    let next_height = rpc.get(first, "/v1/status")?["committed_height"].as_u64().unwrap_or(0) + 1;
    sender.set_sequence_number(sequence_number);
    let txn = sender.transfer(to, args.amount, ChainId::new(args.chain_id));
    let txn = txn.try_as_signed_user_txn().expect("user txn").clone();
    let hash = txn.committed_hash().to_hex();
    rpc.client
        .post(format!("{first}/v1/transactions"))
        .body(bcs::to_bytes(&vec![txn])?)
        .send()?
        .error_for_status()?;
    println!("\nsubmitted txn 0x{hash} (sequence number {sequence_number}) to {first}");

    // Wait for the commit, then for every validator to execute that block.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.timeout);
    let height = wait_for_commit(&rpc, first, next_height, &hash, deadline)?;
    println!("committed in block {height} after {:.2?}", started.elapsed());
    for url in &rpc.urls {
        while rpc.get(url, "/v1/status")?["executed_height"].as_u64().unwrap_or(0) < height {
            if Instant::now() > deadline {
                bail!("{url} did not execute block {height} within {}s", args.timeout);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    println!("executed on all {} validators after {:.2?}\n", rpc.urls.len(), started.elapsed());

    // After.
    let from_after = rpc.accounts(from)?;
    let to_after = rpc.accounts(to)?;
    print_balances("after", &rpc.urls, &from_after, &to_after);

    // Checks.
    let mut ok = true;
    if from_after.iter().any(|a| *a != from_after[0]) || to_after.iter().any(|a| *a != to_after[0]) {
        println!("FAIL validators disagree on the balances");
        ok = false;
    }
    let received = to_after[0].0 as i128 - to_before[0].0 as i128;
    let spent = from_before[0].0 as i128 - from_after[0].0 as i128;
    let fee = spent - args.amount as i128;
    println!("\nreceiver gained {received} octas, sender paid {spent} octas ({} amount + {fee} gas fee)", args.amount);
    if received != args.amount as i128 {
        println!("FAIL receiver should have gained exactly {} octas", args.amount);
        ok = false;
    }
    if fee < 0 {
        println!("FAIL sender paid less than the amount");
        ok = false;
    }
    if from_after[0].1 != sequence_number + 1 {
        println!("FAIL sender sequence number is {}, expected {}", from_after[0].1, sequence_number + 1);
        ok = false;
    }
    ensure!(ok, "transfer check failed");
    println!("OK transfer applied and all validators agree");
    Ok(())
}

fn parse_address(s: &str, flag: &str) -> Result<AccountAddress> {
    AccountAddress::from_hex_literal(s)
        .or_else(|_| AccountAddress::from_str_strict(s))
        .with_context(|| format!("{flag} must be an account index or a 0x address, got {s}"))
}

/// Height of the block that committed `hash`, as seen by `url`.
fn wait_for_commit(rpc: &Rpc, url: &str, mut next: u64, hash: &str, deadline: Instant) -> Result<u64> {
    loop {
        let commits = rpc.get(url, &format!("/v1/commits?from={next}"))?;
        for c in commits.as_array().into_iter().flatten() {
            let height = c["height"].as_u64().unwrap_or(0);
            next = next.max(height + 1);
            if c["txn_hashes"].as_array().into_iter().flatten().any(|h| h.as_str() == Some(hash)) {
                return Ok(height);
            }
        }
        if Instant::now() > deadline {
            bail!("transaction not committed in time; is the chain committing blocks? (curl {url}/v1/status)");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn print_balances(when: &str, urls: &[String], from: &[(u64, u64)], to: &[(u64, u64)]) {
    println!("{when:<7} {:<28} {:>24} {:>24}", "validator", "sender (APT)", "receiver (APT)");
    for (i, url) in urls.iter().enumerate() {
        println!(
            "        {:<28} {:>24.8} {:>24.8}",
            url.trim_start_matches("http://"),
            from[i].0 as f64 / APT,
            to[i].0 as f64 / APT
        );
    }
}
