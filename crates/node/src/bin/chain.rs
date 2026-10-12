//! The `chain` binary (design doc 8.1, 8.2):
//! - `chain keygen`: a validator key for this machine (prints the public key to share).
//! - `chain genesis`: genesis.json from the validators' public keys and addresses.
//! - `chain run`: run one validator.
//! - `chain localnet`: all of the above for N validators on 127.0.0.1, as processes.

use anyhow::{Context, Result, bail};
use aptos_crypto::PrivateKey;
use chain_genesis::LocalAccount;
use chain_node::{
    genesis_file::{GenesisBalance, GenesisFile},
    run::{NodeOptions, run_node},
};
use clap::{Parser, Subcommand};
use std::{
    net::SocketAddr,
    path::PathBuf,
    process::{Child, Command},
    time::Duration,
};

const APT: u64 = 100_000_000;

#[derive(Parser)]
#[command(name = "chain", about = "High-throughput L1 POC node")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a validator key; prints its public key for the genesis ceremony.
    Keygen(KeygenArgs),
    /// Build genesis.json from every validator's public key and address.
    Genesis(GenesisArgs),
    /// Run one validator.
    Run(RunArgs),
    /// Generate keys and a genesis for N validators on 127.0.0.1 and run them as processes.
    Localnet(LocalnetArgs),
}

#[derive(clap::Args)]
struct KeygenArgs {
    /// Where to write the private key (hex). Keep it on this machine.
    #[arg(long, default_value = "validator.key")]
    out: PathBuf,
}

#[derive(clap::Args)]
struct GenesisArgs {
    /// One per validator: <public-key-hex>@<ip>:<quic-port>, optionally ,<stake>.
    #[arg(long = "validator", required = true)]
    validators: Vec<String>,
    /// Funded accounts, derived from --account-seed (load generators use the same seed).
    #[arg(long, default_value_t = 40_000)]
    accounts: usize,
    #[arg(long, default_value_t = 31)]
    account_seed: u8,
    #[arg(long, default_value_t = 42)]
    chain_id: u8,
    #[arg(long, default_value = "genesis.json")]
    out: PathBuf,
}

#[derive(clap::Args)]
struct LocalnetArgs {
    #[arg(long, default_value_t = 4)]
    validators: usize,
    #[arg(long, default_value = "localnet")]
    dir: PathBuf,
    /// QUIC ports are base_port + i.
    #[arg(long, default_value_t = 7000)]
    base_port: u16,
    /// RPC ports are rpc_base_port + i.
    #[arg(long, default_value_t = 8080)]
    rpc_base_port: u16,
    /// Funded accounts, derived from --account-seed (load generators use the same seed).
    #[arg(long, default_value_t = 20_000)]
    accounts: usize,
    #[arg(long, default_value_t = 31)]
    account_seed: u8,
    #[arg(long, default_value_t = 2)]
    exec_threads: usize,
    /// Block pacing in milliseconds (0 = propose as soon as possible).
    #[arg(long, default_value_t = 0)]
    block_interval_ms: u64,
}

#[derive(clap::Args)]
struct RunArgs {
    #[arg(long)]
    genesis: PathBuf,
    /// File holding the hex ed25519 private key.
    #[arg(long)]
    key: PathBuf,
    #[arg(long)]
    data_dir: PathBuf,
    /// RPC listen address; use 0.0.0.0:8080 for load generators on other machines.
    #[arg(long, default_value = "127.0.0.1:8080")]
    rpc: SocketAddr,
    /// Block-STM threads; defaults to all cores but two (consensus, network, RPC).
    #[arg(long)]
    exec_threads: Option<usize>,
    #[arg(long, default_value_t = 1_000)]
    base_timeout_ms: u64,
    #[arg(long, default_value_t = 4)]
    exec_lag: u64,
    #[arg(long, default_value_t = 10_000)]
    max_block_txns: usize,
    /// Block pacing in milliseconds (0 = propose as soon as possible).
    #[arg(long, default_value_t = 0)]
    block_interval_ms: u64,
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Commands::Keygen(args) => keygen(args),
        Commands::Genesis(args) => genesis(args),
        Commands::Localnet(args) => localnet(args),
        Commands::Run(args) => run_node(NodeOptions {
            genesis: args.genesis,
            key: args.key,
            data_dir: args.data_dir,
            rpc: args.rpc,
            exec_threads: args.exec_threads.unwrap_or_else(|| {
                std::thread::available_parallelism().map_or(2, |n| n.get().saturating_sub(2).max(1))
            }),
            base_timeout_ms: args.base_timeout_ms,
            exec_lag: args.exec_lag,
            max_block_txns: args.max_block_txns,
            block_interval_ms: args.block_interval_ms,
        }),
    }
}

fn keygen(args: KeygenArgs) -> Result<()> {
    if args.out.exists() {
        bail!("{} exists; refusing to overwrite a validator key", args.out.display());
    }
    let key = aptos_keygen::KeyGen::from_os_rng().generate_ed25519_private_key();
    std::fs::write(&args.out, hex::encode(key.to_bytes()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&args.out, std::fs::Permissions::from_mode(0o600))?;
    }
    eprintln!("private key written to {} (keep it here)", args.out.display());
    println!("{}", hex::encode(key.public_key().to_bytes()));
    Ok(())
}

fn genesis(args: GenesisArgs) -> Result<()> {
    let validators = args
        .validators
        .iter()
        .map(|spec| {
            let (key, rest) = spec
                .split_once('@')
                .with_context(|| format!("expected <public-key>@<ip>:<port>[,<stake>], got {spec}"))?;
            let (address, stake) = match rest.split_once(',') {
                Some((address, stake)) => (address, stake.parse()?),
                None => (rest, 1),
            };
            address
                .parse::<SocketAddr>()
                .with_context(|| format!("bad address {address}"))?;
            let key = aptos_crypto::ed25519::Ed25519PublicKey::try_from(hex::decode(key)?.as_slice())?;
            Ok(GenesisFile::validator_entry(&key, address.to_string(), stake))
        })
        .collect::<Result<Vec<_>>>()?;
    let genesis = make_genesis(args.chain_id, validators, args.accounts, args.account_seed);
    genesis.committee()?;
    genesis.save(&args.out)?;
    eprintln!(
        "{}: {} validators, {} accounts (seed {}), genesis id {}",
        args.out.display(),
        genesis.validators.len(),
        args.accounts,
        args.account_seed,
        &genesis.hash().to_hex()[..16]
    );
    Ok(())
}

fn make_genesis(
    chain_id: u8,
    validators: Vec<chain_node::genesis_file::GenesisValidator>,
    accounts: usize,
    account_seed: u8,
) -> GenesisFile {
    let accounts = LocalAccount::generate([account_seed; 32], accounts)
        .iter()
        .map(|a| GenesisBalance {
            address: a.address().to_hex_literal(),
            balance: 1_000_000 * APT,
        })
        .collect();
    let mut seed = [0u8; 32];
    seed[..8].copy_from_slice(&rand_u64().to_be_bytes());
    GenesisFile {
        chain_id,
        epoch: 1,
        epoch_seed: hex::encode(seed),
        validators,
        accounts,
    }
}

fn localnet(args: LocalnetArgs) -> Result<()> {
    if args.dir.exists() {
        bail!(
            "{} exists; remove it to start a fresh network (restarts are not supported yet)",
            args.dir.display()
        );
    }
    std::fs::create_dir_all(&args.dir)?;

    // Keys: private keys stay in each validator's directory; genesis holds public keys only.
    let mut keygen = aptos_keygen::KeyGen::from_os_rng();
    let keys: Vec<_> = (0..args.validators)
        .map(|_| keygen.generate_ed25519_private_key())
        .collect();
    let validators = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            GenesisFile::validator_entry(
                &k.public_key(),
                format!("127.0.0.1:{}", args.base_port + i as u16),
                1,
            )
        })
        .collect();
    let genesis = make_genesis(42, validators, args.accounts, args.account_seed);
    let genesis_path = args.dir.join("genesis.json");
    genesis.save(&genesis_path)?;

    // Directories are numbered by committee index, which matches the RPC port.
    let committee = genesis.committee()?;
    let exe = std::env::current_exe()?;
    let mut children: Vec<(u16, Child)> = vec![];
    for key in &keys {
        let index = committee.index_of(&key.public_key()).context("member")?;
        let dir = args.dir.join(format!("validator-{index}"));
        std::fs::create_dir_all(&dir)?;
        std::fs::write(dir.join("key"), hex::encode(key.to_bytes()))?;
        let log = std::fs::File::create(dir.join("node.log"))?;
        let rpc = format!("127.0.0.1:{}", args.rpc_base_port + index);
        let child = Command::new(&exe)
            .args(["run", "--genesis"])
            .arg(&genesis_path)
            .arg("--key")
            .arg(dir.join("key"))
            .arg("--data-dir")
            .arg(dir.join("data"))
            .args(["--rpc", &rpc, "--exec-threads", &args.exec_threads.to_string()])
            .args(["--block-interval-ms", &args.block_interval_ms.to_string()])
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        eprintln!(
            "validator {index}: quic 127.0.0.1:{} rpc http://{rpc} log {}",
            args.base_port + index,
            dir.join("node.log").display()
        );
        children.push((index, child));
    }
    eprintln!(
        "localnet up: {} validators, {} accounts (seed {}). Ctrl-C stops it.",
        args.validators, args.accounts, args.account_seed
    );

    // Run until a validator exits; then stop the rest.
    loop {
        let mut exited = None;
        for (index, child) in children.iter_mut() {
            if let Some(status) = child.try_wait()? {
                exited = Some((*index, status));
                break;
            }
        }
        if let Some((index, status)) = exited {
            eprintln!("validator {index} exited ({status}); stopping the localnet");
            for (_, child) in children.iter_mut() {
                child.kill().ok();
            }
            bail!("validator {index} exited");
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn rand_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}
