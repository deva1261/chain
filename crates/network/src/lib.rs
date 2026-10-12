//! The validator mesh (design doc D5, section 7): every validator holds one QUIC connection to
//! every other, authenticated by committee keys, with no discovery layer.
//!
//! - Identity: TLS gives each connection encryption with a throwaway certificate; then each side
//!   signs the connection's TLS exporter secret with its validator ed25519 key. The secret is
//!   unique to the session, so the signature cannot be replayed or relayed into another one.
//!   Peers whose key is not in the committee are disconnected.
//! - Dial rule: of each pair, the validator with the lower public key dials (backoff 100 ms to
//!   10 s) and the other only accepts, so there is one connection per pair.
//! - Messages: one unidirectional stream per message, so a large block never delays a vote
//!   (no head-of-line blocking); stream priority orders consensus traffic first.
//! - Liveness: keep-alive 1 s, idle timeout 5 s (design doc 7.2).

use anyhow::{Context, Result, anyhow, bail, ensure};
use aptos_crypto::{Signature, ed25519::Ed25519Signature};
use aptos_crypto_derive::{BCSCryptoHash, CryptoHasher};
use chain_types::{Committee, ValidatorIndex, ValidatorSigner};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

/// Largest message accepted (a full block of batches fits comfortably).
pub const MAX_MESSAGE_BYTES: usize = 64 << 20;
const ALPN: &[u8] = b"chain/1";
const EXPORTER_LABEL: &[u8] = b"chain validator auth";
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// What each side signs to prove its validator identity on a connection.
#[derive(Serialize, Deserialize, CryptoHasher, BCSCryptoHash)]
struct PeerAuth {
    epoch: u64,
    validator: ValidatorIndex,
    exporter: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct Hello {
    validator: ValidatorIndex,
    signature: Ed25519Signature,
}

/// Counters for monitoring.
#[derive(Default, Debug)]
pub struct NetStats {
    pub sent: AtomicU64,
    pub received: AtomicU64,
    /// Sends dropped because the peer was not connected.
    pub dropped: AtomicU64,
    pub rejected_peers: AtomicU64,
}

type Deliver<M> = Arc<dyn Fn(ValidatorIndex, M) + Send + Sync>;

struct Shared<M> {
    me: ValidatorIndex,
    signer: ValidatorSigner,
    committee: Committee,
    peers: RwLock<HashMap<ValidatorIndex, quinn::Connection>>,
    deliver: Deliver<M>,
    priority: fn(&M) -> i32,
    stats: NetStats,
}

/// A running mesh endpoint. Dropping it closes every connection.
pub struct QuicNetwork<M> {
    shared: Arc<Shared<M>>,
    endpoint: quinn::Endpoint,
    runtime: Option<tokio::runtime::Runtime>,
}

impl<M: Serialize + DeserializeOwned + Send + 'static> QuicNetwork<M> {
    /// Listens on `listen` and connects to the rest of `committee`, whose `network_address`es
    /// are `host:port`. Inbound messages go to `deliver`; `priority` ranks outbound ones.
    pub fn start(
        signer: ValidatorSigner,
        committee: Committee,
        listen: SocketAddr,
        deliver: impl Fn(ValidatorIndex, M) + Send + Sync + 'static,
        priority: fn(&M) -> i32,
    ) -> Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("quic")
            .enable_all()
            .build()?;
        let me = signer.index();
        let shared = Arc::new(Shared {
            me,
            signer,
            committee,
            peers: RwLock::new(HashMap::new()),
            deliver: Arc::new(deliver),
            priority,
            stats: NetStats::default(),
        });
        let endpoint = runtime.block_on(async { make_endpoint(listen) })?;

        // Accept loop.
        let accept_endpoint = endpoint.clone();
        let accept_shared = shared.clone();
        runtime.spawn(async move {
            while let Some(incoming) = accept_endpoint.accept().await {
                let shared = accept_shared.clone();
                tokio::spawn(async move {
                    if let Ok(connection) = incoming.await {
                        run_connection(shared, connection, None).await;
                    }
                });
            }
        });

        // Dial the peers this node is responsible for.
        let my_key = shared.signer.public_key().to_bytes();
        for (index, peer) in shared.committee.validators().iter().enumerate() {
            let index = index as ValidatorIndex;
            if index == me || peer.public_key.to_bytes() <= my_key {
                continue;
            }
            let address: SocketAddr = peer
                .network_address
                .parse()
                .with_context(|| format!("validator {index} address {}", peer.network_address))?;
            runtime.spawn(dial_loop(shared.clone(), endpoint.clone(), index, address));
        }

        Ok(Self {
            shared,
            endpoint,
            runtime: Some(runtime),
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.endpoint.local_addr()?)
    }

    pub fn connected_peers(&self) -> Vec<ValidatorIndex> {
        let mut peers: Vec<_> = self.shared.peers.read().unwrap().keys().copied().collect();
        peers.sort();
        peers
    }

    pub fn stats(&self) -> &NetStats {
        &self.shared.stats
    }

    /// A cheap handle for sending from other threads.
    pub fn sender(&self) -> QuicSender<M> {
        QuicSender {
            shared: self.shared.clone(),
            runtime: self.runtime.as_ref().expect("running").handle().clone(),
            _message: PhantomData,
        }
    }
}

impl<M> Drop for QuicNetwork<M> {
    fn drop(&mut self) {
        self.endpoint.close(0u32.into(), b"shutdown");
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(1));
        }
    }
}

pub struct QuicSender<M> {
    shared: Arc<Shared<M>>,
    runtime: tokio::runtime::Handle,
    _message: PhantomData<fn(M)>,
}

impl<M> Clone for QuicSender<M> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            runtime: self.runtime.clone(),
            _message: PhantomData,
        }
    }
}

impl<M: Serialize + Send + 'static> QuicSender<M> {
    /// Sends `msg` to validator `to`; dropped (and counted) if not connected. Never blocks.
    pub fn send(&self, to: ValidatorIndex, msg: &M) {
        let connection = self.shared.peers.read().unwrap().get(&to).cloned();
        let Some(connection) = connection else {
            self.shared.stats.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Ok(bytes) = bcs::to_bytes(msg) else { return };
        let priority = (self.shared.priority)(msg);
        let shared = self.shared.clone();
        self.runtime.spawn(async move {
            let result: Result<()> = async {
                let mut stream = connection.open_uni().await?;
                stream.set_priority(priority).ok();
                stream.write_all(&bytes).await?;
                stream.finish()?;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => shared.stats.sent.fetch_add(1, Ordering::Relaxed),
                Err(_) => shared.stats.dropped.fetch_add(1, Ordering::Relaxed),
            };
        });
    }
}

fn make_endpoint(listen: SocketAddr) -> Result<quinn::Endpoint> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let certified = rcgen::generate_simple_self_signed(vec!["chain-validator".to_string()])?;
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::Pkcs8(certified.key_pair.serialize_der().into());
    let mut server_tls = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    server_tls.alpn_protocols = vec![ALPN.to_vec()];

    let mut client_tls = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCertificate(provider)))
        .with_no_client_auth();
    client_tls.alpn_protocols = vec![ALPN.to_vec()];

    let mut transport = quinn::TransportConfig::default();
    transport
        .keep_alive_interval(Some(Duration::from_secs(1)))
        .max_idle_timeout(Some(Duration::from_secs(5).try_into()?))
        .max_concurrent_uni_streams(10_000u32.into());
    let transport = Arc::new(transport);

    let mut server = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_tls)?,
    ));
    server.transport_config(transport.clone());
    let mut client = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(client_tls)?,
    ));
    client.transport_config(transport);

    let mut endpoint = quinn::Endpoint::server(server, listen)?;
    endpoint.set_default_client_config(client);
    Ok(endpoint)
}

/// Keeps a connection to `peer` up: connect, run it until it drops, back off, repeat.
async fn dial_loop<M: DeserializeOwned + Send + 'static>(
    shared: Arc<Shared<M>>,
    endpoint: quinn::Endpoint,
    peer: ValidatorIndex,
    address: SocketAddr,
) {
    let mut backoff = Duration::from_millis(100);
    loop {
        if let Ok(connecting) = endpoint.connect(address, "chain-validator")
            && let Ok(connection) = connecting.await
        {
            backoff = Duration::from_millis(100);
            run_connection(shared.clone(), connection, Some(peer)).await;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(10));
    }
}

/// Authenticates a connection, registers it, and delivers its messages until it closes.
/// `expected` is the peer we dialed (accepted connections may come from anyone).
async fn run_connection<M: DeserializeOwned + Send + 'static>(
    shared: Arc<Shared<M>>,
    connection: quinn::Connection,
    expected: Option<ValidatorIndex>,
) {
    let peer = match tokio::time::timeout(HANDSHAKE_TIMEOUT, authenticate(&shared, &connection))
        .await
        .map_err(|_| anyhow!("handshake timed out"))
        .and_then(|r| r)
    {
        Ok(peer) if expected.is_none_or(|e| e == peer) => peer,
        _ => {
            shared.stats.rejected_peers.fetch_add(1, Ordering::Relaxed);
            connection.close(1u32.into(), b"authentication failed");
            return;
        },
    };
    shared.peers.write().unwrap().insert(peer, connection.clone());

    while let Ok(mut stream) = connection.accept_uni().await {
        let shared = shared.clone();
        tokio::spawn(async move {
            if let Ok(bytes) = stream.read_to_end(MAX_MESSAGE_BYTES).await
                && let Ok(msg) = bcs::from_bytes::<M>(&bytes)
            {
                shared.stats.received.fetch_add(1, Ordering::Relaxed);
                (shared.deliver)(peer, msg);
            }
        });
    }

    // Closed: forget it unless a newer connection to the same peer already replaced it.
    let mut peers = shared.peers.write().unwrap();
    if peers
        .get(&peer)
        .is_some_and(|c| c.stable_id() == connection.stable_id())
    {
        peers.remove(&peer);
    }
}

/// Both sides send a signed Hello on a fresh stream and check the other's.
async fn authenticate<M>(shared: &Shared<M>, connection: &quinn::Connection) -> Result<ValidatorIndex> {
    let mut exporter = vec![0u8; 32];
    connection
        .export_keying_material(&mut exporter, EXPORTER_LABEL, b"")
        .map_err(|_| anyhow!("TLS exporter unavailable"))?;
    let epoch = shared.committee.epoch();

    let hello = Hello {
        validator: shared.me,
        signature: shared.signer.sign(&PeerAuth {
            epoch,
            validator: shared.me,
            exporter: exporter.clone(),
        })?,
    };
    let mut out = connection.open_uni().await?;
    out.write_all(&bcs::to_bytes(&hello)?).await?;
    out.finish()?;

    let mut incoming = connection.accept_uni().await?;
    let theirs: Hello = bcs::from_bytes(&incoming.read_to_end(1024).await?)?;
    // Our own Hello reflected back must not count as a peer.
    ensure!(theirs.validator != shared.me, "peer claims to be us");
    let key = &shared
        .committee
        .validator(theirs.validator)
        .context("peer is not in the committee")?
        .public_key;
    let claim = PeerAuth {
        epoch,
        validator: theirs.validator,
        exporter,
    };
    if theirs.signature.verify(&claim, key).is_err() {
        bail!("bad identity signature from validator {}", theirs.validator);
    }
    Ok(theirs.validator)
}

/// Certificates carry no identity here (the signed Hello does), so any certificate is accepted;
/// TLS handshake signatures are still verified properly.
#[derive(Debug)]
struct AnyCertificate(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
