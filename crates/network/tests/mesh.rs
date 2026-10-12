//! The QUIC mesh on localhost: full connectivity, authenticated delivery, impostors rejected.

use aptos_crypto::{PrivateKey, ed25519::Ed25519PrivateKey};
use aptos_keygen::KeyGen;
use chain_network::QuicNetwork;
use chain_types::{Committee, ValidatorIndex, ValidatorInfo, ValidatorSigner};
use std::{
    net::{SocketAddr, UdpSocket},
    sync::mpsc,
    time::{Duration, Instant},
};

fn free_port() -> u16 {
    UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn committee(keys: &[Ed25519PrivateKey], ports: &[u16]) -> Committee {
    Committee::new(
        1,
        keys.iter()
            .zip(ports)
            .map(|(k, p)| ValidatorInfo {
                public_key: k.public_key(),
                network_address: format!("127.0.0.1:{p}"),
                stake: 1,
            })
            .collect(),
    )
    .unwrap()
}

type Net = QuicNetwork<String>;

fn start(
    key: Ed25519PrivateKey,
    index: Option<ValidatorIndex>,
    committee: &Committee,
    tx: mpsc::Sender<(ValidatorIndex, ValidatorIndex, String)>,
) -> Net {
    let signer = match index {
        Some(i) => ValidatorSigner::new(i, key),
        None => ValidatorSigner::for_committee(key, committee).unwrap(),
    };
    let me = signer.index();
    let addr: SocketAddr = committee.validator(me).unwrap().network_address.parse().unwrap();
    QuicNetwork::start(
        signer,
        committee.clone(),
        addr,
        move |from, msg| {
            tx.send((me, from, msg)).ok();
        },
        |_| 0,
    )
    .unwrap()
}

fn wait_until(deadline: Duration, mut done: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    done()
}

fn keys(seed: u8, n: usize) -> Vec<Ed25519PrivateKey> {
    let mut keygen = KeyGen::from_seed([seed; 32]);
    (0..n).map(|_| keygen.generate_ed25519_private_key()).collect()
}

#[test]
fn mesh_connects_and_delivers_authenticated_messages() {
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let keys = keys(1, 4);
    let committee = committee(&keys, &ports);
    let (tx, rx) = mpsc::channel();
    let nets: Vec<Net> = keys
        .into_iter()
        .map(|k| start(k, None, &committee, tx.clone()))
        .collect();
    let indices: Vec<ValidatorIndex> = (0..4).collect();

    assert!(
        wait_until(Duration::from_secs(10), || nets
            .iter()
            .all(|n| n.connected_peers().len() == 3)),
        "full mesh did not form: {:?}",
        nets.iter().map(|n| n.connected_peers()).collect::<Vec<_>>()
    );
    // Network i is not necessarily validator i; find each one's index from its peers.
    let me_of = |n: &Net| -> ValidatorIndex {
        *indices.iter().find(|i| !n.connected_peers().contains(i)).unwrap()
    };
    for net in &nets {
        let from = me_of(net);
        let sender = net.sender();
        for to in indices.iter().filter(|&&to| to != from) {
            sender.send(*to, &format!("{from}->{to}"));
        }
    }
    let mut got = vec![];
    while got.len() < 12 {
        got.push(rx.recv_timeout(Duration::from_secs(10)).expect("message lost"));
    }
    for (to, from, msg) in got {
        assert_eq!(msg, format!("{from}->{to}"), "sender identity must be authenticated");
    }
}

#[test]
fn impostor_without_the_committee_key_is_rejected() {
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let keys = keys(2, 4);
    let committee = committee(&keys, &ports);
    let (tx, _rx) = mpsc::channel();
    // Validators 0-2 honest; someone else runs on validator 3's address, claiming index 3.
    let mut honest = vec![];
    let mut real_three = None;
    for k in keys {
        let index = committee.index_of(&k.public_key()).unwrap();
        if index == 3 {
            real_three = Some(k);
        } else {
            honest.push(start(k, None, &committee, tx.clone()));
        }
    }
    drop(real_three);
    let impostor_key = KeyGen::from_seed([99; 32]).generate_ed25519_private_key();
    let impostor = start(impostor_key, Some(3), &committee, tx);

    assert!(wait_until(Duration::from_secs(10), || honest
        .iter()
        .all(|n| n.connected_peers().len() == 2)));
    std::thread::sleep(Duration::from_secs(2));
    for net in &honest {
        assert!(!net.connected_peers().contains(&3), "impostor accepted");
    }
    assert!(impostor.connected_peers().is_empty());
    // Handshakes really were attempted and refused (by either side).
    let rejected: u64 = honest
        .iter()
        .chain(std::iter::once(&impostor))
        .map(|n| n.stats().rejected_peers.load(std::sync::atomic::Ordering::Relaxed))
        .sum();
    assert!(rejected > 0, "no handshake with the impostor was attempted");
}
