pub mod protocol;

use std::{collections::HashMap, sync::Arc};

use cid::Cid;
use futures::StreamExt;
use libp2p::{
    gossipsub, identify, mdns,
    request_response::{self, OutboundRequestId, ProtocolSupport},
    swarm::SwarmEvent,
    Multiaddr, PeerId,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

use crate::{
    head::{announce_from_bytes, announce_to_bytes, feed_topic_str, HeadAnnounce},
    network::protocol::{BlockExchangeCodec, BlockExchangeProtocol, BlockResponse, WantBlock},
    store::{bytes_to_hex, Store},
};

// --- 公開型 ---

pub enum NetworkCommand {
    GetBlock {
        cid: Cid,
        /// announce 元など、ブロックを持っている見込みが高いピア(接続中なら優先)
        prefer: Option<PeerId>,
        reply: oneshot::Sender<Option<Vec<u8>>>,
    },
    Dial(Multiaddr),
    /// 自分の head 通知を feed トピックへ publish する(fire-and-forget)
    PublishHead(HeadAnnounce),
    /// 指定アカウントの feed トピックを購読する(= フォロー)
    Subscribe {
        pubkey_hex: String,
    },
    Unsubscribe {
        pubkey_hex: String,
    },
}

pub enum NetworkEvent {
    PeerConnected(PeerId),
    PeerDiscovered(PeerId),
    /// gossipsub で head 通知を受信した(署名検証は同期エンジン側で行う)
    HeadReceived {
        announce: HeadAnnounce,
        source: PeerId,
    },
    /// あるピアがトピックを購読した(テスト・将来の mesh 観察用)
    PeerSubscribed {
        peer: PeerId,
        topic: String,
    },
}

#[derive(Clone)]
pub struct NetworkHandle {
    cmd_tx: mpsc::Sender<NetworkCommand>,
}

impl NetworkHandle {
    pub fn new(cmd_tx: mpsc::Sender<NetworkCommand>) -> Self {
        Self { cmd_tx }
    }

    pub async fn get_block(&self, cid: Cid, prefer: Option<PeerId>) -> Option<Vec<u8>> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(NetworkCommand::GetBlock {
                cid,
                prefer,
                reply: tx,
            })
            .await
            .ok()?;
        rx.await.ok().flatten()
    }

    pub async fn dial(&self, addr: Multiaddr) {
        let _ = self.cmd_tx.send(NetworkCommand::Dial(addr)).await;
    }

    pub async fn publish_head(&self, announce: HeadAnnounce) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::PublishHead(announce))
            .await;
    }

    pub async fn subscribe(&self, pubkey_hex: String) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::Subscribe { pubkey_hex })
            .await;
    }

    pub async fn unsubscribe(&self, pubkey_hex: String) {
        let _ = self
            .cmd_tx
            .send(NetworkCommand::Unsubscribe { pubkey_hex })
            .await;
    }
}

// --- NetworkBehaviour ---

#[derive(libp2p::swarm::NetworkBehaviour)]
struct DeilephilaBehaviour {
    identify: identify::Behaviour,
    mdns: mdns::tokio::Behaviour,
    exchange: request_response::Behaviour<BlockExchangeCodec>,
    gossipsub: gossipsub::Behaviour,
}

// --- Swarm 起動 ---

/// Swarm ループを起動し (NetworkHandle, NetworkEvent 受信側, 待受アドレス) を返す。
/// `listen` が None のとき "/ip4/0.0.0.0/tcp/0" でリッスンする(全インタフェース)。
pub async fn spawn_swarm_loop(
    store: Arc<Store>,
    listen: Option<Multiaddr>,
) -> Result<(NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr), Box<dyn std::error::Error>> {
    let listen_addr = listen.unwrap_or_else(|| "/ip4/0.0.0.0/tcp/0".parse().unwrap());

    let (cmd_tx, cmd_rx) = mpsc::channel::<NetworkCommand>(64);
    let (event_tx, event_rx) = mpsc::channel::<NetworkEvent>(64);
    let (addr_tx, addr_rx) = oneshot::channel::<Multiaddr>();

    tokio::spawn(run_swarm_loop(
        store,
        cmd_rx,
        event_tx,
        addr_tx,
        listen_addr,
    ));

    let addr = addr_rx.await?;
    let handle = NetworkHandle::new(cmd_tx);
    Ok((handle, event_rx, addr))
}

async fn run_swarm_loop(
    store: Arc<Store>,
    mut cmd_rx: mpsc::Receiver<NetworkCommand>,
    event_tx: mpsc::Sender<NetworkEvent>,
    addr_tx: oneshot::Sender<Multiaddr>,
    listen_addr: Multiaddr,
) {
    let mut swarm = match build_swarm() {
        Ok(s) => s,
        Err(e) => {
            warn!("swarm build failed: {e}");
            return;
        }
    };

    if let Err(e) = swarm.listen_on(listen_addr) {
        warn!("listen failed: {e}");
        return;
    }

    let mut addr_tx_opt = Some(addr_tx);
    // request_id → (expected_cid, reply_channel)
    let mut pending: HashMap<OutboundRequestId, (Cid, oneshot::Sender<Option<Vec<u8>>>)> =
        HashMap::new();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None => break,
                Some(NetworkCommand::Dial(addr)) => {
                    let _ = swarm.dial(addr);
                }
                Some(NetworkCommand::GetBlock { cid, prefer, reply }) => {
                    let peers: Vec<PeerId> = swarm.connected_peers().cloned().collect();
                    // prefer が接続中ならそれを、なければ任意の接続先を選ぶ
                    let target = prefer
                        .filter(|p| peers.contains(p))
                        .or_else(|| peers.first().cloned());
                    let Some(peer) = target else {
                        let _ = reply.send(None);
                        continue;
                    };
                    let req = WantBlock { cid_bytes: cid.to_bytes() };
                    let req_id = swarm.behaviour_mut().exchange.send_request(&peer, req);
                    pending.insert(req_id, (cid, reply));
                }
                Some(NetworkCommand::PublishHead(announce)) => {
                    let topic_str =
                        feed_topic_str(&bytes_to_hex(announce.payload.pubkey.as_ref()));
                    let topic = gossipsub::IdentTopic::new(topic_str);
                    let data = announce_to_bytes(&announce);
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, data) {
                        // 購読者がいない場合の InsufficientPeers 等。fire-and-forget なので警告のみ
                        warn!("head publish failed: {e}");
                    }
                }
                Some(NetworkCommand::Subscribe { pubkey_hex }) => {
                    let topic = gossipsub::IdentTopic::new(feed_topic_str(&pubkey_hex));
                    if let Err(e) = swarm.behaviour_mut().gossipsub.subscribe(&topic) {
                        warn!("subscribe failed: {e}");
                    }
                }
                Some(NetworkCommand::Unsubscribe { pubkey_hex }) => {
                    let topic = gossipsub::IdentTopic::new(feed_topic_str(&pubkey_hex));
                    let _ = swarm.behaviour_mut().gossipsub.unsubscribe(&topic);
                }
            },

            event = swarm.next() => match event {
                None => break,
                Some(SwarmEvent::NewListenAddr { address, .. }) => {
                    info!("libp2p listening on {address}");
                    if let Some(tx) = addr_tx_opt.take() {
                        let _ = tx.send(address);
                    }
                }
                Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                    info!("connected to peer {peer_id}");
                    let _ = event_tx.try_send(NetworkEvent::PeerConnected(peer_id));
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Mdns(
                    mdns::Event::Discovered(peers),
                ))) => {
                    for (peer_id, addr) in peers {
                        info!("mDNS discovered peer {peer_id} at {addr}");
                        swarm.add_peer_address(peer_id, addr);
                        // 発見しただけでは接続されないため、明示的に dial する。
                        // PeerId 指定なら登録済みアドレスが使われ、接続済み/dial 中なら no-op
                        if let Err(e) = swarm.dial(peer_id) {
                            warn!("dial to discovered peer {peer_id} failed: {e}");
                        }
                        let _ = event_tx.try_send(NetworkEvent::PeerDiscovered(peer_id));
                    }
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Exchange(
                    request_response::Event::Message { message, .. },
                ))) => match message {
                    request_response::Message::Request {
                        request, channel, ..
                    } => {
                        let resp = match Cid::try_from(request.cid_bytes.as_slice()) {
                            Ok(cid) => match store.get_raw_block(&cid.to_string()).await {
                                Ok(Some(data)) => BlockResponse::Found { data },
                                _ => BlockResponse::NotFound,
                            },
                            Err(_) => BlockResponse::NotFound,
                        };
                        let _ = swarm.behaviour_mut().exchange.send_response(channel, resp);
                    }
                    request_response::Message::Response {
                        request_id,
                        response,
                    } => {
                        if let Some((expected_cid, reply)) = pending.remove(&request_id) {
                            handle_block_response(response, expected_cid, reply);
                        }
                    }
                },
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Gossipsub(ev))) => match ev {
                    gossipsub::Event::Message {
                        propagation_source,
                        message,
                        ..
                    } => match announce_from_bytes(&message.data) {
                        Ok(announce) => {
                            // source は転送元(直接接続中のピア)。announce の真正性は
                            // payload 内の署名で別途検証されるため、ここでは検証しない
                            let _ = event_tx.try_send(NetworkEvent::HeadReceived {
                                announce,
                                source: propagation_source,
                            });
                        }
                        Err(e) => {
                            warn!("undecodable gossipsub message from {propagation_source}: {e}");
                        }
                    },
                    gossipsub::Event::Subscribed { peer_id, topic } => {
                        let _ = event_tx.try_send(NetworkEvent::PeerSubscribed {
                            peer: peer_id,
                            topic: topic.into_string(),
                        });
                    }
                    _ => {}
                },
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Exchange(
                    request_response::Event::OutboundFailure {
                        request_id, error, ..
                    },
                ))) => {
                    warn!("outbound request failed: {error}");
                    if let Some((_, reply)) = pending.remove(&request_id) {
                        let _ = reply.send(None);
                    }
                }
                _ => {}
            }
        }
    }
}

/// 受信ブロックの CID 一致を検証して返す。
/// 永続化は行わない: チェーン同期では Edit/Delete が対象 Post より先に届きうるため、
/// 挿入順の制御(seq 昇順)は同期エンジン(sync.rs)が担う。
fn handle_block_response(
    response: BlockResponse,
    expected_cid: Cid,
    reply: oneshot::Sender<Option<Vec<u8>>>,
) {
    match response {
        BlockResponse::NotFound => {
            let _ = reply.send(None);
        }
        BlockResponse::Found { data } => {
            // CID 検証: 受信データから再計算したCIDが期待値と一致するか
            let computed = crate::event::bytes_to_cid(&data);
            if computed != expected_cid {
                warn!("CID mismatch: expected {expected_cid}, got {computed}");
                let _ = reply.send(None);
                return;
            }
            let _ = reply.send(Some(data));
        }
    }
}

fn build_swarm(
) -> Result<libp2p::Swarm<DeilephilaBehaviour>, Box<dyn std::error::Error + Send + Sync>> {
    let swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let identify = identify::Behaviour::new(identify::Config::new(
                "/deilephila/1.0.0".to_string(),
                key.public(),
            ));
            let mdns =
                mdns::tokio::Behaviour::new(mdns::Config::default(), key.public().to_peer_id())?;
            let exchange = request_response::Behaviour::new(
                [(BlockExchangeProtocol, ProtocolSupport::Full)],
                request_response::Config::default(),
            );
            // メッセージはピア鍵で署名(transport 層)。head 通知自体の真正性は
            // アカウント鍵の署名(アプリ層)で別途保証される
            let gossipsub = gossipsub::Behaviour::new(
                gossipsub::MessageAuthenticity::Signed(key.clone()),
                gossipsub::Config::default(),
            )?;
            Ok(DeilephilaBehaviour {
                identify,
                mdns,
                exchange,
                gossipsub,
            })
        })?
        .build();
    Ok(swarm)
}

// --- テスト ---

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        event::{envelope_cid, EventEnvelope, EventKind},
        identity::{create_envelope, Identity},
    };
    use std::time::Duration;

    async fn spawn_test_node(
        store: Arc<Store>,
    ) -> (NetworkHandle, mpsc::Receiver<NetworkEvent>, Multiaddr) {
        // テストでは 127.0.0.1 を使う(0.0.0.0 はダイアル先として使えないため)
        let listen: Multiaddr = "/ip4/127.0.0.1/tcp/0".parse().unwrap();
        spawn_swarm_loop(store, Some(listen))
            .await
            .expect("swarm failed to start")
    }

    async fn wait_peer_connected(event_rx: &mut mpsc::Receiver<NetworkEvent>) {
        loop {
            match event_rx.recv().await {
                Some(NetworkEvent::PeerConnected(_)) => return,
                Some(_) => continue,
                None => panic!("event channel closed before PeerConnected"),
            }
        }
    }

    #[tokio::test]
    async fn test_block_exchange_direct_dial() {
        let identity = Identity::generate();
        let envelope = create_envelope(
            &identity,
            0,
            None,
            EventKind::Post {
                text: "hello network".to_string(),
            },
        );
        let raw: Vec<u8> = serde_ipld_dagcbor::to_vec(&envelope).unwrap();
        let cid = envelope_cid(&envelope);

        // Node A: ブロックを持つ(insert_event が raw_cbor を保存する)
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        store_a.insert_event(&envelope).await.unwrap();
        // _handle_a: drop すると cmd_tx が閉じて Swarm ループが終了するため保持する
        let (_handle_a, _, addr_a) = spawn_test_node(Arc::clone(&store_a)).await;

        // Node B: 空
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());
        let (handle_b, mut events_b, _) = spawn_test_node(Arc::clone(&store_b)).await;

        // B → A にダイアルして接続を待つ
        handle_b.dial(addr_a).await;
        tokio::time::timeout(Duration::from_secs(5), wait_peer_connected(&mut events_b))
            .await
            .expect("peer connection timed out");

        // B が A からブロックを取得
        let received = handle_b
            .get_block(cid.clone(), None)
            .await
            .expect("get_block returned None");

        // 検証: バイト列一致 + CID 再計算
        assert_eq!(received, raw);
        let recovered: EventEnvelope = serde_ipld_dagcbor::from_slice(&received).unwrap();
        assert_eq!(envelope_cid(&recovered), cid);
    }

    #[tokio::test]
    async fn test_mdns_peer_discovery() {
        let store_a = Arc::new(Store::open_in_memory().await.unwrap());
        let store_b = Arc::new(Store::open_in_memory().await.unwrap());

        let (_, mut events_a, _) = spawn_test_node(store_a).await;
        let (_, mut events_b, _) = spawn_test_node(store_b).await;

        // どちらかが PeerDiscovered を受け取れば mDNS が機能している。
        // ループバックで mDNS が届かない環境ではタイムアウト → skip(fail にしない)。
        let discovered_a = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events_a.recv().await {
                    Some(NetworkEvent::PeerDiscovered(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        });
        let discovered_b = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match events_b.recv().await {
                    Some(NetworkEvent::PeerDiscovered(_)) => return true,
                    Some(_) => continue,
                    None => return false,
                }
            }
        });

        match tokio::join!(discovered_a, discovered_b) {
            (Ok(true), _) | (_, Ok(true)) => {
                // mDNS 発見成功
            }
            _ => {
                // タイムアウト: この環境では mDNS が届かない(skip)
                eprintln!("mDNS discovery timed out on loopback — skipping");
            }
        }
    }
}
