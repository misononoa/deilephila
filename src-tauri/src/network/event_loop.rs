//! Swarm を所有する単一タスクの `select!` ループとイベントハンドラ群。
//! Swarm は `!Sync` のためこのループだけが触り、Core とは mpsc/oneshot で会話する
//! (architecture.md §5)。

use std::{collections::HashMap, sync::Arc};

use cid::Cid;
use futures::StreamExt;
use libp2p::{
    gossipsub, identify, kad, mdns,
    request_response::{self, OutboundRequestId},
    swarm::SwarmEvent,
    Multiaddr, PeerId,
};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::{
    head::{feed_topic_str, record_from_bytes, record_to_bytes, IpnsRecord},
    network::behaviour::{build_swarm, DeilephilaBehaviour, DeilephilaBehaviourEvent},
    network::ipns::{expires_from_validity, head_record_key, validate_inbound_head_record},
    network::protocol::{BlockResponse, WantBlock},
    network::{NetworkCommand, NetworkEvent},
    store::Store,
    util::{bytes_to_cid, bytes_to_hex},
};

pub(crate) async fn run_swarm_loop(
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

    // NAT越え(M7)導入までは常にレコードを保持・応答するサーバーモードで動かす
    // (既定の自動判定は確認済み外部アドレスを要求し、LAN/ループバックでは
    // クライアントモードに留まって put/get に応答しないため)
    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    if let Err(e) = swarm.listen_on(listen_addr) {
        warn!("listen failed: {e}");
        return;
    }

    let mut addr_tx_opt = Some(addr_tx);
    // request_id → 取得中ブロックの進行状態
    let mut pending: HashMap<OutboundRequestId, PendingBlock> = HashMap::new();
    // DHT get_record クエリ → (reply_channel, 収集済み候補)
    let mut pending_resolves: HashMap<
        kad::QueryId,
        (oneshot::Sender<Vec<IpnsRecord>>, Vec<IpnsRecord>),
    > = HashMap::new();

    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => match cmd {
                None => break,
                Some(NetworkCommand::Dial(addr)) => {
                    let _ = swarm.dial(addr);
                }
                Some(NetworkCommand::GetBlock { cid, prefer, reply }) => {
                    // prefer(ブロックを持つ見込みが高いピア)を先頭候補に、
                    // 残りの接続中ピアを後続候補とする。NotFound や送信失敗の
                    // たびに次の候補へフォールバックする(ブロックを持たない
                    // ピアと接続していても、保持ピアがいれば取得できる)
                    let mut candidates: Vec<PeerId> = Vec::new();
                    if let Some(p) = prefer.filter(|p| swarm.is_connected(p)) {
                        candidates.push(p);
                    }
                    for p in swarm.connected_peers() {
                        if !candidates.contains(p) {
                            candidates.push(*p);
                        }
                    }
                    request_block_from_next(&mut swarm, &mut pending, cid, reply, candidates);
                }
                Some(NetworkCommand::PublishHead(record)) => {
                    publish_head_record(&mut swarm, &record);
                }
                Some(NetworkCommand::ResolveIpns { pubkey, reply }) => {
                    let query_id = swarm
                        .behaviour_mut()
                        .kademlia
                        .get_record(head_record_key(&pubkey));
                    pending_resolves.insert(query_id, (reply, Vec::new()));
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
                        swarm.add_peer_address(peer_id, addr.clone());
                        // DHT のルーティングテーブルにも載せる(kad は自前のテーブルを持つ)
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
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
                        if let Some(PendingBlock { cid, reply, remaining }) =
                            pending.remove(&request_id)
                        {
                            match verify_block_response(response, &cid) {
                                Some(data) => {
                                    let _ = reply.send(Some(data));
                                }
                                // NotFound / CID 不一致は次の候補ピアへ
                                None => request_block_from_next(
                                    &mut swarm,
                                    &mut pending,
                                    cid,
                                    reply,
                                    remaining,
                                ),
                            }
                        }
                    }
                },
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Identify(
                    identify::Event::Received { peer_id, info, .. },
                ))) => {
                    // 直接 dial で接続したピア(mDNS 経由でない)も DHT に参加させる。
                    // identify が伝えるリッスンアドレスをルーティングテーブルへ登録する
                    for addr in info.listen_addrs {
                        swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                    }
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Kademlia(ev))) => {
                    handle_kad_event(&mut swarm, ev, &mut pending_resolves);
                }
                Some(SwarmEvent::Behaviour(DeilephilaBehaviourEvent::Gossipsub(ev))) => match ev {
                    gossipsub::Event::Message {
                        propagation_source,
                        message,
                        ..
                    } => match record_from_bytes(&message.data) {
                        Ok(record) => {
                            // source は転送元(直接接続中のピア)。レコードの真正性は
                            // payload 内の署名で別途検証されるため、ここでは検証しない
                            let _ = event_tx.try_send(NetworkEvent::HeadReceived {
                                record,
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
                    // 送信失敗(切断済みピア等)も次の候補ピアへフォールバック
                    if let Some(PendingBlock { cid, reply, remaining }) =
                        pending.remove(&request_id)
                    {
                        request_block_from_next(&mut swarm, &mut pending, cid, reply, remaining);
                    }
                }
                _ => {}
            }
        }
    }
}

/// GetBlock の進行状態。`remaining` は未試行の候補ピアで、NotFound や
/// 送信失敗のたびに先頭から消費してフォールバックする。
struct PendingBlock {
    cid: Cid,
    reply: oneshot::Sender<Option<Vec<u8>>>,
    remaining: Vec<PeerId>,
}

/// 候補の先頭ピアへ WantBlock を送り pending に登録する。候補が尽きていたら
/// None を返して取得失敗とする。
fn request_block_from_next(
    swarm: &mut libp2p::Swarm<DeilephilaBehaviour>,
    pending: &mut HashMap<OutboundRequestId, PendingBlock>,
    cid: Cid,
    reply: oneshot::Sender<Option<Vec<u8>>>,
    mut remaining: Vec<PeerId>,
) {
    if remaining.is_empty() {
        let _ = reply.send(None);
        return;
    }
    let peer = remaining.remove(0);
    let req = WantBlock {
        cid_bytes: cid.to_bytes(),
    };
    let req_id = swarm.behaviour_mut().exchange.send_request(&peer, req);
    pending.insert(
        req_id,
        PendingBlock {
            cid,
            reply,
            remaining,
        },
    );
}

/// 受信ブロックの CID 一致を検証して返す(NotFound・CID 不一致は None)。
/// 永続化は行わない: チェーン同期では Edit/Delete が対象 Post より先に届きうるため、
/// 挿入順の制御(seq 昇順)は同期エンジン(sync.rs)が担う。
fn verify_block_response(response: BlockResponse, expected_cid: &Cid) -> Option<Vec<u8>> {
    match response {
        BlockResponse::NotFound => None,
        BlockResponse::Found { data } => {
            // CID 検証: 受信データから再計算したCIDが期待値と一致するか
            let computed = bytes_to_cid(&data);
            if computed != *expected_cid {
                warn!("CID mismatch: expected {expected_cid}, got {computed}");
                return None;
            }
            Some(data)
        }
    }
}

/// IPNS-headレコードを gossipsub(即時)と DHT(永続)の両経路へ流す。
fn publish_head_record(swarm: &mut libp2p::Swarm<DeilephilaBehaviour>, record: &IpnsRecord) {
    let data = record_to_bytes(record);

    let topic_str = feed_topic_str(&bytes_to_hex(record.payload.name.as_ref()));
    let topic = gossipsub::IdentTopic::new(topic_str);
    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, data.clone()) {
        // 購読者がいない場合の InsufficientPeers 等。fire-and-forget なので警告のみ
        warn!("head record gossip publish failed: {e}");
    }

    let kad_record = kad::Record {
        key: head_record_key(record.payload.name.as_ref()),
        value: data,
        publisher: None,
        expires: expires_from_validity(record.payload.validity),
    };
    // put_record はまずローカル store に入れてから複製クエリを開始する。
    // ピア不在などのクエリ失敗は OutboundQueryProgressed 側で警告する
    if let Err(e) = swarm
        .behaviour_mut()
        .kademlia
        .put_record(kad_record, kad::Quorum::One)
    {
        warn!("head record dht put failed: {e:?}");
    }
}

fn handle_kad_event(
    swarm: &mut libp2p::Swarm<DeilephilaBehaviour>,
    ev: kad::Event,
    pending_resolves: &mut HashMap<
        kad::QueryId,
        (oneshot::Sender<Vec<IpnsRecord>>, Vec<IpnsRecord>),
    >,
) {
    use libp2p::kad::store::RecordStore;

    match ev {
        // FilterBoth 設定により、他ノードからの put はここで検証してから手動格納する
        kad::Event::InboundRequest {
            request:
                kad::InboundRequest::PutRecord {
                    source,
                    record: Some(record),
                    ..
                },
        } => {
            let existing_seq = swarm
                .behaviour_mut()
                .kademlia
                .store_mut()
                .get(&record.key)
                .and_then(|existing| record_from_bytes(&existing.value).ok())
                .map(|r| r.payload.sequence);
            match validate_inbound_head_record(&record, existing_seq) {
                Ok(accepted) => {
                    if let Err(e) = swarm.behaviour_mut().kademlia.store_mut().put(accepted) {
                        warn!("head record store failed: {e:?}");
                    }
                }
                Err(reason) => {
                    debug!("rejected head record from {source}: {reason}");
                }
            }
        }
        kad::Event::OutboundQueryProgressed {
            id, result, step, ..
        } => {
            match result {
                kad::QueryResult::GetRecord(res) => {
                    if let Some((_, candidates)) = pending_resolves.get_mut(&id) {
                        match res {
                            Ok(kad::GetRecordOk::FoundRecord(peer_record)) => {
                                // デコード不能な候補は落とす。署名検証と argmax 選択は
                                // NetworkHandle::resolve_ipns 側で行う
                                if let Ok(decoded) = record_from_bytes(&peer_record.record.value)
                                {
                                    candidates.push(decoded);
                                }
                            }
                            Ok(kad::GetRecordOk::FinishedWithNoAdditionalRecord { .. }) => {}
                            Err(e) => debug!("dht get_record: {e}"),
                        }
                    }
                    // クエリ終端で収集済み候補を返す(候補ゼロ = 空 Vec)
                    if step.last {
                        if let Some((reply, candidates)) = pending_resolves.remove(&id) {
                            let _ = reply.send(candidates);
                        }
                    }
                }
                kad::QueryResult::PutRecord(Err(e)) => {
                    // ローカル store には格納済み。複製失敗のみの警告
                    warn!("dht put replication failed: {e}");
                }
                _ => {}
            }
        }
        _ => {}
    }
}
