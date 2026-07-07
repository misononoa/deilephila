//! NetworkBehaviour 合成と Swarm 構築。プロトコルの追加(M6 の `GetLatestHead`
//! request-response 等)はこのファイルの behaviour 定義と build_swarm に閉じる。

use libp2p::{
    gossipsub, identify, kad, mdns,
    request_response::{self, ProtocolSupport},
    StreamProtocol,
};

use crate::network::protocol::{BlockExchangeCodec, BlockExchangeProtocol};

#[derive(libp2p::swarm::NetworkBehaviour)]
pub(crate) struct DeilephilaBehaviour {
    pub(crate) identify: identify::Behaviour,
    pub(crate) mdns: mdns::tokio::Behaviour,
    pub(crate) exchange: request_response::Behaviour<BlockExchangeCodec>,
    pub(crate) gossipsub: gossipsub::Behaviour,
    pub(crate) kademlia: kad::Behaviour<kad::store::MemoryStore>,
}

pub(crate) fn build_swarm(
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
            // FilterBoth: 他ノードからの put をそのまま store に入れず、
            // InboundRequest::PutRecord で検証してから手動で格納する(§4.2)
            let mut kad_config = kad::Config::new(StreamProtocol::new("/deilephila/kad/1.0.0"));
            kad_config.set_record_filtering(kad::StoreInserts::FilterBoth);
            let peer_id = key.public().to_peer_id();
            let kademlia = kad::Behaviour::with_config(
                peer_id,
                kad::store::MemoryStore::new(peer_id),
                kad_config,
            );
            Ok(DeilephilaBehaviour {
                identify,
                mdns,
                exchange,
                gossipsub,
                kademlia,
            })
        })?
        .build();
    Ok(swarm)
}
