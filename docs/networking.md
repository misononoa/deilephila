# ネットワーク層

rust-libp2p を基盤に、単一の libp2p Swarm でピア発見・コンテンツ交換・リアルタイム通知・NAT越えを担う。

## 1. NetworkBehaviour 構成

```rust
#[derive(NetworkBehaviour)]
struct DeilephilaBehaviour {
    identify:  identify::Behaviour,                              // ピア情報交換
    mdns:      mdns::tokio::Behaviour,                          // LAN内ピア発見
    exchange:  request_response::Behaviour<BlockExchangeCodec>, // ブロック取得/提供
    gossipsub: gossipsub::Behaviour,                            // head 更新のリアルタイム配信
    kademlia:  kad::Behaviour,                                  // DHT: ピア発見 + IPNS-headレコード
    reqresp:   request_response::Behaviour<GetLatestHeadCodec>, // GetLatestHead 照会(§4.3)
    autonat:   autonat::Behaviour,                              // 自分の到達性判定
    dcutr:     dcutr::Behaviour,                                // hole punching
    relay:     relay::client::Behaviour,                        // リレー経由接続・最終手段
}
```

各 Behaviour を導入するマイルストーンは [mvp.md](mvp.md) §3 を参照。

- mDNS で発見したピアには明示的に dial して接続する(`add_peer_address` はアドレスの記録のみで接続しないため)。
- IPNS-headレコード(署名・seq・EOL)は自前で生成/検証し、`kademlia`(DHT)の `Record` として put/get する。リアルタイム配信は同じレコードを `gossipsub` に流す(§4)。rust-libp2p に IPNS のターンキー実装は無いため。
- トランスポートは TCP+Noise+Yamux をベースに QUIC を併用する(§5)。接続暗号は Noise で、アプリ層の署名(Ed25519)とは別レイヤ。

## 2. ピア発見(Discovery)

完全分散を維持しつつ「最初の接続先」をどう得るかは P2P の現実的課題であり、多層で対応する。

1. ブートストラップノード: 既知の公開ピア(運営/コミュニティ提供)の multiaddr を初期リストに同梱する。中央サーバーではなく単なる入口であり、後で別ピアに乗り換え可能。単一障害点にならないよう複数アドレスを持たせ、ユーザーが追加・変更できるようにする。
2. Kademlia DHT: 接続後は DHT でピアを発見・拡張する。
3. mDNS: 同一LAN内のピアを自動発見する(開発・ローカル利用に便利)。
4. (将来)ピア交換: 接続済みピアから既知ピアリストを受け取る。

## 3. コンテンツ交換(IPFS層)

### 3.1 ブロック交換プロトコル

ブロック交換は `request_response` のカスタムプロトコル + 自前ブロックストア(SQLite)で実装する。ブロックは DAG-CBOR、CID は sha2-256 multihash + dag-cbor codec。本アプリは公開 IPFS との相互運用を必要としないため bitswap 互換は不要。IPLD・IPNS・DHT・gossipsub はブロック交換の transport プロトコルと独立しているため、この層の実装を差し替えても他層に影響しない。

ワイヤ形式は u32(ビッグエンディアン)長プレフィックス付きバイト列。受信側は申告長がリクエスト(CID bytes)256B・レスポンス(ブロック本体)1MiB の上限を超える場合、バッファを確保せず `InvalidData` として即拒否する(悪意あるピアによるメモリ DoS の防止)。

### 3.2 ブロックのライフサイクル(複製・保持)

複製範囲・保持期間はクライアントが任意設定できる([mvp.md](mvp.md) §4 R2)。デフォルトは以下。

- 自チェーンは常時ピン留め(上限適用外)。オンライン中はブロック交換プロトコルで提供する。
- フォロー相手の最新IPNS-headレコード(極小)は常時保持する。ポインタとブロックを分離して持ち、表示名(§4)とフォローグラフ探索(§4.3)の可用性の床を守る。
- フォロー相手のイベントブロックはアカウント別上限＋全体上限の範囲で LRU キャッシュする。
- 保持しているブロックはブロック交換プロトコルで提供する。
- 整合性は受信時の CID+署名検証で担保されるため、複製ポリシーを緩めても安全。

## 4. head(最新イベント)の配信と発見

CIDは不変なので、「今の head はどれか」を指す可変ポインタを別途解決する必要がある。

ポインタは署名付き IPNS-headレコードに一本化する。アカウントが既に Ed25519 鍵であるため、IPNS名 = アカウント公開鍵とし、アイデンティティと可変ポインタを1鍵で統一する。レコードは head CID を指し、`sequence` と `validity(EOL)` を内包する。

head 解決は「複数ソースから得た候補レコードのうち、署名検証OKかつ最大 sequence のものを選ぶ」という argmax統一規則で行う。gossipsub・DHT・フォローグラフ探索はすべて候補レコードのソースであり、優劣の階層ではない。これにより stale なレコードに引きずられず、常に最良の head へ収束する。

```
IPNS-headレコード = { name: pubkey, value: head_cid, sequence, validity(EOL),
                 display_name, profile_cid,   // プロフィールスナップショット(data-model.md §3)
                 signature }
   候補ソース:
     ├─ gossipsub topic deilephila/feed/<pubkey>   … 即時・低コスト(§4.1)
     ├─ kademlia DHT の Record                   … 永続・後発フォロワー向け(§4.2)
     └─ フォローグラフ内の最良レコード探索        … 上記で取得不能/鮮度不審時に起動(§4.3)
   選択: argmax_sequence( 検証OKの候補群 )
```

### 4.1 即時ソース: gossipsub(IPNS-headレコードの搬送)

- 各ユーザーは公開鍵から導出したトピック `deilephila/feed/<pubkey>` に対し、新規投稿のたびに IPNS-headレコードそのもの(`sequence` を +1 して head CID を更新)を publish する(段階導入の詳細は [mvp.md](mvp.md) §3)。
- フォロー = そのトピックを subscribe すること。オンライン中のフォロワーは即座に新着を受信する。
- 受信側は署名と sequence を検証し、候補として保持する。ブロック交換プロトコルで `head_cid` を取得し、`prev` を辿り未取得分を埋める。

### 4.2 永続ソース: kad DHT 上の IPNS

- gossipsub はオンラインのピア間でしか届かない。後から来たフォロワーや、投稿者が一時オフラインだった場合に備え、同一の IPNS-headレコードを DHT に put する。
- DHT から get したレコードも候補の一つとして §4 の argmax統一規則に投入する。
- レコードは EOL を持ち、生存させるには発信者がオンライン中に定期 republish する。EOL 前であれば任意のピアが他人のレコードを再配布できる(内容で検証されるため)。
- デフォルト値: validity は発行時刻 + 48時間、republish は 12時間周期に加えアカウントの unlock 時に行う。デスクトップアプリはオンライン時間が細切れなため、周期を IPFS の既定(4時間)より疎にし、起動時の再発行で実効的な生存を確保する。
- 受信側は他ノードから put されたレコードを store へ格納する前に検証する。受理条件は、署名検証に成功すること、レコードキーが payload の `name` から導出したものと一致すること(他アカウントのキーの汚染を拒否)、既に保持するレコードの `sequence` を上回ること(stale put による巻き戻しを拒否)。

### 4.3 フォローグラフ内の最良レコード探索

- gossipsub・DHT からポインタが取得できない、または鮮度が疑わしいとき(典型: 発信者 P が EOL を超えて長期オフラインで DHT から失効)、request-response プロトコル `GetLatestHead(pubkey)` を、P の feedトピック `deilephila/feed/<P>` のメッシュ参加者(= P をフォローしている他ピア)へ送り、フォローグラフ内に存在する最良の署名済みレコードを探索する。
- 各ピアは手元の最大 sequence の署名済み head を返す。失効済み IPNS-headレコードや最新イベントの封筒でも、署名と sequence は検証可能なので候補として有効。
- 得られた応答も §4 の同じ argmax統一規則に合流させる(フォールバックの別経路ではなく、ソースの追加)。追記専用＋署名チェーンにより、悪意あるピアは stale な head を返す(出し惜しみ)ことはできても、より新しい head を偽造できない(forward-forge 不可)。最悪でも一時的に古い状態を見るだけで済む。
- データ本体(イベントブロック)の可用性も同じフォロワー集合がブロック交換プロトコルで供給する。ポインタもデータもフォロワー集合が可用性ネットワークとして担う。

対象が新規・オンラインのフォロワー0・IPNS失効、の三重苦が揃うと発見不能になる。これは純P2Pの還元不能な床として受容する。

### 4.4 タイムライン構築アルゴリズム

```
for 各フォロー対象 author:
    cands = collect( gossip受信, DHT(IPNS), 必要なら GetLatestHead探索 )
    rec   = argmax_sequence( cands を署名検証でフィルタ )   // §4 argmax統一規則
    cid   = rec.head_cid
    while cid が未取得 かつ 必要件数に未到達:
        block = exchange.get(cid)
        verify(block)               // CID一致・署名・seq連続性
        store.upsert(block)
        cid = block.prev
マージしてタイムスタンプ順に表示(SQLite の timeline クエリ、[data-model.md](data-model.md) §6)
```

## 5. NAT越え(接続性)

デスクトップアプリ同士の直接接続のため、libp2p の標準スタックを使う。

1. AutoNAT: 自分がグローバル到達可能かを判定する。
2. DCUtR + hole punching: 双方がNAT背後でも直接接続を試みる。
3. Circuit Relay v2: 直接接続が無理な場合、リレー経由で通信する(最終手段)。リレーはコミュニティ提供の公開ノードを利用可能。
4. QUIC: UDPベースで hole punching と相性が良い。

## 6. スパム / 濫用への素地

中央権威がないため、購読ベースの自然なフィルタを基本とする。

- 自分がフォローしていないユーザーのデータは原則受け取らない(タイムラインは購読集合に限定)。
- 署名・CID検証に失敗するピアはスコアを下げ、gossipsub のメッシュから除外する(libp2p gossipsub の peer scoring を活用)。
- クライアント側ブロック/ミュートリスト(ローカル)。
- (将来)Web of Trust 的な推薦・共有ブロックリスト。

## 7. Application Core との連携

```
Application Core  ──(mpsc: NetworkCommand)──▶  Swarmループ
Application Core  ◀──(mpsc: NetworkEvent)───  Swarmループ
```

- `NetworkCommand`: `PublishHead`(署名付き IPNS-headレコードを gossipsub+DHT へ同時搬送), `ResolveIpns(PubKey)`(DHTから取得), `QueryLatestHead(PubKey)`(フォローグラフ探索 §4.3), `GetBlock { cid, prefer }`(prefer = ブロックを持つ見込みが高いピアを優先。NotFound や送信失敗時は残りの接続中ピアへ順に問い合わせ、全候補が尽きたら失敗を返す), `Subscribe { pubkey_hex }`, `Unsubscribe { pubkey_hex }`, `Dial(Multiaddr)`。
- `NetworkEvent`: `HeadReceived { record, source }`(ソース問わず候補として通知。署名検証は受け手の同期処理で実施), `PeerConnected`, `PeerDiscovered`, `PeerSubscribed`。
- 各コマンド/イベントを導入するマイルストーンは [mvp.md](mvp.md) §3 を参照。
- 受信ブロックの永続化はネットワーク層では行わない(CID 一致の検証と転送のみ)。チェーン検証と seq 昇順での取り込みは Application Core 側の同期処理が担う([data-model.md](data-model.md) §6)。
- Swarm は単一タスクが所有し、`select!` でコマンドとSwarmイベントを多重化する。同期処理は Swarm ループの外で動かす(`GetBlock` がループとの mpsc 往復を要するため、ループ内から待つとデッドロックする)。
