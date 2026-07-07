# アーキテクチャ

## 1. 設計の核となるアイデア

### 1.1 アカウント

アカウントは公開鍵暗号のキーペアで表現される。
公開鍵をIDとして扱う。

### 1.2 イベントとイベントチェーン

投稿の作成、編集、削除、プロフィールの編集、フォローの追加、削除などのユーザー操作は全てイベント。
イベントは自身が持つCIDによって識別され、自身の1つ前のイベントのCIDを持つ。このチェーン構造の追記専用のイベントログをイベントチェーンと呼ぶ。アカウント単位で全てのイベントが同一チェーンに載る。
ユーザーの現在の状態はこのイベントチェーンの畳み込み(fold)の結果として一貫して導出する。
イベントは発行元のアカウントによって署名される。一度発行されたイベントは不変。

```
   genesis            ev1              ev2           ev3(head)
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│CID1       │◀─┐ │CID2       │◀─┐ │CID3       │◀─┐ │CID4 (head)│
│prev:─     │  └-│prev:CID1  │  └-│prev:CID2  │  └-│prev:CID3  │
│seq:0      │prev│seq:1      │prev│seq:2      │prev│seq:3      │
│kind:Post  │    │kind:Post  │    │kind:Edit  │    │kind:Delete│
│sig        │    │sig        │    │sig        │    │sig        │
└───────────┘    └───────────┘    └───────────┘    └───────────┘
```

最新イベント(head)から遡ってデータを取得する設計上、可変であるheadのCIDを常に取得できる必要がある。自身のアカウントの公開鍵から導出されるIPNS-headレコード(headポインタ)を gossipsub + DHT で配信することで、他のアカウント(ピア)からheadを発見可能とする。
フォローは相手の公開鍵から導出されるトピックを購読し、headポインタの更新を受け取ることを意味する。
IPNS-headレコードはEOL(validity)を持ち、期間内は任意ピアが再配布できる。参照したいアカウントのheadポインタをgossipsub/DHTで直接取得することができない場合、フォローグラフ内の最良レコードを探索し、利用可能な全ソースをargmax統一規則で選択、headとして扱う。
これにより、参照先のピアが一時的にオフラインであっても参照性の低下を軽減できる。

## 2. 全体構成

単一のデスクトップアプリ内に、フロントエンド(Vue/TS)とバックエンド(Rust)をTauriで統合する。バックエンド内に libp2pノードを常駐させ、これがP2Pネットワークへの参加・コンテンツ交換・リアルタイム通知のすべてを担う。

```
┌───────────────────────────────────────────────────────────┐
│                     Tauri Application                     │
│                                                           │
│  ┌────────────────────┐      ┌──────────────────────────┐ │
│  │   Frontend (Vue3)  │      │     Backend (Rust)       │ │
│  │   + TypeScript     │◀────▶│                          │ │
│  │                    │ IPC  │  ┌────────────────────┐  │ │
│  │  - タイムラインUI    │(cmd/ │  │  Application Core  │  │ │
│  │  - 投稿作成         │event)│  │  (投稿/フォロー/署名) │  │ │
│  │  - プロフィール      │      │  └─────────┬──────────┘  │ │
│  │  - 設定             │      │            │             │ │
│  └────────────────────┘      │  ┌─────────▼──────────┐  │ │
│                              │  │   Local Store      │  │ │
│                              │  │  SQLite + Keystore │  │ │
│                              │  └─────────┬──────────┘  │ │
│                              │            │             │ │
│                              │  ┌─────────▼──────────┐  │ │
│                              │  │    libp2p Node     │  │ │
│                              │  │    (async task)    │  │ │
│                              │  └─────────┬──────────┘  │ │
│                              └────────────┼─────────────┘ │
└───────────────────────────────────────────┼───────────────┘
                                            │
                            ┌───────────────▼───────────────┐
                            │   P2P Network (other peers)   │
                            └───────────────────────────────┘
```

## 3. レイヤ責務

### 3.1 Frontend(Vue 3 + TypeScript)
- 表示とユーザー操作のみを担当。秘密鍵やネットワークの詳細は持たない。
- Tauri の `invoke()` でバックエンドコマンドを呼び、`listen()` でイベント(新着投稿等)を受信。
- 状態管理は Vue の `ref` / composable のみ(規模が要求するようになったら Pinia 導入を検討)。

### 3.2 Backend / Application Core(Rust)

アプリのビジネスロジックの中心。以下のモジュールに分割

- `identity`: 鍵ペア生成・署名・検証、`EventEnvelope` 構築
- `event`: イベント型定義(`EventEnvelope`/`EventKind`)・DAG-CBOR シリアライズ・CID 計算・署名検証
- `chain`: チェーン走査・fold(タイムライン・プロフィール・フォロー状態の再構築)
- `head`: `HeadAnnounce`(署名付き head 通知)の生成・検証・シリアライズ、feed トピック名の導出
- `keystore`: Argon2 + AES-256-GCM による秘密鍵の暗号化保管
- `store`: SQLite による永続化・projection インデックス・キャッシュ(フォローリスト含む)
- `network`: libp2p ノードのラッパー(後述)
- `sync`: head 通知を起点とするチェーン同期(`prev` 遡行での未取得ブロック回収・検証・seq 昇順での取り込み)
- `app`: Application Core の集約。`AppState`(アンロック中のアカウント状態)とコマンド本体(投稿・フォロー・unlock 等)、`NetworkEvent` 消費ループ(チェーン同期の起動)、UI への通知(`UiEvent` を `Notifier` チャネルへ送出)を持つ。Tauri に依存しないため、統合テストからもフロントと同じ経路で駆動できる
- `commands`: Tauri コマンドの境界(IPC グルー)。`app` の関数を `#[tauri::command]` で包むだけの薄い層で、関数名がフロントの invoke 名になる

### 3.3 Local Store

- SQLite: 取得済み投稿のキャッシュ、フォローリスト、プロフィール、ピア情報、未読管理などのインデックス。タイムラインの高速表示はここから行う。([data-model.md](data-model.md) §6)
- Keystore: 秘密鍵の安全な保管。パスフレーズからArgon2で鍵を導出しAES-256-GCMで暗号化、アプリデータディレクトリのキーストアに保存。OSのセキュアストレージには依存しない。

### 3.4 libp2p Node

非同期タスクとして常駐し、tokio上で動く。Application Core とはチャネル(mpsc)でコマンド/イベントをやり取りする。([networking.md](networking.md))

## 4. プロセス内のデータフロー例

### 4.1 投稿フロー

1. フロントエンド: ユーザーが本文を入力 → invoke("create_post", { text })
2. identity: EventEnvelope{ payload: {seq, kind:Post, prev, author, ts}, signature } を構築・署名
3. store: イベントを DAG-CBOR にシリアライズ → CID計算 → SQLite(events テーブル)に保存
4. store: projection(posts等)を更新、headを更新
5. network: 署名付きIPNS-headレコードをgossipsubで配信し、DHTへpublish
6. フロントエンド: invokeの応答(イベントCID)を受けてフロントがタイムラインを再取得

### 4.2 タイムライン受信フロー

1. network: フォロー対象トピックの gossipsub で head 通知を受信 → NetworkEvent::HeadReceived
2. sync: 通知の署名を検証(payload 内の公開鍵で)
3. sync: 新しい head CID から prev を辿り、未取得のイベントを request_response カスタムブロック交換で取得
4. sync: 各イベントの署名・author・seq 連続性を検証 → seq 昇順で store に取り込み(events + projection 更新)
5. app: 新規イベントを取り込んだら `UiEvent::TimelineUpdated` を通知 → ブリッジが Tauri イベント "timeline-updated" に変換しフロントへ → フロントがタイムラインを再取得

## 5. 並行性モデル

- libp2pのSwarm は単一の `select!`ループで駆動(Swarm は `!Sync`のため単一タスク所有が原則)。
- Application Core からのリクエストは `mpsc` チャネル経由でこのループに渡し、結果は `oneshot` で返す。
- 重い処理(チェーン走査、署名一括検証)は別タスク/`spawn_blocking` にオフロード。

## 6. クレート選定(Rust)

| 用途 | 選定 |
|------|-------------|
| P2P | libp2p(identify, mdns, request-response, gossipsub, kad, autonat, dcutr, relay) |
| コンテンツ交換 | request_response + カスタムcodec + cid + multihash + 自前ブロックストア(SQLite) |
| シリアライズ | serde + serde_ipld_dagcbor |
| 署名 | ed25519-dalek |
| 非同期 | tokio |
| DB | sqlx(SQLite) |
| パスフレーズ暗号化,鍵保管 | aes-gcm + argon2 |
