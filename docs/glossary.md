# 用語集・表記規則

deilephila の設計・実装で用いる用語を定義し、表記と文書の書き方を統一する。新規の記述・コード・コミットメッセージはここに従う。

## 1. 表記・執筆規約

### 1.1 用語の表記

- 英語由来の技術用語は英語形に統一する: `head` / `fold` / `projection` / `sequence` / `genesis`。日本語の名詞・助詞が続く場合は半角スペースを挟む(例: `head` ポインタ、`fold` 結果、`head` を返す)。
- コード識別子・型名・フィールド名はバッククォートで表記する: `EventEnvelope` / `EventKind` / `head_cid` / `validity` / `PubKey`。
- 定着したカタカナ語はカタカナのまま使う: ピア / ノード / ブロック / トピック / タイムライン / フォロー / フォロワー / プロフィール / リレー。
- 同義語は「正準」列の語に寄せる。別名は初出の併記にとどめ、新規記述では正準語を使う。
- 括弧とコロンは半角の `()` `:` を使う。読点・句点は「、」「。」。
- ユーザー向けUIの文言は本表に縛られない(例: 内部概念の「イベント」もUIでは「投稿」と表示してよい)。

### 1.2 文体

- 太字(`**`)は使わない。強調したいことは文の構造と順序で表現する。
- テーブルは純粋な表形式データにのみ使う(用語表・フィールド定義・スキーマ・マイルストーン表など)。モジュールの責務・手順・性質の列挙は箇条書きか散文で書く。
- 引用記法(`>`)による注記は使わず、通常の段落・箇条書きに含める。

### 1.3 文書の構成

- 各ドキュメントは現在目指している最終仕様を記述する。実装段階の注記(「実装済み」「Mn で実装」等)は [mvp.md](mvp.md) にのみ置き、他ファイルで段階情報が必要な場合は mvp.md §3 への参照で示す。
- 方式の採用経緯・不採用案・要決定事項は [mvp.md](mvp.md) に集約する。他ファイルには採用した結果とその理由のみを書く。
- [main.md](main.md) は簡潔に保つ(決定の要約と目次のみ。詳細は各専門ファイルへ)。
- 設計判断を反映するときは main.md の D表 / 該当専門ファイル / mvp.md の R節 の3箇所の整合を確認する。
- ファイル間参照は markdown リンクに節番号を添える(例: `[networking.md](networking.md) §4`)。節番号を変更したら参照元も追随させる。
- 新しい用語を導入したら本ファイルの用語表に追記する。

## 2. アイデンティティ・暗号

| 正準 | 識別子・英語 | 定義 | 詳説 |
|------|------------|------|------|
| アカウント | — | ユーザーの同一性。Ed25519 公開鍵そのもの(サーバー登録なし、自己主権型)。 | [data-model.md](data-model.md) §1 |
| 公開鍵 | `PubKey`(`[u8; 32]`) | Ed25519 公開鍵。アカウントID・IPNS名・署名検証鍵を兼ねる。 | [data-model.md](data-model.md) §1 |
| 秘密鍵 | — | Ed25519 秘密鍵。Keystore に暗号化保管。紛失＝復旧不可(R5)。 | [architecture.md](architecture.md) §3.3 |
| 署名 | `signature`(`[u8; 64]`) | `payload` の canonical DAG-CBOR に対する Ed25519 署名。著者性を保証。 | [data-model.md](data-model.md) §7 |
| Keystore | — | 秘密鍵の保管庫。パスフレーズから Argon2 で鍵導出し AES-256-GCM で暗号化、アプリデータディレクトリの `keystore.bin` に保存(OSセキュアストレージ非依存)。 | [architecture.md](architecture.md) §3.3 |

## 3. データモデル(イベントチェーン)

| 正準 | 識別子・英語 | 定義 | 詳説 |
|------|------------|------|------|
| イベント | `EventEnvelope` | チェーンに追記される最小単位。`payload` + `signature` の署名付き封筒。 | [data-model.md](data-model.md) §2.1 |
| 封筒 | envelope | 署名付き外殻の呼称(= `EventEnvelope`)。 | [data-model.md](data-model.md) §2.1 |
| ペイロード | `EventPayload` | 署名対象の本体。`seq` / `kind` / `prev` / `author` / `timestamp`(種別ごとの本体は `kind` が内包)。 | [data-model.md](data-model.md) §2.1 |
| イベント種別 | `EventKind` | `Post` / `Edit` / `Delete` / `Reply` / `Profile` / `Follow` …。 | [data-model.md](data-model.md) §2.3 |
| イベントチェーン | — | ユーザーごとの署名付き追記専用ハッシュチェーン。全状態 = この `fold`。 | [data-model.md](data-model.md) §2 |
| 投稿 | `Post`(kind) | テキスト投稿。`EventKind` の一種。UI上の表示概念名でもある。 | [data-model.md](data-model.md) §2.3 |
| 編集 | `Edit`(kind) | 既存投稿の訂正イベント。`target` の `Post` を上書きせず追記、`seq` 最大が有効。 | [data-model.md](data-model.md) §4 |
| 削除 | `Delete`(kind) | 削除マーカー(旧称 `Tombstone`)。論理削除(実データは残存しうる)。 | [data-model.md](data-model.md) §4 |
| genesis | `seq: 0` | チェーン最初のイベント(`prev` が `None`)。 | [data-model.md](data-model.md) §2.2 |
| seq | `sequence`(`u64`) | チェーン内連番。欠損・fork 検出に使用。IPNS-headレコードの `sequence` と対応。 | [data-model.md](data-model.md) §2.1 |
| prev | `prev`(`Option<Cid>`) | 1つ前のイベントの CID。チェーンのリンク。 | [data-model.md](data-model.md) §2.1 |
| CID | — | コンテンツ識別子。sha2-256 multihash + dag-cbor codec。不変アドレス。 | [data-model.md](data-model.md) §2.1 |
| DAG-CBOR | — | イベントの canonical シリアライズ形式(決定的)。 | [data-model.md](data-model.md) §7 |
| head | `head_cid` | チェーン最新イベントの CID。唯一の可変ポインタ。 | [networking.md](networking.md) §4 |
| fold | — | イベント列を畳み込んで現在状態(投稿一覧・プロフィール・フォロー)を導く操作。 | [data-model.md](data-model.md) §2 |
| projection | — | `fold` 結果を SQLite に投影したキャッシュ。正典ではなく再構築可能。 | [data-model.md](data-model.md) §6 |
| 正典 | source of truth | 最終的な真実の所在。deilephila ではチェーン(SQLiteは projection)。 | [data-model.md](data-model.md) §6 |

## 4. ネットワーク

| 正準 | 識別子・英語 | 定義 | 詳説 |
|------|------------|------|------|
| IPNS-headレコード | `IpnsRecord` | head を指す署名付き可変ポインタ。 | [data-model.md](data-model.md) §2.4 |
| IPNS名 | `name` | レコードの名前。アカウント公開鍵から導出。 | [data-model.md](data-model.md) §2.4 |
| value | `value` | IPNS-headレコードが指す値(= `head_cid`)。 | [data-model.md](data-model.md) §2.4 |
| validity | `validity` / EOL | レコードが失効する時刻(EOL)。失効前にオンライン中 `republish` する。 | [data-model.md](data-model.md) §2.4 |
| argmax統一規則 | — | 候補レコード群から「署名検証OK かつ 最大 `sequence`」を選ぶ唯一の head 解決規則。 | [networking.md](networking.md) §4 |
| フォローグラフ探索 | `GetLatestHead` | gossipsub/DHT で取得不能・鮮度不審時に、フォロワー集合へ最良レコードを照会する request-response。 | [networking.md](networking.md) §4.3 |
| ピア | peer / `PeerId` | ネットワーク上の他参加者。 | [networking.md](networking.md) §2 |
| ノード | — | 自インスタンスの libp2p Node(常駐 async タスク)。 | [architecture.md](architecture.md) §3.4 |
| ブロック | block | ブロック交換プロトコルで授受される DAG-CBOR バイト列。イベント1件 = 1ブロック。 | [networking.md](networking.md) §3 |
| トピック | `deilephila/feed/<pubkey>` | gossipsub の購読単位。フォロー = subscribe。 | [networking.md](networking.md) §4.1 |
| DHT | Kademlia / `kad` | 分散ハッシュテーブル。ピア発見と IPNS-headレコードの永続配信。 | [networking.md](networking.md) §1 |
| NAT越え | autonat / dcutr / relay | 到達性判定・hole punching・リレーの総称。 | [networking.md](networking.md) §5 |
| peer scoring | — | gossipsub のピア評価。検証失敗ピアをメッシュから除外。 | [networking.md](networking.md) §6 |
| タイムライン | — | フォロー集合の投稿を時系列にマージした表示。 | [networking.md](networking.md) §4.4 |
| multiaddr | — | ピアの到達アドレス表現(libp2p)。 | [networking.md](networking.md) §2 |
