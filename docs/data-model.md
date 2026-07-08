# データモデル

すべての永続データはIPFS上のコンテンツアドレス済みブロック(DAG-CBOR)として表現し、CIDで参照する。 作成者の真正性はEd25519署名で保証する。

## 1. アイデンティティ

- アカウント = Ed25519キーペア
- アカウント生成 = キーペア生成
- ユーザーIDの表記は現状公開鍵のhex(64桁)をUI・IPC 共通で用いる。
- 秘密鍵は Keystore に暗号化保管([architecture.md](architecture.md) §3.3)。

## 2. イベントチェーン

ユーザーの行為(投稿・編集・削除・プロフィール変更・フォロー等)はすべて1種類の署名付きイベントとして表現し、ユーザーごとにイベントチェーンへ追記する(イベントソーシング)。ユーザーの現在の全状態 = このチェーンの fold として一貫して導出する([architecture.md](architecture.md) §1.2)。

### 2.1 イベントの封筒構造

各イベントは「署名付き封筒(envelope)」として保存される。署名対象は `payload` 全体。

```rust
struct EventEnvelope {
    payload: EventPayload,
    signature: [u8; 64],     // Ed25519署名(payloadのcanonical DAG-CBORに対して)
}

// フィールド宣言順は DAG-CBOR canonical 順(キー長昇順→辞書順)に合わせる
struct EventPayload {
    seq: u64,                // チェーン内の連番(0 = genesis)
    kind: EventKind,         // Post / Edit / Delete / Profile / Follow ...
    prev: Option<Cid>,       // 1つ前のイベントのCID(genesis のみ None)
    author: [u8; 32],        // 著者の公開鍵(= アカウント)
    timestamp: i64,          // Unix epoch(ミリ秒、著者申告値)
}
```

- イベントを canonical DAG-CBOR でシリアライズ → ハッシュ → CID を得る。これがそのイベントの不変アドレスとなる。
- `prev` により、ユーザーごとに1本の追記専用ログ(イベントチェーン)が形成される。
- `seq` は欠損・分岐検出に使う。同一 seq で異なる CID が観測されたら fork = 不正/鍵漏洩の疑い。
- 編集・削除も過去イベントを書き換えず、対象を指す新イベントとして追記する(後述 §4)。「投稿」という対象は、その生成イベント(`Post`)の CID で一意に参照される。

### 2.2 チェーン構造図

```
   genesis            ev1              ev2           ev3(head)
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│CID1       │◀─┐ │CID2       │◀─┐ │CID3       │◀─┐ │CID4       │
│prev:─     │  └-│prev:CID1  │  └-│prev:CID2  │  └-│prev:CID3  │
│seq:0      │prev│seq:1      │prev│seq:2      │prev│seq:3      │
│kind:Post  │    │kind:Post  │    │kind:Edit  │    │kind:Delete│
│sig        │    │sig        │    │sig        │    │sig        │
└───────────┘    └───────────┘    └───────────┘    └───────────┘
```

- head = 最新イベントの CID。これだけが時間とともに変化する可変ポインタとなる。
- 表示状態の構築 = head から `prev` を辿ってイベントを集め、fold して現在の投稿一覧・プロフィール・フォロー状態を得る。

### 2.3 イベント種別(EventKind)

| kind | 用途 | フィールド |
|------|------|------|
| `Post` | 投稿の作成 | `{ text: String }` |
| `Edit` | 既存投稿の訂正 | `{ target: Cid, text: String }` |
| `Delete` | 投稿の削除マーカー | `{ target: Cid }` |
| `Reply` | 投稿への返信 | `{ text: String, target: Cid }` |
| `Profile` | プロフィール更新 | `{ display_name: String, bio: String, avatar_cid?: Cid }` |
| `Follow` | フォロー状態の公開 | `{ added: [PubKey], removed: [PubKey] }` |

```rust
pub enum EventKind {
    Post {
        text: String,
    },
    Edit {
        text: String,
        target: Cid,
    },
    Delete {
        target: Cid,
    },
    Reply {
        text: String,
        target: Cid,
    },
    Profile {
        bio: String,
        avatar_cid: Option<Cid>,
        display_name: String,
    },
    Follow {
        added: Vec<[u8; 32]>,
        removed: Vec<[u8; 32]>,
    },
}
```

投稿・編集・削除・プロフィール・フォローをすべて同一粒度のイベントとして同じチェーンに載せることで、CID一致,署名,イベントチェーンの連続性の検証パスと配信経路を1本に統一し、将来のプロトコルの拡張性を担保する。

### 2.4 head ポインタ(IPNS-headレコード)

イベントチェーンの head(最新イベント CID)だけが可変であるため、これを指すポインタは署名付き IPNS-headレコードで表現する([networking.md](networking.md) §4)。IPNS名 = アカウント公開鍵なので、アイデンティティとポインタが1鍵で統一される。

| フィールド | 内容 |
|-----------|------|
| `name` | アカウント公開鍵(から導出される IPNS 名) |
| `value` | head CID |
| `sequence` | headイベントのseq |
| `validity` | レコードが失効する時刻(EOL) |
| `display_name` | プロフィール表示名をレコードに同梱(§3) |
| `profile_cid` | 最新 `Profile` イベントの CID |
| `signature` | 上記すべてに対する署名(`name` の鍵で検証) |

headポインタは新規イベント(投稿・編集・削除・プロフィール変更…)のたびに更新し、常にイベントチェーン上の最新のイベントを指す。

- レコードはイベントと同じ canonical DAG-CBOR でシリアライズし、署名対象は payload のシリアライズ済みバイト列とする(IPNS 仕様の protobuf 形式は不採用。経緯は [mvp.md](mvp.md) §4 R1)。
- `validity` は Unix epoch ミリ秒の絶対時刻。現在時刻が `validity` 以上なら失効(EOL)とみなす。失効済みレコードも署名と `sequence` は検証可能であり、head 解決の候補としては有効([networking.md](networking.md) §4.3)。
- 同一 `sequence` の候補が複数あるときは `validity` が最大のものを採用する(head 解決の比較キーは (sequence, validity) の辞書式、[networking.md](networking.md) §4)。republish は `sequence` を変えず validity のみ更新した再発行であるため。
- `display_name` は未設定のとき空文字列とする(SQLite projection と同じ規約)。

## 3. プロフィール

表示名はユーザーが参照されるたびに必要な高頻度・高価値メタデータである一方、ブロック(イベント)のキャッシュ範囲はクライアント任意([networking.md](networking.md) §3.2)のため、長期オフライン＋全フォロワーのキャッシュ失効で、普通の投稿と同じ扱いだと表示名すら取得できなくなるという問題がある。これを避けるため、重要度に応じて可用性を階層化する。

| Tier | 置き場所 | 内容 | 可用性 |
|------|---------|------|--------|
| Tier 0 | IPNS-headレコード(§2.4)に同梱 | `display_name` + `profile_cid` | = head の可用性 |
| Tier 1 | チェーン上最新の `Profile` イベント | bio、アバター等のフル項目 | ベストエフォート |

- 正典はチェーンの `Profile` イベントであり、スナップショットは可用性のための非正規化キャッシュ。イベントチェーンをfoldし最新の`Profile`イベントに到達できたらそちらを優先する(不一致時はチェーンが勝つ)。
- 現在のフルプロフィール = チェーンを head から走査して見つかる最新の `Profile` イベントだが、 IPNS-headレコードに `profile_cid`があれば走査せず1ホップで直接取得できる。
- 表示名変更時: チェーンに `Profile` イベントを追記(head 更新)し、次の IPNS-headレコードが新 `display_name` / `profile_cid` を自然に同梱する(投稿が無くても profile 変更だけで seq+1 の再発行が可能)。
- プロフィール描画の劣化段階: IPNS-headレコードあり→表示名を描画 / チェーン到達→フルプロフィール / 何も無し→短縮公開鍵プレースホルダ
- アバター画像は別ブロックとして IPFS に置き `Profile` イベントの `avatar_cid` で参照。

## 4. 編集・削除

チェーンは追記専用、IPFS ブロックは不変であるため、編集・削除は過去イベントの上書きではなく、対象を指す新イベントの追記で表現する。

- 編集: `Edit { target: Cid, text }` イベントを追記する。`target` は対象投稿の生成イベント(`Post`)の CID。fold 時、同一 target に複数の `Edit` イベントがあれば `seq` 最大のものを採用する。
- 削除: `Delete { target: Cid }` イベント を追記する。他ピアは `Delete` を見たら当該投稿を非表示にする。
- `Edit` / `Delete` は `target` が同一 author の `Post` を指す場合のみ有効。fold は target の author 一致を強制し、他 author の投稿を指すイベントは無視する。イベント自体は署名・チェーン構造としては正当なので、保存(§6 `events`)とチェーン同期は妨げない(意味論の判定は fold の責務)。

いずれも元のイベントブロックは不変・残存し続け、他ピアがキャッシュしていれば取得可能。編集前の本文も削除済み投稿も、原理的には取得されうる。

## 5. フォロー / フォロワー

- フォロー(自分→相手): head更新の購読リスト。ローカルの `follows` テーブルに保持する(§6)。`Follow` イベントによるフォローリストの公開は任意で、行わなくてもよい。
- フォロワー(相手→自分): P2Pでは「誰が自分を購読しているか」を確実に知ることはできないため、フォロワー数は近似値/観測値であることを設計前提とする。

フォロー = 相手の公開鍵に紐づく gossipsub トピックを購読し、head 更新を受け取ること([networking.md](networking.md))。

## 6. ローカルインデックス(SQLite)

IPFS/イベントチェーンが"正典"だが、表示のたびに走査・fold するのは非効率なため、SQLite に生イベントの保管と fold 結果の projection の両方を持つ。
projection は純粋な関数(`events` の fold)なので、DBが壊れても `events` / チェーンから再構築可能。

| テーブル | 役割 | 主な列 |
|-----------------|------|--------|
| `events` | 検証済み生イベント(チェーンそのもの。検証は挿入前に実施) | cid, author, seq, prev_cid, timestamp, kind_tag, kind_json, raw_cbor(DAG-CBOR 原文 — ブロック提供と再検証の源泉) |
| `posts` | `Post`+`Edit`+`Delete` を fold した表示用投稿 | cid(=生成イベント), author, seq, text(編集適用後), timestamp, edited, deleted, latest_edit_seq(last-write-wins 判定用) |
| `accounts` | プロフィール fold 結果 + head 記録 | pubkey, display_name, bio, latest_head_cid, last_seen |
| `head_records` | 最新 IPNS-headレコードの常時保持(自分 + フォロー相手。[networking.md](networking.md) §3.2) | pubkey, sequence, record_bytes(署名済みレコードの DAG-CBOR 原文), updated_at |
| `follows` | フォローリスト | pubkey, since |
| `peers` | ピア情報 | peer_id, multiaddrs, last_connected |

- タイムラインは独立したテーブルではなく、`posts` を「自分 + `follows` の購読集合」で絞り timestamp 降順に並べるクエリとして実装する。
- 投稿の受信/作成時に生イベントを `events` へ追記し、`kind` に対応する projection テーブルを更新する
    - `Post`→行追加
    - `Edit`→`posts.text` 更新
    - `Delete`→`deleted` フラグ
    - `Profile`→`accounts`
- 受信時の取り込みは seq 昇順で行う。`Edit`/`Delete` が対象の `Post` より先に来ると projection を更新できないため。

## 7. 正規化と署名検証

署名対象は canonical DAG-CBOR(決定的シリアライズ)。再シリアライズで同一バイト列になることを保証する。

検証順序:

1. CIDとブロック内容の一致(コンテンツアドレスの完全性)
2. `author` 公開鍵で署名検証(著者性)
3. `prev`/`seq` の連続性。

スパム対策の為、いずれかの検証に失敗したブロックは破棄し、ピアの評価を下げる。
