# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## プロジェクトの現状

deilephila は **完全分散型(P2P)ソーシャルネットワーク**のデスクトップアプリ。設計は概ね固まり、現在は実装フェーズ。**M0–M5 完了、次は M6(フォローグラフ探索 `GetLatestHead`)**。進捗の正典は [docs/mvp.md](docs/mvp.md) §3・§5。設計は [docs/](docs/) が正典であり、実装の各ステップで該当ドキュメントを必ず参照すること。

## 作業の進め方(重要)

**マイルストーン/機能/モジュール単位で実装を行い、実装後にその内容・アーキテクチャをユーザーが理解できるよう説明する。**

- 実装後は必ず **「何を・なぜその設計で・どう動くか」を解説する**。コードを読んで理解できるよう、設計意図・データフロー・重要な選択肢を説明すること。
- 設計判断は**議論駆動**: 論点を1つずつ確定し、合意後に docs へ反映する(勝手に設計・docs を変更しない)。docs の設計どおりに実装するとコードが煩雑になる・現実的でないなどのデメリットが見込まれる場合は、実装前にその理由を示してユーザーと相談し、合意した上で設計変更 → docs 更新 → 実装の順で進める。
- 実装前に方針を簡潔に示し、合意を得てから着手する。方針提示には **①モジュール構成と各役割、②テストの実施方法(テストケースの例とその検証方法)** を必ず含める。
- ユーザーが疑問・質問を持ったら最優先で答え、理解を確認してから次へ進む。
- 作業(実装・設計判断・仕様変更など)のたびに、[docs/](docs/) の更新が必要かを必ず確認し、必要なら該当ファイルを更新する。更新時は「docs を編集するときの原則」に従う。

## コマンド

パッケージマネージャは **pnpm**(`pnpm-lock.yaml`)。

| 目的 | コマンド |
|------|---------|
| 開発(アプリ起動。Vite + Tauri) | `pnpm tauri dev` |
| フロントのみ(ブラウザ) | `pnpm dev` |
| ビルド(型チェック→Viteビルド→Tauriバンドル) | `pnpm tauri build` |
| フロント型チェック | `pnpm check`(`vue-tsc --noEmit` のエイリアス。`pnpm exec` は使わない) |
| Rust ビルド/チェック | `cd src-tauri && cargo build` / `cargo check` |
| Rust テスト(単体) | `cd src-tauri && cargo test` / 単一: `cargo test <name>` |

> M1 以降、データモデルと署名は **単体テスト付き**で固める方針([docs/mvp.md](docs/mvp.md) §3)。Rust 側テストは `cargo test` が基準。

- `DEILEPHILA_DATA_DIR=<dir>` — アプリデータディレクトリを上書き(インスタンスごとに分離)
- `DEILEPHILA_DIAL=<multiaddr>` — 起動時に手動ダイヤルするピア(カンマ区切り可)

## アーキテクチャ(要点 — 詳細は docs/)

Tauri アプリ1プロセス内に Frontend(Vue3/TS) と Backend(Rust) を統合。Backend に **libp2p ノード**を常駐させ P2P を担う。

```
Vue3/TS  ──IPC(invoke/event)──▶  Rust Application Core  ──mpsc──▶  libp2p Swarm (単一asyncタスク)
                                  └ Local Store: SQLite(projection) + Keystore(暗号化秘密鍵)
```

設計を理解するうえで複数ファイルの読解が要る「大きな構造」：

- **アイデンティティ = Ed25519 公開鍵**(自己主権、サーバー登録なし)。秘密鍵はパスフレーズ由来鍵(Argon2 + AES-256-GCM)で暗号化しアプリデータディレクトリに保管。
- **データ = 署名付き追記専用イベントチェーン**。投稿・編集・削除・プロフィール・フォローを単一の `EventEnvelope{payload, signature}` で表現し、payload が `prev`(前イベントのCID) を持つハッシュチェーン。DAG-CBOR でシリアライズし CID で参照。種別は `EventKind`(Post / Edit / Delete / Profile / Follow …)。**現在の状態 = チェーンの fold**(イベントソーシング)。編集・削除も過去を上書きせず新イベントで表現。
- **head ポインタ = 署名付き IPNS-headレコード**(IPNS名 = アカウント公開鍵)。head CID・`sequence`・`validity(EOL)`・表示名スナップショットを内包。gossipsub(即時)と kad DHT(永続)の両経路で搬送し、取得不能/鮮度不審時はフォローグラフ内を `GetLatestHead` で探索。**解決規則は「署名検証OK かつ 最大 sequence」の argmax統一規則**(階層的フォールバックではない)。rust-libp2p に IPNS 実装は無く、レコードはイベントと同じ canonical DAG-CBOR+署名+seq+EOL を**自前実装**して `kad::Record` に載せる(IPNS 仕様の protobuf 形式は不採用)。
- **整合性と可用性の分離**: 整合性は署名+CID で常に保証され複製ポリシーと無関係。可用性は創発的・無保証。クライアントが複製範囲/保持期間を任意設定し、デフォルトは「フォロー相手を上限内LRU複製 + 最新IPNS-headレコードは常時保持 + 自チェーンは常時ピン留め」。
- **SQLite は projection であって正典ではない**。壊れてもチェーンから再構築可能。表示の高速化のためのキャッシュ/インデックス。
- **並行性**: Swarm は `!Sync` ゆえ単一タスクが所有し `select!` で駆動。Core からは `mpsc`、応答は `oneshot`。重い処理は `spawn_blocking`。

## ドキュメント構成(正典)

| ファイル | 内容 |
|---------|------|
| [docs/main.md](docs/main.md) | 概要・確定設計判断(D1–D8)・目次。**まずここを読む** |
| [docs/architecture.md](docs/architecture.md) | レイヤ構成・モジュール責務・データフロー・並行性 |
| [docs/data-model.md](docs/data-model.md) | チェーン/署名/アイデンティティ/プロフィール/IPNS-headレコード/SQLite |
| [docs/networking.md](docs/networking.md) | libp2p構成・discovery・bitswap・head 発見(§4)・NAT越え |
| [docs/mvp.md](docs/mvp.md) | MVPスコープ・マイルストーン(M0–M8)・未解決論点(R1–R5) |
| [docs/glossary.md](docs/glossary.md) | 用語集・表記規則。**用語の正準形はここが基準** |

確定済み: R1(head 発見)/R2(複製・可用性)。**未解決の論点**: R3(編集・削除セマンティクス)、R4(スパム・モデレーション)、R5(鍵管理・リカバリ)。

## docs を編集するときの原則

**正典は [docs/glossary.md](docs/glossary.md) §1(表記・執筆規約)**。docs を書く・直すときは必ず従う。要点:

- **文体**: 太字(`**`)は使わない。テーブルは純粋な表データ(スキーマ・フィールド定義・用語表等)のみで、責務・手順の列挙は箇条書き。引用記法(`>`)不使用。括弧・コロンは半角。
- **最終仕様を書く**: 実装段階の注記(「実装済み」「Mnで実装」等)は mvp.md にのみ置く。他ファイルで段階情報が要る場合は mvp.md §3 への参照で示す。方式の採用経緯・不採用案・要決定も mvp.md に集約(他ファイルは採用結果と理由のみ)。
- **main.md は簡潔に保つ**(決定の要約のみ。詳細は各専門ファイルへ。過去にmain.mdへ詳述しすぎて差し戻しあり)。
- 決定を反映する際は **main.md の D表 / 該当専門ファイル / mvp.md の R節** の3箇所の整合を必ず確認する。
- ファイル間参照は markdown リンク + 節番号で張る(例: `[networking.md](networking.md) §4`)。節番号を変えたら参照元も追随。
- **用語は glossary の正準形に従う**: 英語由来の技術用語(`head` / `fold` / `projection` / `sequence` 等)は英語形、コード識別子はバッククォート、定着カタカナ語(ピア・ブロック・タイムライン等)はカタカナ。新語を導入したら glossary に追記する。
