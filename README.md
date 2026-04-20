# Discord タスク判定 → Google Sheets BOT

Discordの指定チャンネルを5分おきにポーリングし、メッセージをClaudeで「タスクか否か」判定し、タスクならGoogle Sheetsに行追加するGitHub Actions BOT。

## 動作概要

1. GitHub Actions の cron が `*/5 * * * *` で `poll.py` を実行
2. Discord REST API で前回処理以降の新規メッセージを取得
3. 各メッセージを Claude Haiku 4.5 に渡してタスク判定 + 項目抽出
4. タスクなら対象シートに1行追加
5. 最後に処理したメッセージIDは同スプレッドシートの `_state` タブ（B1セル）に保存

## セットアップ

### 1. Discord BOT 側

1. https://discord.com/developers/applications で BOT 作成
2. **Bot** タブで Token を取得（後で `DISCORD_BOT_TOKEN` として使う）
3. **Privileged Gateway Intents** の `MESSAGE CONTENT INTENT` を ON
4. **OAuth2 → URL Generator** で以下を選択して招待URLを生成し、対象サーバーに招待
   - Scopes: `bot`
   - Bot Permissions: `View Channels`, `Read Message History`

対象: サーバー `1486237426161680607` / チャンネル `1492073497671897229`

### 2. Google Sheets 側

- スプレッドシート: https://docs.google.com/spreadsheets/d/15NVyczKmcIOW9NmFX0cV8dKf9TieP7FUEH2rHobj1Uw/edit
- サービスアカウント（`bionic-repeater-493405-n9@...`）のメールアドレスを、上記シートに「編集者」で共有
- メインシートのタブ名を `MAIN_SHEET_NAME` に合わせる（デフォルト `タスク一覧`）
- `_state` タブは初回実行時に自動生成される

### 3. GitHub リポジトリに登録

```bash
cd C:\Users\souro\discord-task-bot
git init
git add .
git commit -m "initial"
gh repo create discord-task-bot --private --source=. --push
```

### 4. GitHub Secrets / Variables 設定

`Settings → Secrets and variables → Actions` で以下を登録：

**Secrets**
| Name | Value |
|---|---|
| `DISCORD_CHANNEL_ID` | `1492073497671897229` |
| `DISCORD_BOT_TOKEN` | Discord BOT のトークン |
| `ANTHROPIC_API_KEY` | Claude API キー |
| `SPREADSHEET_ID` | `15NVyczKmcIOW9NmFX0cV8dKf9TieP7FUEH2rHobj1Uw` |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | `bionic-repeater-493405-n9-65d3f8c4fd74.json` の **中身全文** |

**Variables**
| Name | Value |
|---|---|
| `MAIN_SHEET_NAME` | `タスク一覧`（実際のタブ名） |

### 5. 動作確認

`Actions` タブから `poll-discord-tasks` を `Run workflow` で手動実行。

## ローカルで試す

```bash
pip install -r requirements.txt
cp .env.example .env   # 中身を埋める
# bash の場合
set -a && source .env && set +a
python poll.py
```

## 列構成（参考）

| 列 | 内容 | 自動入力 |
|---|---|---|
| A | # | 連番 |
| B | タスク名 | Claude が生成 |
| C | カテゴリ | Claude が分類 |
| D | 担当者 | 抽出 or 投稿者名 |
| E | 優先度 | 高/中/低 |
| F | 開始日 | 抽出 |
| G | 期限日 | 抽出 |
| H | ステータス | `未着手` 固定 |
| I | 進捗(%) | 0 |
| J | 完了日 | 空 |
| K | コメント・備考 | 元メッセージ抜粋 + Discordリンク |
| L | 工数(h) | 抽出 |
| M-N | 実績(h)/達成率 | 空 |
| O | 未着手メモ | Claude の備考 |
| P-S | 進行中/完了/遅延/保留メモ | 空 |

## 費用目安

- Claude Haiku 4.5: 1メッセージ約 $0.0001（1万件で$1）
- GitHub Actions: public repo は無料、private でも無料枠2000分/月に十分収まる（5分cron × 1分以内 = 月288分）
- Google Sheets API / Discord API: 無料

## 注意

- `*/5 * * * *` の GitHub Actions cron は混雑時に実行が数分遅延することがあります
- BOT の投稿（`author.bot == true`）は自動スキップ
- 空メッセージ・埋め込みのみのメッセージはスキップ
