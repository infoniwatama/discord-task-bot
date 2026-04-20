"""Discord PM agent — monitors a channel, creates/updates tasks in Google Sheets.

Per cron cycle:
  1. Resolve any pending approval proposals by reading their reactions.
  2. Fetch new Discord messages since last processed ID.
  3. Ask Gemini to classify each message: create / update(high|low) / ignore.
  4. Apply creates and high-confidence updates directly.
     Post a Discord approval request for low-confidence updates; resolve next cycle.
  5. Record every action to `_log`, every rejection to `_rejections` (fed back
     into future prompts so the model stops repeating the same mistake).
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import quote

import requests
from google.oauth2.service_account import Credentials
import gspread


class RateLimitError(Exception):
    pass


DISCORD_API = "https://discord.com/api/v10"
GEMINI_MODEL = "gemini-2.5-flash-lite"
GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
)
GEMINI_MIN_INTERVAL_SEC = 7.0  # free tier is 10 RPM in practice
JST = timezone(timedelta(hours=9))
APPROVAL_TIMEOUT_HOURS = 24
RECENT_REJECTIONS_LIMIT = 20
APPROVE_EMOJI = "✅"
REJECT_EMOJI = "❌"

LEGACY_CHANNEL_ID = os.environ.get("DISCORD_CHANNEL_ID", "")  # used only for legacy migration
APPROVAL_CHANNEL_ID = os.environ.get("DISCORD_APPROVAL_CHANNEL_ID", "")
DISCORD_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
MAIN_SHEET_NAME = os.environ.get("MAIN_SHEET_NAME", "タスク管理")
GUILD_ID = os.environ["DISCORD_GUILD_ID"]
CHANNEL_BLACKLIST = {
    c.strip() for c in os.environ.get("DISCORD_CHANNEL_BLACKLIST", "").split(",") if c.strip()
}

STATE_SHEET = "_state"
PENDING_SHEET = "_pending"
LOG_SHEET = "_log"
REJECT_SHEET = "_rejections"

PENDING_HEADER = [
    "approval_msg_id", "approval_channel_id", "target_task_no", "target_row_index",
    "changes_json", "proposal_text", "source_msg_id", "source_msg_link",
    "created_at_iso", "status",
]
LOG_HEADER = [
    "timestamp_jst", "action", "task_no", "changes_json", "source_msg_link", "note",
]
REJECT_HEADER = [
    "timestamp_jst", "source_msg_content", "proposed_changes_json",
    "target_task_snapshot", "reason",
]

# main sheet column indexes (1-based)
COL = {
    "no": 1, "name": 2, "category": 3, "assignee": 4, "priority": 5,
    "start": 6, "due": 7, "status": 8, "progress": 9, "done": 10,
    "comment": 11, "estimated": 12, "actual": 13, "achievement": 14,
    "memo_todo": 15, "memo_doing": 16, "memo_done": 17, "memo_delay": 18, "memo_hold": 19,
}
STATUS_TO_MEMO_COL = {
    "未着手": COL["memo_todo"],
    "進行中": COL["memo_doing"],
    "完了": COL["memo_done"],
    "遅延": COL["memo_delay"],
    "保留": COL["memo_hold"],
}

SYSTEM_PROMPT = """あなたはDiscordメッセージを監視するプロジェクトマネージャーです。
1つのメッセージに **複数の話題** が含まれることがあるので、
各話題を個別のアクションに分解してください(create 2件+update 1件 なども可)。

【チャンネル文脈(最重要)】
- `pj_` で始まるチャンネルは **そのプロジェクト専用** です
  例: `pj_hiro⭐` での発言は hiro プロジェクトに関する内容のみ
- 判定対象メッセージに付与される「投稿チャンネル」と既存タスクの「チャンネル」が
  一致するタスクだけをupdate対象候補としてください
- 違うプロジェクトチャンネル間で混ざらないようにしてください(pj_hiroの「完了」発言で
  pj_アモンのタスクを更新してはいけない)
- `pj_` 以外のチャンネル(例: `一般`, `yt制作部門`, `メモ_xxx`)はプロジェクトに紐付かないため、
  このスコープ制約は適用しません(全タスクから候補を選んで可)
- createの `category` はチャンネル名が `pj_xxx` の場合は `xxx` を優先、
  それ以外は内容に応じて分類(動画編集/開発/事務/調査/営業 等)

【分類ルール】
- create: 新しい依頼・作業指示(まだタスク化されていないもの)
- update: 既存タスクの進捗報告・完了報告・期限変更・担当変更など
- ignore: 雑談・挨拶・感想・画像やリンク単独投稿・タスク性のない発言

【update の厳格ルール(最重要・必ず守る)】
次の **いずれか** を満たさないメッセージを update にしてはいけません:
  (a) タスク番号を明示している(例「#3」「No.7」)
  (b) 既存タスク名の主要キーワードを**直接**含んでいる
      (例: 既存「ロゴ作成」→ メッセージに「ロゴ」「ロゴ作成」がある)
  (c) Discordリプライで、元メッセージが既存タスクと紐づいている
上記いずれにも該当しない場合、同じチャンネルでの別話題とみなし
update せず **create または ignore** を選んでください。

【confidence 判定(updateの時のみ)】
- high: タスク番号明示 or タスク名の主要キーワードを正確に含む or リプライ
- low: 上記のいずれか1つは満たすが、複数タスクに当てはまり得る表現

【判断ミスの典型例(やってはいけない)】
- チャンネル「pj_○○」で既存タスクA・B・Cがある時、新しい作業指示Dが来たら、
  AでもBでもCでもなくDとして**新規create**する(勝手にAに紐付けない)
- 数字だけの報告(「100件できた」等)は、タスク名の中に紐付く対象がないなら update しない

【出力フォーマット】
必ず以下のJSON構造**のみ**を返す(markdown・前置き禁止):
{
  "actions": [ アクション1, アクション2, ... ]
}

メッセージ全体がタスク性なしなら `{"actions": [{"action":"ignore","reasoning":"..."}]}` とする。
複数タスクが含まれるメッセージは配列に複数要素を入れる。

【各アクションのフィールド】

create:
{
  "action": "create",
  "reasoning": "20文字以内",
  "task_name": "30文字以内",
  "category": "開発/動画編集/事務/調査/営業/その他 等",
  "assignee": "担当者名(投稿者でよい場合は空文字)",
  "priority": "高" | "中" | "低",
  "due_date": "YYYY-MM-DD または空文字",
  "estimated_hours": 数値 または null,
  "notes": "短い備考"
}

update:
{
  "action": "update",
  "reasoning": "20文字以内",
  "confidence": "high" | "low",
  "target_task_no": 既存タスクの#番号(整数),
  "changes": {
    "status": "未着手|進行中|完了|遅延|保留 のいずれか (任意)",
    "progress": 0〜100の整数 (任意),
    "assignee": "新担当 (任意)",
    "due_date": "YYYY-MM-DD (任意)",
    "actual_hours": 数値 (任意),
    "note": "このメッセージから追記すべきメモ本文 (任意)"
  }
}

ignore:
{"action":"ignore","reasoning":"20文字以内"}

【重要】
- 1メッセージで話題が複数(例「A作って、Bも修正して、C完了した」)なら必ず分割
- 日付の相対表現(明日/今週中など)は「今日」基準で絶対日付に変換
- 既存タスクリスト・過去の却下例を参考に、誤update提案を避ける
- target_task_no は必ず既存タスク一覧にある#を指定"""


def headers_discord() -> dict[str, str]:
    return {"Authorization": f"Bot {DISCORD_TOKEN}"}


def _respect_429(r: requests.Response) -> float | None:
    if r.status_code == 429:
        try:
            retry_after = float(r.json().get("retry_after", 1.0)) + 0.2
        except Exception:
            retry_after = 1.0
        return min(retry_after, 5.0)
    return None


def discord_get(path: str, params: dict | None = None) -> Any:
    for _ in range(4):
        r = requests.get(f"{DISCORD_API}{path}", headers=headers_discord(),
                         params=params, timeout=30)
        delay = _respect_429(r)
        if delay is not None:
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def discord_post(path: str, body: dict) -> Any:
    for _ in range(4):
        r = requests.post(f"{DISCORD_API}{path}", headers=headers_discord(),
                          json=body, timeout=30)
        delay = _respect_429(r)
        if delay is not None:
            time.sleep(delay)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def discord_put(path: str) -> None:
    r = requests.put(f"{DISCORD_API}{path}", headers=headers_discord(), timeout=30)
    r.raise_for_status()


def fetch_new_messages(channel_id: str, after_id: str | None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {"limit": 100}
    if after_id:
        params["after"] = after_id
    messages = discord_get(f"/channels/{channel_id}/messages", params=params)
    messages.sort(key=lambda m: int(m["id"]))
    return messages


def list_text_channels() -> list[dict[str, Any]]:
    channels = discord_get(f"/guilds/{GUILD_ID}/channels")
    # type 0 = GUILD_TEXT, type 5 = GUILD_ANNOUNCEMENT (also text-based)
    return [c for c in channels if c.get("type") in (0, 5)]


def fetch_latest_message_id(channel_id: str) -> str | None:
    try:
        recent = discord_get(f"/channels/{channel_id}/messages", params={"limit": 1})
    except Exception as e:
        print(f"[warn] no access to channel {channel_id}: {e}", file=sys.stderr)
        return None
    return recent[0]["id"] if recent else None


def get_bot_user_id() -> str:
    me = discord_get("/users/@me")
    return me["id"]


def post_channel_message(channel_id: str, content: str) -> dict[str, Any]:
    return discord_post(f"/channels/{channel_id}/messages", {"content": content})


def add_reaction(channel_id: str, message_id: str, emoji: str) -> None:
    url = f"{DISCORD_API}/channels/{channel_id}/messages/{message_id}/reactions/{quote(emoji, safe='')}/@me"
    for _ in range(3):
        r = requests.put(url, headers=headers_discord(), timeout=30)
        if r.status_code == 429:
            retry_after = float(r.json().get("retry_after", 1.0)) + 0.2
            time.sleep(min(retry_after, 5))
            continue
        r.raise_for_status()
        return
    r.raise_for_status()


def get_reaction_user_ids(channel_id: str, message_id: str, emoji: str) -> list[str]:
    users = discord_get(
        f"/channels/{channel_id}/messages/{message_id}/reactions/{quote(emoji, safe='')}"
    )
    return [u["id"] for u in users]


def message_link(msg: dict[str, Any]) -> str:
    guild = msg.get("guild_id") or GUILD_ID
    return f"https://discord.com/channels/{guild}/{msg['channel_id']}/{msg['id']}"


def get_sheets_client() -> gspread.Client:
    info = json.loads(GOOGLE_SA_JSON)
    creds = Credentials.from_service_account_info(info, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ])
    return gspread.authorize(creds)


def get_or_create_tab(sh: gspread.Spreadsheet, name: str, header: list[str]) -> gspread.Worksheet:
    """Create tab with header if missing. Never overwrite existing row 1 —
    _state stores a key/value pair there and must not be clobbered."""
    try:
        return sh.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=name, rows=100, cols=max(2, len(header)))
        ws.update(values=[header], range_name=f"A1:{chr(64+len(header))}1")
        return ws


STATE_HEADER = ["channel_id", "last_message_id"]


def migrate_legacy_state(state_ws: gspread.Worksheet) -> None:
    """Convert old single-channel state (row 1 = [last_message_id, <id>]) to the
    per-channel schema. Safe to run on every startup — no-op if already migrated."""
    row1 = state_ws.row_values(1)
    if row1 == STATE_HEADER:
        return
    if len(row1) >= 2 and row1[0] == "last_message_id" and LEGACY_CHANNEL_ID:
        old_id = row1[1]
        state_ws.update(values=[STATE_HEADER], range_name="A1:B1")
        if old_id:
            state_ws.append_row([LEGACY_CHANNEL_ID, old_id], value_input_option="USER_ENTERED")
        print(f"[migrate] state → per-channel schema (preserved {LEGACY_CHANNEL_ID}→{old_id})")
        return
    state_ws.update(values=[STATE_HEADER], range_name="A1:B1")


def get_channel_state(state_ws: gspread.Worksheet, channel_id: str) -> str | None:
    values = state_ws.get_all_values()
    for row in values[1:]:
        if len(row) >= 2 and row[0] == channel_id:
            return row[1] or None
    return None


def set_channel_state(state_ws: gspread.Worksheet, channel_id: str, message_id: str) -> None:
    values = state_ws.get_all_values()
    for i, row in enumerate(values[1:], start=2):
        if row and row[0] == channel_id:
            state_ws.update_cell(i, 2, message_id)
            return
    state_ws.append_row([channel_id, message_id], value_input_option="USER_ENTERED")


def find_header_row(main_ws: gspread.Worksheet) -> int:
    """Header row is the first one whose column A is exactly '#'."""
    col_a = main_ws.col_values(1)
    for i, v in enumerate(col_a, start=1):
        if v.strip() == "#":
            return i
    return 1


import re as _re
_CHANNEL_URL_RE = _re.compile(r"/channels/\d+/(\d+)/\d+")


def load_tasks_compact(main_ws: gspread.Worksheet) -> list[dict[str, Any]]:
    """Returns list of dicts for every numbered task row below the header.
    Also extracts the source channel_id from any Discord link in the 備考 column."""
    values = main_ws.get_all_values()
    header_row = find_header_row(main_ws)
    tasks = []
    for idx, row in enumerate(values[header_row:], start=header_row + 1):
        if not row or not row[0].strip():
            continue
        try:
            task_no = int(row[0])
        except ValueError:
            continue
        comment = row[10] if len(row) > 10 else ""
        m = _CHANNEL_URL_RE.search(comment)
        channel_id = m.group(1) if m else ""
        tasks.append({
            "row_index": idx,
            "no": task_no,
            "name": row[1] if len(row) > 1 else "",
            "category": row[2] if len(row) > 2 else "",
            "assignee": row[3] if len(row) > 3 else "",
            "status": row[7] if len(row) > 7 else "",
            "progress": row[8] if len(row) > 8 else "",
            "due": row[6] if len(row) > 6 else "",
            "channel_id": channel_id,
        })
    return tasks


def compact_task_list_for_prompt(
    tasks: list[dict[str, Any]],
    channel_name_by_id: dict[str, str] | None = None,
) -> str:
    if not tasks:
        return "(なし)"
    channel_name_by_id = channel_name_by_id or {}
    lines = []
    for t in tasks[:50]:  # cap to avoid token bloat; newest are at top
        ch_name = channel_name_by_id.get(t.get("channel_id", ""), "")
        ch_label = f" チャンネル:#{ch_name}" if ch_name else ""
        lines.append(
            f"#{t['no']} 『{t['name']}』 カテゴリ:{t.get('category') or '—'} "
            f"担当:{t['assignee'] or '未設定'} "
            f"状態:{t['status'] or '未着手'} 進捗:{t['progress'] or '0'}% "
            f"期限:{t['due'] or '未定'}{ch_label}"
        )
    return "\n".join(lines)


def load_recent_rejections(reject_ws: gspread.Worksheet, limit: int = RECENT_REJECTIONS_LIMIT) -> list[dict[str, Any]]:
    values = reject_ws.get_all_values()
    data = values[1:] if len(values) > 1 else []
    data.reverse()  # most recent first
    out = []
    for row in data[:limit]:
        if len(row) < 5:
            continue
        out.append({
            "msg": row[1],
            "proposed": row[2],
            "target": row[3],
            "reason": row[4],
        })
    return out


def rejections_for_prompt(rejections: list[dict[str, Any]]) -> str:
    if not rejections:
        return "(なし)"
    lines = []
    for r in rejections:
        lines.append(
            f"- メッセージ「{r['msg'][:60]}」→ 対象「{r['target'][:40]}」への提案「{r['proposed'][:80]}」→ 却下理由「{r['reason']}」"
        )
    return "\n".join(lines)


def append_pending(
    pending_ws: gspread.Worksheet,
    approval_msg_id: str,
    channel_id: str,
    task_no: int,
    row_index: int,
    changes: dict,
    proposal_text: str,
    source_msg_id: str,
    source_link: str,
) -> None:
    now_iso = datetime.now(JST).isoformat()
    pending_ws.append_row(
        [approval_msg_id, channel_id, task_no, row_index,
         json.dumps(changes, ensure_ascii=False), proposal_text, source_msg_id,
         source_link, now_iso, "pending"],
        value_input_option="USER_ENTERED",
    )


def append_log(log_ws: gspread.Worksheet, action: str, task_no: int | str, changes: dict | None,
               source_link: str, note: str = "") -> None:
    log_ws.append_row(
        [datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"), action, task_no,
         json.dumps(changes or {}, ensure_ascii=False), source_link, note],
        value_input_option="USER_ENTERED",
    )


def append_rejection(reject_ws: gspread.Worksheet, source_msg: str, changes: dict,
                     target_snapshot: str, reason: str) -> None:
    reject_ws.append_row(
        [datetime.now(JST).strftime("%Y-%m-%d %H:%M:%S"), source_msg[:500],
         json.dumps(changes, ensure_ascii=False), target_snapshot[:200], reason[:200]],
        value_input_option="USER_ENTERED",
    )


def build_create_row(row_no: int, msg: dict[str, Any], j: dict[str, Any]) -> list[Any]:
    author = msg["author"].get("global_name") or msg["author"]["username"]
    assignee = j.get("assignee") or author
    created_at = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
    created_jst = created_at.astimezone(JST)
    posted_date = created_jst.strftime("%Y-%m-%d")
    stamp = created_jst.strftime("%m/%d %H:%M")
    link = message_link(msg)
    content_excerpt = (msg.get("content") or "")[:300]
    comment = (
        f"━━━ {stamp} @{author} ━━━\n"
        f"{content_excerpt}\n"
        f"🔗 {link}"
    )
    todo_memo = ""
    if j.get("notes"):
        todo_memo = f"[{stamp} @{author}] {j['notes']}"
    return [
        row_no,
        j.get("task_name", ""),
        j.get("category", ""),
        assignee,
        j.get("priority", "中"),
        posted_date,                   # F: 開始日 = 投稿日
        j.get("due_date", ""),
        "未着手",
        0,
        "",
        comment,
        j.get("estimated_hours") or "",
        "", "",
        todo_memo,
        "", "", "", "",
    ]


def insert_task_at_top(main_ws: gspread.Worksheet, row: list[Any]) -> None:
    header_row = find_header_row(main_ws)
    main_ws.insert_row(row, index=header_row + 1, value_input_option="USER_ENTERED")


def next_task_number(main_ws: gspread.Worksheet) -> int:
    col_a = main_ws.col_values(COL["no"])
    header_row = find_header_row(main_ws)
    nums = [int(v) for v in col_a[header_row:] if v.strip().isdigit()]
    return (max(nums) + 1) if nums else 1


def current_row_index_for_task(main_ws: gspread.Worksheet, task_no: int) -> int | None:
    col_a = main_ws.col_values(COL["no"])
    header_row = find_header_row(main_ws)
    for i, v in enumerate(col_a[header_row:], start=header_row + 1):
        if v.strip().isdigit() and int(v) == task_no:
            return i
    return None


def apply_changes_to_row(
    main_ws: gspread.Worksheet,
    row_index: int,
    changes: dict,
    source: dict | None = None,
) -> dict:
    """Applies each change via individual cell updates. Returns dict of applied changes.
    `source` may include {author, link, stamp} to annotate the memo entry."""
    applied: dict[str, Any] = {}
    current = main_ws.row_values(row_index)
    def getcol(c: int) -> str:
        return current[c-1] if len(current) >= c else ""

    if "status" in changes:
        new_status = changes["status"]
        main_ws.update_cell(row_index, COL["status"], new_status)
        applied["status"] = new_status
        if new_status == "完了":
            today = datetime.now(JST).strftime("%Y-%m-%d")
            main_ws.update_cell(row_index, COL["done"], today)
            applied["done_date"] = today
            # if progress not explicitly set, push to 100
            if "progress" not in changes:
                main_ws.update_cell(row_index, COL["progress"], 100)
                applied["progress"] = 100

    if "progress" in changes:
        main_ws.update_cell(row_index, COL["progress"], changes["progress"])
        applied["progress"] = changes["progress"]

    if "assignee" in changes and changes["assignee"]:
        main_ws.update_cell(row_index, COL["assignee"], changes["assignee"])
        applied["assignee"] = changes["assignee"]

    if "due_date" in changes and changes["due_date"]:
        main_ws.update_cell(row_index, COL["due"], changes["due_date"])
        applied["due_date"] = changes["due_date"]

    if "actual_hours" in changes and changes["actual_hours"] is not None:
        main_ws.update_cell(row_index, COL["actual"], changes["actual_hours"])
        applied["actual_hours"] = changes["actual_hours"]

    if "note" in changes and changes["note"]:
        status_for_memo = changes.get("status") or getcol(COL["status"]) or "未着手"
        memo_col = STATUS_TO_MEMO_COL.get(status_for_memo, COL["memo_todo"])
        existing = getcol(memo_col)
        src = source or {}
        author = src.get("author", "")
        link = src.get("link", "")
        stamp = src.get("stamp") or datetime.now(JST).strftime("%m/%d %H:%M")
        header = f"[{stamp}" + (f" @{author}" if author else "") + "]"
        entry = f"{header} {changes['note']}"
        if link:
            entry += f"\n  🔗 {link}"
        new_memo = (existing + ("\n\n" if existing else "") + entry).strip()
        main_ws.update_cell(row_index, memo_col, new_memo)
        applied["memo_added"] = changes["note"]

    return applied


# === Gemini ===


def judge_message(
    msg: dict[str, Any],
    tasks_prompt: str,
    rejections_prompt: str,
    channel_name: str = "",
) -> dict[str, Any]:
    author = msg["author"].get("global_name") or msg["author"]["username"]
    content = msg.get("content", "")
    today = datetime.now(JST).strftime("%Y-%m-%d")
    ref = msg.get("referenced_message")
    ref_info = ""
    if ref:
        ref_author = ref.get("author", {}).get("global_name") or ref.get("author", {}).get("username", "不明")
        ref_info = f"\n(このメッセージは {ref_author} の『{(ref.get('content') or '')[:80]}』へのリプライです)"

    user_prompt = (
        f"今日の日付: {today}\n"
        f"投稿チャンネル: #{channel_name}\n"
        f"投稿者: {author}\n"
        f"--- 既存タスク一覧(新しい順) ---\n{tasks_prompt}\n"
        f"--- 過去に却下された提案(同じ誤りを避けるための参考) ---\n{rejections_prompt}\n"
        f"--- 判定対象メッセージ ---\n{content}{ref_info}"
    )

    body = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0},
    }
    r = requests.post(GEMINI_URL, params={"key": GEMINI_API_KEY}, json=body, timeout=30)
    if r.status_code == 429:
        raise RateLimitError(r.text)
    r.raise_for_status()
    data = r.json()
    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
        text = text.strip()
    parsed = json.loads(text)
    # Normalise to {"actions": [...]}
    if isinstance(parsed, list):
        actions = parsed
    elif isinstance(parsed, dict):
        if "actions" in parsed and isinstance(parsed["actions"], list):
            actions = parsed["actions"]
        elif "action" in parsed:
            actions = [parsed]  # legacy single-action shape
        else:
            actions = [{"action": "ignore", "reasoning": "unparseable"}]
    else:
        actions = [{"action": "ignore", "reasoning": "unparseable"}]
    return {"actions": actions}


# === Main flow ===


def process_pending_approvals(
    sh: gspread.Spreadsheet,
    main_ws: gspread.Worksheet,
    pending_ws: gspread.Worksheet,
    log_ws: gspread.Worksheet,
    reject_ws: gspread.Worksheet,
    bot_user_id: str,
) -> None:
    values = pending_ws.get_all_values()
    now = datetime.now(JST)
    STATUS_COL = 10  # column J in the new schema
    for i, row in enumerate(values[1:], start=2):
        if len(row) < len(PENDING_HEADER):
            continue
        if row[9] != "pending":
            continue
        approval_msg_id = row[0]
        ch_id = row[1] or APPROVAL_CHANNEL_ID
        try:
            task_no = int(row[2])
        except ValueError:
            continue
        target_row_index = int(row[3]) if row[3].isdigit() else None
        try:
            changes = json.loads(row[4]) if row[4] else {}
        except json.JSONDecodeError:
            changes = {}
        source_link = row[7]
        created_at = datetime.fromisoformat(row[8]) if row[8] else now

        approvers = [uid for uid in get_reaction_user_ids(ch_id, approval_msg_id, APPROVE_EMOJI) if uid != bot_user_id]
        rejectors = [uid for uid in get_reaction_user_ids(ch_id, approval_msg_id, REJECT_EMOJI) if uid != bot_user_id]

        if approvers:
            if target_row_index is None:
                target_row_index = current_row_index_for_task(main_ws, task_no)
            if target_row_index:
                source = {"author": "", "link": source_link,
                          "stamp": datetime.now(JST).strftime("%m/%d %H:%M")}
                applied = apply_changes_to_row(main_ws, target_row_index, changes, source=source)
                pending_ws.update_cell(i, STATUS_COL, "approved")
                append_log(log_ws, "update_approved", task_no, applied, source_link, f"approved by {approvers[0]}")
                print(f"[approved] #{task_no} applied: {applied}")
            else:
                pending_ws.update_cell(i, STATUS_COL, "error_row_missing")
                append_log(log_ws, "update_error", task_no, changes, source_link, "target row not found")
        elif rejectors:
            pending_ws.update_cell(i, STATUS_COL, "rejected")
            snapshot = ""
            if target_row_index is None:
                target_row_index = current_row_index_for_task(main_ws, task_no)
            if target_row_index:
                snap_row = main_ws.row_values(target_row_index)
                snapshot = f"#{task_no} {snap_row[1] if len(snap_row)>1 else ''} 状態{snap_row[7] if len(snap_row)>7 else ''}"
            source_content = row[5][:200]  # proposal_text for learning context
            append_rejection(reject_ws, source_content, changes, snapshot,
                             reason=f"rejected by {rejectors[0]}")
            append_log(log_ws, "update_rejected", task_no, changes, source_link, f"rejected by {rejectors[0]}")
            print(f"[rejected] #{task_no} changes discarded")
        else:
            age_hours = (now - created_at).total_seconds() / 3600
            if age_hours >= APPROVAL_TIMEOUT_HOURS:
                pending_ws.update_cell(i, STATUS_COL, "expired")
                append_log(log_ws, "update_expired", task_no, changes, source_link,
                           f"no reaction within {APPROVAL_TIMEOUT_HOURS}h")
                print(f"[expired] #{task_no}")


def format_changes_japanese(changes: dict) -> str:
    labels = {
        "status": "ステータス",
        "progress": "進捗",
        "assignee": "担当者",
        "due_date": "期限日",
        "actual_hours": "実績(h)",
        "note": "メモ追記",
    }
    parts = []
    for k, v in changes.items():
        name = labels.get(k, k)
        parts.append(f"・{name}: {v}")
    return "\n".join(parts) if parts else "(変更なし)"


def process_new_messages(
    sh: gspread.Spreadsheet,
    main_ws: gspread.Worksheet,
    state_ws: gspread.Worksheet,
    pending_ws: gspread.Worksheet,
    log_ws: gspread.Worksheet,
    reject_ws: gspread.Worksheet,
    bot_user_id: str,
) -> None:
    migrate_legacy_state(state_ws)
    skip_ids = set(CHANNEL_BLACKLIST)
    if APPROVAL_CHANNEL_ID:
        skip_ids.add(APPROVAL_CHANNEL_ID)

    all_channels = list_text_channels()
    channel_name_by_id = {c["id"]: c.get("name", "") for c in all_channels}
    channels = [c for c in all_channels if c["id"] not in skip_ids]
    if not channels:
        print("No channels to monitor.")
        return

    tasks = load_tasks_compact(main_ws)
    rejections = load_recent_rejections(reject_ws)
    tasks_prompt = compact_task_list_for_prompt(tasks, channel_name_by_id)
    rejections_prompt = rejections_for_prompt(rejections)

    last_api_call = 0.0
    stop_all = False

    for ch in channels:
        if stop_all:
            break
        ch_id = ch["id"]
        ch_name = ch.get("name", ch_id)
        last_id = get_channel_state(state_ws, ch_id)

        if last_id is None:
            seed = fetch_latest_message_id(ch_id)
            if seed:
                set_channel_state(state_ws, ch_id, seed)
                print(f"[seed] #{ch_name}: starting from {seed}")
            continue  # no backlog processing on first encounter

        try:
            messages = fetch_new_messages(ch_id, last_id)
        except Exception as e:
            print(f"[warn] #{ch_name} fetch failed: {e}", file=sys.stderr)
            continue

        if not messages:
            continue

        print(f"#{ch_name}: {len(messages)} new (after {last_id})")

        progress_id = last_id
        for msg in messages:
            mid = msg["id"]
            if msg["author"].get("bot"):
                progress_id = mid
                continue
            content = (msg.get("content") or "").strip()
            if not content:
                progress_id = mid
                continue

            wait = GEMINI_MIN_INTERVAL_SEC - (time.time() - last_api_call)
            if wait > 0:
                time.sleep(wait)

            try:
                j = judge_message(msg, tasks_prompt, rejections_prompt,
                                  channel_name=ch_name)
                last_api_call = time.time()
            except RateLimitError as e:
                print(f"[rate limit] stop at {mid}: {e}", file=sys.stderr)
                stop_all = True
                break
            except Exception as e:
                print(f"[judge error] {mid}: {e}", file=sys.stderr)
                progress_id = mid
                continue

            link = message_link(msg)
            actions = j.get("actions") or [{"action": "ignore", "reasoning": "no actions"}]
            tasks_changed = False

            for a in actions:
                action = a.get("action")
                if action == "create":
                    row_no = next_task_number(main_ws)
                    row = build_create_row(row_no, msg, a)
                    insert_task_at_top(main_ws, row)
                    append_log(log_ws, "create", row_no, {"name": a.get("task_name")}, link,
                               a.get("reasoning", ""))
                    print(f"[CREATE] #{row_no}: {a.get('task_name')}")
                    tasks_changed = True

                elif action == "update":
                    target_no = a.get("target_task_no")
                    changes = a.get("changes") or {}
                    confidence = a.get("confidence", "low")
                    row_index = current_row_index_for_task(main_ws, int(target_no)) if target_no else None

                    if not row_index:
                        append_log(log_ws, "update_skipped", target_no or "?", changes, link,
                                   "target task not found")
                        print(f"[skip] update target #{target_no} not found")
                    elif confidence == "high":
                        src_author = msg["author"].get("global_name") or msg["author"]["username"]
                        created_at = datetime.fromisoformat(msg["timestamp"].replace("Z", "+00:00"))
                        stamp = created_at.astimezone(JST).strftime("%m/%d %H:%M")
                        source = {"author": src_author, "link": link, "stamp": stamp}
                        applied = apply_changes_to_row(main_ws, row_index, changes, source=source)
                        append_log(log_ws, "update_auto", target_no, applied, link,
                                   a.get("reasoning", ""))
                        print(f"[UPDATE-AUTO] #{target_no}: {applied}")
                        tasks_changed = True
                    else:
                        task_name = next((t["name"] for t in tasks if t["no"] == int(target_no)), "?")
                        proposal_text = (
                            f"🔔 タスク更新提案 (#{target_no}「{task_name}」)\n"
                            f"変更内容:\n{format_changes_japanese(changes)}\n"
                            f"{APPROVE_EMOJI} 承認 / {REJECT_EMOJI} 却下 ({APPROVAL_TIMEOUT_HOURS}h以内)\n"
                            f"元発言: {link}\n"
                            f"判断理由: {a.get('reasoning','')}"
                        )
                        try:
                            posted = post_channel_message(APPROVAL_CHANNEL_ID, proposal_text)
                            add_reaction(APPROVAL_CHANNEL_ID, posted["id"], APPROVE_EMOJI)
                            add_reaction(APPROVAL_CHANNEL_ID, posted["id"], REJECT_EMOJI)
                            append_pending(pending_ws, posted["id"], APPROVAL_CHANNEL_ID,
                                           int(target_no), row_index, changes, proposal_text,
                                           mid, link)
                            append_log(log_ws, "update_proposed", target_no, changes, link,
                                       a.get("reasoning", ""))
                            print(f"[PROPOSE] #{target_no} awaiting approval (msg {posted['id']})")
                        except Exception as e:
                            append_log(log_ws, "propose_error", target_no, changes, link, str(e))
                            print(f"[propose error] #{target_no}: {e}", file=sys.stderr)
                else:
                    print(f"[ignore] {mid}: {a.get('reasoning','')}")

            if tasks_changed:
                tasks = load_tasks_compact(main_ws)
                tasks_prompt = compact_task_list_for_prompt(tasks, channel_name_by_id)

            progress_id = mid

        if progress_id and progress_id != last_id:
            set_channel_state(state_ws, ch_id, progress_id)
            print(f"#{ch_name}: state → {progress_id}")


def main() -> None:
    sh = get_sheets_client().open_by_key(SPREADSHEET_ID)
    main_ws = sh.worksheet(MAIN_SHEET_NAME)
    state_ws = get_or_create_tab(sh, STATE_SHEET, STATE_HEADER)
    pending_ws = get_or_create_tab(sh, PENDING_SHEET, PENDING_HEADER)
    log_ws = get_or_create_tab(sh, LOG_SHEET, LOG_HEADER)
    reject_ws = get_or_create_tab(sh, REJECT_SHEET, REJECT_HEADER)

    bot_user_id = get_bot_user_id()

    process_pending_approvals(sh, main_ws, pending_ws, log_ws, reject_ws, bot_user_id)
    process_new_messages(sh, main_ws, state_ws, pending_ws, log_ws, reject_ws, bot_user_id)


if __name__ == "__main__":
    main()
