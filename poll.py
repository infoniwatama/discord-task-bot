"""Discord channel poller -> Claude task judge -> Google Sheets writer.

Runs once per invocation (designed for GitHub Actions cron).
State (last processed message ID) is stored in a `_state` tab of the same sheet.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import requests
from google.oauth2.service_account import Credentials
import gspread


class RateLimitError(Exception):
    pass


DISCORD_API = "https://discord.com/api/v10"
GEMINI_MODEL = "gemini-2.5-flash-lite"
GEMINI_MIN_INTERVAL_SEC = 4.5  # free tier ~15 RPM — keep under
GEMINI_URL = (
    f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
)
JST = timezone(timedelta(hours=9))

CHANNEL_ID = os.environ["DISCORD_CHANNEL_ID"]
DISCORD_TOKEN = os.environ["DISCORD_BOT_TOKEN"]
GEMINI_API_KEY = os.environ["GEMINI_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
GOOGLE_SA_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
MAIN_SHEET_NAME = os.environ.get("MAIN_SHEET_NAME", "タスク一覧")
STATE_SHEET_NAME = "_state"


SYSTEM_PROMPT = """あなたはDiscordのメッセージを分析し、タスクかどうかを判定するアシスタントです。

「タスク」とは、具体的に誰かが実行すべき作業・アクションを示すものです。
- タスクの例: 「〇〇を修正して」「明日までに資料作成」「バグ直しといて」「〇〇について調査お願い」
- タスクでない例: 雑談、感想、質問の回答、リンク共有のみ、報告のみ、挨拶

必ず以下のJSON形式のみで回答してください（前置き・後置き・markdown記法は一切禁止）:
{
  "is_task": true | false,
  "task_name": "簡潔なタスク名（30文字以内）",
  "category": "カテゴリ（例: 開発 / 動画編集 / 事務 / 調査 / その他）",
  "assignee": "担当者（メッセージから読み取れれば。不明なら空文字）",
  "priority": "高" | "中" | "低",
  "start_date": "YYYY-MM-DD または空文字",
  "due_date": "YYYY-MM-DD または空文字",
  "estimated_hours": 数値 または null,
  "notes": "備考（短く）"
}

is_task が false の場合、他のフィールドは空文字や0で構いません。
日付が「明日」「今週中」など相対表現の場合は、与えられた「今日」の日付を基準に絶対日付へ変換してください。"""


def get_sheets_client() -> gspread.Client:
    creds_info = json.loads(GOOGLE_SA_JSON)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    return gspread.authorize(creds)


def get_last_message_id(sh: gspread.Spreadsheet) -> str | None:
    try:
        ws = sh.worksheet(STATE_SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=STATE_SHEET_NAME, rows=2, cols=2)
        ws.update("A1:B1", [["last_message_id", ""]])
        return None
    val = ws.acell("B1").value
    return val or None


def set_last_message_id(sh: gspread.Spreadsheet, message_id: str) -> None:
    ws = sh.worksheet(STATE_SHEET_NAME)
    ws.update("A1:B1", [["last_message_id", message_id]])


def fetch_new_messages(after_id: str | None) -> list[dict[str, Any]]:
    """Fetch messages newer than after_id, oldest first."""
    headers = {"Authorization": f"Bot {DISCORD_TOKEN}"}
    params: dict[str, Any] = {"limit": 100}
    if after_id:
        params["after"] = after_id

    url = f"{DISCORD_API}/channels/{CHANNEL_ID}/messages"
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    messages = r.json()
    # Discord returns newest-first; we want oldest-first for sequential processing
    messages.sort(key=lambda m: int(m["id"]))
    return messages


def judge_task(message: dict[str, Any]) -> dict[str, Any]:
    author = message["author"].get("global_name") or message["author"]["username"]
    content = message.get("content", "")
    today = datetime.now(JST).strftime("%Y-%m-%d")

    user_prompt = (
        f"今日の日付: {today}\n"
        f"投稿者: {author}\n"
        f"メッセージ本文:\n{content}"
    )

    body = {
        "system_instruction": {"parts": [{"text": SYSTEM_PROMPT}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0,
        },
    }
    r = requests.post(
        GEMINI_URL,
        params={"key": GEMINI_API_KEY},
        json=body,
        timeout=30,
    )
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
    # Gemini occasionally wraps the object in a list
    if isinstance(parsed, list):
        parsed = parsed[0] if parsed else {"is_task": False}
    return parsed


def next_row_number(ws: gspread.Worksheet) -> int:
    col_a = ws.col_values(1)
    # Header on row 1; data starts row 2
    return max(0, len(col_a) - 1) + 1


def append_task_row(
    ws: gspread.Worksheet,
    judgement: dict[str, Any],
    message: dict[str, Any],
) -> None:
    row_no = next_row_number(ws)
    author = message["author"].get("global_name") or message["author"]["username"]
    assignee = judgement.get("assignee") or author
    created_at = datetime.fromisoformat(message["timestamp"].replace("Z", "+00:00"))
    created_jst = created_at.astimezone(JST).strftime("%Y-%m-%d %H:%M")
    msg_url = (
        f"https://discord.com/channels/{message['guild_id']}/"
        f"{message['channel_id']}/{message['id']}"
        if message.get("guild_id")
        else ""
    )
    comment = (
        f"[Discord {created_jst}] {message.get('content', '')[:200]}\n"
        f"link: {msg_url}"
    ).strip()

    row = [
        row_no,                              # A: #
        judgement.get("task_name", ""),       # B: タスク名
        judgement.get("category", ""),        # C: カテゴリ
        assignee,                             # D: 担当者
        judgement.get("priority", "中"),       # E: 優先度
        judgement.get("start_date", ""),      # F: 開始日
        judgement.get("due_date", ""),        # G: 期限日
        "未着手",                              # H: ステータス
        0,                                    # I: 進捗(%)
        "",                                   # J: 完了日
        comment,                              # K: コメント・備考
        judgement.get("estimated_hours") or "",  # L: 工数(h)
        "",                                   # M: 実績(h)
        "",                                   # N: 達成率
        judgement.get("notes", ""),           # O: 未着手メモ
        "",                                   # P: 進行中メモ
        "",                                   # Q: 完了メモ
        "",                                   # R: 遅延メモ
        "",                                   # S: 保留メモ
    ]
    # Insert at row 2 so newest tasks appear at the top (below the header row)
    ws.insert_row(row, index=2, value_input_option="USER_ENTERED")


def main() -> None:
    sh = get_sheets_client().open_by_key(SPREADSHEET_ID)
    main_ws = sh.worksheet(MAIN_SHEET_NAME)

    last_id = get_last_message_id(sh)
    messages = fetch_new_messages(last_id)

    if not messages:
        print("No new messages.")
        return

    print(f"Fetched {len(messages)} new message(s). Last seen: {last_id}")

    progress_id = last_id  # advances only on successfully processed messages
    last_api_call = 0.0

    for msg in messages:
        # Skip bot messages (including self) to avoid loops
        if msg["author"].get("bot"):
            progress_id = msg["id"]
            continue
        content = (msg.get("content") or "").strip()
        if not content:
            progress_id = msg["id"]
            continue

        # rate limit guard
        wait = GEMINI_MIN_INTERVAL_SEC - (time.time() - last_api_call)
        if wait > 0:
            time.sleep(wait)

        try:
            j = judge_task(msg)
            last_api_call = time.time()
        except RateLimitError as e:
            print(f"[rate limit] stop at {msg['id']}: {e}", file=sys.stderr)
            break  # keep progress_id at prior msg so we retry this one next run
        except Exception as e:
            print(f"[judge error] {msg['id']}: {e}", file=sys.stderr)
            progress_id = msg["id"]  # unrecoverable; skip to avoid infinite loop
            continue

        if j.get("is_task"):
            try:
                append_task_row(main_ws, j, msg)
                print(f"[TASK] {msg['id']}: {j.get('task_name')}")
            except Exception as e:
                print(f"[sheet error] {msg['id']}: {e}", file=sys.stderr)
        else:
            print(f"[skip] {msg['id']}: not a task")
        progress_id = msg["id"]

    if progress_id and progress_id != last_id:
        set_last_message_id(sh, progress_id)
        print(f"Updated last_message_id -> {progress_id}")


if __name__ == "__main__":
    main()
