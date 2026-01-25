import os
import json
import requests
from datetime import datetime, timezone, timedelta
import re


KST = timezone(timedelta(hours=9))
ROLLOVER_HOUR = 11  # 오전 11시 기준

def kst_now():
    return datetime.now(KST)

def effective_date(now=None):
    """
    오전 11시 전이면 '어제', 11시(포함) 이후면 '오늘'을 반환
    """
    now = now or kst_now()
    base = now.date()
    if now.hour < ROLLOVER_HOUR:
        base = base - timedelta(days=1)
    return base

def effective_date_str(now=None):
    return effective_date(now).strftime("%Y-%m-%d")


# ==============================
# ✅ Notion 속성 이름 (DB에 보이는 컬럼 이름 그대로)
# ==============================
TITLE_PROP = "name"         # title
STATUS_PROP = "states"      # status: 시작 전 / 진행 중 / 완료 / 보류
CATEGORY_PROP = "label"     # select: (네 DB에 맞게)
PRIORITY_PROP = "priority"  # select: -, 1, 2, 3, 4
DATE_PROP = "date"          # date

# ==============================
# ✅ 카테고리 출력 순서 + 아이콘
# ==============================
CATEGORY_ORDER = [
    ("메인업무", "1️⃣"),
    ("외주", "2️⃣"),
    ("스포클", "3️⃣"),
    ("유튜브", "4️⃣"),
    ("기타", "ℹ️"),
]

# ✅ 중요도 정렬 순서 (1이 가장 중요)
PRIORITY_ORDER = ["1", "2", "3", "4", "-"]

# ✅ 디스코드 임베드 컬러 (FF57CF)
EMBED_COLOR = int("FF57CF", 16)

# ✅ 메시지 ID 저장 파일
STATE_FILE = "discord_state.json"


def normalize_notion_db_id(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    m = re.search(r"[0-9a-fA-F]{32}", raw.replace("-", ""))
    if m:
        return m.group(0)
    raw2 = raw.replace("-", "")
    if re.fullmatch(r"[0-9a-fA-F]{32}", raw2):
        return raw2
    return raw


# ==============================
# ✅ STATE 저장/로드
# ==============================
def load_state():
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: dict):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# ==============================
# ✅ Notion 데이터 가져오기
# ==============================
def fetch_notion_data():
    notion_api_key = os.getenv("NOTION_API_KEY")
    database_id_raw = os.getenv("NOTION_DATABASE_ID")
    database_id = normalize_notion_db_id(database_id_raw)

    if not notion_api_key or not database_id:
        raise ValueError("NOTION_API_KEY 또는 NOTION_DATABASE_ID가 비어있습니다.")

    url = f"https://api.notion.com/v1/databases/{database_id}/query"

    headers = {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }

    all_results = []
    start_cursor = None

    # date 비어있지 않은 것만 전체 조회(페이지네이션)
    while True:
        payload = {
            "page_size": 100,
            "filter": {
                "property": DATE_PROP,
                "date": {"is_not_empty": True}
            }
        }

        if start_cursor:
            payload["start_cursor"] = start_cursor

        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        data = response.json()

        all_results.extend(data.get("results", []))

        if data.get("has_more"):
            start_cursor = data.get("next_cursor")
        else:
            break

    return {"results": all_results}


def safe_get_title(page):
    title_arr = page["properties"][TITLE_PROP]["title"]
    if not title_arr:
        return None
    return title_arr[0]["plain_text"]


def safe_get_select_name(page, prop_name):
    prop = page["properties"].get(prop_name)
    if not prop:
        return None
    if prop["type"] == "select":
        return prop["select"]["name"] if prop["select"] else None
    return None


def safe_get_status_name(page):
    prop = page["properties"].get(STATUS_PROP)
    if not prop:
        return None

    # Notion 'Status' 타입
    if prop["type"] == "status":
        return prop["status"]["name"] if prop["status"] else None

    # Notion 'Select' 타입
    if prop["type"] == "select":
        return prop["select"]["name"] if prop["select"] else None

    return None


def parse_date_yyyy_mm_dd(s: str):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def safe_get_date_range(page):
    prop = page["properties"].get(DATE_PROP)
    if not prop:
        return (None, None)

    if prop["type"] == "date" and prop["date"]:
        start_raw = prop["date"].get("start")
        end_raw = prop["date"].get("end")

        start_d = parse_date_yyyy_mm_dd(start_raw)
        end_d = parse_date_yyyy_mm_dd(end_raw) if end_raw else None

        # end가 없으면 start 하루짜리로 취급
        if start_d and not end_d:
            end_d = start_d

        return (start_d, end_d)

    return (None, None)


def priority_rank(priority_value):
    if priority_value in PRIORITY_ORDER:
        return PRIORITY_ORDER.index(priority_value)
    return len(PRIORITY_ORDER)


# ==============================
# ✅ 출력 포맷
#   - 완료: 취소선
#   - 보류: 밑줄(underline)
# ==============================
def format_task_line(title, status):
    s = status if status else "시작 전"
    line = f"({s}) {title}"

    if s == "완료":
        line = f"~~{line}~~"
    elif s == "보류":
        line = f"__{line}__"

    return line


def group_tasks_for_date(data, target_date):
    grouped = {cat: [] for cat, _ in CATEGORY_ORDER}

    for page in data.get("results", []):
        start_d, end_d = safe_get_date_range(page)
        if not start_d or not end_d:
            continue

        if not (start_d <= target_date <= end_d):
            continue

        title = safe_get_title(page)
        if not title:
            continue

        status = safe_get_status_name(page)
        category = safe_get_select_name(page, CATEGORY_PROP)
        priority = safe_get_select_name(page, PRIORITY_PROP)

        if category not in grouped:
            category = "기타"

        grouped[category].append((priority, status, title))

    for cat in grouped:
        grouped[cat].sort(key=lambda x: priority_rank(x[0]))

    return grouped


def create_discord_payload(data, eff_str):
    eff_date = datetime.strptime(eff_str, "%Y-%m-%d").date()
    grouped = group_tasks_for_date(data, eff_date)

    lines = [f"📅 **{eff_str}**", ""]

    for idx, (cat, icon) in enumerate(CATEGORY_ORDER):
        lines.append(f"{icon} **{cat}**")
        items = grouped.get(cat, [])
        if not items:
            lines.append("할 일 없음")
        else:
            for (_, s, t) in items:
                lines.append(format_task_line(title=t, status=s))
        if idx != len(CATEGORY_ORDER) - 1:
            lines.append("")

    return {
        "embeds": [{
            "description": "\n".join(lines),
            "color": EMBED_COLOR
        }]
    }


# ==============================
# ✅ Discord 전송 / 수정
# ==============================
def clean_webhook_url(url: str) -> str:
    return url.split("?")[0].strip()

def send_new_message(webhook_url, payload):
    base = clean_webhook_url(webhook_url)
    r = requests.post(base, params={"wait": "true"}, json=payload)
    r.raise_for_status()
    return r.json()["id"]

def edit_message(webhook_url, message_id, payload):
    base = clean_webhook_url(webhook_url)
    url = f"{base}/messages/{message_id}"
    r = requests.patch(url, json=payload)
    r.raise_for_status()
    return True


def main():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("DISCORD_WEBHOOK_URL이 비어있습니다.")

    eff = effective_date_str()  # 11시 기준 날짜

    notion_data = fetch_notion_data()
    payload = create_discord_payload(notion_data, eff)

    state = load_state()
    saved_date = state.get("date")
    saved_message_id = state.get("message_id")

    if saved_date == eff and saved_message_id:
        edit_message(webhook_url, saved_message_id, payload)
        print(f"✅ Edited message: {saved_message_id}")
    else:
        new_id = send_new_message(webhook_url, payload)
        state = {"date": eff, "message_id": new_id}
        save_state(state)
        print(f"✅ Created new message: {new_id}")


if __name__ == "__main__":
    main()