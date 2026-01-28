import os
import json
import requests
from datetime import datetime, timezone, timedelta
import re

# Google Calendar
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ==============================
# ✅ Time / Rollover
# ==============================
KST = timezone(timedelta(hours=9))
ROLLOVER_HOUR = 11  # 오전 11시 기준


def kst_now():
    return datetime.now(KST)


def effective_date(now=None):
    """
    오전 11시 전이면 '어제', 11시(포함) 이후면 '오늘'
    """
    now = now or kst_now()
    base = now.date()
    if now.hour < ROLLOVER_HOUR:
        base = base - timedelta(days=1)
    return base


def day_bounds_kst(date_obj):
    """
    해당 날짜의 00:00:00 ~ 24:00:00 KST 범위
    """
    start = datetime(date_obj.year, date_obj.month, date_obj.day, 0, 0, 0, tzinfo=KST)
    end = start + timedelta(days=1)
    return start, end


def format_time_kst(dt: datetime):
    # 예: 2pm / 2:30pm
    h = dt.hour
    m = dt.minute
    ap = "am" if h < 12 else "pm"
    h12 = h % 12
    if h12 == 0:
        h12 = 12
    if m == 0:
        return f"{h12}{ap}"
    return f"{h12}:{m:02d}{ap}"


# ==============================
# ✅ Notion property names
# ==============================
TITLE_PROP = "name"         # title
STATUS_PROP = "states"      # status/select: 시작 전 / 진행 중 / 완료 / 보류
CATEGORY_PROP = "label"     # select: 캘린더 / 메인업무 / 외주 / 스포클 / 유튜브 / 기타
PRIORITY_PROP = "priority"  # select: -, 1, 2, 3, 4
DATE_PROP = "date"          # date (range ok, time ok)

# Calendar sync key (Notion 속성: Text / Rich text)
GCAL_EVENT_ID_PROP = "gcal_event_id"  # rich_text

# ==============================
# ✅ Category order
# ==============================
CATEGORY_ORDER = [
    ("캘린더", "📧"),
    ("메인업무", "1️⃣"),
    ("외주", "2️⃣"),
    ("스포클", "3️⃣"),
    ("유튜브", "4️⃣"),
    ("기타", "ℹ️"),
]

PRIORITY_ORDER = ["1", "2", "3", "4", "-"]
EMBED_COLOR = int("FF57CF", 16)
STATE_FILE = "discord_state.json"


# ==============================
# ✅ Utils
# ==============================
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


def parse_date_yyyy_mm_dd(s: str):
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def parse_notion_datetime(s: str):
    """
    Notion date.start/end는
    - "2026-01-28" (date only)
    - "2026-01-28T14:30:00.000+09:00" (datetime)
    둘 다 올 수 있음
    """
    if not s:
        return None
    try:
        if len(s) <= 10:
            d = parse_date_yyyy_mm_dd(s)
            if not d:
                return None
            return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=KST)
        # isoformat with timezone
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(KST)
    except Exception:
        return None


def safe_get_rich_text(page, prop_name):
    prop = page["properties"].get(prop_name)
    if not prop:
        return None
    if prop["type"] == "rich_text":
        arr = prop["rich_text"]
        if not arr:
            return None
        return "".join([x.get("plain_text", "") for x in arr])
    return None


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
    if prop["type"] == "status":
        return prop["status"]["name"] if prop["status"] else None
    if prop["type"] == "select":
        return prop["select"]["name"] if prop["select"] else None
    return None


def safe_get_date_range(page):
    """
    date 범위 포함해서, target_date 포함여부 체크용 (date only 기반)
    """
    prop = page["properties"].get(DATE_PROP)
    if not prop:
        return (None, None)
    if prop["type"] == "date" and prop["date"]:
        start_raw = prop["date"].get("start")
        end_raw = prop["date"].get("end")
        start_d = parse_date_yyyy_mm_dd(start_raw)
        end_d = parse_date_yyyy_mm_dd(end_raw) if end_raw else None
        if start_d and not end_d:
            end_d = start_d
        return (start_d, end_d)
    return (None, None)


def safe_get_date_start_dt(page):
    """
    정렬용: date.start를 datetime으로 파싱 (시간 있으면 반영됨)
    """
    prop = page["properties"].get(DATE_PROP)
    if not prop:
        return None
    if prop["type"] == "date" and prop["date"]:
        start_raw = prop["date"].get("start")
        return parse_notion_datetime(start_raw)
    return None


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
# ✅ Notion API helpers
# ==============================
def notion_headers():
    notion_api_key = os.getenv("NOTION_API_KEY")
    if not notion_api_key:
        raise ValueError("NOTION_API_KEY가 비어있습니다.")
    return {
        "Authorization": f"Bearer {notion_api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }


def get_database_id():
    database_id_raw = os.getenv("NOTION_DATABASE_ID")
    database_id = normalize_notion_db_id(database_id_raw)
    if not database_id:
        raise ValueError("NOTION_DATABASE_ID가 비어있습니다.")
    return database_id


def query_notion_database(filter_payload=None):
    database_id = get_database_id()
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = notion_headers()

    all_results = []
    start_cursor = None

    while True:
        payload = {"page_size": 100}
        if filter_payload:
            payload["filter"] = filter_payload
        if start_cursor:
            payload["start_cursor"] = start_cursor

        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()

        all_results.extend(data.get("results", []))
        if data.get("has_more"):
            start_cursor = data.get("next_cursor")
        else:
            break

    return all_results


def create_notion_page(props: dict):
    database_id = get_database_id()
    url = "https://api.notion.com/v1/pages"
    headers = notion_headers()
    payload = {
        "parent": {"database_id": database_id},
        "properties": props
    }
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


def update_notion_page(page_id: str, props: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = notion_headers()
    payload = {"properties": props}
    resp = requests.patch(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


def archive_notion_page(page_id: str):
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = notion_headers()
    payload = {"archived": True}
    resp = requests.patch(url, headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


# ==============================
# ✅ Google Calendar -> Notion Sync
# ==============================
def build_gcal_service():
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_JSON이 비어있습니다.")
    info = json.loads(raw)
    scopes = ["https://www.googleapis.com/auth/calendar.readonly"]
    creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def fetch_gcal_events_for_date(service, calendar_id: str, date_obj):
    start_dt, end_dt = day_bounds_kst(date_obj)
    time_min = start_dt.astimezone(timezone.utc).isoformat()
    time_max = end_dt.astimezone(timezone.utc).isoformat()

    events = []
    page_token = None
    while True:
        res = service.events().list(
            calendarId=calendar_id,
            timeMin=time_min,
            timeMax=time_max,
            singleEvents=True,
            orderBy="startTime",
            showDeleted=False,  # ✅ 취소된/삭제된 이벤트는 API에서 제외
            pageToken=page_token
        ).execute()

        events.extend(res.get("items", []))
        page_token = res.get("nextPageToken")
        if not page_token:
            break
    return events


def is_declined_for_me(ev) -> bool:
    """
    내가 '참석하지 않음' 누른 일정은 제외
    - GCAL_OWNER_EMAIL로 내 이메일 지정하면 가장 정확
    - 대체: attendees 중 self=True가 있고 declined면 제외
    """
    attendees = ev.get("attendees") or []
    my_email = (os.getenv("GCAL_OWNER_EMAIL") or "").strip().lower()

    for a in attendees:
        email = (a.get("email") or "").strip().lower()
        status = (a.get("responseStatus") or "").strip().lower()
        is_self = bool(a.get("self"))

        if status == "declined":
            if my_email and email == my_email:
                return True
            if is_self:
                return True

    return False


def notion_props_for_gcal_event(ev):
    """
    - name: '제목 2:30pm' 형태로 들어감
    - label: 캘린더
    - states: 현재시간 기준 시작 전/진행 중/완료
    - date: 실제 시작/끝 시간을 date 속성에 저장 (노션 정렬 가능하게)
    - gcal_event_id: 고유 id
    """
    summary = ev.get("summary") or "(제목 없음)"
    ev_status = (ev.get("status") or "").lower()

    start = ev.get("start", {})
    end = ev.get("end", {})

    start_dt = None
    end_dt = None

    # timed event
    if start.get("dateTime"):
        start_dt = datetime.fromisoformat(start["dateTime"].replace("Z", "+00:00")).astimezone(KST)
    elif start.get("date"):
        sd = parse_date_yyyy_mm_dd(start["date"])
        if sd:
            start_dt = datetime(sd.year, sd.month, sd.day, 0, 0, 0, tzinfo=KST)

    if end.get("dateTime"):
        end_dt = datetime.fromisoformat(end["dateTime"].replace("Z", "+00:00")).astimezone(KST)
    elif end.get("date"):
        # all-day 종료는 보통 다음날 00:00로 옴(구글 방식)
        ed = parse_date_yyyy_mm_dd(end["date"])
        if ed:
            end_dt = datetime(ed.year, ed.month, ed.day, 0, 0, 0, tzinfo=KST)

    if start_dt and not end_dt:
        end_dt = start_dt + timedelta(hours=1)

    # title 만들기
    title = summary
    if start.get("dateTime") and start_dt:
        title = f"{summary} {format_time_kst(start_dt)}"

    # 상태 자동 계산
    now_kst = kst_now()
    if ev_status == "cancelled":
        states_value = "보류"
    else:
        if start_dt and end_dt:
            if now_kst < start_dt:
                states_value = "시작 전"
            elif start_dt <= now_kst < end_dt:
                states_value = "진행 중"
            else:
                states_value = "완료"
        else:
            states_value = "시작 전"

    # Notion date: 시간까지 넣어서 정렬 가능하게
    # - timed: start/end ISO
    # - all-day: start만 date로 (end는 None)
    date_payload = None
    if start.get("dateTime") and start_dt:
        date_payload = {
            "date": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat() if end_dt else None
            }
        }
    elif start.get("date"):
        # all-day
        date_payload = {
            "date": {
                "start": start.get("date"),
                "end": None
            }
        }
    else:
        # fallback: 오늘 날짜만
        today = kst_now().date().strftime("%Y-%m-%d")
        date_payload = {
            "date": {
                "start": today,
                "end": None
            }
        }

    props = {
        TITLE_PROP: {
            "title": [{"text": {"content": title}}]
        },
        CATEGORY_PROP: {
            "select": {"name": "캘린더"}
        },
        PRIORITY_PROP: {
            "select": {"name": "-"}
        },
        DATE_PROP: date_payload,
        GCAL_EVENT_ID_PROP: {
            "rich_text": [{"text": {"content": ev["id"]}}]
        }
    }

    # states는 status 타입일 수도 / select 타입일 수도 있어서
    # 일단 status로 넣고, 실패하면 호출부에서 select로 재시도
    props[STATUS_PROP] = {"status": {"name": states_value}}
    return props


def sync_gcal_to_notion(eff_date_obj):
    calendar_id = os.getenv("GCAL_ID")
    if not calendar_id:
        raise ValueError("GCAL_ID가 비어있습니다.")

    service = build_gcal_service()
    events = fetch_gcal_events_for_date(service, calendar_id, eff_date_obj)

    # ✅ 이번 날짜의 '캘린더' 페이지들만 가져오기 (정리/아카이브용)
    eff_str = eff_date_obj.strftime("%Y-%m-%d")
    existing_pages = query_notion_database({
        "and": [
            {"property": CATEGORY_PROP, "select": {"equals": "캘린더"}},
            {"property": DATE_PROP, "date": {"on_or_after": eff_str}},
            {"property": DATE_PROP, "date": {"on_or_before": eff_str}},
            {"property": GCAL_EVENT_ID_PROP, "rich_text": {"is_not_empty": True}},
        ]
    })

    by_event_id = {}
    for p in existing_pages:
        eid = safe_get_rich_text(p, GCAL_EVENT_ID_PROP)
        if eid:
            by_event_id[eid] = p

    valid_event_ids = set()

    for ev in events:
        if "id" not in ev:
            continue

        # ✅ 취소/거절은 아예 스킵
        if (ev.get("status") or "").lower() == "cancelled":
            continue
        if is_declined_for_me(ev):
            continue

        eid = ev["id"]
        valid_event_ids.add(eid)

        props = notion_props_for_gcal_event(ev)

        if eid in by_event_id:
            page_id = by_event_id[eid]["id"]
            try:
                update_notion_page(page_id, props)
            except requests.HTTPError:
                # states가 select 타입인 경우 재시도
                props2 = dict(props)
                props2[STATUS_PROP] = {"select": {"name": props[STATUS_PROP]["status"]["name"]}}
                update_notion_page(page_id, props2)
        else:
            try:
                create_notion_page(props)
            except requests.HTTPError:
                props2 = dict(props)
                props2[STATUS_PROP] = {"select": {"name": props[STATUS_PROP]["status"]["name"]}}
                create_notion_page(props2)

    # ✅ 기존에 있었는데 이제는(취소/거절/삭제 등) valid에 없는 것 → 아카이브
    for eid, page in by_event_id.items():
        if eid not in valid_event_ids:
            archive_notion_page(page["id"])


# ==============================
# ✅ Discord message builder
# ==============================
def priority_rank(priority_value):
    if priority_value in PRIORITY_ORDER:
        return PRIORITY_ORDER.index(priority_value)
    return len(PRIORITY_ORDER)


def format_task_line(title, status):
    s = status if status else "시작 전"
    line = f"({s}) {title}"
    if s == "완료":
        line = f"~~{line}~~"
    elif s == "보류":
        line = f"__{line}__"
    return line


def fetch_notion_data_all_with_date():
    results = query_notion_database({
        "property": DATE_PROP,
        "date": {"is_not_empty": True}
    })
    return {"results": results}


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

        start_dt = safe_get_date_start_dt(page)  # ✅ 캘린더 정렬용(시간 포함)

        # (priority, status, title, start_dt)
        grouped[category].append((priority, status, title, start_dt))

    # ✅ 정렬:
    # - 캘린더: start_dt 오름차순(빠른 시간 먼저), 없으면 뒤로
    # - 그 외: priority 기준
    for cat in grouped:
        if cat == "캘린더":
            grouped[cat].sort(key=lambda x: (x[3] is None, x[3] or datetime.max.replace(tzinfo=KST), x[2]))
        else:
            grouped[cat].sort(key=lambda x: (priority_rank(x[0]), x[2]))

    return grouped


def create_discord_payload(data, eff_str):
    eff_date = datetime.strptime(eff_str, "%Y-%m-%d").date()
    grouped = group_tasks_for_date(data, eff_date)

    lines = [f"📅 **{eff_str}**", ""]

    for idx, (cat, icon) in enumerate(CATEGORY_ORDER):
        if cat == "캘린더":
            lines.append(f"{icon} **캘린더**")
        else:
            lines.append(f"{icon} **{cat}**")

        items = grouped.get(cat, [])
        if not items:
            lines.append("할 일 없음")
        else:
            for (_, s, t, _) in items:
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
# ✅ Discord webhook
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

    state = load_state()

    eff_date_obj = effective_date()
    eff = eff_date_obj.strftime("%Y-%m-%d")

    # ✅ 1) 캘린더 -> 노션 동기화는 매번 실행 (30분마다 같이 돌기)
    sync_gcal_to_notion(eff_date_obj)

    # ✅ 2) 노션 -> 디스코드
    notion_data = fetch_notion_data_all_with_date()
    payload = create_discord_payload(notion_data, eff)

    saved_date = state.get("date")
    saved_message_id = state.get("message_id")

    if saved_date == eff and saved_message_id:
        edit_message(webhook_url, saved_message_id, payload)
        print(f"✅ Edited message: {saved_message_id}")
    else:
        new_id = send_new_message(webhook_url, payload)
        state["date"] = eff
        state["message_id"] = new_id
        save_state(state)
        print(f"✅ Created new message: {new_id}")


if __name__ == "__main__":
    main()
