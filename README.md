# 📋 Notion To Discord Daily Task Bot

Notion 데이터베이스의 할 일을 Discord로 자동으로 전송하는 봇입니다.  
매일 오전 11시를 기준으로 날짜가 바뀌며, 설정한 시간마다 자동으로 Discord 메시지를 업데이트합니다.

## ✨ 주요 기능

- 🔄 **자동 업데이트**: 30분마다 Notion 데이터를 확인하고 Discord 메시지 업데이트
- 📅 **스마트 날짜 관리**: 오전 11시 기준으로 "오늘" 할 일 자동 선택
- 🎨 **카테고리별 정리**: 카테고리1, 카테고리2, 카테고리3, 카테고리4, 카테고리5 로 자동 분류
- ⭐ **우선순위 정렬**: 중요도 순으로 자동 정렬 (1 > 2 > 3 > 4 > -)
- ✅ **상태 표시**: 완료(취소선), 보류(밑줄) 자동 표시
- 💬 **메시지 재사용**: 같은 날짜면 새 메시지가 아닌 기존 메시지 수정

## 📸 미리보기
<img width="525" height="539" alt="Image" src="https://github.com/user-attachments/assets/72c7a329-e088-44f7-afeb-eda97729eab6" />

디스코드의 스레드를 이용해서 깔끔하게 todo 기록을 관리할 수 있어요.


Discord에 이런 형태로 표시됩니다:

```
📅 2026-01-26

1️⃣ 메인업무
(진행 중) 프로젝트 A 마무리
~~(완료) 회의 자료 준비~~

2️⃣ 외주
(시작 전) 클라이언트 미팅

3️⃣ 스포클
할 일 없음

4️⃣ 유튜브
__(보류) 영상 편집__

ℹ️ 기타
(진행 중) 블로그 포스팅
```

---

# 🚀 설정 가이드

## 📋 목차
1. [사전 준비](#1-사전-준비)
2. [Notion 설정](#2-notion-설정)
3. [Discord 웹훅 만들기](#3-discord-웹훅-만들기)
4. [GitHub 레포지토리 만들기](#4-github-레포지토리-만들기)
5. [Notion API 연동](#5-notion-api-연동)
6. [GitHub Secrets 설정](#6-github-secrets-설정)
7. [외부 스케줄러 설정](#7-외부-스케줄러-설정-cron-joborg)
8. [테스트 및 확인](#8-테스트-및-확인)

---

## 1. 사전 준비

### 필요한 것들
- [ ] Notion 계정
- [ ] Discord 서버 (관리자 권한)
- [ ] GitHub 계정
- [ ] 이메일 주소 (Cron-job.org 가입용)

---

## 2. Notion 설정

### 2-1. Notion 데이터베이스 만들기
<img width="747" height="479" alt="Image" src="https://github.com/user-attachments/assets/24e6fbdf-f2a5-4ed1-94d7-bbae33aee809" />

1. Notion에서 새 페이지 생성
2. `/table` 입력하여 **Table - Inline** 선택
3. 다음 속성(컬럼) 추가:

| 속성 이름 | 타입 | 설명 |
|---------|------|------|
| `name` | Title | 할 일 제목 |
| `states` | Status 또는 Select | 상태 (시작 전 / 진행 중 / 완료 / 보류) |
| `label` | Select | 카테고리 (메인업무 / 외주 / 스포클 / 유튜브 / 기타) |
| `priority` | Select | 중요도 (-, 1, 2, 3, 4) |
| `date` | Date | 날짜 (날짜 범위 가능) |

### 2-2. 상태 옵션 설정

`states` 속성의 옵션:
- 시작 전
- 진행 중
- 완료
- 보류

### 2-3. 카테고리 옵션 설정

`label` 속성의 옵션:
- 카테고리1
- 카테고리2
- 카테고리3
- 카테고리4
- 카테고리5

### 2-4. 우선순위 옵션 설정

`priority` 속성의 옵션:
- 1
- 2
- 3
- 4
- -

---

## 3. Discord 웹훅 만들기

### 3-1. 웹훅 생성

1. Discord 서버에서 봇이 메시지를 보낼 **채널** 선택
2. 채널 설정 (⚙️) → **연동** → **웹후크**
3. **새 웹후크** 클릭
4. 웹후크 이름 설정 (예: `Notion Bot`)
5. **웹후크 URL 복사** 클릭하여 URL 복사
6. **저장** 클릭

### 3-2. 웹훅 URL 형식 확인

복사한 URL은 다음과 같은 형식이어야 합니다:
```
https://discord.com/api/webhooks/123456789/abcdefghijklmnop
```

> ⚠️ **중요**: 이 URL은 절대 공개하지 마세요!

---

## 4. GitHub 레포지토리 만들기

### 4-1. 레포지토리 생성

1. https://github.com 접속 및 로그인
2. 오른쪽 위 **+** → **New repository** 클릭
3. 설정:
   - Repository name: `notion-discord-alert-bot` (원하는 이름)
   - Public 또는 Private 선택 (둘 다 가능)
   - **Add a README file** 체크
4. **Create repository** 클릭

### 4-2. 파일 업로드

레포지토리에 다음 파일들을 추가하세요:

**1) `.github/workflows/notify.yml`**

먼저 폴더 구조를 만들어야 합니다:
- **Add file** → **Create new file**
- 파일 이름에 `.github/workflows/notify.yml` 입력 (자동으로 폴더 생성됨)

내용:
```yaml
name: Notion to Discord Notification

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  notify:
    runs-on: ubuntu-latest
    permissions:
      contents: write

    steps:
      - name: Checkout repository
        uses: actions/checkout@v3
        with:
          token: ${{ secrets.GITHUB_TOKEN }}

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.x"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests

      - name: Run notification script
        env:
          NOTION_API_KEY: ${{ secrets.NOTION_API_KEY }}
          NOTION_DATABASE_ID: ${{ secrets.NOTION_DATABASE_ID }}
          DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}
        run: python script.py

      - name: Commit and Push
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add discord_state.json
          if ! git diff --cached --quiet; then
            git commit -m "Update discord message state [skip ci]"
            git pull origin main --no-rebase || true
            git push origin main || echo "Push failed, but continuing..."
          fi
```

**Commit changes** 클릭

**2) `script.py`**

- **Add file** → **Create new file**
- 파일 이름: `script.py`

내용:
```python
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
    ("카테고리1", "1️⃣"),
    ("카테고리2", "2️⃣"),
    ("카테고리3", "3️⃣"),
    ("카테고리4", "4️⃣"),
    ("카테고리5", "ℹ️"),
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
```

**Commit changes** 클릭

**3) `.gitignore`**

- **Add file** → **Create new file**
- 파일 이름: `.gitignore`

내용:
```
.env
__pycache__/
*.pyc
.DS_Store
```

**Commit changes** 클릭

**4) `discord_state.json`** (선택사항)

- **Add file** → **Create new file**
- 파일 이름: `discord_state.json`

내용:
```json
{
  "date": "",
  "message_id": ""
}
```

**Commit changes** 클릭

---

## 5. Notion API 연동

### 5-1. Notion Integration 만들기

1. https://www.notion.so/my-integrations 접속
2. **+ New integration** 클릭
3. 설정:
   - Name: `Discord Bot` (원하는 이름)
   - Associated workspace: 본인의 워크스페이스 선택
   - Type: **Internal**
4. **Submit** 클릭
5. **Internal Integration Token** 복사 (나중에 사용)
   - 형식: `secret_xxxxxxxxxxxxxxxxxxxxx`

### 5-2. Database에 Integration 연결

1. Notion에서 만든 데이터베이스 페이지 열기
2. 오른쪽 위 **⋯** (점 3개) → **연결** → **연결 추가**
3. 방금 만든 Integration (`Discord Bot`) 선택
4. **확인** 클릭

### 5-3. Database ID 복사

1. 데이터베이스 페이지 열기
2. 주소창의 URL 복사

URL 형식:
```
https://www.notion.so/xxxxxxxxxxxxxxxxxxxxxxxxxxxx?v=yyyyyyyyyyyyyyyyyyyyyy
```

Database ID는 URL에서 `?` 앞부분:
```
xxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

또는 전체 URL을 복사해도 됩니다 (스크립트가 자동으로 ID 추출).

---

## 6. GitHub Secrets 설정

### 6-1. Secrets 추가

1. GitHub 레포지토리 → **Settings** 탭
2. 왼쪽 **Secrets and variables** → **Actions**
3. **New repository secret** 클릭

### 6-2. 3개의 Secret 추가

**Secret 1: NOTION_API_KEY**
```
Name: NOTION_API_KEY
Value: secret_xxxxxxxxxxxxxxxxxxxx (5-1에서 복사한 Integration Token)
```
**Add secret** 클릭

**Secret 2: NOTION_DATABASE_ID**
```
Name: NOTION_DATABASE_ID
Value: xxxxxxxxxxxxxxxxxxxxxxxxxxxx (5-3에서 복사한 Database ID 또는 전체 URL)
```
**Add secret** 클릭

**Secret 3: DISCORD_WEBHOOK_URL**
```
Name: DISCORD_WEBHOOK_URL
Value: https://discord.com/api/webhooks/... (3-1에서 복사한 웹훅 URL)
```
**Add secret** 클릭

### 6-3. 확인

Secrets 목록에 다음 3개가 있어야 합니다:
- ✅ NOTION_API_KEY
- ✅ NOTION_DATABASE_ID
- ✅ DISCORD_WEBHOOK_URL

---

## 7. 외부 스케줄러 설정 (Cron-job.org)

GitHub Actions의 스케줄은 불안정하므로 외부 스케줄러를 사용합니다.

### 7-1. GitHub Personal Access Token 만들기

1. GitHub 오른쪽 위 **프로필 사진** → **Settings**
2. 왼쪽 맨 아래 **Developer settings**
3. **Personal access tokens** → **Tokens (classic)**
4. **Generate new token** → **Generate new token (classic)**
5. 설정:
   - Note: `Cron Job for Notion Discord Bot`
   - Expiration: **No expiration**
   - Select scopes:
     - ✅ **repo** (전체 체크)
     - ✅ **workflow**
6. **Generate token** 클릭
7. 생성된 토큰 복사 (예: `ghp_xxxxxxxxxxxx`)
   - ⚠️ **중요**: 이 페이지를 벗어나면 다시 볼 수 없으니 바로 복사하세요!

### 7-2. Cron-job.org 가입

1. https://cron-job.org 접속
2. **Sign up** 클릭
3. 이메일, 비밀번호 입력
4. 이메일 인증 완료

### 7-3. Cron Job 만들기

1. 로그인 후 **Create cronjob** 클릭

#### COMMON 탭:

**Title:**
```
Notion Discord Bot - Every 30 minutes
```

**URL:**
```
https://api.github.com/repos/본인GitHub아이디/레포이름/actions/workflows/notify.yml/dispatches
```

예시:
```
https://api.github.com/repos/parkinhwi/notion-discord-alert-bot/actions/workflows/notify.yml/dispatches
```

**Enable job:** ✅ 체크

**Execution schedule:**
- **Every 30 minutes** 선택

또는 Custom:
```
Minutes: */30
Hours: *
Days: *
Months: *
Weekdays: *
```

#### ADVANCED 탭:

**Request method:**
```
POST
```

**Headers** (+ Add header 버튼 4번 클릭):

Header 1:
```
Name: Accept
Value: application/vnd.github+json
```

Header 2:
```
Name: Authorization
Value: Bearer ghp_xxxxxxxxxxxxxxxxx
```
⚠️ `Bearer ` 다음에 공백 1칸 + 7-1에서 복사한 토큰

Header 3:
```
Name: X-GitHub-Api-Version
Value: 2022-11-28
```

Header 4:
```
Name: Content-Type
Value: application/json
```

**Body:**
```json
{"ref":"main"}
```

2. **CREATE** 클릭

### 7-4. 테스트

1. 생성된 크론잡 목록에서 찾기
2. 오른쪽 **▶️ Run** 클릭
3. "Job has been executed successfully" 메시지 확인

---

## 8. 테스트 및 확인

### 8-1. GitHub Actions 확인

1. GitHub 레포지토리 → **Actions** 탭
2. 새 워크플로우 실행 확인
   - Event: **workflow_dispatch**
   - 상태: 초록색 체크 ✅

### 8-2. Discord 확인

Discord 채널에서 봇 메시지 확인:
- 📅 날짜 표시
- 카테고리별 할 일 목록
- 상태 표시 (완료는 취소선, 보류는 밑줄)

### 8-3. 수동 테스트

언제든지 수동으로 실행 가능:
1. GitHub → Actions 탭
2. 왼쪽 "Notion to Discord Notification" 클릭
3. **Run workflow** → **Run workflow** 클릭

---

## ⚙️ 커스터마이징

### 카테고리 변경

`script.py`의 `CATEGORY_ORDER` 수정:

```python
CATEGORY_ORDER = [
    ("업무", "💼"),
    ("개인", "👤"),
    ("공부", "📚"),
]
```

### 실행 시간 변경

Cron-job.org에서:
- 1시간마다: `0 * * * *`
- 30분마다: `*/30 * * * *`
- 매일 오전 9시: `0 9 * * *`

### 날짜 기준 시간 변경

`script.py`의 `ROLLOVER_HOUR` 수정:

```python
ROLLOVER_HOUR = 9  # 오전 9시 기준으로 변경
```

### 임베드 색상 변경

`script.py`의 `EMBED_COLOR` 수정:

```python
EMBED_COLOR = int("FF5733", 16)  # 주황색
EMBED_COLOR = int("3498DB", 16)  # 파란색
EMBED_COLOR = int("2ECC71", 16)  # 초록색
```

---

## 🔧 문제 해결

### 워크플로우가 실행되지 않아요
- GitHub Secrets 3개가 모두 설정되었는지 확인
- Cron-job.org에서 수동 실행(▶️) 테스트
- GitHub Actions 탭에서 오류 로그 확인

### Discord에 메시지가 안 보여요
- Discord 웹훅 URL이 올바른지 확인
- 웹훅이 생성된 채널 확인
- GitHub Actions 로그에서 오류 확인

### Notion 데이터를 못 가져와요
- Notion Integration이 데이터베이스에 연결되었는지 확인
- Database ID가 올바른지 확인
- Notion 속성 이름이 코드와 일치하는지 확인

### "404 Not Found" 오류
- Cron-job.org의 URL에서 레포 이름, 사용자 이름 확인
- `notify.yml` 파일 이름 확인

---

## 📝 라이선스

MIT License

---

## 🤝 기여

이슈나 개선 사항이 있으면 자유롭게 Issue를 열어주세요!

---

## 📧 문의

문제가 있으시면 GitHub Issues에 남겨주세요.

---

**Made with ❤️ for Notion & Discord users**
