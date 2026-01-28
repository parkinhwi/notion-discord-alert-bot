# 📋 Notion ↔ Google Calendar → Discord Daily Task Bot

Notion 데이터베이스의 할 일을 Discord로 자동 전송하고,  
Google Calendar 일정도 Notion에 동기화한 뒤 Discord에 함께 보여주는 봇입니다.

매일 **오전 11시(KST)** 를 기준으로 “오늘” 기준 날짜가 결정되며,  
설정한 주기마다 자동으로 Discord 메시지를 업데이트합니다.

---

## ✨ 주요 기능

### ✅ Notion → Discord
- 🔄 **자동 업데이트**: 지정 주기(예: 30분)마다 Notion을 조회해 Discord 메시지 업데이트
- 📅 **스마트 날짜 기준**: 오전 11시 전이면 “어제”, 11시 이후면 “오늘” 기준으로 출력
- 🎨 **카테고리별 정리**: 설정한 카테고리 순서대로 섹션 출력
- ⭐ **우선순위 정렬**: 중요도 순 자동 정렬 (1 > 2 > 3 > 4 > -)
- ✅ **상태 표시**: 완료(취소선), 보류(밑줄) 자동 표시
- 💬 **메시지 재사용**: 같은 날짜면 새 메시지 생성이 아니라 기존 메시지를 수정(Edit)

### 📅 Google Calendar → Notion Sync
- 🔁 **캘린더 일정 자동 동기화**: Google Calendar 일정을 Notion에 자동 생성/업데이트
- 🚫 **취소/거절 일정 제외**:
  - 취소된 일정(canceled)은 가져오지 않음
  - 내가 “참석하지 않음(Declined)” 누른 일정도 제외
- 🧠 **변경사항 반영**: 일정 이름 변경/시간 변경/날짜 이동 시 Notion에서도 자동 업데이트
- 🧹 **정리(옵션)**: 동기화 대상에서 빠진(삭제/취소/거절) 일정은 Notion에서 자동 아카이브 처리 가능
- ⏱️ **상태 자동 판정**: 현재시간 기준으로
  - 시작 전 / 진행 중 / 완료 로 states가 자동 설정됨

---

## 📸 미리보기
<img width="525" height="539" alt="Image" src="https://github.com/user-attachments/assets/72c7a329-e088-44f7-afeb-eda97729eab6" />

디스코드의 스레드를 이용해서 깔끔하게 todo 기록을 관리할 수 있어요.


Discord에 이런 형태로 표시됩니다:

```
📅 2026-01-26

📧 캘린더
(시작 전) 외주사 A 미팅 12pm

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
4. [Google Calendar API 설정](#4-google-calendar-api-설정)
5. [GitHub 레포지토리 만들기](#5-github-레포지토리-만들기)
6. [Notion API 연동](#6-notion-api-연동)
7. [GitHub Secrets 설정](#7-github-secrets-설정)
8. [외부 스케줄러 설정](#8-외부-스케줄러-설정-cron-joborg)
9. [테스트 및 확인](#9-테스트-및-확인)

---

## 1. 사전 준비

### 필요한 것들
- [ ] Notion 계정
- [ ] Discord 서버 (관리자 권한)
- [ ] Google 계정 (캘린더 사용)
- [ ] GitHub 계정
- [ ] 이메일 주소 (Cron-job.org 가입용)

---

## 2. Notion 설정

### 2-1. Notion 데이터베이스 만들기

Notion에서 `/table` → **Table - Inline**로 데이터베이스를 만들고 아래 속성을 추가합니다.

| 속성 이름 | 타입 | 설명 |
|---------|------|------|
| `name` | Title | 할 일 제목 |
| `states` | Status 또는 Select | 상태 (시작 전 / 진행 중 / 완료 / 보류) |
| `label` | Select | 카테고리 (캘린더 / 메인업무 / 외주 / 스포클 / 유튜브 / 기타) |
| `priority` | Select | 중요도 (-, 1, 2, 3, 4) |
| `date` | Date | 날짜 (시간 포함 가능 / 날짜 범위 가능) |
| `gcal_event_id` | Text (Rich text) | **캘린더 동기화 키(중복 방지/업데이트용)** |

> `gcal_event_id`는 캘린더에서 들어온 일정의 고유 ID를 저장해  
> 같은 일정을 중복 생성하지 않고, 수정사항을 업데이트하기 위해 필요합니다.

### 2-2. 상태(states) 옵션
- 시작 전
- 진행 중
- 완료
- 보류

### 2-3. 카테고리(label) 옵션
- 캘린더
- 메인업무
- 외주
- 스포클
- 유튜브
- 기타

### 2-4. 우선순위(priority) 옵션
- 1
- 2
- 3
- 4
- -

---

## 3. Discord 웹훅 만들기

1. Discord 서버에서 봇이 메시지를 보낼 **채널** 선택
2. 채널 설정(⚙️) → **연동** → **웹후크**
3. **새 웹후크** 생성 → URL 복사

웹훅 URL은 아래 형식이어야 합니다:

```
https://discord.com/api/webhooks/123456789/abcdefghijklmnop
```

> ⚠️ 이 URL은 절대 공개하지 마세요.

---

## 4. Google Calendar API 설정

이 봇은 **Service Account(JSON)** 로 Google Calendar를 읽습니다.

필요한 값:
- `GOOGLE_SERVICE_ACCOUNT_JSON` : 서비스 계정 JSON(문자열로 저장)
- `GCAL_ID` : 불러올 캘린더 ID (보통 이메일 또는 캘린더 고유 ID)
- `GCAL_OWNER_EMAIL` : “내가 참석하지 않음(Declined)” 판단용 내 이메일(권장)

> 캘린더 공유 설정에서 서비스 계정 이메일에 **캘린더 읽기 권한**을 반드시 부여해야 합니다.

---

## 5. GitHub 레포지토리 만들기

레포지토리에 아래 파일을 추가합니다.

- `.github/workflows/notify.yml`
- `script.py`
- `.gitignore`
- `discord_state.json` (권장)

---

## 6. Notion API 연동

1. https://www.notion.so/my-integrations 에서 Integration 생성
2. 토큰(Internal Integration Token) 복사
3. 데이터베이스에서 Integration 연결(Share)

---

## 7. GitHub Secrets 설정

GitHub → Settings → Secrets and variables → Actions → New repository secret

필수 secrets:
- `NOTION_API_KEY`
- `NOTION_DATABASE_ID`
- `DISCORD_WEBHOOK_URL`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `GCAL_ID`
- `GCAL_OWNER_EMAIL` (권장)

옵션:
- `GCAL_SYNC_EVERY_MINUTES` : 캘린더 동기화 주기(분). 예) `"60"`, `"360"`

---

## 8. 외부 스케줄러 설정 (Cron-job.org)

GitHub Actions 스케줄 대신 cron-job.org로 `workflow_dispatch` 호출을 추천합니다.

- 30분마다 실행: `*/30 * * * *`

---

## 9. 테스트 및 확인

1. GitHub Actions 탭에서 workflow 실행 로그 확인
2. Discord 채널에서 메시지 생성/수정 확인
3. Google Calendar 일정이 Notion의 `캘린더` 카테고리로 들어오는지 확인
4. “취소/거절” 일정이 제외되는지 확인

---

## ⚙️ Notion 필터(오늘/어제/내일 버튼용)

Notion에서 날짜에 시간이 포함돼도 정상적으로 필터되도록 아래 수식을 사용하세요.

### 오늘
```notion
and(
  not(empty(prop("date"))),
  dateStart(prop("date")) < dateAdd(today(), 1, "days"),
  if(
    empty(dateEnd(prop("date"))),
    dateStart(prop("date")) >= today(),
    dateEnd(prop("date")) >= today()
  )
)
```
### 어제
```
and(
  not(empty(prop("date"))),
  dateStart(prop("date")) < today(),
  if(
    empty(dateEnd(prop("date"))),
    dateStart(prop("date")) >= dateAdd(today(), -1, "days"),
    dateEnd(prop("date")) >= dateAdd(today(), -1, "days")
  )
)
```

### 내일
```
and(
  not(empty(prop("date"))),
  dateStart(prop("date")) < dateAdd(today(), 2, "days"),
  if(
    empty(dateEnd(prop("date"))),
    dateStart(prop("date")) >= dateAdd(today(), 1, "days"),
    dateEnd(prop("date")) >= dateAdd(today(), 1, "days")
  )
)
```

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
