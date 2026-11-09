# bugetBackend - Mobile React Native DB Middleware

FastAPI 기반의 모바일 가계부(React Native) 전담 PostgreSQL 트랜잭션 미들웨어 백엔드 서버입니다.

React Native Android 환경에서 PostgreSQL 직접 JDBC 연결 시 발생하는 JVM 클래스 결핍(`NoClassDefFoundError: ManagementFactory`) 및 SCRAM 인증 에러(`SCRAM-SHA-256 Auth Type 10`)를 완벽히 해결하고, 안정적인 원격 트랜잭션을 제공합니다.

---

## 🚀 빠른 시작 (Quick Start)

### 1. 환경 변수 확인 (`.env`)
```bash
cp .env.example .env
```
필요에 따라 `.env` 파일의 PostgreSQL 접속 정보 또는 포트를 수정합니다.

```env
PG_HOST=your_db_host          # DB 호스트/IP 예시 (예: localhost 또는 원격 DB IP)
PG_PORT=your_db_port          # DB 포트 예시 (예: 5432)
PG_DATABASE=your_db_name      # 데이터베이스명 예시
PG_USER=your_db_user          # 사용자명 예시
PG_PASSWORD=your_password     # 비밀번호 예시

SERVER_HOST=your_server_host  # 서버 바인딩 호스트 예시 (예: 0.0.0.0)
SERVER_PORT=your_server_port  # 서버 포트 예시 (예: 8000)
```

### 2. 원클릭 백그라운드 실행 및 제어 스크립트

```bash
# 백그라운드 서버 구동 (기본 동작)
./run.sh          # 또는 ./start.sh

# 서버 실행 상태 확인 (PID, 포트, DB 연결 상태)
./status.sh       # 또는 ./run.sh status

# 실시간 서버 로그 모니터링
./run.sh logs     # 또는 tail -f logs/buget_backend.log

# 서버 안전 종료
./stop.sh         # 또는 ./run.sh stop

# 서버 재시작
./run.sh restart
```

> **참고**: 터미널에서 직접 포그라운드로 로그를 보며 디버깅 실행하려면 `./run.sh foreground` 명령을 사용합니다.

- **서버 포트**: `your_server_port` (예: `8000`)
- **Base URL**: `http://your-server-ip:your-server-port` (외부/모바일) 또는 `http://localhost:your-server-port` (로컬)
- **Swagger API 문서**: `http://your-server-ip:your-server-port/docs`
- **로그 파일 위치**: `logs/buget_backend.log`

---

## 📌 API 명세 요약

| Method | Endpoint | 설명 |
|---|---|---|
| `GET` | `/` | 서버 상태 및 라우트 안내 |
| `GET` | `/api/health` | 데이터베이스 연결 헬스체크 |
| `POST` | `/api/backup/{user_key}` | 7개 테이블 초기화 및 트랜잭션 일괄 백업 |
| `GET` | `/api/restore/{user_key}` | 7개 테이블 원본 JSON 트리 구조 복구 |

### 클라이언트 JSON 키 <-> PostgreSQL 대상 테이블 매핑 (7개)

| 클라이언트 JSON Key | PostgreSQL 대상 테이블 | 레코드 식별자(ID) 우선순위 | 설명 |
|---|---|---|---|
| `transactions` | `transactions` | `id`, `_id`, `key`, `uuid` | 거래 내역 목록 |
| `recurringRules` | `recurring_rules` | `id`, `ruleId`, `rule_id`, `name` | 반복 규칙 목록 |
| `customHolidays` | `holidays` | `id`, `date`, `holidayDate`, `name` | 사용자 지정 공휴일 목록 |
| `rawAppNotifications` | `app_notifications_raw` | `id`, `notificationId`, `sbnId`, `key` | 수신 앱 원본 알림 전체 |
| `parseExceptions` | `parse_exceptions` | `id`, `exceptionId`, `pattern`, `keyword` | 알림 파싱 예외 규칙 |
| `manualInstallments` | `manual_installments` | `id`, `installmentId`, `transactionId` | 수동 분할 납부(할부) 정보 |
| `user` | `user_settings` | `id`, `userKey`, `userId` | 사용자 설정 및 프로필 객체 (`synced_at` 동기화 시간 저장) |

> **동기화 시간(synced_at) 및 호환성 안내**:
> - 사용자 테이블(`user_settings`)에 `synced_at` 및 `sync_time` 컬럼(TIMESTAMPTZ)이 추가되어 DB 동기화 완료 시점이 항상 최신으로 기록됩니다.
> - 백업 응답(`POST /api/backup/{user_key}`)과 복구 응답(`GET /api/restore/{user_key}`) 모두 `synced_at` (ISO 8601 형식 문자열)을 반환하며, `user` 객체 내부(`user.syncedAt`, `user.synced_at`)에도 동기화 시간이 함께 포함됩니다.
> - 백업 시 클라이언트 JSON 키(`recurringRules`, `customHolidays` 등)와 DB 테이블명(`recurring_rules`, `holidays` 등)을 모두 지원합니다.
> - 복구(`GET /api/restore/{user_key}`) 시에도 모바일 앱이 참조하기 편하도록 카멜케이스 키와 스네이크케이스 키를 동시에 제공합니다.

---

## 📱 React Native 연동 클라이언트 예시 (`apiService.js` / `.ts`)

```typescript
const BASE_URL = 'http://your-server-ip:your-server-port'; // 예: 'http://192.168.1.100:8000' 또는 'http://localhost:8000'

/**
 * 1. 전체 백업 업로드
 */
export async function backupData(userKey: string, payload: any) {
  try {
    const response = await fetch(`${BASE_URL}/api/backup/${encodeURIComponent(userKey)}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.message || data.detail || '백업에 실패했습니다.');
    }
    return data;
  } catch (error: any) {
    console.error('[Backup Error]', error);
    throw error;
  }
}

/**
 * 2. 전체 복구 다운로드
 */
export async function restoreData(userKey: string) {
  try {
    const response = await fetch(`${BASE_URL}/api/restore/${encodeURIComponent(userKey)}`, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      },
    });

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.message || data.detail || '복구에 실패했습니다.');
    }
    return data;
  } catch (error: any) {
    console.error('[Restore Error]', error);
    throw error;
  }
}
```

---

## ⚠️ 안드로이드 HTTP 통신 설정 (Cleartext Traffic)
안드로이드 9(Pie) 이상에서는 `http://` 통신이 기본적으로 차단됩니다.  
모바일 앱 프로젝트의 `android/app/src/main/AndroidManifest.xml`에 다음 속성을 반드시 추가해 주세요:

```xml
<application
    ...
    android:usesCleartextTraffic="true">
    ...
</application>
```
