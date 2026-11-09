#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
bugetBackend: React Native 모바일 앱용 PostgreSQL 미들웨어 백엔드 서버
=============================================================================
- 모바일(React Native Android)의 직접 JDBC 연결 호환성 문제를 우회하고,
  데이터베이스 트랜잭션을 전담하는 고성능 FastAPI 미들웨어입니다.
- 클라이언트 JSON 페이로드 키 매핑 지원:
  1. transactions        -> transactions
  2. recurringRules      -> recurring_rules
  3. customHolidays      -> holidays
  4. rawAppNotifications -> app_notifications_raw
  5. parseExceptions     -> parse_exceptions
  6. manualInstallments  -> manual_installments
  7. user                -> user_settings (동기화 시간 synced_at / sync_time 컬럼 관리)
- 단일 Transaction(BEGIN ~ COMMIT/ROLLBACK) 보장 및 CORS 전체 허용.
=============================================================================
"""

import json
import logging
import os
import sys
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor, execute_batch
from fastapi import FastAPI, HTTPException, Request, Response, status, Path as FastPath, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# 로깅 설정
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("buget-backend")

# ---------------------------------------------------------------------------
# 데이터베이스 및 서버 환경 설정 (환경 변수 또는 기본값)
# ---------------------------------------------------------------------------
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DATABASE = os.getenv("PG_DATABASE", "buget")
PG_USER = os.getenv("PG_USER", "buget")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")

SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))

# ---------------------------------------------------------------------------
# 백업 및 복구 대상 테이블 & 클라이언트 JSON 키 매핑 정의
# ---------------------------------------------------------------------------
TABLE_SPECS = [
    {
        "table": "transactions",
        "client_key": "transactions",
        "aliases": ["transactions"],
        "id_fields": ["id", "_id", "key", "uuid"],
        "prefix": "tx",
        "description": "거래 내역",
    },
    {
        "table": "recurring_rules",
        "client_key": "recurringRules",
        "aliases": ["recurringRules", "recurring_rules"],
        "id_fields": ["id", "ruleId", "rule_id", "_id", "key", "uuid", "name"],
        "prefix": "rule",
        "description": "반복 규칙",
    },
    {
        "table": "holidays",
        "client_key": "customHolidays",
        "aliases": ["customHolidays", "holidays"],
        "id_fields": ["id", "date", "holidayDate", "holiday_date", "_id", "key", "name"],
        "prefix": "holiday",
        "description": "사용자 정의 공휴일",
    },
    {
        "table": "app_notifications_raw",
        "client_key": "rawAppNotifications",
        "aliases": ["rawAppNotifications", "app_notifications_raw"],
        "id_fields": ["id", "notificationId", "sbnId", "key", "postTime", "timestamp"],
        "prefix": "noti",
        "description": "앱 원본 알림 데이터",
    },
    {
        "table": "parse_exceptions",
        "client_key": "parseExceptions",
        "aliases": ["parseExceptions", "parse_exceptions"],
        "id_fields": ["id", "exceptionId", "exception_id", "_id", "key", "pattern", "keyword"],
        "prefix": "exc",
        "description": "파싱 예외 규칙",
    },
    {
        "table": "manual_installments",
        "client_key": "manualInstallments",
        "aliases": ["manualInstallments", "manual_installments"],
        "id_fields": ["id", "installmentId", "installment_id", "transactionId", "txId", "_id", "key"],
        "prefix": "inst",
        "description": "수동 할부 설정",
    },
    {
        "table": "user_settings",
        "client_key": "user",
        "aliases": ["user", "user_settings"],
        "id_fields": ["id", "userKey", "user_key", "userId", "user_id", "key"],
        "prefix": "user",
        "description": "사용자 정보 및 설정 (동기화 시간 관리)",
    },
]

# 대상 테이블 7개 목록
TARGET_TABLES = [spec["table"] for spec in TABLE_SPECS]

# ---------------------------------------------------------------------------
# 커넥션 풀 관리
# ---------------------------------------------------------------------------
db_pool: Optional[pool.ThreadedConnectionPool] = None


def init_db_pool():
    """PostgreSQL 커넥션 풀 생성"""
    global db_pool
    try:
        logger.info(
            f"PostgreSQL 커넥션 풀 초기화 중... ({PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DATABASE})"
        )
        db_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=PG_HOST,
            port=PG_PORT,
            dbname=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            connect_timeout=10,
        )
        logger.info("PostgreSQL 커넥션 풀 초기화 성공")
    except Exception as e:
        logger.error(f"PostgreSQL 커넥션 풀 생성 실패: {e}")
        db_pool = None


def close_db_pool():
    """PostgreSQL 커넥션 풀 종료"""
    global db_pool
    if db_pool:
        db_pool.closeall()
        logger.info("PostgreSQL 커넥션 풀이 안전하게 종료되었습니다.")


@contextmanager
def get_db_connection():
    """
    DB 커넥션을 풀에서 가져오고 안전하게 반환하는 Context Manager.
    풀이 없거나 실패할 경우 단일 커넥션 생성을 시도합니다.
    """
    global db_pool
    conn = None
    from_pool = False
    try:
        if db_pool:
            try:
                conn = db_pool.getconn()
                from_pool = True
            except Exception as pool_err:
                logger.warning(f"풀에서 커넥션 획득 실패, 단일 커넥션으로 폴백 시도: {pool_err}")
                conn = None

        if conn is None:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DATABASE,
                user=PG_USER,
                password=PG_PASSWORD,
                connect_timeout=10,
            )
            from_pool = False

        yield conn
    finally:
        if conn:
            if from_pool and db_pool:
                try:
                    db_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"커넥션 풀 반환 실패: {e}")
            else:
                try:
                    conn.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# 테이블 자동 생성 및 스키마 검증 (CREATE TABLE IF NOT EXISTS & ALTER TABLE)
# ---------------------------------------------------------------------------
def ensure_tables_exist():
    """
    모바일 앱에서 전송하는 7개 테이블을 데이터베이스에 자동 준비합니다.
    모든 테이블은 (user_key, id)를 복합 기본키(Composite Primary Key)로 가지며,
    동기화 시간(synced_at) 컬럼을 보장합니다.
    """
    logger.info("데이터베이스 스키마 테이블 점검 및 생성 시작...")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for table_name in TARGET_TABLES:
                # 0. 기존 테이블 존재 여부 및 data 컬럼 존재 여부 확인
                cur.execute(
                    """
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = 'data';
                    """,
                    (table_name,),
                )
                has_data_col = cur.fetchone() is not None

                cur.execute(
                    """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s;
                    """,
                    (table_name,),
                )
                table_exists = cur.fetchone() is not None

                # 기존 테이블이 존재하지만 data 컬럼이 없는 구버전 스키마인 경우
                if table_exists and not has_data_col:
                    cur.execute(f"SELECT count(*) FROM {table_name};")
                    cnt = cur.fetchone()[0]
                    if cnt == 0:
                        logger.warning(
                            f"테이블 '{table_name}'에 'data' 컬럼이 없고 데이터가 비어 있어(0건) 최신 스키마로 재생성합니다."
                        )
                        cur.execute(f"DROP TABLE {table_name} CASCADE;")
                    else:
                        logger.warning(
                            f"테이블 '{table_name}'에 'data' 컬럼이 누락되어 있어 data 컬럼을 추가합니다."
                        )
                        cur.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS data JSONB DEFAULT '{{}}'::jsonb NOT NULL;"
                        )
                        cur.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS id VARCHAR(255);"
                        )
                        cur.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
                        )
                        cur.execute(
                            f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
                        )

                # 1. 기본 테이블 생성
                # - user_key: 사용자 고유 식별자 (복합 PK 1)
                # - id: 레코드 고유 식별자 (복합 PK 2)
                # - data: 원본 JSON 필드 전체를 보존하는 JSONB
                # - synced_at: DB 동기화 시간 컬럼
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    user_key VARCHAR(128) NOT NULL,
                    id VARCHAR(255) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_key, id)
                );
                CREATE INDEX IF NOT EXISTS idx_{table_name}_user_key ON {table_name} (user_key);
                """
                cur.execute(create_table_sql)

                # 2. 기존 테이블에 synced_at 컬럼 추가 보장
                cur.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS synced_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
                )

                # 3. 사용자 테이블(user_settings)에 sync_time 컬럼 추가 및 기존 데이터 갱신
                if table_name == "user_settings":
                    cur.execute(
                        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS sync_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;"
                    )
                    cur.execute(
                        f"""
                        UPDATE {table_name} 
                        SET synced_at = COALESCE(updated_at, CURRENT_TIMESTAMP), 
                            sync_time = COALESCE(updated_at, CURRENT_TIMESTAMP) 
                        WHERE synced_at IS NULL OR sync_time IS NULL;
                        """
                    )

            conn.commit()
    logger.info(f"7개 대상 테이블 점검 완료: {', '.join(TARGET_TABLES)} (동기화 시간 컬럼 적용)")


# ---------------------------------------------------------------------------
# FastAPI 애플리케이션 수명주기(Lifespan)
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # [STARTUP]
    logger.info("==================================================")
    logger.info("🚀 bugetBackend API 서버 구동 시작")
    logger.info("==================================================")
    init_db_pool()
    try:
        ensure_tables_exist()
    except Exception as e:
        logger.error(f"초기 테이블 생성 중 경고/오류 발생 (DB 연결 확인 필요): {e}")

    yield

    # [SHUTDOWN]
    logger.info("==================================================")
    logger.info("🛑 bugetBackend API 서버 종료 중...")
    logger.info("==================================================")
    close_db_pool()


app = FastAPI(
    title="Buget App Database Middleware API",
    description="React Native 모바일 가계부 앱의 PostgreSQL 트랜잭션 전담 미들웨어 백엔드",
    version="1.2.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS 설정 (모바일 앱 및 웹뷰 전체 허용)
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# 글로벌 예외 처리기 (모바일 앱 팝업에서 식별 가능한 JSON 에러 반환)
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"처리되지 않은 서버 예외 발생: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "success": False,
            "error": exc.__class__.__name__,
            "message": "서버 내부 처리 중 오류가 발생했습니다.",
            "detail": str(exc),
            "path": request.url.path,
        },
    )


# ---------------------------------------------------------------------------
# 헬스체크 및 루트 엔드포인트
# ---------------------------------------------------------------------------
@app.get("/", tags=["System"])
def root():
    return {
        "service": "buget-backend",
        "status": "online",
        "description": "PostgreSQL Middleware API for Mobile React Native App",
        "endpoints": {
            "backup": "POST /api/backup/{user_key}",
            "restore": "GET /api/restore/{user_key}",
            "health": "GET /api/health",
        },
        "supported_mappings": {
            "transactions": "transactions",
            "recurringRules": "recurring_rules",
            "customHolidays": "holidays",
            "rawAppNotifications": "app_notifications_raw",
            "parseExceptions": "parse_exceptions",
            "manualInstallments": "manual_installments",
            "user": "user_settings (synced_at/sync_time recorded)",
        },
    }


@app.get("/api/health", tags=["System"])
def health_check():
    """데이터베이스 연결 상태 확인"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
        return {
            "success": True,
            "status": "healthy",
            "database": {
                "host": PG_HOST,
                "port": PG_PORT,
                "name": PG_DATABASE,
                "user": PG_USER,
                "connected": True,
            },
        }
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={
                "success": False,
                "status": "unhealthy",
                "database": {
                    "host": PG_HOST,
                    "port": PG_PORT,
                    "name": PG_DATABASE,
                    "user": PG_USER,
                    "connected": False,
                },
                "error": str(e),
            },
        )


# ---------------------------------------------------------------------------
# [1] 백업(업로드) API
# ---------------------------------------------------------------------------
class BackupResponse(BaseModel):
    success: bool = True
    message: str = "백업 성공"
    details: Optional[Dict[str, int]] = None
    synced_at: Optional[str] = None


def _execute_backup_transaction(clean_user_key: str, payload: Dict[str, Any]) -> Tuple[Dict[str, int], str]:
    """
    수신된 payload를 바탕으로 clean_user_key에 대한 7개 테이블 백업 트랜잭션을 실행합니다.
    사용자 테이블(user_settings) 및 대상 테이블에 DB 동기화 시간(synced_at / sync_time)을 명시적으로 기록합니다.
    """
    inserted_counts: Dict[str, int] = {}
    now_dt = datetime.now(timezone.utc)
    now_iso = now_dt.isoformat()

    with get_db_connection() as conn:
        with conn:
            with conn.cursor() as cur:
                # 1. 7개 대상 테이블 기존 데이터 일괄 삭제
                for spec in TABLE_SPECS:
                    table = spec["table"]
                    cur.execute(
                        f"DELETE FROM {table} WHERE user_key = %s;",
                        (clean_user_key,),
                    )

                # 2. 클라이언트 JSON 키 매핑을 순회하며 빠짐없이 INSERT
                for spec in TABLE_SPECS:
                    table = spec["table"]
                    client_key = spec["client_key"]

                    # 페이로드에서 데이터 추출 (기본 키 및 별칭 지원)
                    raw_data = None
                    for candidate in spec["aliases"]:
                        if candidate in payload and payload[candidate] is not None:
                            raw_data = payload[candidate]
                            break

                    if raw_data is None:
                        inserted_counts[client_key] = 0
                        inserted_counts[table] = 0
                        continue

                    # A. user_settings 가 단일 dict로 전송된 경우 특수 처리 (동기화 시간 주입)
                    if table == "user_settings" and isinstance(raw_data, dict):
                        user_id = str(
                            raw_data.get("id")
                            or raw_data.get("userKey")
                            or raw_data.get("user_key")
                            or "__user__"
                        )
                        # 원본 data JSON에도 동기화 시간 기록
                        raw_data["synced_at"] = now_iso
                        raw_data["syncedAt"] = now_iso
                        raw_data["sync_time"] = now_iso
                        user_json = json.dumps(raw_data, ensure_ascii=False, default=str)
                        cur.execute(
                            f"""
                            INSERT INTO {table} (user_key, id, data, updated_at, synced_at, sync_time)
                            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (user_key, id)
                            DO UPDATE SET 
                                data = EXCLUDED.data, 
                                updated_at = CURRENT_TIMESTAMP,
                                synced_at = CURRENT_TIMESTAMP,
                                sync_time = CURRENT_TIMESTAMP;
                            """,
                            (clean_user_key, user_id, user_json),
                        )
                        inserted_counts[client_key] = 1
                        inserted_counts[table] = 1
                        continue

                    # B. 배열 또는 기타 데이터 정규화
                    if isinstance(raw_data, list):
                        items_list = raw_data
                    elif isinstance(raw_data, dict):
                        items_list = [raw_data]
                    else:
                        items_list = [raw_data]

                    rows_to_insert = []
                    seen_ids = set()

                    for idx, item in enumerate(items_list):
                        # user_settings 리스트인 경우 동기화 시간 반영
                        if table == "user_settings" and isinstance(item, dict):
                            item["synced_at"] = now_iso
                            item["syncedAt"] = now_iso
                            item["sync_time"] = now_iso

                        # ID 추출
                        candidate_id = None
                        if isinstance(item, dict):
                            for field in spec["id_fields"]:
                                val = item.get(field)
                                if val is not None and str(val).strip():
                                    candidate_id = str(val).strip()
                                    break
                        elif isinstance(item, (str, int)) and str(item).strip():
                            candidate_id = str(item).strip()

                        if not candidate_id:
                            candidate_id = f"{spec['prefix']}_{idx}"

                        # DB id 컬럼 길이(VARCHAR(255)) 초과 방지
                        if len(candidate_id) > 240:
                            candidate_id = candidate_id[:240]

                        # 동일 배치 내 ID 중복 충돌 방지
                        item_id = candidate_id
                        dup_seq = 1
                        while item_id in seen_ids:
                            item_id = f"{candidate_id}_{dup_seq}"
                            dup_seq += 1
                        seen_ids.add(item_id)

                        # JSON 직렬화 (빠짐없이 보존)
                        if isinstance(item, (dict, list)):
                            item_json = json.dumps(item, ensure_ascii=False, default=str)
                        else:
                            if table == "holidays":
                                item_json = json.dumps(
                                    {"date": str(item), "id": item_id},
                                    ensure_ascii=False,
                                )
                            else:
                                item_json = json.dumps(
                                    {"value": item, "id": item_id},
                                    ensure_ascii=False,
                                )

                        rows_to_insert.append((clean_user_key, item_id, item_json))

                    if rows_to_insert:
                        if table == "user_settings":
                            insert_sql = f"""
                            INSERT INTO {table} (user_key, id, data, updated_at, synced_at, sync_time)
                            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (user_key, id)
                            DO UPDATE SET 
                                data = EXCLUDED.data, 
                                updated_at = CURRENT_TIMESTAMP,
                                synced_at = CURRENT_TIMESTAMP,
                                sync_time = CURRENT_TIMESTAMP;
                            """
                        else:
                            insert_sql = f"""
                            INSERT INTO {table} (user_key, id, data, updated_at, synced_at)
                            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                            ON CONFLICT (user_key, id)
                            DO UPDATE SET 
                                data = EXCLUDED.data, 
                                updated_at = CURRENT_TIMESTAMP,
                                synced_at = CURRENT_TIMESTAMP;
                            """
                        execute_batch(cur, insert_sql, rows_to_insert, page_size=500)
                        inserted_counts[client_key] = len(rows_to_insert)
                        inserted_counts[table] = len(rows_to_insert)
                    else:
                        inserted_counts[client_key] = 0
                        inserted_counts[table] = 0

                # 만약 user가 페이로드에 포함되지 않았더라도 기존 user_settings가 남아있거나 단독 생성 가능한 경우 동기화 시간 갱신
                if inserted_counts.get("user", 0) == 0:
                    cur.execute(
                        f"""
                        INSERT INTO user_settings (user_key, id, data, updated_at, synced_at, sync_time)
                        VALUES (%s, '__user__', %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (user_key, id)
                        DO UPDATE SET 
                            synced_at = CURRENT_TIMESTAMP, 
                            sync_time = CURRENT_TIMESTAMP,
                            updated_at = CURRENT_TIMESTAMP,
                            data = jsonb_set(
                                jsonb_set(user_settings.data, '{{synced_at}}', to_jsonb(%s::text), true),
                                '{{syncedAt}}', to_jsonb(%s::text), true
                            );
                        """,
                        (clean_user_key, json.dumps({"userKey": clean_user_key, "synced_at": now_iso, "syncedAt": now_iso}), now_iso, now_iso),
                    )

    return inserted_counts, now_iso


@app.post(
    "/api/backup/{user_key}",
    response_model=BackupResponse,
    summary="사용자 데이터 전체 백업",
    tags=["Backup & Restore"],
)
async def backup_user_data(user_key: str, payload: Dict[str, Any]):
    """
    클라이언트가 전송한 전체 백업 데이터를 단일 트랜잭션으로 저장합니다.

    - **지원하는 클라이언트 JSON 키 매핑**:
      1. `transactions`        -> `transactions` 테이블
      2. `recurringRules`      -> `recurring_rules` 테이블
      3. `customHolidays`      -> `holidays` 테이블
      4. `rawAppNotifications` -> `app_notifications_raw` 테이블
      5. `parseExceptions`     -> `parse_exceptions` 테이블
      6. `manualInstallments`  -> `manual_installments` 테이블
      7. `user`                -> `user_settings` 테이블 (동기화 시간 synced_at, sync_time 자동 기록)

    - **동작 방식**:
      1. 해당 `user_key`에 속한 대상 7개 테이블의 기존 데이터를 일괄 DELETE.
      2. 수신된 JSON Payload 내부의 각 배열/객체를 순회하여 PostgreSQL 테이블에 빠짐없이 INSERT.
      3. 사용자 테이블(user_settings)의 `synced_at` / `sync_time` 컬럼 및 data JSON 내부에 동기화 시각 기록.
      4. 전체 과정은 단일 Transaction(BEGIN ~ COMMIT)으로 보장되며 오류 시 완전 ROLLBACK.
    """
    if not user_key or not user_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="유효한 user_key가 필요합니다.",
        )

    clean_user_key = user_key.strip()

    try:
        inserted_counts, sync_time_iso = _execute_backup_transaction(clean_user_key, payload)

        logger.info(
            f"백업 완료 - user_key: {clean_user_key}, 동기화시간: {sync_time_iso}, 통계: {inserted_counts}"
        )
        return BackupResponse(
            success=True,
            message="백업 성공",
            details=inserted_counts,
            synced_at=sync_time_iso,
        )

    except Exception as exc:
        logger.error(f"백업 트랜잭션 오류 (user_key={clean_user_key}): {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "error": "BackupTransactionError",
                "message": "데이터베이스 백업 처리 중 오류가 발생하여 모든 변경사항이 안전하게 롤백되었습니다.",
                "detail": str(exc),
            },
        )


@app.post(
    "/api/backup",
    response_model=BackupResponse,
    summary="사용자 데이터 전체 백업 (Query 또는 Body userKey 지원)",
    tags=["Backup & Restore"],
    include_in_schema=False,
)
async def backup_user_data_alt(
    payload: Dict[str, Any],
    user_key: Optional[str] = Query(None, alias="user_key"),
    userKey: Optional[str] = Query(None, alias="userKey"),
):
    key = (
        user_key
        or userKey
        or payload.get("userKey")
        or payload.get("user_key")
        or (payload.get("user", {}).get("key") if isinstance(payload.get("user"), dict) else None)
        or (payload.get("user", {}).get("userKey") if isinstance(payload.get("user"), dict) else None)
    )
    if not key or not str(key).strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_key 또는 userKey가 경로, 쿼리 또는 요청 본문에 필요합니다.",
        )
    return await backup_user_data(user_key=str(key), payload=payload)


# ---------------------------------------------------------------------------
# [2] 복구(다운로드) API
# ---------------------------------------------------------------------------
def _execute_restore_query(clean_user_key: str) -> Dict[str, Any]:
    """
    clean_user_key에 대한 7개 테이블 데이터를 조회하여 클라이언트 키 및 테이블명에 모두 매핑합니다.
    사용자 테이블의 동기화 시간(synced_at) 정보도 누락 없이 포함하여 반환합니다.
    """
    result_tree: Dict[str, Any] = {}
    latest_synced_at = None

    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for spec in TABLE_SPECS:
                table = spec["table"]
                client_key = spec["client_key"]

                select_sql = f"""
                SELECT id, data, synced_at, updated_at 
                FROM {table} 
                WHERE user_key = %s 
                ORDER BY created_at ASC, id ASC;
                """
                cur.execute(select_sql, (clean_user_key,))
                rows = cur.fetchall()

                # user_settings는 단일 dict 객체 또는 복합 데이터 복원
                if table == "user_settings":
                    if len(rows) == 1:
                        user_data = rows[0]["data"]
                        sync_dt = rows[0].get("synced_at") or rows[0].get("updated_at")
                        if sync_dt:
                            sync_str = sync_dt.isoformat() if hasattr(sync_dt, "isoformat") else str(sync_dt)
                            latest_synced_at = sync_str
                            if isinstance(user_data, dict):
                                user_data["synced_at"] = sync_str
                                user_data["syncedAt"] = sync_str
                                user_data["sync_time"] = sync_str
                    elif len(rows) > 1:
                        user_data = [r["data"] for r in rows]
                        sync_dt = rows[0].get("synced_at") or rows[0].get("updated_at")
                        if sync_dt:
                            latest_synced_at = sync_dt.isoformat() if hasattr(sync_dt, "isoformat") else str(sync_dt)
                    else:
                        user_data = {}
                    result_tree[client_key] = user_data
                    result_tree[table] = user_data
                else:
                    table_data = [r["data"] for r in rows]
                    result_tree[client_key] = table_data
                    result_tree[table] = table_data

    # 최상위 루트 트리에도 동기화 시간 정보 추가
    if latest_synced_at:
        result_tree["synced_at"] = latest_synced_at
        result_tree["syncedAt"] = latest_synced_at
        result_tree["sync_time"] = latest_synced_at

    return result_tree


@app.get(
    "/api/restore/{user_key}",
    summary="사용자 데이터 전체 복구",
    tags=["Backup & Restore"],
)
async def restore_user_data(user_key: str):
    """
    클라이언트가 요청한 `user_key`의 데이터를 7개 테이블에서 조회하여
    모바일 앱이 요구하는 클라이언트 JSON 키(`recurringRules`, `customHolidays`, ...)와
    원본 DB 테이블명 둘 다 호환되도록 반환합니다.
    사용자 테이블의 동기화 시간 정보(synced_at)도 함께 제공됩니다.
    """
    if not user_key or not user_key.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="유효한 user_key가 필요합니다.",
        )

    clean_user_key = user_key.strip()

    try:
        result_tree = _execute_restore_query(clean_user_key)
        logger.info(
            f"복구 완료 - user_key: {clean_user_key}, 추출된 항목 키: {list(result_tree.keys())}"
        )
        return JSONResponse(status_code=status.HTTP_200_OK, content=result_tree)

    except Exception as exc:
        logger.error(f"복구 조회 오류 (user_key={clean_user_key}): {exc}", exc_info=True)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "success": False,
                "error": "RestoreQueryError",
                "message": "데이터 복구 조회 중 오류가 발생했습니다.",
                "detail": str(exc),
            },
        )


@app.get(
    "/api/restore",
    summary="사용자 데이터 전체 복구 (Query userKey 지원)",
    tags=["Backup & Restore"],
    include_in_schema=False,
)
async def restore_user_data_alt(
    user_key: Optional[str] = Query(None, alias="user_key"),
    userKey: Optional[str] = Query(None, alias="userKey"),
):
    key = user_key or userKey
    if not key or not str(key).strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_key 또는 userKey 쿼리 파라미터가 필요합니다.",
        )
    return await restore_user_data(user_key=str(key))


# ---------------------------------------------------------------------------
# 직접 실행 진입점 (python main.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    logger.info(f"Uvicorn 서버 시작: http://{SERVER_HOST}:{SERVER_PORT}")
    uvicorn.run(
        "main:app",
        host=SERVER_HOST,
        port=SERVER_PORT,
        reload=False,
        access_log=True,
    )
