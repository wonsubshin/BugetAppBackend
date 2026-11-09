#!/usr/bin/env bash
# ==============================================================================
# bugetBackend 백엔드 관리 스크립트 (백그라운드 구동 / 중지 / 상태 / 로그)
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 1. 디렉토리 및 환경 설정
VENV_DIR="$SCRIPT_DIR/.venv"
LOG_DIR="$SCRIPT_DIR/logs"
PID_FILE="$LOG_DIR/buget_backend.pid"
LOG_FILE="$LOG_DIR/buget_backend.log"

mkdir -p "$LOG_DIR"

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

SERVER_PORT="${SERVER_PORT:-8000}"
SERVER_HOST="${SERVER_HOST:-0.0.0.0}"

# 2. 가상환경 및 의존성 준비 함수
prepare_env() {
    if [ ! -d "$VENV_DIR" ]; then
        echo "⚙️  Python 가상환경(.venv) 생성 중..."
        python3 -m venv "$VENV_DIR"
    fi

    source "$VENV_DIR/bin/activate"

    if [ -f requirements.txt ]; then
        pip install --upgrade pip > /dev/null 2>&1
        pip install -r requirements.txt > /dev/null 2>&1
    fi
}

# 3. 실행 중 여부 확인 함수
is_running() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null || true)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
    fi

    # PID 파일 외에도 포트 점유 여부 확인
    if fuser -n tcp "$SERVER_PORT" >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# 4. 백그라운드 시작
start_server() {
    if is_running; then
        local existing_pid
        existing_pid=$(cat "$PID_FILE" 2>/dev/null || fuser -n tcp "$SERVER_PORT" 2>/dev/null || echo "알 수 없음")
        echo "⚠️  bugetBackend 서버가 이미 실행 중입니다 (PID: $existing_pid, Port: $SERVER_PORT)."
        echo "   - 상태 확인: ./run.sh status"
        echo "   - 로그 확인: tail -f logs/buget_backend.log"
        exit 0
    fi

    echo "=================================================="
    echo "🚀 bugetBackend 백그라운드 구동 준비..."
    echo "=================================================="
    prepare_env

    echo "📦 백그라운드 프로세스 시작 중 (Port: $SERVER_PORT)..."
    setsid "$VENV_DIR/bin/python" main.py </dev/null >> "$LOG_FILE" 2>&1 &
    local new_pid=$!
    echo "$new_pid" > "$PID_FILE"

    # 프로세스 안정화 및 헬스체크 대기
    sleep 2

    if kill -0 "$new_pid" 2>/dev/null; then
        echo "=================================================="
        echo "✅ bugetBackend 서버가 백그라운드에서 성공적으로 구동되었습니다!"
        echo "=================================================="
        echo "• 프로세스 PID : $new_pid"
        echo "• 바인딩 주소  : http://${SERVER_HOST}:${SERVER_PORT}"
        echo "• API 문서     : http://${SERVER_HOST}:${SERVER_PORT}/docs"
        echo "• 로그 파일    : $LOG_FILE"
        echo "--------------------------------------------------"
        echo "💡 유용한 명령어:"
        echo "  - 상태 확인 : ./run.sh status (또는 ./status.sh)"
        echo "  - 실시간 로그: ./run.sh logs   (또는 tail -f logs/buget_backend.log)"
        echo "  - 서버 종료 : ./run.sh stop   (또는 ./stop.sh)"
        echo "=================================================="
    else
        echo "❌ 서버 시작 실패. 최근 로그를 확인하세요:"
        echo "--------------------------------------------------"
        tail -n 20 "$LOG_FILE"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# 5. 포그라운드 시작 (디버깅용)
start_foreground() {
    echo "=================================================="
    echo "🚀 bugetBackend 포그라운드(직접 터미널) 구동..."
    echo "=================================================="
    prepare_env
    exec "$VENV_DIR/bin/python" main.py
}

# 6. 서버 중지
stop_server() {
    echo "=================================================="
    echo "🛑 bugetBackend 서버 종료 중..."
    echo "=================================================="

    local stopped=0
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null || true)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "• 프로세스 종료 신호 전송 (PID: $pid)..."
            kill "$pid" 2>/dev/null || true
            for _ in {1..10}; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    stopped=1
                    break
                fi
                sleep 0.5
            done
            if [ $stopped -eq 0 ]; then
                echo "• 강제 종료 (SIGKILL PID: $pid)..."
                kill -9 "$pid" 2>/dev/null || true
            fi
        fi
        rm -f "$PID_FILE"
    fi

    # 포트 기준 잔여 프로세스 방어 종료
    if fuser -n tcp "$SERVER_PORT" >/dev/null 2>&1; then
        echo "• 포트 $SERVER_PORT 점유 프로세스 정리..."
        fuser -k -n tcp "$SERVER_PORT" >/dev/null 2>&1 || true
    fi

    echo "✅ bugetBackend 서버가 안전하게 종료되었습니다."
    echo "=================================================="
}

# 7. 서버 상태 확인
status_server() {
    echo "=================================================="
    echo "🔍 bugetBackend 서버 상태 점검"
    echo "=================================================="

    if is_running; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null || true)
        if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
            pid=$(fuser -n tcp "$SERVER_PORT" 2>/dev/null | awk '{print $NF}' || echo "알 수 없음")
        fi
        echo "• 상태     : 🟢 실행 중 (RUNNING)"
        echo "• PID      : $pid"
        echo "• 포트     : $SERVER_PORT"
        echo "• 로그 파일: $LOG_FILE"

        if command -v curl >/dev/null 2>&1; then
            local health_url="http://127.0.0.1:${SERVER_PORT}/api/health"
            local res
            res=$(curl -s -m 2 "$health_url" 2>/dev/null || true)
            if [ -n "$res" ]; then
                echo "• 헬스체크 : $res"
            fi
        fi
    else
        echo "• 상태 : 🔴 중지됨 (STOPPED)"
    fi
    echo "=================================================="
}

# 8. 로그 확인
logs_server() {
    if [ -f "$LOG_FILE" ]; then
        echo "📜 실시간 로그 출력 (종료하려면 Ctrl+C):"
        tail -f -n 50 "$LOG_FILE"
    else
        echo "⚠️ 로그 파일($LOG_FILE)이 아직 생성되지 않았습니다."
    fi
}

# 명령어 라우팅
ACTION="${1:-start}"

case "$ACTION" in
    start)
        start_server
        ;;
    foreground|--foreground|-f)
        start_foreground
        ;;
    stop)
        stop_server
        ;;
    restart)
        stop_server
        sleep 1
        start_server
        ;;
    status)
        status_server
        ;;
    logs)
        logs_server
        ;;
    *)
        echo "사용법: $0 [start|stop|restart|status|logs|foreground]"
        exit 1
        ;;
esac
