#!/usr/bin/env bash
# Запуск tv-plst server picker.
# Использование: ./run.sh [start|stop|restart|status]
set -e
cd "$(dirname "$0")"
LOG="/var/log/tv-plst.log"
PIDF="/var/run/tv-plst.pid"
PY="python3"

case "${1:-start}" in
  start)
    if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then
      echo "Already running (pid $(cat "$PIDF"))"; exit 0
    fi
    nohup $PY server.py > "$LOG" 2>&1 &
    echo $! > "$PIDF"
    echo "Started pid $(cat "$PIDF"). Log: $LOG"
    ;;
  stop)
    if [ -f "$PIDF" ]; then
      kill "$(cat "$PIDF")" 2>/dev/null || true
      rm -f "$PIDF"
      echo "Stopped"
    else
      echo "Not running"
    fi
    ;;
  restart)
    "$0" stop || true
    "$0" start
    ;;
  status)
    if [ -f "$PIDF" ] && kill -0 "$(cat "$PIDF")" 2>/dev/null; then
      echo "Running (pid $(cat "$PIDF"))"
    else
      echo "Not running"
    fi
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status}" >&2
    exit 1
    ;;
esac
