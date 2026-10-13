#!/usr/bin/env python3
"""fifo_server.py - сервер вычисления выражений на двух именованных каналах (FIFO).
Запросы читает из REQ, ответы пишет в RESP. Останавливается по Ctrl+C или команде --shutdown клиента."""
import os
import signal
import sys

from common import REQ, RESP, SHUTDOWN, answer


def cleanup():
    for path in (REQ, RESP):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass


def stop(signum, frame):
    raise SystemExit(0)      # выход через finally -> FIFO будут удалены


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    cleanup()                              # убираем остатки прошлого аварийного запуска
    os.mkfifo(REQ, 0o600)
    os.mkfifo(RESP, 0o600)
    print(f"[сервер] PID={os.getpid()}, FIFO созданы: {REQ}, {RESP}", flush=True)
    try:
        while True:
            # open на чтение БЛОКИРУЕТСЯ, пока какой-нибудь клиент не откроет FIFO на запись.
            # Это не зависание - сервер просто ждёт клиента.
            with open(REQ, "r", encoding="utf-8") as req:
                for line in req:           # цикл кончится по EOF, когда клиент закроет свой конец
                    line = line.strip()
                    if not line:
                        continue
                    if line == SHUTDOWN:
                        reply = "OK bye"
                    else:
                        reply = answer(line)
                    print(f"[сервер] запрос {line!r} -> {reply}", flush=True)
                    # open на запись блокируется, пока клиент не откроет RESP на чтение
                    with open(RESP, "w", encoding="utf-8") as resp:
                        resp.write(reply + "\n")
                    if line == SHUTDOWN:
                        return
    finally:
        cleanup()
        print("[сервер] FIFO удалены, выход", flush=True)


if __name__ == "__main__":
    sys.exit(main())
