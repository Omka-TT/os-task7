#!/usr/bin/env python3
"""sock_server.py - сервер на Unix-сокете (AF_UNIX), по потоку на каждого клиента.
Запуск: python3 sock_server.py [--delay 0.1]   (delay - имитация работы над каждым запросом)"""
import argparse
import os
import signal
import socket
import sys
import threading
import time

from common import SHUTDOWN, SOCK_PATH, answer


def handle(conn, cid, delay, stop, t0):
    name = threading.current_thread().name
    try:
        with conn, conn.makefile("rw", encoding="utf-8", newline="\n") as f:
            for line in f:                         # читаем запросы клиента, пока он не закроет соединение
                line = line.strip()
                if not line:
                    continue
                if line == SHUTDOWN:
                    stop.set()
                    reply = "OK bye"
                else:
                    time.sleep(delay)              # "работа"; другие клиенты в это время обслуживаются параллельно
                    reply = answer(line)
                print(f"[сервер +{time.monotonic() - t0:5.2f} с] клиент #{cid} ({name}): {line!r} -> {reply}",
                      flush=True)
                f.write(reply + "\n")
                f.flush()
                if line == SHUTDOWN:
                    break
    except (BrokenPipeError, ConnectionResetError) as e:
        print(f"[сервер] клиент #{cid} оборвал соединение: {e}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=0.1)
    args = ap.parse_args()

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda s, f: stop.set())
    signal.signal(signal.SIGTERM, lambda s, f: stop.set())

    if os.path.exists(SOCK_PATH):
        os.unlink(SOCK_PATH)                       # остаток прошлого запуска, иначе bind даст "Address already in use"
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    threads = []
    t0 = time.monotonic()
    try:
        srv.bind(SOCK_PATH)
        os.chmod(SOCK_PATH, 0o600)
        srv.listen(16)
        srv.settimeout(0.2)                        # чтобы раз в 0.2 с проверять флаг остановки
        print(f"[сервер] PID={os.getpid()}, слушаю {SOCK_PATH}", flush=True)
        cid = 0
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
            except socket.timeout:
                continue
            cid += 1
            t = threading.Thread(target=handle, args=(conn, cid, args.delay, stop, t0), daemon=True)
            t.start()
            threads.append(t)
    finally:
        srv.close()
        try:
            os.unlink(SOCK_PATH)
        except FileNotFoundError:
            pass
        for t in threads:
            t.join(2)
        print("[сервер] сокет удалён, выход", flush=True)


if __name__ == "__main__":
    sys.exit(main())
