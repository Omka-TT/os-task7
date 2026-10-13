#!/usr/bin/env python3
"""sock_client.py - клиент Unix-сокета. Примеры:
   python3 sock_client.py "2+3*4" "(10-4)**2"
   python3 sock_client.py                 - ввод с клавиатуры
   python3 sock_client.py --shutdown      - остановить сервер"""
import socket
import sys

from common import SHUTDOWN, SOCK_PATH, TIMEOUT


def connect():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(TIMEOUT)
    try:
        s.connect(SOCK_PATH)
    except (FileNotFoundError, ConnectionRefusedError):
        s.close()
        raise RuntimeError(f"не удалось подключиться к {SOCK_PATH}: сервер не запущен")
    return s


def ask_many(exprs):
    """Одно соединение, несколько запросов по очереди. Возвращает список ответов."""
    s = connect()
    replies = []
    with s, s.makefile("rw", encoding="utf-8", newline="\n") as f:
        for e in exprs:
            f.write(e + "\n")
            f.flush()
            replies.append(f.readline().strip())
    return replies


def main():
    args = sys.argv[1:]
    if args == ["--shutdown"]:
        args = [SHUTDOWN]
    elif not args:
        args = [l.strip() for l in sys.stdin if l.strip()]
    try:
        for e, r in zip(args, ask_many(args)):
            print(f"{e} = {r}")
    except (RuntimeError, OSError) as ex:
        print(f"ОШИБКА клиента: {ex}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
