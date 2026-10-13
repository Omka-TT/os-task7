#!/usr/bin/env python3
"""fifo_client.py - клиент. Примеры:
   python3 fifo_client.py "2+3*4" "(10-4)**2"   - отправить выражения по очереди
   python3 fifo_client.py                       - вводить выражения с клавиатуры (Ctrl+D - конец)
   python3 fifo_client.py --shutdown            - остановить сервер"""
import errno
import os
import signal
import sys
import time

from common import REQ, RESP, SHUTDOWN, TIMEOUT


def on_alarm(signum, frame):
    raise TimeoutError(f"нет ответа за {TIMEOUT} с")


def ask(expr):
    # ПОРЯДОК ВАЖЕН. Сначала открываем FIFO запросов на запись и отправляем запрос,
    # и только потом открываем FIFO ответов на чтение. Если открыть в другом порядке,
    # клиент и сервер будут ждать друг друга в open() вечно (взаимная блокировка).
    fd = None
    for _ in range(20):                    # O_NONBLOCK: если читателя нет, получим ENXIO сразу, а не зависание
        try:
            fd = os.open(REQ, os.O_WRONLY | os.O_NONBLOCK)
            break
        except FileNotFoundError:
            raise RuntimeError("FIFO не найден: сервер не запущен")
        except OSError as e:
            if e.errno != errno.ENXIO:
                raise
            time.sleep(0.05)               # сервер мог на миг отойти от open(); подождём
    if fd is None:
        raise RuntimeError("никто не читает FIFO запросов: сервер не запущен")
    try:
        os.write(fd, (expr + "\n").encode("utf-8"))
    finally:
        os.close(fd)                       # закрыли свой конец - сервер увидит EOF после строки
    signal.alarm(TIMEOUT)
    try:
        with open(RESP, "r", encoding="utf-8") as f:   # ждёт, пока сервер откроет RESP на запись
            return f.readline().strip()
    finally:
        signal.alarm(0)


def main():
    signal.signal(signal.SIGALRM, on_alarm)
    args = sys.argv[1:]
    if args == ["--shutdown"]:
        args = [SHUTDOWN]
    elif not args:
        args = [l.strip() for l in sys.stdin if l.strip()]
    rc = 0
    for expr in args:
        try:
            print(f"{expr} = {ask(expr)}", flush=True)
        except (RuntimeError, TimeoutError) as e:
            print(f"ОШИБКА клиента: {e}", file=sys.stderr)
            rc = 1
            break
    return rc


if __name__ == "__main__":
    sys.exit(main())
