#!/usr/bin/env python3
"""sock_multi_client.py N - запускает N клиентов, каждый в своём ПРОЦЕССЕ, одновременно.
Каждый клиент проверяет, что ответы верны. Код выхода 0 - все клиенты получили правильные ответы."""
import multiprocessing as mp
import os
import sys
import time

from common import answer
from sock_client import connect


def client(i, barrier):
    exprs = [f"{i}+{i}*2", f"({i}+1)**2", f"100/{i + 1}"]
    s = connect()
    barrier.wait()                       # все N процессов отправляют запросы в один и тот же момент
    ok = True
    with s, s.makefile("rw", encoding="utf-8", newline="\n") as f:
        for e in exprs:
            f.write(e + "\n")
            f.flush()
            got = f.readline().strip()
            want = answer(e)             # эталон считаем локально и сверяем с ответом сервера
            if got != want:
                ok = False
            print(f"[клиент {i}, PID {os.getpid()}] {e} = {got}" + ("" if got == want else f"  ОЖИДАЛОСЬ {want}"),
                  flush=True)
    os._exit(0 if ok else 1)


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    ctx = mp.get_context("fork")
    barrier = ctx.Barrier(n)
    procs = [ctx.Process(target=client, args=(i + 1, barrier)) for i in range(n)]
    t0 = time.monotonic()
    for p in procs:
        p.start()
    for p in procs:
        p.join(30)
    dt = time.monotonic() - t0
    bad = [p.pid for p in procs if p.exitcode != 0]
    print(f"клиентов: {n}, общее время: {dt:.2f} c, с ошибками: {len(bad)}")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
