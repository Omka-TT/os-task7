#!/usr/bin/env python3
"""shm_sync_demo.py - почему разделяемая память опасна: читатель без синхронизации видит недописанные данные.
Писатель заполняет сегмент по частям (20 кусков). Читатель в двух режимах:
  без синхронизации - читает в случайный момент;  с синхронизацией - ждёт Event 'запись закончена'."""
import multiprocessing as mp
import random
import sys
import time
from multiprocessing import shared_memory

from common import stop_resource_tracker

CHUNKS = 20
CHUNK_SIZE = 100_000
TRIALS = 10
ctx = mp.get_context("fork")


def writer(name, started, done):
    shm = shared_memory.SharedMemory(name=name)
    started.set()
    for i in range(CHUNKS):
        shm.buf[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE] = bytes([i + 1]) * CHUNK_SIZE   # кусок i заполнен значением i+1
        time.sleep(0.005)
    done.set()
    shm.close()


def reader(name, started, done, use_sync, res):
    shm = shared_memory.SharedMemory(name=name)
    started.wait()
    if use_sync:
        done.wait()                                   # синхронизация: ждём конец записи
    else:
        time.sleep(random.uniform(0, 0.12))           # без синхронизации: читаем "когда получится"
    snapshot = bytes(shm.buf[:CHUNKS * CHUNK_SIZE])
    shm.close()
    unfinished = sum(1 for i in range(CHUNKS)
                     if snapshot[i * CHUNK_SIZE:(i + 1) * CHUNK_SIZE] != bytes([i + 1]) * CHUNK_SIZE)
    res.send(unfinished)                              # сколько кусков ещё не были записаны


def trial(use_sync):
    shm = shared_memory.SharedMemory(create=True, size=CHUNKS * CHUNK_SIZE)
    try:
        started, done = ctx.Event(), ctx.Event()
        res_r, res_w = ctx.Pipe(duplex=False)
        w = ctx.Process(target=writer, args=(shm.name, started, done))
        r = ctx.Process(target=reader, args=(shm.name, started, done, use_sync, res_w))
        r.start()
        w.start()
        res_w.close()
        bad = res_r.recv()
        w.join()
        r.join()
        return bad
    finally:
        shm.close()
        shm.unlink()


def main():
    for use_sync in (False, True):
        bads = [trial(use_sync) for _ in range(TRIALS)]
        broken = sum(1 for b in bads if b)
        title = "С синхронизацией (Event)" if use_sync else "БЕЗ синхронизации"
        print(f"{title}: прогонов с неполными данными {broken} из {TRIALS}; "
              f"недописанных кусков по прогонам: {bads}")
    return 0


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        stop_resource_tracker()
    sys.exit(rc)
