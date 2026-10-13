#!/usr/bin/env python3
"""bench_ipc.py - передача массива из 10^7 чисел (float64, 80 МБ) от процесса к процессу
четырьмя способами: shared_memory, multiprocessing.Pipe, Unix-сокет, FIFO.
Время = от начала отправки до момента, когда получатель имеет ВСЕ данные (проверка crc32 в время не входит)."""
import array
import multiprocessing as mp
import os
import shutil
import socket
import statistics
import sys
import tempfile
import time
import zlib
from multiprocessing import shared_memory

from common import stop_resource_tracker

N = 10_000_000
NBYTES = N * 8
REPEATS = 5
CHUNK = 1 << 20
ctx = mp.get_context("fork")


# ---------- получатели (работают в дочернем процессе) ----------
def recv_pipe(r, w, res, up, crc):
    w.close()                                 # свой экземпляр конца записи закрываем: иначе EOF не придёт
    up.set()
    buf = r.recv_bytes()
    t1 = time.perf_counter()
    res.send((t1, len(buf) == NBYTES and zlib.crc32(buf) == crc))


def recv_sock(r, w, res, up, crc):
    w.close()
    buf = bytearray(NBYTES)
    mv = memoryview(buf)
    up.set()
    off = 0
    while off < NBYTES:
        n = r.recv_into(mv[off:])
        if n == 0:
            break
        off += n
    t1 = time.perf_counter()
    res.send((t1, off == NBYTES and zlib.crc32(buf) == crc))


def recv_fifo(path, res, up, crc):
    buf = bytearray(NBYTES)
    mv = memoryview(buf)
    up.set()
    fd = os.open(path, os.O_RDONLY)           # блокируется, пока отправитель не откроет FIFO на запись
    off = 0
    while off < NBYTES:
        n = os.readv(fd, [mv[off:]])
        if n == 0:
            break
        off += n
    t1 = time.perf_counter()
    os.close(fd)
    res.send((t1, off == NBYTES and zlib.crc32(buf) == crc))


def recv_shm(name, go, res, up, crc):
    up.set()
    go.wait()                                 # СИНХРОНИЗАЦИЯ: ждём, пока отправитель допишет данные
    shm = shared_memory.SharedMemory(name=name)
    view = shm.buf[:NBYTES]
    t1 = time.perf_counter()
    ok = zlib.crc32(view) == crc
    res.send((t1, ok))
    view.release()
    shm.close()                               # закрываем свой доступ; удаляет сегмент (unlink) только создатель


# ---------- отправители (родительский процесс) ----------
def run_child(target, args_builder):
    res_r, res_w = ctx.Pipe(duplex=False)
    up = ctx.Event()
    p = ctx.Process(target=target, args=args_builder(res_w, up))
    p.start()
    res_w.close()
    up.wait()
    return p, res_r


def finish(p, res_r, t0):
    if not res_r.poll(120):
        p.kill()
        raise RuntimeError("получатель не ответил")
    t1, ok = res_r.recv()
    p.join(10)
    return t1 - t0, ok


def bench_pipe(mv, crc):
    r, w = ctx.Pipe(duplex=False)
    p, res_r = run_child(recv_pipe, lambda rw, up: (r, w, rw, up, crc))
    r.close()                                 # родитель только пишет: свой конец чтения закрываем
    t0 = time.perf_counter()
    w.send_bytes(mv)
    w.close()
    return finish(p, res_r, t0)


def bench_sock(mv, crc):
    a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    p, res_r = run_child(recv_sock, lambda rw, up: (b, a, rw, up, crc))
    b.close()
    t0 = time.perf_counter()
    a.sendall(mv)
    a.close()
    return finish(p, res_r, t0)


def bench_fifo(mv, crc):
    d = tempfile.mkdtemp(prefix="lab7_bench_")
    path = os.path.join(d, "data.fifo")
    os.mkfifo(path)
    try:
        p, res_r = run_child(recv_fifo, lambda rw, up: (path, rw, up, crc))
        t0 = time.perf_counter()
        fd = os.open(path, os.O_WRONLY)       # здесь же блокируется, пока получатель не откроет на чтение
        off = 0
        while off < NBYTES:
            off += os.write(fd, mv[off:off + CHUNK])
        os.close(fd)
        return finish(p, res_r, t0)
    finally:
        shutil.rmtree(d, ignore_errors=True)  # FIFO и каталог удалены в любом случае


def bench_shm(mv, crc):
    shm = shared_memory.SharedMemory(create=True, size=NBYTES)
    go = ctx.Event()
    try:
        p, res_r = run_child(recv_shm, lambda rw, up: (shm.name, go, rw, up, crc))
        t0 = time.perf_counter()
        shm.buf[:NBYTES] = mv                 # копирование массива в разделяемый сегмент входит в время
        go.set()                              # "данные готовы"
        return finish(p, res_r, t0)
    finally:
        shm.close()
        shm.unlink()                          # удаляем сегмент из /dev/shm


def bench_shm_prefilled(mv, crc):
    """Данные УЖЕ лежат в сегменте (процесс создал их сразу там, без копирования).
    Передача сводится к "сегмент готов, вот его имя": от размера данных не зависит."""
    shm = shared_memory.SharedMemory(create=True, size=NBYTES)
    go = ctx.Event()
    try:
        shm.buf[:NBYTES] = mv                 # заполнение сегмента - ДО замера (это не передача)
        p, res_r = run_child(recv_shm, lambda rw, up: (shm.name, go, rw, up, crc))
        t0 = time.perf_counter()
        go.set()
        return finish(p, res_r, t0)
    finally:
        shm.close()
        shm.unlink()


# (название, функция, нужна ли синхронизация со стороны программиста, объём данных)
CASES = [
    ("shared_memory (с копированием в сегмент)", bench_shm,
     "ДА: нужен Event/семафор (когда данные готовы, кто сейчас пишет)",
     "ограничен /dev/shm и ОЗУ; здесь 80 МБ"),
    ("shared_memory (данные уже в сегменте)", bench_shm_prefilled,
     "ДА: нужен Event/семафор",
     "ограничен /dev/shm и ОЗУ; здесь 80 МБ"),
    ("Pipe (multiprocessing)", bench_pipe,
     "нет: ядро само блокирует читателя и писателя",
     "поток любой длины (буфер ядра 64 КБ); здесь 80 МБ"),
    ("Unix-сокет (socketpair)", bench_sock,
     "нет: блокирующие send/recv (но нужен протокол границ сообщений)",
     "поток любой длины; здесь 80 МБ"),
    ("FIFO (именованный канал)", bench_fifo,
     "нет: блокирующие read/write (но важен порядок open)",
     "поток любой длины (буфер ядра 64 КБ); здесь 80 МБ"),
]


def main():
    print(f"подготовка массива из {N:,} чисел float64 ({NBYTES / 1e6:.0f} МБ)...", flush=True)
    data = array.array("d", range(N))
    mv = memoryview(data).cast("B")
    crc = zlib.crc32(data)
    shm_before = sorted(os.listdir("/dev/shm"))
    results = {}
    all_ok = True
    for name, fn, _, _ in CASES:
        times = []
        for _ in range(REPEATS):
            dt, ok = fn(mv, crc)
            times.append(dt)
            all_ok = all_ok and ok
        results[name] = times
        print(f"{name}: " + ", ".join(f"{t * 1000:.0f}" for t in times) + " мс", flush=True)

    print(f"\nТаблица (время - медиана из {REPEATS} передач, в скобках минимум):\n")
    print("| механизм | нужна ли синхронизация со стороны программиста | объём данных | время передачи, мс |")
    print("|---|---|---|---|")
    for name, _, sync, volume in CASES:
        times = results[name]
        print(f"| {name} | {sync} | {volume} | {statistics.median(times) * 1000:.2f} ({min(times) * 1000:.2f}) |")
    leaked = sorted(set(os.listdir("/dev/shm")) - set(shm_before))
    print(f"\nданные получены без искажений (crc32 совпал) во всех {REPEATS * len(CASES)} передачах: {all_ok}")
    print(f"остатки в /dev/shm после работы: {leaked if leaked else 'нет'}")
    return 0 if all_ok and not leaked else 1


if __name__ == "__main__":
    try:
        rc = main()
    finally:
        stop_resource_tracker()
    sys.exit(rc)
