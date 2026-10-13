#!/usr/bin/env python3
"""pipe_demo.py - две типичные ошибки с pipe и ответ на контрольный вопрос 2.
Часть A: запись в pipe, у которого закрыты все читающие концы.
Часть B: в родителе не закрыт конец записи - читатель никогда не получает EOF."""
import os
import select
import signal
import sys
import time


def part_a():
    print("--- A. запись в pipe без читателей ---")
    r, w = os.pipe()
    os.close(r)                                        # закрыли ВСЕ читающие концы
    try:
        os.write(w, b"x")                              # в Python SIGPIPE по умолчанию игнорируется
    except BrokenPipeError as e:
        print(f"Python: исключение BrokenPipeError (errno EPIPE): {e}")
    os.close(w)

    r, w = os.pipe()
    os.close(r)                                        # читающих концов не осталось нигде (закрыли ДО fork)
    pid = os.fork()
    if pid == 0:
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)  # поведение "как в C": сигнал не игнорируется
        os.write(w, b"x")                              # читателей нет ни у кого -> ядро шлёт SIGPIPE
        os._exit(0)
    os.close(w)
    _, status = os.waitpid(pid, 0)
    if os.WIFSIGNALED(status):
        print(f"процесс с SIG_DFL убит сигналом {signal.Signals(os.WTERMSIG(status)).name}")


def part_b():
    print("--- B. забыли закрыть конец записи в родителе ---")
    r, w = os.pipe()
    sys.stdout.flush()
    pid = os.fork()
    if pid == 0:
        os.close(w)                                    # ребёнок свой конец записи закрыл
        data = b""
        while True:
            ready, _, _ = select.select([r], [], [], 1.0)
            if not ready:
                print("ребёнок: за 1 с нет ни данных, ни EOF - родитель держит конец записи открытым", flush=True)
                continue
            chunk = os.read(r, 100)
            if chunk == b"":
                print(f"ребёнок: получил EOF, прочитано {data!r}", flush=True)
                os._exit(0)
            data += chunk
    os.close(r)
    os.write(w, b"hello")
    time.sleep(1.6)                                    # ребёнок уже дважды увидел "тишину"
    print("родитель: закрываю конец записи (os.close)", flush=True)
    os.close(w)                                        # теперь записывающих концов не осталось -> EOF
    os.waitpid(pid, 0)


if __name__ == "__main__":
    part_a()
    part_b()
