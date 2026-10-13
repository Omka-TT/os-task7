#!/usr/bin/env bash
# run_all.sh - автоматический прогон всех программ и проверка, что после них ничего не осталось.
cd "$(dirname "$0")" || exit 1
PY=${PY:-python3}
fail=0
SRV=

cleanup_on_exit() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; }
trap cleanup_on_exit EXIT

echo "=== состояние ДО ==="
echo "/dev/shm:       $(ls /dev/shm | tr '\n' ' ')"
echo "FIFO/сокеты:    $(ls /tmp/lab7* 2>/dev/null | tr '\n' ' ')"

echo; echo "=== 1. FIFO: сервер + 3 клиента по очереди ==="
$PY fifo_server.py & SRV=$!
sleep 0.5
$PY fifo_client.py "2+3*4" "(10-4)**2" "7/0" || fail=1
$PY fifo_client.py "100/8" "2**10" || fail=1
$PY fifo_client.py "-5+abs(3)" || fail=1
$PY fifo_client.py --shutdown || fail=1
wait "$SRV"; SRV=

echo; echo "=== 2. Unix-сокет: сервер + 5 одновременных клиентов-процессов ==="
$PY sock_server.py --delay 0.1 & SRV=$!
sleep 0.5
$PY sock_multi_client.py 5 || fail=1
$PY sock_client.py "6*7" || fail=1
$PY sock_client.py --shutdown || fail=1
wait "$SRV"; SRV=

echo; echo "=== 3. pipe_demo ==="
$PY pipe_demo.py || fail=1

echo; echo "=== 4. shm_sync_demo ==="
$PY shm_sync_demo.py || fail=1

echo; echo "=== 5. bench_ipc: 10^7 чисел ==="
$PY bench_ipc.py || fail=1

echo; sleep 0.5
echo "=== состояние ПОСЛЕ ==="
left=0
echo "/dev/shm:       $(ls /dev/shm | tr '\n' ' ')"
leftover=$(ls /tmp/lab7* 2>/dev/null)
echo "FIFO/сокеты:    ${leftover:-нет}"
[ -n "$leftover" ] && left=1
echo "очереди сообщений (ipcs -q):"; ipcs -q 2>/dev/null | tail -n +2
procs=$(ps -eo pid,comm,args | awk '$2 ~ /^python/ && /(fifo_server|fifo_client|sock_server|sock_client|sock_multi_client|bench_ipc|shm_sync_demo|pipe_demo)\.py/')
zombies=$(ps -eo pid,stat,comm | awk '$2 ~ /Z/')
echo "лишние процессы: ${procs:-нет}"
echo "зомби:           ${zombies:-нет}"
[ -n "$procs" ] && left=1
[ -n "$zombies" ] && left=1
echo; [ $fail -eq 0 ] && [ $left -eq 0 ] && echo "ИТОГ: ВСЁ OK, ничего не осталось" || echo "ИТОГ: ЕСТЬ ПРОБЛЕМЫ"
exit $((fail || left))
