"""common.py - общее для всех программ: пути IPC-объектов и безопасный вычислитель выражений."""
import ast
import operator

REQ = "/tmp/lab7_req.fifo"        # FIFO запросов: клиент -> сервер
RESP = "/tmp/lab7_resp.fifo"      # FIFO ответов:  сервер -> клиент
SOCK_PATH = "/tmp/lab7.sock"      # Unix-сокет
SHUTDOWN = "__shutdown__"         # служебная команда остановки сервера
TIMEOUT = 5                       # сколько клиент ждёт ответ, секунд

MAX_LEN = 200
_BIN = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
        ast.Div: operator.truediv, ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod, ast.Pow: operator.pow}
_UN = {ast.UAdd: operator.pos, ast.USub: operator.neg}


def _ev(node):
    if isinstance(node, ast.Expression):
        return _ev(node.body)
    if isinstance(node, ast.Constant) and type(node.value) in (int, float):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _BIN:
        a, b = _ev(node.left), _ev(node.right)
        if isinstance(node.op, ast.Pow) and (abs(b) > 1000 or abs(a) > 10 ** 100):
            raise ValueError("слишком большая степень")
        return _BIN[type(node.op)](a, b)
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UN:
        return _UN[type(node.op)](_ev(node.operand))
    raise ValueError("недопустимая конструкция (разрешены числа, + - * / // % ** и скобки)")


def answer(line):
    """Вычисляет выражение БЕЗ eval (разбор через ast). Возвращает 'OK <результат>' или 'ERR <причина>'."""
    try:
        if len(line) > MAX_LEN:
            raise ValueError("слишком длинное выражение")
        return "OK " + str(_ev(ast.parse(line.strip(), mode="eval")))
    except ZeroDivisionError:
        return "ERR деление на ноль"
    except SyntaxError:
        return "ERR некорректное выражение"
    except Exception as e:   # сервер не должен падать от плохого ввода
        return f"ERR {e}"


def stop_resource_tracker():
    """multiprocessing.shared_memory сам запускает служебный процесс resource_tracker. После выхода
    программы он ненадолго остаётся "сиротой" и виден в ps как зомби, пока его не подберёт init.
    Завершаем и забираем его явно до выхода (внутренний метод Python 3.12; если его нет - ничего не делаем)."""
    try:
        from multiprocessing import resource_tracker
        resource_tracker._resource_tracker._stop()
    except Exception:
        pass
