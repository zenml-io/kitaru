import ast
from pathlib import Path

CHATBOT_SOURCE = (
    Path(__file__).resolve().parents[1] / "examples" / "chatbot" / "chatbot.py"
)


def _module() -> ast.Module:
    return ast.parse(CHATBOT_SOURCE.read_text())


def _is_kitaru_call(node: ast.AST, method: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == method
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "kitaru"
    )


def _kitaru_checkpoint_decorator(function: ast.FunctionDef) -> ast.Call | None:
    for decorator in function.decorator_list:
        if isinstance(decorator, ast.Call) and _is_kitaru_call(decorator, "checkpoint"):
            return decorator
    return None


def _module_level_function(module: ast.Module, name: str) -> ast.FunctionDef:
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"No module-level function named {name!r}")


def _nested_function(module: ast.Module, name: str) -> ast.FunctionDef:
    for node in ast.walk(module):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    raise AssertionError(f"No function named {name!r}")


def test_say_and_wait_does_not_call_kitaru_save_directly() -> None:
    say_and_wait = _nested_function(_module(), "say_and_wait")

    direct_saves = [
        node for node in ast.walk(say_and_wait) if _is_kitaru_call(node, "save")
    ]

    assert direct_saves == []


def test_history_persistence_helper_is_explicit_checkpoint() -> None:
    persist_history = _module_level_function(_module(), "persist_history")
    checkpoint = _kitaru_checkpoint_decorator(persist_history)

    assert checkpoint is not None
    cache_keyword = next(
        (keyword for keyword in checkpoint.keywords if keyword.arg == "cache"),
        None,
    )
    assert isinstance(cache_keyword, ast.keyword)
    assert isinstance(cache_keyword.value, ast.Constant)
    assert cache_keyword.value.value is False
    assert any(_is_kitaru_call(node, "save") for node in ast.walk(persist_history))


def test_say_and_wait_still_opts_out_of_adapter_tool_checkpoint() -> None:
    module = _module()
    kitaru_agent_calls = [
        node
        for node in ast.walk(module)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "KitaruAgent"
    ]

    assert len(kitaru_agent_calls) == 1
    call = kitaru_agent_calls[0]
    keywords = {keyword.arg: keyword.value for keyword in call.keywords}

    allow_waits = keywords.get("allow_sync_tool_body_waits")
    assert isinstance(allow_waits, ast.Constant)
    assert allow_waits.value is True

    tool_overrides = keywords.get("tool_checkpoint_config_by_name")
    assert isinstance(tool_overrides, ast.Dict)
    assert any(
        isinstance(key, ast.Constant)
        and key.value == "say_and_wait"
        and isinstance(value, ast.Constant)
        and value.value is False
        for key, value in zip(tool_overrides.keys, tool_overrides.values, strict=True)
    )
