"""No-secret contract checks.

This script is intended to catch deterministic interface drift without requiring
Telegram tokens or external network access.

It should run in minimal environments and fail fast with actionable messages.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _find_calls(tree: ast.AST, attr_name: str) -> int:
    n = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == attr_name:
                n += 1
    return n


def main() -> int:
    failures: list[str] = []

    # 1) Pipeline must call ingesters via .ingest, not .fetch
    pipeline_path = ROOT / "engine" / "pipeline.py"
    tree = ast.parse(_read(pipeline_path))
    if _find_calls(tree, "fetch"):
        failures.append("engine/pipeline.py still calls ing.fetch(...). Must call ing.ingest(...).")
    if not _find_calls(tree, "ingest"):
        failures.append("engine/pipeline.py does not appear to call ing.ingest(...).")

    # 2) Telegram error handler must exist
    tc_path = ROOT / "bot" / "telegram_commands.py"
    tc_tree = ast.parse(_read(tc_path))
    fn_names = {n.name for n in ast.walk(tc_tree) if isinstance(n, ast.AsyncFunctionDef)}
    if "telegram_error_handler" not in fn_names:
        failures.append("bot/telegram_commands.py missing async telegram_error_handler.")
    if "cmd_help" not in fn_names:
        failures.append("bot/telegram_commands.py missing cmd_help (imported by main.py).")

    # 3) Store must support get_signals_since with optional source/limit
    store_path = ROOT / "storage" / "sqlite_store.py"
    store_tree = ast.parse(_read(store_path))
    # Best-effort: ensure signature contains 'source' and 'limit'
    class_fns = [n for n in ast.walk(store_tree) if isinstance(n, ast.FunctionDef) and n.name == "get_signals_since"]
    if not class_fns:
        failures.append("storage/sqlite_store.py missing get_signals_since")
    else:
        args = [a.arg for a in class_fns[0].args.args]
        if "source" not in args or "limit" not in args:
            failures.append("SQLiteStore.get_signals_since should accept (since, source=None, limit=None) for command compatibility")

    # 4) Pipeline build_daily_payload must accept include_sections
    bp_fns = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "build_daily_payload"]
    if bp_fns:
        bp_args = [a.arg for a in bp_fns[0].args.args]
        if "include_sections" not in bp_args:
            failures.append("engine/pipeline.build_daily_payload missing include_sections kwarg (used by /trends)")

    if failures:
        print("CONTRACT_CHECK_FAIL")
        for f in failures:
            print("-", f)
        return 1

    print("CONTRACT_CHECK_PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
