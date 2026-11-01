"""Layering checker — enforces Constitution Principle II.

Exits with code 1 if any violation is found:
- services/ MUST NOT import from routers/
- repositories/ MUST NOT import from services/ or routers/
- models/ MUST NOT import from routers/, services/ or repositories/
- routers/ and services/ MUST NOT use SQLAlchemy query constructs (select, insert, update,
  delete, text, func, ...) or call ``.execute(`` / ``.scalars(`` / ``.scalar(`` — only
  repositories touch the database.

Run with:
    uv run python -m hds.tools.check_imports
"""

import ast
import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).parent.parent  # apps/api/src/hds/

_IMPORT_RULES: list[tuple[str, list[str]]] = [
    # (layer_dir, [forbidden_import_prefixes])
    ("services", ["hds.routers"]),
    ("repositories", ["hds.services", "hds.routers"]),
    ("models", ["hds.routers", "hds.services", "hds.repositories"]),
]

_NO_QUERY_LAYERS = ("routers", "services")
_QUERY_NAMES = frozenset(
    {"select", "insert", "update", "delete", "text", "func", "and_", "or_", "not_", "exists"}
)
_QUERY_METHODS = frozenset({"execute", "scalars", "scalar"})


def _parse(path: Path) -> ast.AST | None:
    try:
        return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except SyntaxError:
        return None


def _collect_imports(tree: ast.AST) -> list[str]:
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    return names


def _query_violations(tree: ast.AST) -> list[str]:
    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == "sqlalchemy" or node.module.startswith("sqlalchemy."):
                found.extend(
                    f"imports sqlalchemy query construct '{a.name}'"
                    for a in node.names
                    if a.name in _QUERY_NAMES
                )
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in _QUERY_METHODS
        ):
            found.append(f"calls '.{node.func.attr}(' directly")
    return found


def check(src_root: Path = _SRC_ROOT) -> list[str]:
    """Return a list of human-readable violations found under ``src_root``."""
    violations: list[str] = []

    for layer_dir, forbidden_prefixes in _IMPORT_RULES:
        layer_path = src_root / layer_dir
        if not layer_path.is_dir():
            continue
        for py_file in layer_path.rglob("*.py"):
            tree = _parse(py_file)
            if tree is None:
                continue
            rel = py_file.relative_to(src_root.parent)
            for imp in _collect_imports(tree):
                for prefix in forbidden_prefixes:
                    if imp == prefix or imp.startswith(prefix + "."):
                        violations.append(
                            f"VIOLATION: {rel} imports '{imp}' (forbidden in {layer_dir}/)"
                        )

    for layer_dir in _NO_QUERY_LAYERS:
        layer_path = src_root / layer_dir
        if not layer_path.is_dir():
            continue
        for py_file in layer_path.rglob("*.py"):
            tree = _parse(py_file)
            if tree is None:
                continue
            rel = py_file.relative_to(src_root.parent)
            violations.extend(
                f"VIOLATION: {rel} {msg} (database access is repository-only)"
                for msg in _query_violations(tree)
            )

    return violations


def main() -> None:
    violations = check()
    if violations:
        for v in violations:
            print(v, file=sys.stderr)
        print(
            f"\n{len(violations)} layering violation(s) found. See Constitution Principle II.",
            file=sys.stderr,
        )
        sys.exit(1)

    print("Import graph OK — no layering violations detected.")


if __name__ == "__main__":
    main()
