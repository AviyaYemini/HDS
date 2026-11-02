"""Only a real activation may write ``activated_at`` (T318, F03).

A static scan of the application source: an assignment or keyword argument named ``activated_at``
is allowed only in the accepted-invitation path and in the repository step that stamps an account
created active. Anything else would turn "never activated" into "was activated".

Covers: FR-226.
"""

import ast
from pathlib import Path

SRC = Path(__file__).resolve().parents[2] / "src" / "hds"
ALLOWED = {
    ("services/invitation.py", "accept_invitation"),
    ("repositories/user.py", "_with_activation"),
}
MODELS = "models/"


def writers(path: Path) -> set[tuple[str, str]]:
    """``(file, enclosing function)`` for every write of ``activated_at`` in one file."""
    relative = path.relative_to(SRC).as_posix()
    found: set[tuple[str, str]] = set()

    def visit(node: ast.AST, function: str) -> None:
        name = node.name if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) else function
        writes = False
        if isinstance(node, ast.keyword):
            writes = node.arg == "activated_at"
        elif isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
            writes = node.attr == "activated_at"
        elif isinstance(node, ast.Subscript) and isinstance(node.ctx, ast.Store):
            writes = isinstance(node.slice, ast.Constant) and node.slice.value == "activated_at"
        if writes:
            found.add((relative, name))
        for child in ast.iter_child_nodes(node):
            visit(child, name)

    visit(ast.parse(path.read_text(encoding="utf-8")), "<module>")
    return found


def test_only_a_real_activation_writes_activated_at() -> None:
    found: set[tuple[str, str]] = set()
    for path in SRC.rglob("*.py"):
        if MODELS not in path.relative_to(SRC).as_posix():
            found |= writers(path)
    assert found <= ALLOWED, f"unexpected writers of activated_at: {sorted(found - ALLOWED)}"
    assert found, "the scan found no writer at all, so it no longer sees the code"
