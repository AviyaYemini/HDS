"""Every hash in a service comes after ``release_connection`` in the same function (T348).

The pool tests park two of the six paths; this scan covers all of them, so a new path or a moved
line cannot hold a database connection while it waits for a hash.

Covers: FR-222.
"""

import ast
from pathlib import Path

SERVICES = Path(__file__).resolve().parents[2] / "src" / "hds" / "services"
HASHING = {"hash_password", "verify_password", "verify_against_dummy"}
EXPECTED = {
    ("auth.py", "login"): 2,
    ("invitation.py", "create_invitation"): 1,
    ("invitation.py", "accept_invitation"): 1,
    ("invitation.py", "reset_password"): 1,
    ("employee.py", "create_employee"): 1,
    ("client.py", "create_client_user"): 1,
    ("first_admin.py", "create"): 1,  # the first Admin (specs/006 R13)
}


def called_name(node: ast.Call) -> str:
    return node.func.attr if isinstance(node.func, ast.Attribute) else getattr(node.func, "id", "")


def test_every_hash_follows_a_release() -> None:
    found: dict[tuple[str, str], int] = {}
    for path in SERVICES.glob("*.py"):
        for function in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if not isinstance(function, ast.AsyncFunctionDef):
                continue
            calls = sorted(
                (n for n in ast.walk(function) if isinstance(n, ast.Call)),
                key=lambda n: (n.lineno, n.col_offset),
            )
            released = 0
            for call in calls:
                name = called_name(call)
                if name == "release_connection":
                    released += 1
                elif name in HASHING:
                    assert released, f"{path.name}:{call.lineno} hashes before releasing"
                    key = (path.name, function.name)
                    found[key] = found.get(key, 0) + 1
    assert found == EXPECTED
