"""The pay package is pure: standard library only, nothing from the app (005 T001).

Covers: 005 research R2 (the pay rules live in one pure package, as Constitution III asks of
scheduling).
"""

import ast
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[3] / "src" / "hds" / "payroll"
STDLIB = sys.stdlib_module_names


def test_it_imports_in_a_bare_interpreter_with_only_the_standard_library() -> None:
    """Only the package's own directory is on the path: no app, no third-party packages."""
    code = (
        "import sys; sys.path[:] = [p for p in sys.path if 'site-packages' not in p]; "
        f"sys.path.insert(0, {str(PACKAGE.parent)!r}); "
        "import payroll; "
        "assert not [m for m in sys.modules if m.startswith(('hds', 'sqlalchemy', 'fastapi', "
        "'pydantic'))]"
    )

    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr


def test_every_module_imports_only_the_standard_library_and_itself() -> None:
    """No module of the package imports anything but the standard library and its siblings."""
    modules = sorted(PACKAGE.rglob("*.py"))
    assert modules, "the package has no modules"
    for path in modules:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                assert node.level in (0, 1), f"{path.name} imports from outside the package"
                if node.level == 1:
                    continue
                names = [node.module or ""]
            else:
                continue
            for name in names:
                assert name.split(".")[0] in STDLIB, f"{path.name} imports {name}"
