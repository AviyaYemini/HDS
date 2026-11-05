"""The scheduling package is pure: no app, database or network import (T033, 004 T002).

Every module uses the standard library only, except ``cpsat.py``, which may also import the CP-SAT
solver library (004 research G2). ``import scheduling`` never loads it.

Covers: Constitution III.
"""

import ast
import subprocess
import sys
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[3] / "src" / "hds" / "scheduling"
STDLIB = sys.stdlib_module_names
# The one third-party library the package may use, and the one module allowed to import it.
SOLVER_MODULE, SOLVER_LIBRARY = "cpsat.py", "ortools"


def test_it_imports_in_a_bare_interpreter_with_only_the_standard_library() -> None:
    """Only the package's own directory is on the path: no app, no third-party packages."""
    code = (
        "import sys; sys.path[:] = [p for p in sys.path if 'site-packages' not in p]; "
        f"sys.path.insert(0, {str(PACKAGE.parent)!r}); "
        "import scheduling; "
        "assert not [m for m in sys.modules if m.startswith(('hds', 'sqlalchemy', 'fastapi', "
        "'ortools'))]"
    )

    result = subprocess.run(
        [sys.executable, "-I", "-c", code], capture_output=True, text=True, check=False
    )

    assert result.returncode == 0, result.stderr


def test_every_module_imports_only_the_standard_library_and_itself() -> None:
    """Standard library and the package only; the CP-SAT module may also import ortools."""
    modules = sorted(PACKAGE.glob("*.py"))
    assert modules, "the package has no modules"
    for path in modules:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                assert node.level in (0, 1), f"{path.name} imports from outside the package"
                if node.level == 1:  # from the package itself
                    continue
                names = [node.module or ""]
            else:
                continue
            allowed = STDLIB | ({SOLVER_LIBRARY} if path.name == SOLVER_MODULE else set())
            for name in names:
                assert name.split(".")[0] in allowed, f"{path.name} imports {name}"
