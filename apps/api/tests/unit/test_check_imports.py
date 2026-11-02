"""Unit tests for the layering checker (T130).

Covers: no spec requirement; guards Constitution II (layering).
"""

from pathlib import Path

from hds.tools.check_imports import check


def _tree(tmp_path: Path, files: dict[str, str]) -> Path:
    root = tmp_path / "hds"
    for rel, body in files.items():
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body, encoding="utf-8")
    return root


def test_clean_tree_has_no_violations(tmp_path: Path) -> None:
    root = _tree(
        tmp_path,
        {
            "routers/a.py": "from sqlalchemy.ext.asyncio import AsyncSession\n",
            "services/a.py": "from hds.repositories.a import R\n",
            "repositories/a.py": "from sqlalchemy import select\nx = select(1)\n",
            "models/a.py": "import uuid\n",
        },
    )
    assert check(root) == []


def test_service_importing_router_is_flagged(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"services/a.py": "from hds.routers.x import y\n"})
    assert len(check(root)) == 1


def test_repository_importing_service_is_flagged(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"repositories/a.py": "import hds.services.x\n"})
    assert len(check(root)) == 1


def test_model_importing_upward_is_flagged(tmp_path: Path) -> None:
    root = _tree(
        tmp_path,
        {
            "models/a.py": "from hds.services.x import y\n",
            "models/b.py": "from hds.repositories.x import y\n",
            "models/c.py": "from hds.routers.x import y\n",
        },
    )
    assert len(check(root)) == 3


def test_router_using_select_is_flagged(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"routers/a.py": "from sqlalchemy import select\n"})
    assert len(check(root)) == 1


def test_service_calling_execute_is_flagged(tmp_path: Path) -> None:
    root = _tree(tmp_path, {"services/a.py": "async def f(db):\n    await db.execute(1)\n"})
    assert len(check(root)) == 1


def test_real_source_tree_is_clean() -> None:
    assert check() == []
