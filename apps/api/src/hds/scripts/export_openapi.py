"""Export the app's OpenAPI document and check it against the contract (T124).

Usage:
    uv run python -m hds.scripts.export_openapi                    # print generated JSON
    uv run python -m hds.scripts.export_openapi --check CONTRACT GENERATED

``--check`` compares the contract's operation surface with the generated document: the set of
(method, path) pairs and, for each, the set of documented 2xx status codes. Descriptions
and schema prose are not compared. Exit code 1 on any difference.
"""

import json
import sys
from pathlib import Path
from typing import Any

import yaml

_METHODS = ("get", "put", "post", "delete", "patch")


def _normalise(path: str) -> str:
    """Strip an ``/api/v1`` prefix so contract paths and generated paths compare equal."""
    return path.removeprefix("/api/v1") or "/"


def surface(doc: dict[str, Any]) -> dict[tuple[str, str], frozenset[str]]:
    ops: dict[tuple[str, str], frozenset[str]] = {}
    for path, item in doc.get("paths", {}).items():
        for method in _METHODS:
            op = item.get(method)
            if op is None:
                continue
            # Only success codes are compared: error responses are not declared per route
            # in the generated document.
            codes = frozenset(str(c) for c in op.get("responses", {}) if str(c).startswith("2"))
            ops[(method.upper(), _normalise(path))] = codes
    return ops


def diff(contract: dict[str, Any], generated: dict[str, Any]) -> list[str]:
    want, got = surface(contract), surface(generated)
    problems: list[str] = []
    for key in sorted(want.keys() - got.keys()):
        problems.append(f"in contract but not implemented: {key[0]} {key[1]}")
    for key in sorted(got.keys() - want.keys()):
        problems.append(f"implemented but not in contract: {key[0]} {key[1]}")
    for key in sorted(want.keys() & got.keys()):
        if want[key] != got[key]:
            problems.append(
                f"status codes differ for {key[0]} {key[1]}: "
                f"contract={sorted(want[key])} generated={sorted(got[key])}"
            )
    return problems


def _check(contract_path: str, generated_path: str) -> int:
    contract = yaml.safe_load(Path(contract_path).read_text(encoding="utf-8"))
    generated = json.loads(Path(generated_path).read_text(encoding="utf-8"))
    problems = diff(contract, generated)
    if problems:
        for p in problems:
            print(p, file=sys.stderr)
        print(f"\n{len(problems)} OpenAPI drift problem(s).", file=sys.stderr)
        return 1
    print("OpenAPI contract and implementation agree.")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["--check"]:
        if len(argv) != 3:
            print("usage: export_openapi --check CONTRACT GENERATED", file=sys.stderr)
            return 2
        return _check(argv[1], argv[2])

    from hds.main import app

    print(json.dumps(app.openapi(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
