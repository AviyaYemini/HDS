"""Export the app's OpenAPI document and check it against the contract (T124).

Usage:
    uv run python -m hds.scripts.export_openapi                    # print generated JSON
    uv run python -m hds.scripts.export_openapi --check CONTRACT GENERATED

``--check`` compares the contract's operation surface with the generated document: the set of
(method, path) pairs, for each the set of documented 2xx status codes, and its query
parameters (name, type, required). Descriptions, schema prose and operation IDs are not
compared. Exit code 1 on any difference.
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
    """Reduce an OpenAPI document to the part the drift check compares.

    Args:
        doc: A parsed OpenAPI document (the contract or the generated one).

    Returns:
        A map of ``(METHOD, path)`` to the set of documented 2xx status codes. Error responses
        are left out because the generated document does not declare them per route.
    """
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


def _resolve(doc: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Follow ``$ref`` chains (``#/components/...``) until a concrete node is reached."""
    seen = 0
    while "$ref" in node and seen < 20:
        target: Any = doc
        for part in node["$ref"].removeprefix("#/").split("/"):
            target = target[part]
        node = target
        seen += 1
    return node


def _type_of(doc: dict[str, Any], schema: dict[str, Any]) -> str:
    """The JSON type of a schema, ignoring ``null`` members of a nullable union."""
    schema = _resolve(doc, schema)
    for key in ("anyOf", "oneOf"):
        if key in schema:
            types = {_type_of(doc, member) for member in schema[key]} - {"null"}
            return "|".join(sorted(types)) or "null"
    if "allOf" in schema:
        return "|".join(sorted({_type_of(doc, member) for member in schema["allOf"]}))
    return str(schema.get("type", "any"))


def query_params(doc: dict[str, Any]) -> dict[tuple[str, str], dict[str, tuple[str, bool]]]:
    """Collect the query parameters of every operation.

    Args:
        doc: A parsed OpenAPI document.

    Returns:
        ``(METHOD, path)`` mapped to ``{parameter name: (type, required)}``. Path, header and
        cookie parameters are ignored.
    """
    result: dict[tuple[str, str], dict[str, tuple[str, bool]]] = {}
    for path, item in doc.get("paths", {}).items():
        for method in _METHODS:
            op = item.get(method)
            if op is None:
                continue
            params: dict[str, tuple[str, bool]] = {}
            for raw in [*item.get("parameters", []), *op.get("parameters", [])]:
                param = _resolve(doc, raw)
                if param.get("in") != "query":
                    continue
                params[param["name"]] = (
                    _type_of(doc, param.get("schema", {})),
                    bool(param.get("required", False)),
                )
            result[(method.upper(), _normalise(path))] = params
    return result


def diff(contract: dict[str, Any], generated: dict[str, Any]) -> list[str]:
    """Compare the contract with the generated document.

    Checks that both declare the same operations, the same 2xx status codes per operation and
    the same query parameters (name, type, required). Summaries, descriptions, operation ids
    and schema bodies are intentionally not compared.

    Args:
        contract: The committed OpenAPI contract (``specs/*/contracts/openapi.yaml``).
        generated: The document produced by the running application.

    Returns:
        One human-readable line per difference; empty when they agree.
    """
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
    want_q, got_q = query_params(contract), query_params(generated)
    for key in sorted(want.keys() & got.keys()):
        wp, gp = want_q.get(key, {}), got_q.get(key, {})
        for name in sorted(wp.keys() - gp.keys()):
            problems.append(
                f"query parameter in contract but not implemented: {key[0]} {key[1]} {name}"
            )
        for name in sorted(gp.keys() - wp.keys()):
            problems.append(
                f"query parameter implemented but not in contract: {key[0]} {key[1]} {name}"
            )
        for name in sorted(wp.keys() & gp.keys()):
            (wt, wr), (gt, gr) = wp[name], gp[name]
            if wt != gt:
                problems.append(
                    f"query parameter type differs for {key[0]} {key[1]} {name}: "
                    f"contract={wt} generated={gt}"
                )
            if wr != gr:
                problems.append(
                    f"query parameter required differs for {key[0]} {key[1]} {name}: "
                    f"contract={wr} generated={gr}"
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
    """Run the command line.

    Args:
        argv: Arguments without the program name: empty to print the generated document, or
            ``--check CONTRACT GENERATED`` to run the drift check.

    Returns:
        Process exit code: 0 on success or agreement, 1 on drift, 2 on a usage error.
    """
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
