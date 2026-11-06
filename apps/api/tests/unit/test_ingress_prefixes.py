"""Every API route the client can reach starts with a prefix the ingress sends to the API (T034).

The web image calls the API on its own origin, and the ingress sends the API's prefixes there and
everything else to the web (specs/006 research R2). A new route under a new prefix would reach the
web server instead and answer 404 in every environment, so each route in the OpenAPI document
must start with one of ``ingress.apiPrefixes`` in ``infra/helm/hds/values.yaml``. The health paths
are the exception: the ingress routes none of them, so they stay inside the cluster
(contracts/ops.md section 1).

Covers: 006 research R2; contracts/ops.md section 1.
"""

from pathlib import Path

import yaml

from hds.main import app

VALUES = Path(__file__).resolve().parents[4] / "infra" / "helm" / "hds" / "values.yaml"
HEALTH = {"/livez", "/readyz", "/health"}


def prefixes() -> list[str]:
    values = yaml.safe_load(VALUES.read_text(encoding="utf-8"))
    found = values["ingress"]["apiPrefixes"]
    assert isinstance(found, list) and found
    return [str(p) for p in found]


def under(path: str, prefix: str) -> bool:
    return path == prefix or path.startswith(prefix.rstrip("/") + "/")


def test_the_prefixes_are_paths_and_none_is_a_health_path() -> None:
    for prefix in prefixes():
        assert prefix.startswith("/") and prefix != "/"
        assert not any(under(health, prefix) for health in HEALTH), prefix


def test_every_documented_route_starts_with_a_prefix_the_ingress_sends() -> None:
    # The document's paths are the routes a client is given (the generated client is built from
    # it); FastAPI's route table nests included routers in a private type, so it is not walked.
    documented = set(app.openapi()["paths"])
    assert documented
    missing = sorted(
        p for p in documented if p not in HEALTH and not any(under(p, x) for x in prefixes())
    )
    assert missing == [], f"routes the ingress would send to the web: {missing}"


def test_the_document_and_the_openapi_file_itself_are_under_a_prefix() -> None:
    # The client fetches the document too, from /api/v1/openapi.json.
    assert any(under("/api/v1/openapi.json", x) for x in prefixes())
