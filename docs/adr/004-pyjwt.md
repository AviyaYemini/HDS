# ADR 004: Replace python-jose with PyJWT

**Date**: 2026-11-02
**Status**: Accepted
**Supersedes**: the JWT library choice in ADR 001 ("Auth: python-jose"). The token strategy in ADR 003 is unchanged.

## Context

`uv run pip-audit` (CI job `dependency-audit`) failed on two advisories with no fixed version:

- **PYSEC-2026-4298** (`python-jose` 3.5.0): HMAC initialisation accepts DER-encoded public keys, so an attacker who holds a service's public key can forge HS256 tokens when the accepted algorithms are not restricted. An incomplete fix for CVE-2024-33663.
- **PYSEC-2026-1325** (`ecdsa` 0.19.2, pulled in by `python-jose`): Minerva timing attack on P-256 signing. The maintainers consider side channels out of scope and plan no fix.

HDS signs with HS256 and passes an explicit algorithm list, so neither is exploitable as configured. The audit gate (Constitution IX) still cannot be satisfied while the packages are installed, and `python-jose` is little maintained.

## Decision

Use **PyJWT** (`pyjwt>=2.8`) in `hds/core/security.py`. `ecdsa`, `rsa`, `six` and the `types-python-jose` stubs leave the dependency tree.

- `decode_access_token` always passes `algorithms=[configured algorithm]` (never read from the token header) and requires the `exp` claim.
- Tokens signed with another key, expired tokens, `alg=none` tokens, tampered payloads and malformed strings are rejected with `ValueError` (tests in `tests/unit/test_jwt_rejection.py`).
- The public functions `create_access_token` and `decode_access_token` keep their signatures; existing token tests pass unchanged.

## Consequences

- `uv run pip-audit` reports no known vulnerabilities.
- If asymmetric signing is ever needed, PyJWT needs the `cryptography` extra; HS256 does not.
