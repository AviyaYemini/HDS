"""Business rules: authorisation, validation and orchestration of repositories.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses. Services raise typed errors from ``hds.core.exceptions``
and re-check the caller's role themselves, so the rules hold whichever transport calls them
(Constitution II and VI).
"""
