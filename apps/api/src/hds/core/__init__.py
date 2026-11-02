"""Cross-cutting infrastructure: settings, database session, security, errors, i18n, logging.

Layer: core. Every other layer may import from here; core MUST NOT import services,
repositories or routers (the only exceptions are the model enums and the user repository that
``deps`` needs to authenticate a request).
"""
