"""The pay package: every pay rule, as pure functions over plain data.

Layer: payroll (pure). Standard library only; MUST NOT import anything from the application, a web
framework, a database driver or the network, and MUST NOT be imported by models or repositories
(005 research R2). Services call it; nothing else computes pay.
"""

from .price import PayLine, PayPart, price
from .settings import DEFAULT_SETTINGS, PaySettings, RestWindow, SpecialDay

__all__ = [
    "DEFAULT_SETTINGS",
    "PayLine",
    "PayPart",
    "PaySettings",
    "RestWindow",
    "SpecialDay",
    "price",
]
