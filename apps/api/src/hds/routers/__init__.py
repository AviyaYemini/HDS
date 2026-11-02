"""HTTP layer: FastAPI routers and their request/response schemas.

Layer: routers (the top layer). Routers validate input, resolve the caller through the
dependencies in ``hds.core.deps``, delegate to services and return schemas. They MUST NOT
contain business rules; every role-restricted rule is enforced again in the service so it holds
without HTTP. Each endpoint sets ``summary`` and ``description`` because they become the OpenAPI
document that clients are generated from (Constitution V).
"""
