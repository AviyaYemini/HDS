"""Database access: the only code that issues queries (Constitution II).

Layer: repositories. May import ``hds.models`` and ``hds.core``; MUST NOT import services or
routers. Tenant-owned tables are read and written through ``OrgScopedRepository`` so every
query carries the caller's organisation, and each write method commits its own transaction.
"""
