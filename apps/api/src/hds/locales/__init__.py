"""Translation catalogues (``en.json``, ``he.json``) keyed by error code.

Layer: data package with no code. Read only by ``hds.core.i18n``. Every error ``code`` raised
anywhere must have a key in both files; ``tests/unit/test_i18n.py`` enforces it. Hebrew is the
primary language (Constitution VIII).
"""
