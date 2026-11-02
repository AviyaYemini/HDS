"""Command-line entry points run with ``python -m hds.scripts.<name>``.

Layer: scripts (operations tooling, outside the request path). May import any application
layer; nothing in the application imports from here. Each script is self-contained and
documents its own side effects.
"""
