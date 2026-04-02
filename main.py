"""
Deployment entrypoint.

Always expose the latest app from auditflow/auditflow_single.py
so Render and local runs do not drift to an older root main.py.
"""

from auditflow.auditflow_single import app  # noqa: F401

