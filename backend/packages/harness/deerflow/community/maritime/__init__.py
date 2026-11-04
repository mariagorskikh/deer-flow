"""Maritime cloud sandbox provider for DeerFlow.

Runs each DeerFlow sandbox as a `Maritime <https://maritime.sh>`_ cloud sandbox,
one per ``(user, thread)``. The full :class:`Sandbox` contract is implemented:
commands through ``sandbox.run``, file transport through the native
``sandbox.files`` API, and ``list_dir`` / ``glob`` / ``grep`` through ``find`` /
``grep``. See ``README.md`` in this package for configuration.

Install the optional SDK before selecting this provider::

    pip install "deerflow-harness[maritime]"
"""

from .provider import MaritimeSandboxProvider
from .sandbox import MaritimeSandbox

__all__ = [
    "MaritimeSandbox",
    "MaritimeSandboxProvider",
]
