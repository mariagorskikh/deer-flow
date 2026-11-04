"""``MaritimeSandboxProvider``: DeerFlow :class:`SandboxProvider` backed by Maritime.

Each DeerFlow ``(user, thread)`` scope maps to one Maritime sandbox named
``deer-flow-<scope token>``. Maritime's create call returns the existing sandbox
when a sandbox with that name has not ended, so a later acquire for the same
scope (in this process or after a Gateway restart) gets the same machine, with
its files and processes, instead of a new one.

``release`` only stops tracking the sandbox. Maritime's own idle timer pauses
it after ``idle_timeout`` seconds without activity (memory, processes and disk
are kept, and no compute is billed while paused), and the next file or command
call resumes it. This replaces the in-process warm pool that other remote
providers keep. ``shutdown`` pauses every tracked sandbox at once.

The SDK is imported lazily (``_import_client``) so the harness installs without
``maritime``; it is needed only once this provider is selected.
"""

from __future__ import annotations

import logging
import shlex
import threading
import uuid
from functools import partial
from typing import TYPE_CHECKING, Any

from deerflow.config import get_app_config
from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.sandbox.acquire_serialization import AcquireSerializer
from deerflow.sandbox.identity import derive_sandbox_scope_token
from deerflow.sandbox.sandbox import Sandbox, _validate_extra_env
from deerflow.sandbox.sandbox_provider import SandboxProvider

from .sandbox import MaritimeSandbox

if TYPE_CHECKING:
    from maritime import Maritime

logger = logging.getLogger(__name__)

_SANDBOX_NAME_PREFIX = "deer-flow-"
DEFAULT_TEMPLATE = "base"
DEFAULT_SIZE = "small"
# Maritime's own default; accepted range is 60 to 86400 seconds.
DEFAULT_IDLE_TIMEOUT = 300
_ON_IDLE_ACTIONS = ("pause", "kill")
# Maritime metadata values are limited to 512 characters and end-user ids to 256.
_MAX_METADATA_VALUE = 512
_MAX_END_USER = 256
_BOOTSTRAP_TIMEOUT = 30


def _bootstrap_script() -> str:
    """Create DeerFlow's ``/mnt/user-data`` layout, owned by the run user.

    Stock Maritime templates run commands as ``user`` with passwordless sudo,
    and ``/mnt`` is owned by root. The files API writes as root and needs none
    of this; it is for agent shell commands that write under ``/mnt/user-data``.
    ``sudo -n`` fails fast instead of waiting for a password on images without
    passwordless sudo.
    """
    dirs = " ".join(shlex.quote(f"{VIRTUAL_PATH_PREFIX}/{name}") for name in ("workspace", "uploads", "outputs"))
    root = shlex.quote(VIRTUAL_PATH_PREFIX)
    return f'mkdir -p {dirs} 2>/dev/null || {{ sudo -n mkdir -p {dirs} && sudo -n chown -R "$(id -u):$(id -g)" {root}; }}'


def _import_client() -> type[Maritime]:
    """Import the Maritime client lazily."""
    try:
        from maritime import Maritime
    except ImportError as e:  # pragma: no cover - depends on the optional dependency
        raise ImportError("MaritimeSandboxProvider requires the optional 'maritime' dependency. Install it with: pip install 'deerflow-harness[maritime]' or pip install maritime.") from e
    return Maritime


class MaritimeSandboxProvider(SandboxProvider):
    """Run each DeerFlow sandbox as a Maritime cloud sandbox."""

    uses_thread_data_mounts = False
    needs_upload_permission_adjustment = True

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sandboxes: dict[str, MaritimeSandbox] = {}
        self._thread_sandboxes: dict[tuple[str, str], str] = {}
        self._acquire_serializer: AcquireSerializer[str] = AcquireSerializer(thread_name_prefix="maritime-acquire-wait")
        self._accepting = True
        self._client: Maritime | None = None
        self._config = self._load_config()

    def _load_config(self) -> dict[str, Any]:
        sandbox_config = get_app_config().sandbox

        def _opt(name: str, default: Any = None) -> Any:
            value = getattr(sandbox_config, name, None)
            return default if value is None else value

        environment = dict(_opt("environment", {}))
        _validate_extra_env(environment)
        on_idle = _opt("on_idle", "pause")
        if on_idle not in _ON_IDLE_ACTIONS:
            raise ValueError(f"sandbox.on_idle must be one of {_ON_IDLE_ACTIONS}, got {on_idle!r}")
        return {
            "api_key": _opt("api_key"),  # None: the SDK reads MARITIME_API_KEY
            "base_url": _opt("base_url"),  # None: MARITIME_API_URL or the SDK default
            "template": _opt("template", DEFAULT_TEMPLATE),
            "size": _opt("size", DEFAULT_SIZE),
            "idle_timeout": int(_opt("idle_timeout", DEFAULT_IDLE_TIMEOUT)),
            "on_idle": on_idle,
            "for_end_user": bool(_opt("for_end_user", True)),
            "environment": environment,
        }

    def _get_client(self) -> Maritime:
        with self._lock:
            if self._client is not None:
                return self._client
        client = _import_client()(api_key=self._config["api_key"], base_url=self._config["base_url"])
        with self._lock:
            if self._client is None:
                self._client = client
            return self._client

    @staticmethod
    def _sandbox_id(thread_id: str, user_id: str) -> str:
        return derive_sandbox_scope_token(user_id=user_id, thread_id=thread_id)

    @staticmethod
    def _sandbox_name(sandbox_id: str) -> str:
        return f"{_SANDBOX_NAME_PREFIX}{sandbox_id}"

    # ── acquire / get / release ─────────────────────────────────────────

    def _check_accepting(self) -> None:
        with self._lock:
            if not self._accepting:
                raise RuntimeError("Maritime sandbox provider is shut down")

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        self._check_accepting()
        if thread_id is None:
            sandbox_id = uuid.uuid4().hex[:16]
            return self._publish(self._create_sandbox(sandbox_id, thread_id=None, user_id=user_id), None)

        key = (user_id or "", thread_id)
        sandbox_id = self._sandbox_id(thread_id, user_id or "")
        with self._acquire_serializer.hold(sandbox_id):
            with self._lock:
                existing = self._thread_sandboxes.get(key)
                if existing is not None and existing in self._sandboxes:
                    return existing
            return self._publish(self._create_sandbox(sandbox_id, thread_id=thread_id, user_id=user_id), key)

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        """Run the synchronous acquire on the serializer's executor, off the event loop."""
        return await self._acquire_serializer.run_on_executor(partial(self.acquire, thread_id, user_id=user_id))

    def _create_sandbox(self, sandbox_id: str, *, thread_id: str | None, user_id: str | None) -> MaritimeSandbox:
        metadata = {"deerflow_sandbox_id": sandbox_id}
        if thread_id and len(thread_id) <= _MAX_METADATA_VALUE:
            metadata["deerflow_thread_id"] = thread_id
        if user_id and len(user_id) <= _MAX_METADATA_VALUE:
            metadata["deerflow_user_id"] = user_id

        create_kwargs: dict[str, Any] = {
            # Same name, same sandbox: Maritime returns a sandbox that has not
            # ended instead of creating a second one.
            "name": self._sandbox_name(sandbox_id),
            "from_": self._config["template"],
            "size": self._config["size"],
            "idle_timeout_seconds": self._config["idle_timeout"],
            "on_idle": self._config["on_idle"],
            # Keeps account secrets out of a machine that runs model-written code.
            "for_end_user": self._config["for_end_user"],
            "metadata": metadata,
        }
        if user_id and len(user_id) <= _MAX_END_USER:
            create_kwargs["end_user"] = user_id
        if self._config["environment"]:
            create_kwargs["env"] = self._config["environment"]

        remote = self._get_client().sandboxes.create(**create_kwargs)

        # Best effort: on failure the files API still works; only shell writes
        # under /mnt/user-data are affected.
        try:
            result = remote.run(_bootstrap_script(), timeout_seconds=_BOOTSTRAP_TIMEOUT)
            if result.exit_code != 0:
                logger.warning("Maritime bootstrap for %s exited with code %s: %s", sandbox_id, result.exit_code, (result.stderr or "").strip())
        except Exception as e:
            logger.warning("Maritime bootstrap for %s failed: %s", sandbox_id, e)

        logger.info("Acquired Maritime sandbox %s (%s, name=%s)", sandbox_id, remote.id, self._sandbox_name(sandbox_id))
        sandbox = MaritimeSandbox(
            sandbox_id,
            remote,
            default_env=self._config["environment"],
            on_terminal_failure=lambda failed_id, reason: self._evict(failed_id, reason, expected=sandbox),
        )
        return sandbox

    def _publish(self, sandbox: MaritimeSandbox, key: tuple[str, str] | None) -> str:
        with self._lock:
            if self._accepting:
                self._sandboxes[sandbox.id] = sandbox
                if key is not None:
                    self._thread_sandboxes[key] = sandbox.id
                return sandbox.id
        # Shut down while create was in flight: stop the machine's compute now.
        self._pause_quietly(sandbox)
        raise RuntimeError("Maritime sandbox provider shut down during acquire")

    def _evict(self, sandbox_id: str, reason: str, *, expected: MaritimeSandbox) -> None:
        """Forget a sandbox that Maritime reports as ended; the next acquire creates a new one."""
        with self._lock:
            if self._sandboxes.get(sandbox_id) is not expected:
                return
            self._sandboxes.pop(sandbox_id, None)
            for key in [k for k, sid in self._thread_sandboxes.items() if sid == sandbox_id]:
                self._thread_sandboxes.pop(key, None)
        logger.warning("Evicted Maritime sandbox %s: %s", sandbox_id, reason)

    def get(self, sandbox_id: str) -> Sandbox | None:
        with self._lock:
            return self._sandboxes.get(sandbox_id)

    def release(self, sandbox_id: str) -> None:
        """Stop tracking the sandbox. Maritime pauses it after ``idle_timeout`` seconds idle."""
        with self._lock:
            sandbox = self._sandboxes.pop(sandbox_id, None)
            for key in [k for k, sid in self._thread_sandboxes.items() if sid == sandbox_id]:
                self._thread_sandboxes.pop(key, None)
        if sandbox is not None:
            logger.info("Released Maritime sandbox %s; it pauses after %ss idle", sandbox_id, self._config["idle_timeout"])

    # ── reset / shutdown ────────────────────────────────────────────────

    def _retire(self) -> list[MaritimeSandbox]:
        with self._lock:
            self._accepting = False
            sandboxes = list(self._sandboxes.values())
            self._sandboxes.clear()
            self._thread_sandboxes.clear()
        self._acquire_serializer.close()
        return sandboxes

    def reset(self) -> None:
        """Stop accepting acquires. Remote sandboxes are left to Maritime's idle timer."""
        self._retire()

    def shutdown(self) -> None:
        """Stop accepting acquires and pause every tracked sandbox. Idempotent."""
        for sandbox in self._retire():
            self._pause_quietly(sandbox)

    @staticmethod
    def _pause_quietly(sandbox: MaritimeSandbox) -> None:
        try:
            sandbox.pause()
        except Exception as e:
            # The idle timer still pauses it; nothing to retry here.
            logger.warning("Failed to pause Maritime sandbox %s: %s", sandbox.id, e)
