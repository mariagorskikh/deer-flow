"""Unit tests for the Maritime community sandbox provider.

These run in CI without ``maritime`` installed. A fake SDK mirrors the
``maritime`` 0.10 surface the provider uses (``sandboxes.create`` with its
keyword-only signature, ``Sandbox.run`` / ``files`` / ``pause``, and errors
carrying ``code``), backed by a temporary directory so file and search
round trips are exercised for real. ``test_integration_real_sandbox`` runs
against a real Maritime sandbox only when ``MARITIME_API_KEY`` and
``DEER_FLOW_RUN_LIVE_TESTS=1`` are set.
"""

from __future__ import annotations

import asyncio
import errno
import os
import re
import shutil
import subprocess
import sys
import types
from dataclasses import dataclass
from pathlib import Path

import pytest

from deerflow.community.maritime.provider import _BOOTSTRAP_TIMEOUT, MaritimeSandboxProvider, _import_client
from deerflow.community.maritime.sandbox import MaritimeSandbox
from deerflow.sandbox.identity import derive_sandbox_scope_token

_HAS_SH = os.name != "nt" and shutil.which("sh") is not None
_needs_sh = pytest.mark.skipif(not _HAS_SH, reason="exercises the real find/grep scripts through a POSIX shell")

# ── Fake Maritime SDK ─────────────────────────────────────────────────


class _FakeSandboxError(Exception):
    """Stand-in for ``maritime.MaritimeSandboxError``: branch on ``code``."""

    def __init__(self, code: str, message: str = "failed", *, command: _FakeCommand | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.command = command


_FakeSandboxError.__name__ = "MaritimeSandboxError"


@dataclass
class _FakeCommand:
    stdout: str = ""
    stderr: str = ""
    exit_code: int | None = 0
    signal: str | None = None


class _FakeFiles:
    def __init__(self, owner: _FakeRemote) -> None:
        self._owner = owner

    def read(self, path: str) -> bytes:
        self._owner.files_calls.append(("read", path))
        self._owner.check()
        host = self._owner.host(path)
        if not host.is_file():
            raise _FakeSandboxError("file_not_found", path)
        return host.read_bytes()

    def write(self, path: str, data: str | bytes, *, idempotency_key: str | None = None) -> None:
        self._owner.files_calls.append(("write", path))
        self._owner.check()
        host = self._owner.host(path)
        host.parent.mkdir(parents=True, exist_ok=True)
        host.write_bytes(data.encode("utf-8") if isinstance(data, str) else data)


class _FakeRemote:
    """A fake ``maritime.Sandbox`` whose guest filesystem is a temp directory.

    String commands run through the local ``sh -c`` with ``/mnt/user-data``
    rewritten into the temp root, so the provider's real find/grep scripts run.
    """

    def __init__(self, root: Path, remote_id: str) -> None:
        self.root = root
        self.id = remote_id
        self.files = _FakeFiles(self)
        self.files_calls: list[tuple[str, str]] = []
        self.run_calls: list[dict] = []
        self.run_result: _FakeCommand | None = None
        self.run_error: Exception | None = None
        self.error: Exception | None = None  # raised by every call once set
        self.pause_calls = 0
        self.ended = False

    def check(self) -> None:
        if self.error is not None:
            raise self.error

    def host(self, path: str) -> Path:
        assert path.startswith("/"), path
        return self.root / path.lstrip("/")

    def run(self, cmd, *, background=False, cwd=None, env=None, stdin=None, timeout_seconds=None, on_stdout=None, on_stderr=None, idempotency_key=None):
        self.run_calls.append({"cmd": cmd, "env": env, "timeout_seconds": timeout_seconds})
        self.check()
        if self.run_error is not None:
            raise self.run_error
        if self.run_result is not None:
            return self.run_result
        if isinstance(cmd, list):
            if cmd[0] == "stat":
                host = self.host(cmd[-1])
                if not host.exists():
                    return _FakeCommand(stderr="stat: cannot statx: No such file or directory", exit_code=1)
                return _FakeCommand(stdout=f"{host.stat().st_size}\n")
            return _FakeCommand()
        if not _HAS_SH:
            return _FakeCommand()
        script = cmd.replace("/mnt/user-data", f"{self.root}/mnt/user-data")
        proc = subprocess.run(["sh", "-c", script], capture_output=True, text=True, env={**os.environ, **(env or {})}, check=False)
        return _FakeCommand(stdout=proc.stdout.replace(str(self.root), ""), stderr=proc.stderr, exit_code=proc.returncode)

    def pause(self, *, idempotency_key=None):
        self.pause_calls += 1
        self.check()


class _FakeSandboxes:
    def __init__(self, root: Path) -> None:
        self._root = root
        self.by_name: dict[str, _FakeRemote] = {}
        self.create_kwargs: list[dict] = []

    # Keyword-only with no **kwargs, mirroring maritime 0.10's
    # Sandboxes.create, so an argument the real SDK rejects fails here too.
    def create(
        self,
        *,
        name=None,
        from_=None,
        size=None,
        vcpu=None,
        memory_mib=None,
        disk_gib=None,
        screen=None,
        network=None,
        end_user=None,
        for_end_user=None,
        env=None,
        metadata=None,
        idle_timeout_seconds=None,
        on_idle=None,
        time_limit_seconds=None,
        keep_awake=None,
        fail_fast=None,
        idempotency_key=None,
    ):
        passed = dict(locals())
        passed.pop("self")
        self.create_kwargs.append({k: v for k, v in passed.items() if v is not None})
        existing = self.by_name.get(name) if name else None
        if existing is not None and not existing.ended:
            return existing
        remote_id = f"sbx-{len(self.create_kwargs)}"
        remote = _FakeRemote(self._root / remote_id, remote_id)
        if name:
            self.by_name[name] = remote
        return remote


class _FakeClient:
    def __init__(self, root: Path) -> None:
        self.sandboxes = _FakeSandboxes(root)
        self.init_kwargs: dict = {}


def _stub_config(sandbox_attrs=None):
    return types.SimpleNamespace(sandbox=types.SimpleNamespace(**(sandbox_attrs or {})))


def _install(monkeypatch, *, client=None, config_attrs=None) -> MaritimeSandboxProvider:
    monkeypatch.setattr("deerflow.community.maritime.provider.get_app_config", lambda: _stub_config(config_attrs))
    if client is not None:

        def _factory(**kwargs):
            client.init_kwargs = kwargs
            return client

        monkeypatch.setattr("deerflow.community.maritime.provider._import_client", lambda: _factory)
    return MaritimeSandboxProvider()


@pytest.fixture
def client(tmp_path: Path) -> _FakeClient:
    return _FakeClient(tmp_path)


def _box(tmp_path: Path, **kwargs) -> tuple[MaritimeSandbox, _FakeRemote]:
    remote = _FakeRemote(tmp_path, "sbx-test")
    return MaritimeSandbox("sb", remote, **kwargs), remote


# ── Lazy import ───────────────────────────────────────────────────────


def test_import_client_missing_raises_actionable(monkeypatch) -> None:
    monkeypatch.setitem(sys.modules, "maritime", None)
    with pytest.raises(ImportError, match=r"deerflow-harness\[maritime\]"):
        _import_client()


def test_acquire_without_sdk_raises_and_shuts_down_cleanly(monkeypatch) -> None:
    monkeypatch.setattr("deerflow.community.maritime.provider.get_app_config", lambda: _stub_config())
    monkeypatch.setitem(sys.modules, "maritime", None)
    provider = MaritimeSandboxProvider()
    with pytest.raises(ImportError, match=r"deerflow-harness\[maritime\]"):
        provider.acquire("thread-1", user_id="u")
    provider.shutdown()
    provider.shutdown()  # idempotent


# ── Config ────────────────────────────────────────────────────────────


def test_load_config_rejects_unknown_on_idle(monkeypatch) -> None:
    with pytest.raises(ValueError, match="on_idle"):
        _install(monkeypatch, config_attrs={"on_idle": "stop"})


def test_load_config_rejects_invalid_environment_key(monkeypatch) -> None:
    with pytest.raises(ValueError, match="POSIX"):
        _install(monkeypatch, config_attrs={"environment": {"bad-key": "1"}})


# ── Acquire / create ──────────────────────────────────────────────────


def test_sandbox_id_and_name_follow_shared_identity(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("t-1", user_id="u-1")
    assert sid == derive_sandbox_scope_token(user_id="u-1", thread_id="t-1")
    name = client.sandboxes.create_kwargs[0]["name"]
    assert name == f"deer-flow-{sid}"
    # Maritime's name rule (SBX-VAL-NAME); names may not start with "sbx-".
    assert re.fullmatch(r"[a-z][a-z0-9-]{0,62}", name)


def test_create_defaults(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    provider.acquire("thread-1", user_id="user-1")
    kwargs = client.sandboxes.create_kwargs[0]
    assert kwargs["from_"] == "base"
    assert kwargs["size"] == "small"
    assert kwargs["idle_timeout_seconds"] == 300
    assert kwargs["on_idle"] == "pause"
    assert kwargs["for_end_user"] is True
    assert kwargs["end_user"] == "user-1"
    assert kwargs["metadata"]["deerflow_thread_id"] == "thread-1"
    assert kwargs["metadata"]["deerflow_user_id"] == "user-1"
    assert "env" not in kwargs
    assert client.init_kwargs == {"api_key": None, "base_url": None}


def test_create_passes_configured_options(monkeypatch, client) -> None:
    config = {
        "api_key": "mk_test",
        "base_url": "https://api.example.test",
        "template": "python:3.12-slim",
        "size": "large",
        "idle_timeout": 900,
        "on_idle": "kill",
        "for_end_user": False,
        "environment": {"PYTHONUNBUFFERED": "1"},
    }
    provider = _install(monkeypatch, client=client, config_attrs=config)
    provider.acquire("thread-1", user_id="user-1")
    kwargs = client.sandboxes.create_kwargs[0]
    assert (kwargs["from_"], kwargs["size"], kwargs["idle_timeout_seconds"], kwargs["on_idle"]) == ("python:3.12-slim", "large", 900, "kill")
    assert kwargs["for_end_user"] is False
    assert kwargs["env"] == {"PYTHONUNBUFFERED": "1"}
    assert client.init_kwargs == {"api_key": "mk_test", "base_url": "https://api.example.test"}


def test_overlong_user_id_is_not_sent_as_end_user_or_metadata(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    provider.acquire("thread-1", user_id="u" * 600)
    kwargs = client.sandboxes.create_kwargs[0]
    assert "end_user" not in kwargs
    assert "deerflow_user_id" not in kwargs["metadata"]


def test_same_thread_reuses_tracked_sandbox(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    first = provider.acquire("thread-1", user_id="u1")
    assert provider.acquire("thread-1", user_id="u1") == first
    assert len(client.sandboxes.create_kwargs) == 1


def test_different_users_get_different_sandboxes(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    a = provider.acquire("thread-1", user_id="u1")
    b = provider.acquire("thread-1", user_id="u2")
    assert a != b
    assert provider.get(a).remote_id != provider.get(b).remote_id


def test_acquire_without_thread_uses_random_ids(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    assert provider.acquire() != provider.acquire()


def test_release_then_acquire_returns_same_remote_sandbox(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    box = provider.get(sid)
    box.write_file("/mnt/user-data/workspace/state.txt", "kept")
    remote_id = box.remote_id

    provider.release(sid)
    assert provider.get(sid) is None
    assert client.sandboxes.by_name[f"deer-flow-{sid}"].pause_calls == 0  # left to the idle timer

    again = provider.acquire("thread-1", user_id="u1")
    assert again == sid
    assert provider.get(again).remote_id == remote_id
    assert provider.get(again).read_file("/mnt/user-data/workspace/state.txt") == "kept"


def test_bootstrap_runs_bounded_and_failure_does_not_fail_acquire(monkeypatch, client, caplog) -> None:
    original_create = client.sandboxes.create

    def create(**kwargs):
        remote = original_create(**kwargs)
        remote.run_error = _FakeSandboxError("internal_error")
        return remote

    monkeypatch.setattr(client.sandboxes, "create", create)
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    remote = client.sandboxes.by_name[f"deer-flow-{sid}"]
    assert "/mnt/user-data/workspace" in remote.run_calls[0]["cmd"]
    assert "sudo -n" in remote.run_calls[0]["cmd"]
    assert remote.run_calls[0]["timeout_seconds"] == _BOOTSTRAP_TIMEOUT
    assert provider.get(sid) is not None
    assert "bootstrap" in caplog.text


@_needs_sh
def test_bootstrap_creates_user_data_layout(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    root = client.sandboxes.by_name[f"deer-flow-{sid}"].root
    for name in ("workspace", "uploads", "outputs"):
        assert (root / "mnt" / "user-data" / name).is_dir()


def test_acquire_async(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = asyncio.run(provider.acquire_async("thread-1", user_id="u1"))
    assert provider.get(sid) is not None
    provider.shutdown()


def test_ended_sandbox_is_evicted_and_replaced(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    box = provider.get(sid)
    remote = client.sandboxes.by_name[f"deer-flow-{sid}"]
    remote.ended = True
    remote.error = _FakeSandboxError("sandbox_ended")

    assert box.execute_command("echo hi").startswith("Error: sandbox_ended")
    assert provider.get(sid) is None

    again = provider.acquire("thread-1", user_id="u1")
    assert again == sid
    assert provider.get(again).remote_id != remote.id


def test_late_failure_from_replaced_adapter_does_not_evict_replacement(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    old = provider.get(sid)
    provider.release(sid)
    replacement = provider.get(provider.acquire("thread-1", user_id="u1"))

    old._sandbox = _FakeRemote(client.sandboxes._root / "gone", "sbx-gone")
    old._sandbox.error = _FakeSandboxError("sandbox_not_found")
    old.execute_command("true")

    assert provider.get(sid) is replacement


def test_regular_error_does_not_evict(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    client.sandboxes.by_name[f"deer-flow-{sid}"].error = _FakeSandboxError("rate_limited")
    assert provider.get(sid).execute_command("true").startswith("Error: rate_limited")
    assert provider.get(sid) is not None


def test_shutdown_pauses_tracked_sandboxes_and_refuses_acquire(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    a = provider.acquire("thread-1", user_id="u1")
    b = provider.acquire("thread-2", user_id="u1")
    provider.release(b)

    provider.shutdown()
    provider.shutdown()

    assert client.sandboxes.by_name[f"deer-flow-{a}"].pause_calls == 1
    assert client.sandboxes.by_name[f"deer-flow-{b}"].pause_calls == 0
    assert provider.get(a) is None
    with pytest.raises(RuntimeError, match="shut down"):
        provider.acquire("thread-3", user_id="u1")


def test_shutdown_survives_pause_failure(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    client.sandboxes.by_name[f"deer-flow-{sid}"].error = _FakeSandboxError("internal_error")
    provider.shutdown()
    assert provider.get(sid) is None


def test_reset_refuses_acquire_without_pausing(monkeypatch, client) -> None:
    provider = _install(monkeypatch, client=client)
    sid = provider.acquire("thread-1", user_id="u1")
    provider.reset()
    assert client.sandboxes.by_name[f"deer-flow-{sid}"].pause_calls == 0
    with pytest.raises(RuntimeError):
        provider.acquire("thread-1", user_id="u1")


def test_pause_treats_ended_sandbox_as_done(tmp_path) -> None:
    box, remote = _box(tmp_path)
    remote.error = _FakeSandboxError("sandbox_ended")
    box.pause()  # does not raise
    remote.error = _FakeSandboxError("internal_error")
    with pytest.raises(_FakeSandboxError):
        box.pause()


# ── execute_command ───────────────────────────────────────────────────


def test_execute_command_joins_output_and_merges_env(tmp_path) -> None:
    box, remote = _box(tmp_path, default_env={"A": "1", "B": "1"})
    remote.run_result = _FakeCommand(stdout="out", stderr="err")
    assert box.execute_command("cmd", env={"B": "2"}) == "out\nerr"
    assert remote.run_calls[-1] == {"cmd": "cmd", "env": {"A": "1", "B": "2"}, "timeout_seconds": None}


def test_execute_command_rejects_invalid_env_key(tmp_path) -> None:
    box, _ = _box(tmp_path)
    with pytest.raises(ValueError):
        box.execute_command("true", env={"bad-key": "x"})


@pytest.mark.parametrize(("timeout", "expected"), [(None, None), (0.2, 1), (2.1, 3), (600.0, 600), (99999.0, 3600)])
def test_execute_command_timeout_is_whole_seconds_in_range(tmp_path, timeout, expected) -> None:
    box, remote = _box(tmp_path)
    remote.run_result = _FakeCommand(stdout="ok")
    box.execute_command("true", timeout=timeout)
    assert remote.run_calls[-1]["timeout_seconds"] == expected


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (_FakeCommand(stdout="5 passed, 1 error\n", exit_code=1), "5 passed, 1 error\n\nExit Code: 1"),
        (_FakeCommand(exit_code=2), "Command exited with code 2"),
        (_FakeCommand(exit_code=None, signal="SIGKILL"), "Command terminated by SIGKILL"),
        (_FakeCommand(), "(no output)"),
    ],
)
def test_execute_command_reports_exit_status(tmp_path, result, expected) -> None:
    box, remote = _box(tmp_path)
    remote.run_result = result
    assert box.execute_command("cmd") == expected


def test_execute_command_timeout_keeps_partial_output(tmp_path) -> None:
    box, remote = _box(tmp_path)
    remote.run_error = _FakeSandboxError("command_timeout", command=_FakeCommand(stdout="partial", exit_code=None, signal="SIGKILL"))
    assert box.execute_command("sleep 999", timeout=1) == "partial\nError: command timed out\nExit Code: 124"
    remote.run_error = _FakeSandboxError("command_timeout")
    assert box.execute_command("sleep 999", timeout=1) == "Error: command timed out\nExit Code: 124"


# ── Files ─────────────────────────────────────────────────────────────


def test_file_round_trip_and_parent_creation(tmp_path) -> None:
    box, remote = _box(tmp_path)
    box.write_file("/mnt/user-data/workspace/a/b/c.txt", "hello\nworld")
    assert box.read_file("/mnt/user-data/workspace/a/b/c.txt") == "hello\nworld"
    assert box.read_file("/mnt/user-data/workspace/a/b/c.txt", start_line=2, end_line=2) == "world"


def test_append_accumulates_and_creates_missing_file(tmp_path) -> None:
    box, _ = _box(tmp_path)
    box.write_file("/mnt/user-data/workspace/log.txt", "a", append=True)
    box.write_file("/mnt/user-data/workspace/log.txt", "b", append=True)
    assert box.read_file("/mnt/user-data/workspace/log.txt") == "ab"


def test_append_propagates_non_missing_read_errors(tmp_path) -> None:
    box, remote = _box(tmp_path)
    remote.error = _FakeSandboxError("internal_error")
    with pytest.raises(_FakeSandboxError):
        box.write_file("/mnt/user-data/workspace/log.txt", "a", append=True)


def test_update_file_writes_binary(tmp_path) -> None:
    box, remote = _box(tmp_path)
    box.update_file("/mnt/user-data/uploads/blob.bin", b"\x00\xff")
    assert remote.host("/mnt/user-data/uploads/blob.bin").read_bytes() == b"\x00\xff"


def test_read_missing_file_returns_error_text(tmp_path) -> None:
    box, _ = _box(tmp_path)
    assert box.read_file("/mnt/user-data/workspace/missing.txt").startswith("Error: file_not_found")


def test_path_traversal_is_rejected(tmp_path) -> None:
    box, remote = _box(tmp_path)
    with pytest.raises(PermissionError):
        box.write_file("/mnt/user-data/../etc/passwd", "x")
    with pytest.raises(PermissionError):
        box.read_file("/mnt/user-data/../../etc/passwd")
    assert remote.files_calls == []


# ── download_file ─────────────────────────────────────────────────────


def test_download_round_trip(tmp_path) -> None:
    box, _ = _box(tmp_path)
    box.update_file("/mnt/user-data/outputs/report.pdf", b"%PDF")
    assert box.download_file("/mnt/user-data/outputs/report.pdf") == b"%PDF"


@pytest.mark.parametrize("path", ["/etc/passwd", "/mnt/user-data/../etc/passwd", "/mnt/user-database/x"])
def test_download_guards_reject_before_touching_sandbox(tmp_path, path) -> None:
    box, remote = _box(tmp_path)
    with pytest.raises(PermissionError):
        box.download_file(path)
    assert remote.run_calls == [] and remote.files_calls == []


def test_download_missing_file_raises_oserror(tmp_path) -> None:
    box, _ = _box(tmp_path)
    with pytest.raises(OSError):
        box.download_file("/mnt/user-data/outputs/missing.bin")


def test_download_over_cap_is_refused_before_transfer(tmp_path) -> None:
    box, remote = _box(tmp_path)
    box.update_file("/mnt/user-data/outputs/big.bin", b"x" * 10)
    remote.files_calls.clear()
    with pytest.raises(OSError) as info:
        box.download_file("/mnt/user-data/outputs/big.bin", max_bytes=9)
    assert info.value.errno == errno.EFBIG
    assert remote.files_calls == []
    assert box.download_file("/mnt/user-data/outputs/big.bin", max_bytes=10) == b"x" * 10


def test_download_rechecks_size_after_transfer(tmp_path) -> None:
    box, remote = _box(tmp_path)
    box.update_file("/mnt/user-data/outputs/grow.bin", b"x" * 20)
    remote.run_result = _FakeCommand(stdout="5\n")  # stale size from the probe
    with pytest.raises(OSError) as info:
        box.download_file("/mnt/user-data/outputs/grow.bin", max_bytes=10)
    assert info.value.errno == errno.EFBIG


def test_download_rejects_invalid_max_bytes(tmp_path) -> None:
    box, _ = _box(tmp_path)
    with pytest.raises(ValueError):
        box.download_file("/mnt/user-data/outputs/x", max_bytes=-1)


# ── list_dir / glob / grep (real shell scripts) ───────────────────────


def _seed(box: MaritimeSandbox) -> None:
    box.write_file("/mnt/user-data/workspace/src/app.py", "import os\nprint('Hello')\n")
    box.write_file("/mnt/user-data/workspace/src/util.js", "console.log('hello')\n")
    box.write_file("/mnt/user-data/workspace/README.md", "# hello\n")


@_needs_sh
def test_list_dir_reports_virtual_paths(tmp_path) -> None:
    box, _ = _box(tmp_path)
    _seed(box)
    entries = box.list_dir("/mnt/user-data/workspace", max_depth=1)
    assert "/mnt/user-data/workspace/src" in entries
    assert "/mnt/user-data/workspace/README.md" in entries
    assert "/mnt/user-data/workspace/src/app.py" not in entries


@_needs_sh
def test_list_dir_missing_path_raises(tmp_path) -> None:
    box, _ = _box(tmp_path)
    with pytest.raises(FileNotFoundError):
        box.list_dir("/mnt/user-data/missing")


def test_list_dir_client_failure_raises_oserror(tmp_path) -> None:
    box, remote = _box(tmp_path)
    remote.run_error = _FakeSandboxError("internal_error")
    with pytest.raises(OSError):
        box.list_dir("/mnt/user-data/workspace")


@_needs_sh
def test_glob_matches_relative_pattern(tmp_path) -> None:
    box, _ = _box(tmp_path)
    _seed(box)
    matches, truncated = box.glob("/mnt/user-data/workspace", "**/*.py")
    assert matches == ["/mnt/user-data/workspace/src/app.py"]
    assert truncated is False


@_needs_sh
def test_glob_caps_results(tmp_path) -> None:
    box, _ = _box(tmp_path)
    for i in range(3):
        box.write_file(f"/mnt/user-data/workspace/f{i}.txt", "x")
    matches, truncated = box.glob("/mnt/user-data/workspace", "*.txt", max_results=2)
    assert len(matches) == 2 and truncated is True


@_needs_sh
def test_grep_is_case_insensitive_by_default_and_honours_glob(tmp_path) -> None:
    box, _ = _box(tmp_path)
    _seed(box)
    matches, _ = box.grep("/mnt/user-data/workspace", "hello")
    assert {m.path for m in matches} == {
        "/mnt/user-data/workspace/src/app.py",
        "/mnt/user-data/workspace/src/util.js",
        "/mnt/user-data/workspace/README.md",
    }
    scoped, _ = box.grep("/mnt/user-data/workspace", "hello", glob="src/*.js")
    assert [(m.path, m.line_number) for m in scoped] == [("/mnt/user-data/workspace/src/util.js", 1)]
    sensitive, _ = box.grep("/mnt/user-data/workspace", "Hello", case_sensitive=True)
    assert [m.path for m in sensitive] == ["/mnt/user-data/workspace/src/app.py"]


@_needs_sh
def test_grep_single_file_root(tmp_path) -> None:
    box, _ = _box(tmp_path)
    _seed(box)
    matches, _ = box.grep("/mnt/user-data/workspace/src/app.py", "print", literal=True)
    assert [(m.path, m.line_number) for m in matches] == [("/mnt/user-data/workspace/src/app.py", 2)]


def test_grep_rejects_invalid_regex(tmp_path) -> None:
    box, remote = _box(tmp_path)
    with pytest.raises(re.error):
        box.grep("/mnt/user-data/workspace", "(")
    assert remote.run_calls == []


# ── Live integration (opt-in) ─────────────────────────────────────────


@pytest.mark.live
@pytest.mark.skipif(
    not os.getenv("MARITIME_API_KEY") or os.getenv("DEER_FLOW_RUN_LIVE_TESTS") != "1",
    reason="requires MARITIME_API_KEY and DEER_FLOW_RUN_LIVE_TESTS=1; creates a billed Maritime sandbox",
)
def test_integration_real_sandbox(monkeypatch) -> None:
    monkeypatch.setattr("deerflow.community.maritime.provider.get_app_config", lambda: _stub_config({"on_idle": "kill", "idle_timeout": 60}))
    provider = MaritimeSandboxProvider()
    try:
        sid = provider.acquire("it-thread", user_id="it-user")
        box = provider.get(sid)
        assert box is not None
        assert "42" in box.execute_command("python3 -c 'print(6 * 7)'")
        box.write_file("/mnt/user-data/workspace/it.txt", "maritime-e2e")
        assert box.read_file("/mnt/user-data/workspace/it.txt") == "maritime-e2e"
        assert box.execute_command("cat /mnt/user-data/workspace/it.txt") == "maritime-e2e"
        assert "/mnt/user-data/workspace/it.txt" in box.list_dir("/mnt/user-data/workspace")
    finally:
        provider.shutdown()
