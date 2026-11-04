"""``MaritimeSandbox``: DeerFlow :class:`Sandbox` backed by a Maritime sandbox.

The Maritime Python SDK (``maritime``) is synchronous, so this adapter calls it
directly. Commands go through ``sandbox.run`` (a string runs under
``/bin/sh -c``). File reads and writes use the native ``sandbox.files`` API,
which acts with root permissions and creates missing parent directories, so
``/mnt/user-data`` needs no remapping. Directory and content search
(``list_dir`` / ``glob`` / ``grep``) shell out to ``find`` / ``grep`` and parse
the output with the shared ``deerflow.sandbox`` helpers, as
``community/tenki`` and ``community/e2b_sandbox`` do.

The SDK is not imported here. Its errors are recognised by their ``code``
attribute, so importing this package never requires ``maritime``.
"""

from __future__ import annotations

import errno
import logging
import math
import posixpath
import re
import shlex
import threading
from typing import TYPE_CHECKING, Any

from deerflow.config.paths import VIRTUAL_PATH_PREFIX
from deerflow.sandbox.download import DEFAULT_DOWNLOAD_LIMIT as _MAX_DOWNLOAD_SIZE
from deerflow.sandbox.download import download_limit
from deerflow.sandbox.read_file_contract import split_file_lines
from deerflow.sandbox.remote_list_dir import parse_remote_list_dir_output, remote_list_dir_command
from deerflow.sandbox.remote_search import parse_remote_search_output, remote_search_command
from deerflow.sandbox.sandbox import Sandbox, _validate_extra_env
from deerflow.sandbox.search import GrepMatch, path_matches, should_ignore_path_under_root, truncate_line

if TYPE_CHECKING:
    from collections.abc import Callable

    from maritime import Sandbox as MaritimeClientSandbox

logger = logging.getLogger(__name__)

# Maritime accepts a foreground ``timeout_seconds`` from 1 to 3600.
_MIN_TIMEOUT_SECONDS = 1
_MAX_TIMEOUT_SECONDS = 3600
# Bound for the size probe that runs before a download.
_STAT_TIMEOUT_SECONDS = 30

# ``MaritimeSandboxError.code`` values meaning the remote sandbox is gone for
# good. The provider drops the adapter and the next acquire creates a new one.
# Every other error (including a paused or resuming sandbox, which Maritime
# resumes on the next call) is surfaced without eviction.
_TERMINAL_ERROR_CODES = frozenset({"sandbox_ended", "sandbox_not_found"})


def _error_code(error: BaseException) -> str | None:
    code = getattr(error, "code", None)
    return code if isinstance(code, str) else None


def _timeout_seconds(timeout: float | None) -> int | None:
    """Convert DeerFlow's float timeout to the whole seconds Maritime accepts."""
    if timeout is None:
        return None
    return min(max(math.ceil(timeout), _MIN_TIMEOUT_SECONDS), _MAX_TIMEOUT_SECONDS)


def _join_output(command: Any) -> str:
    stdout = getattr(command, "stdout", "") or ""
    stderr = getattr(command, "stderr", "") or ""
    if stdout and stderr:
        return f"{stdout}\n{stderr}"
    return stdout or stderr


class MaritimeSandbox(Sandbox):
    """DeerFlow Sandbox adapter that delegates to a live Maritime sandbox.

    Args:
        id: DeerFlow-side sandbox id (the provider's cache key).
        sandbox: A ``maritime.Sandbox`` client. The provider owns its lifecycle.
        default_env: Static environment merged into every command, overridden
            per call by the ``env`` passed to :meth:`execute_command`.
        on_terminal_failure: Optional callback ``(sandbox_id, reason)`` invoked
            when an operation reports that the remote sandbox has ended, so the
            provider can evict it.
    """

    #: Every call is a new ``/bin/sh -c`` process; no shell state survives
    #: into the next command.
    persistent_shell_sessions = False

    def __init__(
        self,
        id: str,
        sandbox: MaritimeClientSandbox,
        *,
        default_env: dict[str, str] | None = None,
        on_terminal_failure: Callable[[str, str], None] | None = None,
    ) -> None:
        super().__init__(id)
        self._sandbox = sandbox
        self._default_env = dict(default_env or {})
        self._on_terminal_failure = on_terminal_failure
        # Serialises the append read-modify-write; the files API has no append.
        self._write_lock = threading.Lock()

    @property
    def remote_id(self) -> str:
        """The Maritime sandbox id (``sbx-...``)."""
        return self._sandbox.id

    def pause(self) -> None:
        """Pause the remote sandbox. Memory, processes and disk are kept."""
        try:
            self._sandbox.pause()
        except Exception as e:
            if _error_code(e) not in _TERMINAL_ERROR_CODES:
                raise

    # ── SDK call helpers ────────────────────────────────────────────────

    def _note_failure(self, error: Exception) -> None:
        if self._on_terminal_failure is None or _error_code(error) not in _TERMINAL_ERROR_CODES:
            return
        try:
            self._on_terminal_failure(self.id, str(error))
        except Exception:
            logger.exception("Terminal Maritime failure callback errored for %s", self.id)

    def _call(self, op: Callable[[MaritimeClientSandbox], Any]) -> Any:
        try:
            return op(self._sandbox)
        except Exception as e:
            self._note_failure(e)
            raise

    def _sh(self, script: str, *, timeout: float | None = None) -> Any:
        return self._call(lambda sbx: sbx.run(script, timeout_seconds=_timeout_seconds(timeout)))

    def _search(self, script: str, what: str) -> Any:
        """Run a ``find`` / ``grep`` script, raising ``OSError`` on client failure."""
        try:
            return self._sh(script)
        except Exception as e:
            raise OSError(f"Failed to {what}: {e}") from e

    @staticmethod
    def _guard_traversal(path: str) -> str:
        if not path:
            raise ValueError("path must be a non-empty string")
        normalized = path.replace("\\", "/")
        for segment in normalized.split("/"):
            if segment == "..":
                raise PermissionError(f"Access denied: path traversal detected in '{path}'")
        return normalized

    # ── command execution ───────────────────────────────────────────────

    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        """Run ``command`` through ``/bin/sh -c`` in the sandbox and return its output.

        Per-call ``env`` is layered over the static config environment and
        applies to this command only. ``timeout`` is rounded up to whole
        seconds and clamped to Maritime's 1 to 3600 range; ``None`` uses the
        SDK default (60 seconds).
        """
        _validate_extra_env(env)
        merged_env = {**self._default_env, **(env or {})} or None
        try:
            result = self._call(lambda sbx: sbx.run(command, env=merged_env, timeout_seconds=_timeout_seconds(timeout)))
        except Exception as e:
            if _error_code(e) == "command_timeout":
                partial = getattr(e, "command", None)
                output = _join_output(partial) if partial is not None else ""
                message = "Error: command timed out\nExit Code: 124"
                return f"{output}\n{message}" if output else message
            logger.error("Failed to execute command in Maritime sandbox %s: %s", self.id, e)
            return f"Error: {e}"

        output = _join_output(result)
        if result.exit_code not in (0, None):
            # Mirror LocalSandbox: keep a nonzero exit in the output text even
            # when the command printed something.
            output = f"{output}\nExit Code: {result.exit_code}" if output else f"Command exited with code {result.exit_code}"
        elif result.exit_code is None and getattr(result, "signal", None):
            output = f"{output}\nTerminated by {result.signal}" if output else f"Command terminated by {result.signal}"
        return output if output else "(no output)"

    # ── file operations ─────────────────────────────────────────────────

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        normalized = self._guard_traversal(path)
        try:
            content = self._call(lambda sbx: sbx.files.read(normalized)).decode("utf-8", errors="replace")
        except Exception as e:
            logger.error("read_file %s failed: %s", normalized, e)
            return f"Error: {e}"
        if start_line is None and end_line is None:
            return content
        lines = split_file_lines(content)
        # Clamp like LocalSandbox.read_file: a negative start would otherwise
        # wrap around through Python's negative-index slicing.
        start = max(start_line or 1, 1)
        end = max(end_line, 0) if end_line is not None else len(lines)
        return "\n".join(lines[start - 1 : end])

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        self._write_bytes(self._guard_traversal(path), content.encode("utf-8"), append=append)

    def update_file(self, path: str, content: bytes) -> None:
        self._write_bytes(self._guard_traversal(path), content, append=False)

    def _write_bytes(self, path: str, data: bytes, *, append: bool) -> None:
        # ``files.write`` replaces the whole file and creates parent directories.
        if not append:
            self._call(lambda sbx: sbx.files.write(path, data))
            return
        with self._write_lock:
            try:
                data = self._call(lambda sbx: sbx.files.read(path)) + data
            except Exception as e:
                if _error_code(e) != "file_not_found":
                    raise
            self._call(lambda sbx: sbx.files.write(path, data))

    def download_file(self, path: str, *, max_bytes: int | None = None) -> bytes:
        limit = download_limit(max_bytes, _MAX_DOWNLOAD_SIZE)
        normalized = self._guard_traversal(path)
        stripped = normalized.lstrip("/")
        allowed = VIRTUAL_PATH_PREFIX.lstrip("/")
        if stripped != allowed and not stripped.startswith(f"{allowed}/"):
            raise PermissionError(f"Access denied: path must be under '{VIRTUAL_PATH_PREFIX}': '{path}'")

        # ``files.read`` returns the whole body, so check the size first and
        # refuse an oversized file before any of it is transferred. The second
        # check below covers a file that grew between the two calls.
        try:
            probe = self._call(lambda sbx: sbx.run(["stat", "-L", "-c", "%s", normalized], timeout_seconds=_STAT_TIMEOUT_SECONDS))
        except Exception as e:
            raise OSError(f"cannot read '{path}' from sandbox: {e}") from e
        if probe.exit_code != 0:
            raise OSError(errno.ENOENT, f"cannot read '{path}' from sandbox: {(probe.stderr or '').strip()}", path)
        try:
            size = int((probe.stdout or "").strip())
        except ValueError as e:
            raise OSError(f"cannot read '{path}' from sandbox: unexpected size {probe.stdout!r}") from e
        if size > limit:
            raise OSError(errno.EFBIG, f"File exceeds maximum download size of {limit} bytes", path)

        try:
            data = self._call(lambda sbx: sbx.files.read(normalized))
        except Exception as e:
            raise OSError(f"cannot read '{path}' from sandbox: {e}") from e
        if len(data) > limit:
            raise OSError(errno.EFBIG, f"File exceeds maximum download size of {limit} bytes", path)
        return bytes(data)

    # ── search ──────────────────────────────────────────────────────────

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        normalized = self._guard_traversal(path)
        r = self._search(remote_list_dir_command(normalized, max_depth), f"list_dir {normalized}")
        return parse_remote_list_dir_output(r.stdout or "", normalized, pipeline_exit_code=r.exit_code)

    def glob(
        self,
        path: str,
        pattern: str,
        *,
        include_dirs: bool = False,
        max_results: int = 200,
    ) -> tuple[list[str], bool]:
        resolved = self._guard_traversal(path)
        types = ("f", "d") if include_dirs else ("f",)
        type_expr = " -o ".join(f"-type {t}" for t in types)
        hard_limit = max(max_results * 4, max_results + 50)
        # -H follows a symlinked search root, as list_dir does.
        search = f"find -H {shlex.quote(resolved)} \\( {type_expr} \\) -print 2>/dev/null"
        r = self._search(remote_search_command(search, resolved, limit=hard_limit), f"glob under {resolved}")
        output = parse_remote_search_output(r.stdout, resolved, tool="find", limit=hard_limit)

        matches: list[str] = []
        root = resolved.rstrip("/") or "/"
        root_prefix = root if root == "/" else f"{root}/"
        # Records are LF-delimited; other splitlines() boundaries can be path characters.
        for entry in output.text.split("\n"):
            if not entry or (entry != root and not entry.startswith(root_prefix)):
                continue
            if should_ignore_path_under_root(entry, root):
                continue
            rel_path = entry[len(root) :].lstrip("/")
            if rel_path and path_matches(pattern, rel_path):
                matches.append(entry)
                # Look one match past the cap so an exactly-full result is not
                # reported as truncated.
                if len(matches) > max_results:
                    return matches[:max_results], True
        return matches, output.truncated

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[GrepMatch], bool]:
        if not literal:
            re.compile(pattern, 0 if case_sensitive else re.IGNORECASE)

        resolved = self._guard_traversal(path)
        # -H always prints the filename, so a single-file root still parses as
        # file:line:text. --include and -m are left out for busybox portability;
        # glob scoping and the result cap are applied below.
        flags = ["-r", "-H", "-n", "-I"]
        if not case_sensitive:
            flags.append("-i")
        flags.append("-F" if literal else "-E")
        total_cap = max(max_results * 4, max_results + 50)
        search = "grep " + " ".join(flags) + f" -e {shlex.quote(pattern)} {shlex.quote(resolved)} 2>/dev/null"
        r = self._search(remote_search_command(search, resolved, limit=total_cap), f"grep under {resolved}")
        output = parse_remote_search_output(r.stdout, resolved, tool="grep", limit=total_cap)

        root = resolved.rstrip("/") or "/"
        root_prefix = root if root == "/" else f"{root}/"
        matches: list[GrepMatch] = []
        for raw in output.text.split("\n"):
            try:
                file_path, line_no_str, line_text = raw.split(":", 2)
                line_number = int(line_no_str)
            except ValueError:
                continue
            if should_ignore_path_under_root(file_path, root):
                continue
            if glob is not None:
                if file_path != root and not file_path.startswith(root_prefix):
                    continue
                rel_path = posixpath.basename(file_path) if file_path == root else file_path[len(root) :].lstrip("/")
                if not path_matches(glob, rel_path):
                    continue
            matches.append(GrepMatch(path=file_path, line_number=line_number, line=truncate_line(line_text)))
            if len(matches) > max_results:
                return matches[:max_results], True
        return matches, output.truncated
