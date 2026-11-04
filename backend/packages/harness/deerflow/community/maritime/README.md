# Maritime backend

Runs each DeerFlow sandbox as a [Maritime](https://maritime.sh) cloud sandbox:
a Linux microVM created from a stock template, a saved snapshot, or a public OCI
image, with no daemon or local virtualization to manage. A cloud-hosted
alternative to the container-based AIO sandbox, like the E2B, Tenki and
OpenSandbox backends.

## Configuration

```yaml
sandbox:
  use: deerflow.community.maritime:MaritimeSandboxProvider
  api_key: $MARITIME_API_KEY  # optional; the SDK reads MARITIME_API_KEY when omitted
  base_url: https://api.maritime.sh  # optional; MARITIME_API_URL or the SDK default when omitted
  template: base              # stock template, saved snapshot name, or public OCI image (default: base)
  size: small                 # small | medium | large | xlarge (default: small)
  idle_timeout: 300           # seconds without activity before Maritime pauses or ends it (60 to 86400; default: 300)
  on_idle: pause              # pause (keep state) or kill (end the sandbox) when idle (default: pause)
  for_end_user: true          # keep Maritime account secrets out of the sandbox (default: true)
  environment:                # injected into every command (and set at create time)
    PYTHONUNBUFFERED: "1"
```

Install the optional SDK before selecting this provider:

```bash
pip install "deerflow-harness[maritime]"
```

The API key needs the `sandboxes` scope, and the account needs a sandbox plan;
without one, create fails with `plan_required`.

## Design

| File | Role |
| --- | --- |
| `provider.py` | `SandboxProvider` lifecycle: one named sandbox per user and thread |
| `sandbox.py`  | `Sandbox` adapter: `execute_command` and file operations |

- **One sandbox per thread.** The sandbox name is `deer-flow-` plus
  `derive_sandbox_scope_token(user_id, thread_id)`. Maritime's create returns
  the existing sandbox when one with that name has not ended, so the same thread
  gets the same machine across turns and across Gateway restarts. The thread and
  user ids are stored as sandbox metadata, and `user_id` as the Maritime
  `end_user`, so sandboxes can be filtered per user with the Maritime list API.
- **Release relies on the idle timer.** `release()` stops tracking the sandbox
  and makes no remote call. After `idle_timeout` seconds without activity,
  Maritime pauses it (memory, processes and disk are kept; no compute is
  billed while paused) or ends it when `on_idle: kill`. The next command or
  file call resumes a paused sandbox, so no in-process warm pool is needed.
- **Shutdown pauses.** `shutdown()` pauses every tracked sandbox at once and
  refuses later acquires. `reset()` refuses later acquires and leaves the
  sandboxes to the idle timer.
- **Ended sandboxes are replaced.** When an operation reports `sandbox_ended`
  or `sandbox_not_found`, the adapter is evicted and the next acquire creates a
  new sandbox under the same name.
- Paused sandboxes are kept until they are deleted; storage above the
  account's free allowance is billed. Use `on_idle: kill` when threads should
  not keep their machine after going idle.

## Contract coverage

The full `Sandbox` surface is implemented:

- `execute_command`: `sandbox.run(command)`, which runs the string under
  `/bin/sh -c`, with per-call env and timeout. The timeout is rounded up to
  whole seconds and clamped to Maritime's 1 to 3600 range. A timed-out command
  keeps its partial output and reports `Error: command timed out` with
  `Exit Code: 124`. Commands are not retried by the adapter; the SDK retries a
  lost request with the same idempotency key, so a command never runs twice.
- `read_file` / `write_file` / `update_file`: native `files.read` /
  `files.write`. The files API acts as root and creates parent directories, so
  `/mnt/user-data` is used as is, with no path remapping. Appends are a
  read-modify-write under a per-sandbox lock.
- `download_file`: restricted to `/mnt/user-data`. The file size is checked
  with `stat` before the transfer and again after it, so an oversized file is
  refused without being read.
- `list_dir` / `glob` / `grep`: `find` / `grep` with busybox-portable flags,
  parsed with the shared `deerflow.sandbox` helpers, as in `tenki` and
  `e2b_sandbox`.

Stock templates run commands as `user` with passwordless `sudo`, and `/mnt` is
owned by root. At acquire the provider creates
`/mnt/user-data/{workspace,uploads,outputs}` and gives it to the run user, so
shell commands can write there too. This step is best effort; the file APIs do
not depend on it.

## Status

Unit tests run in CI without `maritime` installed and mock the SDK.
`test_integration_real_sandbox` runs against a real Maritime sandbox when
`MARITIME_API_KEY` is set and `DEER_FLOW_RUN_LIVE_TESTS=1`.
