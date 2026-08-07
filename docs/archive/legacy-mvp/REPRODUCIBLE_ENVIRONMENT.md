# Reproducible environment

## Local Mac baseline

The baseline is macOS arm64 with Python 3.14.6. From the repository root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[test]'
.venv/bin/python -m pytest -q
```

The hosted run #10 baseline was 344 tests. Commit 68a8d7d validated the
tests/docs fixture baseline at 352 tests; current HEAD d6e18e5 adds the
approved SSH error regressions and validates 354 tests. The in-process SSH fixture in
`tests/ssh_test_server.py` uses strict known-hosts verification, SFTP upload,
remote SHA-256 verification, and deterministic fault fixtures. HTTP fault
responses for webhook/LLM integration tests are in `tests/http_fault_stubs.py`;
it is a test facility, not a production HTTP client.

Run the focused facilities with:

```bash
.venv/bin/python -m pytest -q tests/test_ssh_execution.py tests/test_http_fault_stubs.py
```

Reproduce the audit gates locally:

```bash
.venv/bin/python -m pip install pip-audit==2.10.1
.venv/bin/python -m pip_audit .
audit_dir=$(mktemp -d)
curl -fsSL https://github.com/gitleaks/gitleaks/releases/download/v8.24.2/gitleaks_8.24.2_darwin_arm64.tar.gz -o "$audit_dir/gitleaks.tgz"
printf '%s  %s\n' '90d13686937ac7429b97a3acbf1e1d0ce90d92ae2d0cf46a690bd8ae5230bea0' "$audit_dir/gitleaks.tgz" | shasum -a 256 -c -
tar -xzf "$audit_dir/gitleaks.tgz" -C "$audit_dir" gitleaks
"$audit_dir/gitleaks" git --no-banner --redact --log-opts='--all'
```

## CI baseline

`.github/workflows/ci.yml` runs the full suite on `ubuntu-24.04` and `macos-14`
with Python 3.12, then runs fail-closed `pip-audit` and full-history Gitleaks.

## Runtime limits

Docker and Compose are not installed on the current Mac, so container
validation is pending an approved runtime and has not been run locally.
Five-node acceptance is pending non-sensitive node endpoints and connection
metadata from the owner; credentials must be injected securely and never
committed or sent in Raft.

Hosted baseline: run #10, <https://github.com/JastChange/wft/actions/runs/30812174950>,
passed Ubuntu 24.04, macOS 14, pip-audit, and full-history Gitleaks.
