# WFT Node Diagnostics M2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [x]) syntax for tracking.

**Goal:** Deliver manual diagnostic task creation, concurrent node execution through AsyncSSH, authoritative file storage, terminal conclusions, cancellation/abandoned handling, and M2 CLI operations required by AC-007 through AC-017.

**Architecture:** The tasks module owns immutable task models, state transitions, concurrency, conclusions, and exit codes. TaskStore is the only authoritative persistence adapter and writes versioned JSON plus independent raw byte streams; AsyncSSHNodeExecutor is constructed with that store and hides Host Key acceptance, SFTP upload, interpreter checks, streaming output, timeout, and cleanup. No SQLite, Web, scheduler, retry, recovery, notification, AI, or Obsidian behavior is introduced in M2.

**Tech Stack:** Python 3.12, Typer, Pydantic 2, AsyncSSH 2, asyncio, PyYAML, pytest, Ruff, mypy, Git CLI.

---

## Locked decisions

- A new task always receives a new UUIDv7 and never resumes an earlier task.
- Task types are installation_validation and fault_diagnosis.
- Nodes run concurrently behind an asyncio semaphore; scripts for one node run serially in Manifest or plan order.
- No SSH, node, or script operation is automatically retried.
- Unknown Host Keys are accepted automatically with known_hosts=None and the actual algorithm/fingerprint is persisted.
- Unexpected script exit codes are completed checks with check_passed=false, not infrastructure failures.
- Connection, interpreter, timeout, upload, and system failures make an installation result INCONCLUSIVE.
- A fault diagnosis can be COMPLETED while evidence indicates problems; execution status and diagnosis are separate.
- Raw stdout, stderr, and execution logs are streamed to independent files without a product size cap.
- JSON writes use a same-directory partial file, flush, fsync, atomic replace, and parent-directory fsync.
- Ctrl-C marks unfinished nodes and the task CANCELLED and exits 2. The next command marks abandoned RUNNING tasks FAILED and never resumes them.
- Task deletion requires a reason plus interactive confirmation, then leaves no tombstone or audit record.
- Storage admission uses a same-filesystem fsync write probe. ENOSPC fails the current operation and later task creation is rejected while the probe cannot complete.
- The accepted release boundary does not add disk-fill, kill -9, or large-output stress tests in M2.

## Locked file map

    src/wft/
    ├── ids.py                         UUIDv7 generation
    ├── clock.py                       UTC RFC 3339 timestamps
    ├── tasks/
    │   ├── models.py                  task/node/script snapshots and results
    │   ├── state.py                   transitions, conclusions, exit codes
    │   ├── create.py                  selection validation and task creation
    │   └── runner.py                  concurrency, cancellation, abandoned tasks
    ├── execution/
    │   ├── interface.py               NodeExecutor protocol
    │   ├── fake.py                    deterministic executor for unit tests
    │   └── asyncssh_executor.py       production SSH/SFTP adapter
    ├── storage/
    │   ├── atomic.py                  versioned atomic JSON
    │   ├── layout.py                  safe task/node/script paths
    │   ├── raw_streams.py             streaming raw writer and file references
    │   └── task_store.py              authoritative TaskStore
    └── cli/
        └── commands/
            ├── run.py                 installation/fault task commands
            └── tasks.py               list/show/delete commands

    tests/
    ├── tasks/
    ├── storage/
    ├── execution/
    └── cli/

## Public model and interface vocabulary

The implementation must keep these names consistent across tasks:

    class TaskType(StrEnum):
        INSTALLATION_VALIDATION = "installation_validation"
        FAULT_DIAGNOSIS = "fault_diagnosis"

    class TaskStatus(StrEnum):
        PENDING = "PENDING"
        RUNNING = "RUNNING"
        COMPLETED = "COMPLETED"
        FAILED = "FAILED"
        CANCELLED = "CANCELLED"

    class NodeStatus(StrEnum):
        PENDING = "PENDING"
        RUNNING = "RUNNING"
        COMPLETED = "COMPLETED"
        FAILED = "FAILED"
        CANCELLED = "CANCELLED"

    class ScriptStatus(StrEnum):
        PENDING = "PENDING"
        RUNNING = "RUNNING"
        COMPLETED = "COMPLETED"
        TIMEOUT = "TIMEOUT"
        FAILED = "FAILED"
        CANCELLED = "CANCELLED"

    class InstallationConclusion(StrEnum):
        PASS = "PASS"
        FAIL = "FAIL"
        INCONCLUSIVE = "INCONCLUSIVE"

    class NodeExecutor(Protocol):
        async def execute(
            self,
            node: NodeSnapshot,
            snapshot: TaskScriptSnapshot,
        ) -> NodeExecutionResult:
            raise NotImplementedError

    class TaskStore:
        def create(self, task: TaskRecord, nodes: tuple[NodeSnapshot, ...],
                   snapshot: ScriptSnapshot) -> None
        def load(self, task_id: str) -> TaskRecord
        def load_node(self, task_id: str, node_key: str) -> NodeSnapshot
        def commit_script_result(self, result: ScriptExecutionResult) -> None
        def commit_node_result(self, result: NodeExecutionResult) -> None
        def update_task(self, task: TaskRecord) -> None
        def iterate_tasks(self) -> Iterator[TaskRecord]
        def delete(self, task_id: str) -> None
        def raw_writer(self, task_id: str, node_key: str,
                       script_id: str, filename: str) -> RawWriter

---

## Task 1: Add M2 dependencies, UUIDv7, timestamps, and immutable domain models

**Files:**
- Modify: pyproject.toml
- Create: src/wft/ids.py
- Create: src/wft/clock.py
- Create: src/wft/tasks/__init__.py
- Create: src/wft/tasks/models.py
- Create: tests/tasks/test_models.py

- [x] **Step 1: Add failing UUIDv7 and model invariant tests**

Create tests/tasks/test_models.py with tests equivalent to:

    from datetime import UTC, datetime
    from pathlib import Path
    from uuid import UUID

    import pytest
    from pydantic import ValidationError

    from wft.ids import new_uuid7
    from wft.tasks.models import (
        InstallationConclusion,
        ScriptStatus,
        StreamReference,
        TaskStatus,
        TaskType,
    )

    def test_new_id_is_unique_uuid7() -> None:
        values = [new_uuid7() for _ in range(100)]
        assert len(values) == len(set(values))
        assert all(UUID(value).version == 7 for value in values)

    def test_stream_reference_requires_real_sha_and_nonnegative_size() -> None:
        reference = StreamReference(
            path="stdout.raw", size_bytes=0, sha256="0" * 64
        )
        assert reference.path == "stdout.raw"
        with pytest.raises(ValidationError):
            StreamReference(path="../secret", size_bytes=-1, sha256="bad")

    def test_terminal_script_invariants() -> None:
        from wft.tasks.models import ScriptExecutionResult
        common = {
            "schema_version": "1.0",
            "task_id": new_uuid7(),
            "node_key": "node-a-01234567",
            "script_id": "memory",
            "script_sha256": "0" * 64,
            "interpreter": "bash",
            "started_at": "2026-08-07T00:00:00Z",
            "finished_at": "2026-08-07T00:00:01Z",
            "expected_exit_codes": (0,),
            "stdout": StreamReference(path="stdout.raw", size_bytes=0, sha256="0" * 64),
            "stderr": StreamReference(path="stderr.raw", size_bytes=0, sha256="0" * 64),
            "execution_log": StreamReference(
                path="execution.log", size_bytes=0, sha256="0" * 64
            ),
        }
        completed = ScriptExecutionResult(
            **common, status=ScriptStatus.COMPLETED, exit_code=0,
            check_passed=True, failure=None
        )
        assert completed.check_passed is True
        with pytest.raises(ValidationError):
            ScriptExecutionResult(
                **common, status=ScriptStatus.TIMEOUT, exit_code=None,
                check_passed=True, failure={"code": "TIMEOUT", "message": "late"}
            )

- [x] **Step 2: Run RED**

Run:

    .venv/bin/pytest tests/tasks/test_models.py -q

Expected: collection fails because wft.ids and wft.tasks do not exist.

- [x] **Step 3: Add AsyncSSH and implement the core utilities**

Add this runtime dependency to pyproject.toml:

    "asyncssh>=2.22,<3",

Implement new_uuid7 without a new library: use current Unix milliseconds for the high 48 bits, set version bits to 7, set RFC 4122 variant bits, and fill the remaining random bits with secrets.randbits. Return the canonical UUID string.

Implement utc_now() as a timezone-aware UTC datetime and format_timestamp() as RFC 3339 with a Z suffix.

- [x] **Step 4: Implement frozen Pydantic models**

Create models for FailureRecord, StreamReference, HostKeyRecord, ScriptSelection, TaskScriptSnapshot, NodeSnapshot, ScriptExecutionResult, CleanupResult, NodeExecutionResult, TaskSummary, and TaskRecord.

Every JSON model must:
- use ConfigDict(frozen=True, extra="forbid");
- include schema_version="1.0";
- validate timestamp strings as timezone-aware RFC 3339;
- reject path traversal in StreamReference.path;
- enforce that check_passed is Boolean only for COMPLETED scripts;
- enforce installation_conclusion is null for fault diagnosis node results;
- keep private_key_path as a path string reference and never load its contents.

- [x] **Step 5: Run GREEN and static checks**

Run:

    .venv/bin/pytest tests/tasks/test_models.py -q
    .venv/bin/ruff format --check src tests
    .venv/bin/ruff check src tests
    .venv/bin/mypy src

Expected: all pass.

- [x] **Step 6: Commit**

    git add pyproject.toml src/wft/ids.py src/wft/clock.py src/wft/tasks tests/tasks/test_models.py
    git commit -m "feat: define M2 task domain models"

## Task 2: Implement safe layout, atomic JSON, schema gates, and raw byte writers

**Files:**
- Create: src/wft/storage/__init__.py
- Create: src/wft/storage/layout.py
- Create: src/wft/storage/atomic.py
- Create: src/wft/storage/raw_streams.py
- Create: tests/storage/test_atomic.py
- Create: tests/storage/test_raw_streams.py

- [x] **Step 1: Write failing path and atomic-file tests**

Create tests which prove:

    def test_node_key_is_stable_and_not_raw_user_path() -> None:
        assert node_key("node-a").startswith("node-a-")
        assert node_key("node-a") == node_key("node-a")
        assert node_key("../node-a") != "../node-a"

    def test_atomic_json_has_sorted_keys_newline_and_no_partial(tmp_path: Path) -> None:
        path = tmp_path / "task.json"
        write_atomic_json(path, {"schema_version": "1.0", "z": 1, "a": 2})
        assert path.read_text() == (
            '{\n  "a": 2,\n  "schema_version": "1.0",\n  "z": 1\n}\n'
        )
        assert not list(tmp_path.glob("*.partial"))

    def test_reader_rejects_unknown_schema_major(tmp_path: Path) -> None:
        path = tmp_path / "task.json"
        path.write_text('{"schema_version":"2.0"}\n')
        with pytest.raises(UnsupportedSchemaVersion):
            read_versioned_json(path)

    def test_iteration_ignores_partial_files(tmp_path: Path) -> None:
        (tmp_path / ".task.json.dead.partial").write_text("{}")
        assert list(iter_json_files(tmp_path)) == []

- [x] **Step 2: Run RED**

Run:

    .venv/bin/pytest tests/storage/test_atomic.py -q

Expected: import failure for wft.storage.

- [x] **Step 3: Implement safe layout and atomic JSON**

TaskLayout must derive every path from an already-validated task UUID, a stable node_key(), and validated script IDs. Never join raw CLI input directly.

write_atomic_json must:
1. create a random same-directory hidden partial file with mode 0600;
2. write UTF-8 indented sorted JSON plus one newline;
3. flush and fsync the file;
4. os.replace to the final path;
5. fsync the parent directory;
6. delete the partial file in finally.

read_versioned_json must reject a missing schema_version and any major version other than 1. iter_json_files must only return final .json files.

- [x] **Step 4: Write RED tests for streaming raw bytes**

Tests must write non-UTF-8 chunks through RawWriter:

    writer = RawWriter(tmp_path / "stdout.raw")
    writer.write(b"prefix\xff")
    writer.write(b"suffix")
    reference = writer.finish()
    assert (tmp_path / "stdout.raw").read_bytes() == b"prefix\xffsuffix"
    assert reference.size_bytes == 13
    assert reference.sha256 == hashlib.sha256(b"prefix\xffsuffix").hexdigest()

Also verify abort() closes and removes a partial raw file, while finish() fsyncs and publishes it.

- [x] **Step 5: Implement RawWriter and storage probe**

RawWriter owns a same-directory partial file, an incremental SHA-256, and a byte counter. It never decodes output. storage_write_probe(data_dir) writes and fsyncs a small hidden file in data_dir, removes it, and maps errno.ENOSPC to StorageFullError.

- [x] **Step 6: Run GREEN and commit**

Run:

    .venv/bin/pytest tests/storage/test_atomic.py tests/storage/test_raw_streams.py -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src

Commit:

    git add src/wft/storage tests/storage
    git commit -m "feat: add authoritative storage primitives"

## Task 3: Create and load authoritative task snapshots

**Files:**
- Create: src/wft/storage/task_store.py
- Create: src/wft/tasks/create.py
- Create: tests/storage/test_task_store.py
- Create: tests/tasks/test_create.py

- [x] **Step 1: Write failing TaskStore creation tests**

Build a real M1 ScriptSnapshot fixture and two Node values, then call create_task(). Verify this exact layout exists:

    tasks/<task-id>/task.json
    tasks/<task-id>/snapshot/inventory.json
    tasks/<task-id>/snapshot/manifest.json
    tasks/<task-id>/snapshot/scripts/check/<sha256>/source
    tasks/<task-id>/nodes/<node-key>/node.json
    tasks/<task-id>/nodes/<node-key>/result.json

Verify:
- task status starts PENDING;
- each node result starts PENDING;
- inventory.json contains connection snapshots but no private-key body;
- all nodes reference the same commit and copied script source;
- a second call creates a different UUIDv7;
- unsupported node OS or script selection fails before a task directory is published.

- [x] **Step 2: Run RED**

Run:

    .venv/bin/pytest tests/storage/test_task_store.py tests/tasks/test_create.py -q

Expected: import failure for TaskStore and create_task.

- [x] **Step 3: Implement TaskStore.create and task snapshot publication**

TaskStore root is data_dir/tasks. create_task receives a CreateTaskRequest containing:
- TaskType;
- NodeSelector;
- selected Node tuple;
- one ScriptSnapshot;
- either ordered script_ids or one plan_id;
- concurrency 1 through 50.

Resolve the selected scripts once, validate every script supports every selected node OS, and copy source files from the immutable M1 snapshot into a hidden task staging directory. Write task, inventory, manifest, node, and initial node-result JSON there. Fsync files and directories, then atomically rename the staging task directory to tasks/<task-id>.

If publication fails, remove only the staging directory. Never overwrite an existing task ID.

- [x] **Step 4: Implement load, iterate, and schema validation**

TaskStore.load and load_node must use read_versioned_json and Pydantic validation. iterate_tasks must:
- ignore hidden partial directories and partial files;
- return tasks ordered newest first by created_at and task_id;
- reject unknown schema major versions instead of silently migrating.

- [x] **Step 5: Run GREEN and commit**

Run:

    .venv/bin/pytest tests/storage/test_task_store.py tests/tasks/test_create.py -q
    .venv/bin/pytest tests/config tests/inventory tests/scripts -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src

Commit:

    git add src/wft/storage/task_store.py src/wft/tasks/create.py tests/storage tests/tasks
    git commit -m "feat: publish authoritative task snapshots"

## Task 4: Define state transitions, conclusions, summaries, and exit codes

**Files:**
- Create: src/wft/tasks/state.py
- Create: tests/tasks/test_state.py

- [x] **Step 1: Write table-driven RED tests**

Cover:
- PENDING -> RUNNING -> COMPLETED;
- PENDING/RUNNING -> CANCELLED;
- RUNNING -> FAILED;
- terminal states cannot transition again;
- all completed passing scripts -> PASS;
- any completed check_passed=false -> FAIL;
- connection/interpreter/timeout/system failure -> INCONCLUSIVE;
- fault diagnosis installation_conclusion is null;
- completed no-problem task -> exit 0;
- completed problem or FAIL/INCONCLUSIVE -> exit 1;
- FAILED/CANCELLED/nonterminal task -> exit 2.

Example:

    @pytest.mark.parametrize(
        ("checks", "expected"),
        [
            ((completed(True), completed(True)), InstallationConclusion.PASS),
            ((completed(True), completed(False)), InstallationConclusion.FAIL),
            ((timeout(),), InstallationConclusion.INCONCLUSIVE),
        ],
    )
    def test_installation_conclusion(checks, expected) -> None:
        assert conclude_installation(checks) is expected

- [x] **Step 2: Run RED**

Run:

    .venv/bin/pytest tests/tasks/test_state.py -q

Expected: import failure for wft.tasks.state.

- [x] **Step 3: Implement pure state functions**

Implement:
- transition_task(task, target, now, failure=None);
- conclude_installation(script_results);
- summarize_nodes(node_results);
- task_exit_code(task, node_results, script_results);
- finish_task(task, node_results, script_results, now).

Keep these functions pure and independent of TaskStore, AsyncSSH, Typer, and SQLite. Unexpected exit codes produce FAIL/problem semantics, while transport failures produce INCONCLUSIVE.

- [x] **Step 4: Run GREEN and commit**

    .venv/bin/pytest tests/tasks/test_state.py -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src
    git add src/wft/tasks/state.py tests/tasks/test_state.py
    git commit -m "feat: enforce task state and conclusion semantics"

## Task 5: Run nodes concurrently with failure isolation and no retries

**Files:**
- Create: src/wft/execution/__init__.py
- Create: src/wft/execution/interface.py
- Create: src/wft/execution/fake.py
- Create: src/wft/tasks/runner.py
- Create: tests/tasks/test_runner.py

- [x] **Step 1: Write RED concurrency and ordering tests**

Use FakeNodeExecutor with per-node delays and scripted outcomes. Verify:
- active execute calls never exceed requested concurrency;
- every node is invoked exactly once;
- a failed node does not cancel another node;
- all selected nodes receive the same TaskScriptSnapshot commit SHA;
- scripts recorded for a node preserve Manifest/plan order;
- no retry occurs after exceptions;
- completed node results are committed before the task finishes.

Use an observing TaskStore subclass or event list which records commit_script_result, commit_node_result, and update_task calls without mocking asyncio.

- [x] **Step 2: Run RED**

Run:

    .venv/bin/pytest tests/tasks/test_runner.py -q

Expected: import failure for runner and execution interface.

- [x] **Step 3: Implement NodeExecutor protocol and FakeNodeExecutor**

NodeExecutor.execute accepts one NodeSnapshot and the task-owned TaskScriptSnapshot. FakeNodeExecutor records active count, call count, and commit SHA, sleeps deterministic delays, and returns configured NodeExecutionResult values. It must not contain retry logic.

- [x] **Step 4: Implement run_task**

run_task must:
1. load and transition the task to RUNNING;
2. create one asyncio task per PENDING node behind Semaphore(task.concurrency);
3. call executor.execute exactly once per node;
4. convert an executor exception into a FAILED node result without cancelling peers;
5. commit every node result independently;
6. aggregate summary, conclusions, and exit code;
7. transition the task to COMPLETED unless the runner itself cannot continue.

A node-level failure does not make the task FAILED. Task FAILED is reserved for a store/system failure that prevents task execution from completing.

- [x] **Step 5: Implement cancellation behavior**

On asyncio.CancelledError:
- cancel outstanding node coroutines;
- retain already committed node results;
- write CANCELLED results for PENDING/RUNNING nodes;
- transition task to CANCELLED with exit code 2;
- re-raise cancellation so the CLI maps it to 2.

- [x] **Step 6: Run GREEN and commit**

    .venv/bin/pytest tests/tasks/test_runner.py -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src
    git add src/wft/execution src/wft/tasks/runner.py tests/tasks/test_runner.py
    git commit -m "feat: run diagnostic nodes with failure isolation"

## Task 6: Execute scripts through AsyncSSH/SFTP

**Files:**
- Modify: pyproject.toml
- Modify and reactivate: tests/support/ssh_test_server.py
- Modify: pyproject.toml Ruff exclusion for tests/support
- Create: src/wft/execution/asyncssh_executor.py
- Create: tests/execution/conftest.py
- Create: tests/execution/test_asyncssh_executor.py

- [x] **Step 1: Install AsyncSSH and reactivate the test server**

Reinstall the editable package after adding AsyncSSH:

    .venv/bin/python -m pip install -e '.[test]'

Remove tests/support from the global Ruff exclusion. Add narrow per-file ignores only for fixture operations which intentionally use blocking local filesystem calls inside the in-process test server. Format and type the reusable server or exclude it from mypy packages, which remain src only.

- [x] **Step 2: Write RED SSH happy-path test**

Generate an ed25519 server key and client key with AsyncSSH, start RunningServer, create a NodeSnapshot pointing at 127.0.0.1 and the ephemeral port, and execute a script which writes distinct binary stdout/stderr.

Verify:
- connection succeeds with no known_hosts prompt;
- HostKeyRecord contains algorithm, SHA256 fingerprint, and accepted_automatically=true;
- script source is uploaded with SFTP;
- interpreter is checked;
- stdout.raw and stderr.raw preserve exact bytes;
- execution.log exists;
- ScriptExecutionResult hashes and sizes match files;
- remote task directory is removed;
- cleanup attempted/succeeded are true.

- [x] **Step 3: Run RED**

Run with loopback permission:

    .venv/bin/pytest tests/execution/test_asyncssh_executor.py::test_executes_and_streams_raw_bytes -q

Expected: import failure for AsyncSSHNodeExecutor.

- [x] **Step 4: Implement AsyncSSHNodeExecutor connection and Host Key capture**

Construct with TaskStore and connect_timeout. Call asyncssh.connect with:
- host, port, username;
- client_keys containing only the node private key path;
- known_hosts=None;
- agent_path=None;
- preferred_auth=("publickey",);
- login_timeout=connect_timeout;
- encoding=None.

After connection, call get_server_host_key(), persist get_algorithm() and get_fingerprint("sha256"), and mark accepted_automatically=true.

- [x] **Step 5: Implement SFTP upload, integrity, and sequential execution**

For each selected script in order:
1. create a task/node-specific remote directory under /tmp with mode 0700;
2. SFTP upload the task-owned source;
3. run remote sha256sum and compare the manifest hash;
4. run command -v for the declared interpreter;
5. invoke the interpreter with the uploaded file;
6. stream stdout and stderr chunks concurrently to TaskStore RawWriter objects;
7. write phase/timing/errors to execution.log;
8. classify expected exit code as check_passed=true and unexpected exit code as false;
9. commit the script result before continuing.

All remote command arguments must be shell-quoted. Do not read output into a single bytes object.

- [x] **Step 6: Write and pass timeout/failure/cleanup tests**

Add tests for:
- missing interpreter -> FAILED script, installation INCONCLUSIVE, later script continues;
- timeout -> TIMEOUT, process group termination, later script continues;
- unexpected nonzero -> COMPLETED and check_passed=false;
- connection failure -> one attempt, FAILED node, installation INCONCLUSIVE;
- cleanup failure is recorded without replacing script results.

Run:

    .venv/bin/pytest tests/execution -q

Expected: all pass with loopback permission.

- [x] **Step 7: Run full checks and commit**

    .venv/bin/pytest tests/execution tests/tasks tests/storage -q
    .venv/bin/ruff format --check src tests
    .venv/bin/ruff check src tests
    .venv/bin/mypy src

Commit:

    git add pyproject.toml src/wft/execution tests/execution tests/support/ssh_test_server.py
    git commit -m "feat: execute diagnostic scripts over AsyncSSH"

## Task 7: Mark abandoned tasks and implement authoritative deletion

**Files:**
- Modify: src/wft/tasks/runner.py
- Modify: src/wft/storage/task_store.py
- Create: tests/tasks/test_abandoned.py
- Create: tests/storage/test_delete.py

- [x] **Step 1: Write RED abandoned-task tests**

Create a RUNNING task with one completed node and one RUNNING node. Call mark_abandoned_tasks_failed(store).

Verify:
- task becomes FAILED, finished_at is set, exit_code is 2;
- completed node result is byte-for-byte unchanged;
- unfinished node becomes FAILED with an ABANDONED failure;
- executor is never invoked;
- a second call changes zero tasks.

- [x] **Step 2: Implement mark_abandoned_tasks_failed**

Iterate authoritative task.json files. For each RUNNING task, independently update unfinished node results, then update the task to FAILED. Do not enqueue work or call NodeExecutor.

- [x] **Step 3: Write RED deletion tests**

TaskStore.delete must:
- accept only a validated task UUID;
- remove the complete authoritative directory;
- leave no tombstone, reason file, audit event, or partial directory;
- fail for missing task IDs;
- never follow a symlink outside tasks root.

The reason and second confirmation belong to the CLI adapter and are intentionally not passed into TaskStore.

- [x] **Step 4: Implement safe deletion and pass tests**

Resolve the expected task directory and verify its parent is exactly tasks root before shutil.rmtree. Reject symlink task roots. Fsync tasks root after deletion.

Run:

    .venv/bin/pytest tests/tasks/test_abandoned.py tests/storage/test_delete.py -q

- [x] **Step 5: Commit**

    git add src/wft/tasks/runner.py src/wft/storage/task_store.py tests/tasks tests/storage
    git commit -m "feat: fail abandoned tasks and delete authoritative records"

## Task 8: Expose M2 run and task CLI commands

**Files:**
- Modify: src/wft/cli/app.py
- Create: src/wft/cli/commands/run.py
- Create: src/wft/cli/commands/tasks.py
- Create: tests/cli/test_m2_commands.py

- [x] **Step 1: Write RED help and command contract tests**

The final M2 surface is:

    wft run installation-validation --config PATH SELECTOR SCRIPT_SELECTION
    wft run fault-diagnosis --config PATH SELECTOR SCRIPT_SELECTION
    wft task list --config PATH [--json]
    wft task show TASK_ID --config PATH [--json]
    wft task delete TASK_ID --config PATH --reason TEXT

Selectors:
- repeatable --node, --group, --tag, or --all;
- exactly one of repeatable --script or --plan;
- optional --concurrency 1 through 50;
- optional --allow-cached-scripts;
- optional --json.

Verify help exposes run/task but no scheduler, resume, retry, Web, analyze, export, or index commands.

- [x] **Step 2: Implement the run composition root**

For each run command:
1. load config and mark abandoned tasks;
2. load/select inventory;
3. prepare the latest script snapshot once;
4. create one task and authoritative snapshot;
5. construct AsyncSSHNodeExecutor with TaskStore;
6. asyncio.run(run_task(...));
7. print task ID/status/conclusion summary;
8. return the task exit code.

Catch KeyboardInterrupt and cancellation, persist CANCELLED, and exit 2. Configuration, Git, storage admission, or runner-system failures exit 2. Never call prepare_snapshot a second time inside a task.

- [x] **Step 3: Implement list/show/delete**

list reads TaskStore.iterate_tasks and prints task IDs, type, status, created/finished times, and summary. show reads authoritative task/node/script JSON without SQLite.

delete must reject an empty reason, display task ID plus reason, require typer.confirm, and call TaskStore.delete only after confirmation. In JSON mode deletion is not exposed because confirmation is interactive. No reason is persisted after successful deletion.

- [x] **Step 4: Add CLI integration tests**

Use a local Git script repository and FakeNodeExecutor injection seam to verify:
- installation PASS exits 0;
- installation FAIL and INCONCLUSIVE exit 1;
- fault diagnosis with unexpected check exits 1 but task status is COMPLETED;
- configuration or execution-system failure exits 2;
- Ctrl-C/cancellation exits 2 and preserves completed nodes;
- list/show read files after any SQLite file is deleted;
- delete rejection preserves files;
- confirmed deletion removes files and leaves no reason/tombstone.

- [x] **Step 5: Run GREEN and commit**

    .venv/bin/pytest tests/cli/test_m2_commands.py -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src
    git add src/wft/cli tests/cli/test_m2_commands.py
    git commit -m "feat: expose manual M2 diagnostic tasks"

## Task 9: Storage-failure and authoritative-source integration gates

**Files:**
- Create: tests/storage/test_authority.py
- Create: tests/storage/test_storage_failures.py
- Modify: src/wft/storage/atomic.py
- Modify: src/wft/storage/task_store.py

- [x] **Step 1: Prove file authority without SQLite**

Run a complete FakeNodeExecutor task, create then delete an unrelated data_dir/index.sqlite file, reconstruct a new TaskStore, and verify task, node, script results, raw files, conclusions, and hashes are unchanged.

No storage module may import sqlite3.

- [x] **Step 2: Prove independent commits survive cancellation**

Cancel a two-node task after the first node commits. Reopen TaskStore and verify:
- first node and all its script evidence remain terminal/readable;
- second node is CANCELLED;
- task is CANCELLED with exit code 2;
- no final JSON is a partial file.

- [x] **Step 3: Prove storage errors fail closed**

Use monkeypatch at the os.open/os.replace boundary to raise OSError(errno.ENOSPC). Verify:
- StorageFullError is raised;
- current task becomes FAILED when task.json can still be committed;
- no history directory is deleted;
- storage_write_probe prevents a new task from being published;
- no retry loop is entered.

The accepted M2 boundary uses deterministic fault injection rather than filling a real disk.

- [x] **Step 4: Run GREEN and commit**

    .venv/bin/pytest tests/storage/test_authority.py tests/storage/test_storage_failures.py -q
    .venv/bin/ruff check src tests
    .venv/bin/mypy src
    git add src/wft/storage tests/storage
    git commit -m "test: prove authoritative storage failure semantics"

## Task 10: M2 documentation, CI, and acceptance evidence

**Files:**
- Modify: README.md
- Modify: docs/operations/configuration.md
- Create: docs/operations/tasks.md
- Create: artifacts/acceptance/m2.md
- Modify: .github/workflows/ci.yml
- Modify: docs/superpowers/plans/2026-08-07-wft-node-diagnostics-m2.md

- [x] **Step 1: Document only available M2 behavior**

Document:
- Python 3.12 and AsyncSSH installation;
- installation_validation versus fault_diagnosis;
- selectors and script/plan selection;
- latest-fetch and explicit cache fallback;
- automatic Host Key acceptance;
- concurrency, serial script order, timeout, no retries;
- authoritative directory layout and unlimited raw-file policy;
- task list/show/delete and deletion reason semantics;
- Ctrl-C and abandoned-task behavior;
- exact exit codes 0/1/2;
- accepted absence of scheduler, resume, backup, migration, SQLite authority, Web writes, and disk-fill/kill-9/large-output release testing.

- [x] **Step 2: Record AC-007 through AC-017 evidence**

artifacts/acceptance/m2.md must map every acceptance ID to exact test names and commands. AC-004 must be upgraded from M1 partial to full task-level same-snapshot evidence. Record simulated AsyncSSH loopback tests accurately; do not describe them as real-node certification.

- [x] **Step 3: Update CI**

CI on Ubuntu 24.04 and macOS 14 must install AsyncSSH and run:
- Ruff format/check;
- mypy;
- full pytest including loopback AsyncSSH integration;
- Manifest validation;
- CLI smoke;
- dependency audit and full-history secret scan.

If macOS fixture behavior differs from Ubuntu, fix the fixture or adapter rather than skipping the platform.

- [x] **Step 4: Run milestone verification**

Run with loopback permission:

    .venv/bin/pytest -q
    .venv/bin/ruff format --check src tests
    .venv/bin/ruff check src tests
    .venv/bin/mypy src
    .venv/bin/wft --help
    git diff --check

Expected: all pass. Validate active Markdown links, JSON/YAML examples, authoritative schemas, and no placeholder markers.

- [x] **Step 5: Mark plan checkboxes and commit**

    git add README.md docs/operations artifacts/acceptance/m2.md .github/workflows/ci.yml docs/superpowers/plans/2026-08-07-wft-node-diagnostics-m2.md
    git commit -m "docs: publish M2 task execution acceptance evidence"

Stop after publishing to Draft PR #10. Update Issue #3 with verification evidence and leave its final confirmation checkbox open. Do not begin M3 until the user confirms M2.

---

## Self-review mapping

- FR-030: Tasks 3, 5, and 8.
- FR-040: Task 6.
- FR-050: Tasks 4, 5, 7, and 8.
- FR-060: Tasks 2, 3, 7, 8, and 9.
- AC-007: Task 6.
- AC-008: Tasks 5 and 6.
- AC-009: Task 6.
- AC-010: Tasks 5 and 6.
- AC-011 and AC-012: Task 4.
- AC-013: Tasks 4 and 8.
- AC-014 and AC-015: Tasks 5 and 7.
- AC-016 and AC-017: Tasks 2, 3, and 9.
- M1 AC-004 task-level closure: Tasks 3, 5, and 8.
- Delivery-plan list/show/delete: Tasks 7 and 8.
