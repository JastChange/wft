from pathlib import Path

import pytest

from wft.ids import new_uuid7
from wft.storage.atomic import (
    UnsupportedSchemaVersion,
    iter_json_files,
    read_versioned_json,
    write_atomic_json,
)
from wft.storage.layout import TaskLayout, node_key


def test_node_key_is_stable_and_not_raw_user_path() -> None:
    assert node_key("node-a").startswith("node-a-")
    assert node_key("node-a") == node_key("node-a")
    assert node_key("../node-a") != "../node-a"


def test_task_layout_never_joins_unvalidated_components(tmp_path: Path) -> None:
    layout = TaskLayout(tmp_path, new_uuid7())

    assert layout.task_json.parent == layout.task_dir
    with pytest.raises(ValueError, match="node key"):
        layout.node_dir("../outside")
    with pytest.raises(ValueError, match="script ID"):
        layout.script_dir(node_key("node-a"), "../outside")


def test_atomic_json_has_sorted_keys_newline_and_no_partial(tmp_path: Path) -> None:
    path = tmp_path / "task.json"

    write_atomic_json(path, {"schema_version": "1.0", "z": 1, "a": 2})

    assert path.read_text() == ('{\n  "a": 2,\n  "schema_version": "1.0",\n  "z": 1\n}\n')
    assert path.stat().st_mode & 0o777 == 0o600
    assert not list(tmp_path.glob("*.partial"))


def test_reader_rejects_unknown_schema_major(tmp_path: Path) -> None:
    path = tmp_path / "task.json"
    path.write_text('{"schema_version":"2.0"}\n')

    with pytest.raises(UnsupportedSchemaVersion):
        read_versioned_json(path)


def test_reader_rejects_missing_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "task.json"
    path.write_text("{}\n")

    with pytest.raises(UnsupportedSchemaVersion):
        read_versioned_json(path)


def test_iteration_ignores_partial_and_hidden_files(tmp_path: Path) -> None:
    (tmp_path / ".task.json.dead.partial").write_text("{}")
    (tmp_path / ".hidden.json").write_text("{}")
    expected = tmp_path / "task.json"
    expected.write_text("{}")

    assert list(iter_json_files(tmp_path)) == [expected]
