from os import environ
from re import findall
from pathlib import Path
from subprocess import run
from sqrrl import SqrrlError
from json import dumps, loads
from pytest import raises, mark
from sys import executable, prefix
from sqrrl.generate import render, write
from annotationlib import Format, get_annotations
from sqrrl.schema import Schema, Table, integer, text

def test_deterministic_generation_and_handwritten_protection(tmp_path, schema):
    assert render(schema) == render(Schema(tuple(reversed(schema.tables))))
    path = write(tmp_path / "models.py", schema)
    modification = path.stat().st_mtime_ns
    write(path, schema, check=True)
    assert path.stat().st_mtime_ns == modification
    path.write_text("# handwritten\n", encoding="utf-8")
    with raises(SqrrlError, match="handwritten"):
        write(path, schema)

    with raises(SqrrlError, match="stale"):
        write(path, schema, check=True)

    assert path.read_text() == "# handwritten\n"
    with raises(SqrrlError, match="stale"):
        write(tmp_path / "missing.py", schema, check=True)

@mark.parametrize("checker", ["mypy", "pyright"])
def test_generated_consumer_types(tmp_path, schema, checker):
    collision = Table(
            "collisions",
            "Collision",
            (
                integer("id").primary_key(),
                text("Column"),
                text("str"),
                text("Optional").nullable(),
                text("Collision"),
                text("value"),
            ),
    )
    schema = Schema(schema.tables + (collision,))
    write(tmp_path / "models.py", schema)
    root = Path(__file__).resolve().parents[1]
    environment_root = Path(prefix)
    config = {
        "pythonVersion": "3.14",
        "typeCheckingMode": "strict",
        "extraPaths": [str(root)],
        "venvPath": str(environment_root.parent),
        "venv": environment_root.name,
    }
    (tmp_path / "pyrightconfig.json").write_text(dumps(config), encoding="utf-8")
    valid = """from typing import assert_type
from sqrrl import Database
from models import Client, User, UserColumns, TaskColumns, SettingKey

async def consumer(database: Database) -> None:
    client = Client(database)
    user = await client.users.create(name="Ada")
    assert_type(user, User)
    assert_type(await client.users.query().where(UserColumns.name.eq("Ada")).all(), list[User])
    assert_type(await client.users.update(user.id, email=None), User)
    await client.tasks.query().where(TaskColumns.owner_id.eq(user.id)).all()
    await client.settings.get(SettingKey(user_id=user.id, key="theme"))
    await client.documents.create(id="text", body=b"bytes", score=1.0)
    collision = await client.collisions.create(Column="column", str="text", Optional=None, Collision="model", value="value")
    assert_type(collision.str, str)
    async with client.transaction() as transaction:
        assert_type(await transaction.users.get(user.id), User)
"""
    valid_path = tmp_path / "valid.py"
    valid_path.write_text(valid, encoding="utf-8")
    command = [executable, "-m", checker]
    if checker == "mypy":
        command += ["--strict", "--no-incremental", "--python-version", "3.14"]
    else:
        command += ["--outputjson"]

    environment = dict(environ) | {"MYPYPATH": str(root)}
    result = run(
            [*command, str(valid_path)], cwd=tmp_path, env=environment, capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr
    invalid = """from sqrrl import Database
from models import Client, UserColumns, TaskColumns, SettingKey

async def consumer(database: Database) -> None:
    client = Client(database)
    await client.users.create(name=123)  # reject
    await client.users.create()  # reject
    await client.users.create(name="Ada", active=1)  # reject
    await client.users.create(name="Ada", unknown="x")  # reject
    await client.users.update(1, token="changed")  # reject
    client.users.query().where(TaskColumns.id.eq(1))  # reject
    client.users.query().order_by(TaskColumns.id.asc())  # reject
    UserColumns.name.eq(123)  # reject
    await client.settings.get(1)  # reject
    await client.users.get(SettingKey(user_id=1, key="x"))  # reject
    client.users.get(1)  # reject
    client.users.query().all()  # reject
    with client.transaction():  # reject
        pass
"""
    invalid_path = tmp_path / "invalid.py"
    invalid_path.write_text(invalid, encoding="utf-8")
    result = run(
            [*command, str(invalid_path)], cwd=tmp_path, env=environment, capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    expected = {number for number, line in enumerate(invalid.splitlines(), 1) if "# reject" in line}
    if checker == "mypy":
        rejected = {int(number) for number in findall(r"invalid\.py:(\d+): error:", result.stdout)}
    else:
        rejected = {
            item["range"]["start"]["line"] + 1
            for item in loads(result.stdout)["generalDiagnostics"]
            if item["severity"] == "error"
        }

    assert rejected == expected, result.stdout + result.stderr

def test_generated_parameter_names_do_not_collide(tmp_path):
    schema = Schema((Table("items", "Item", (integer("id").primary_key(), text("self"), text("key"))),))
    source = render(schema)
    assert "self: str" in source and "key: str" in source
    compile(source, "generated.py", "exec")

def test_generated_metadata_preserves_schema(schema, models):
    assert models._SCHEMA == schema.normalize()

def test_generated_annotations_resolve_to_runtime_types(models):
    assert get_annotations(models.User, format=Format.VALUE)["name"] is str
    assert get_annotations(models.UserRepository.get, format=Format.VALUE)["return"] is models.User
    assert get_annotations(models.UserRepository.create, format=Format.VALUE)["return"] is models.User
    assert get_annotations(models.UserRepository.get, format=Format.STRING)["return"] == "User"

def test_generation_reports_unreadable_existing_file(tmp_path, schema, monkeypatch):
    path = tmp_path / "models.py"
    path.write_text("# handwritten\n", encoding="utf-8")
    read_text = Path.read_text

    def inaccessible(target, *args, **kwargs):
        if target == path:
            raise PermissionError("unreadable models")

        return read_text(target, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", inaccessible)
    with raises(PermissionError, match="unreadable"):
        write(path, schema)

    assert read_text(path, encoding="utf-8") == "# handwritten\n"
