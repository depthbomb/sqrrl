from pytest import mark
from subprocess import run
from sys import executable
from textwrap import dedent
from json import loads, dumps

def invoke(directory, *arguments, success=True):
    result = run([executable, "-m", "sqrrl", *arguments], cwd=directory, capture_output=True, text=True, check=False)
    assert (result.returncode == 0) == success, result.stdout + result.stderr

    return result

def test_fresh_consumer_workflow_without_schema_import_on_apply(tmp_path):
    invoke(tmp_path, "init")
    invoke(tmp_path, "generate")
    invoke(tmp_path, "generate", "--check")
    invoke(tmp_path, "migrate", "diff", "initial")
    invoke(tmp_path, "migrate", "check")
    assert "No schema changes" in invoke(tmp_path, "migrate", "diff", "noop").stdout
    invoke(tmp_path, "migrate", "up", "--db", "app.db")
    assert "applied" in invoke(tmp_path, "migrate", "status", "--db", "app.db").stdout
    (tmp_path / "consumer.py").write_text(
            dedent('''\
    from asyncio import run
    from sqrrl import Database
    from models import Client, NoteColumns
    
    async def main():
        async with await Database.open("app.db") as database:
            client = Client(database)
            note = await client.notes.create(title="hello")
            assert (await client.notes.update(note.id, done=True)).done
            assert (await client.notes.query().where(NoteColumns.done.eq(True)).only()).title == "hello"
    
    run(main())
    '''),
            encoding="utf-8",
    )
    (tmp_path / "schema.py").write_text('raise RuntimeError("schema must not be imported")\n', encoding="utf-8")
    result = run([executable, "consumer.py"], cwd=tmp_path, text=True, capture_output=True, check=False)
    assert result.returncode == 0, result.stderr
    invoke(tmp_path, "migrate", "up", "--db", "app.db")
    invoke(tmp_path, "migrate", "status", "--db", "app.db")
    invoke(tmp_path, "generate", success=False)

def test_cli_check_drift_errors_and_custom_backfill(tmp_path):
    invoke(tmp_path, "init")
    invoke(tmp_path, "init", success=False)
    invoke(tmp_path, "generate", "--check", success=False)
    invoke(tmp_path, "migrate", "status", "--db", "missing.db", success=False)
    assert not (tmp_path / "missing.db").exists()
    invoke(tmp_path, "migrate", "diff", "initial")
    (tmp_path / "backfill.sql").write_text("INSERT INTO notes(title) VALUES ('seed');", encoding="utf-8")
    invoke(tmp_path, "migrate", "custom", "seed", "--sql", "backfill.sql")
    invoke(tmp_path, "migrate", "up", "--db", "app.db")
    source = (tmp_path / "schema.py").read_text()
    (tmp_path / "schema.py").write_text(
            source.replace('text("title"),', 'text("title"), text("description").nullable(),'), encoding="utf-8"
    )
    invoke(tmp_path, "migrate", "check", success=False)
    invoke(tmp_path, "migrate", "diff", "description")
    invoke(tmp_path, "migrate", "up", "--db", "app.db")

def test_config_rejects_output_outside_project(tmp_path):
    invoke(tmp_path, "init")
    path = tmp_path / "sqrrl.json"
    config = loads(path.read_text())
    config["output"] = "../outside.py"
    path.write_text(dumps(config), encoding="utf-8")
    assert "inside the project" in invoke(tmp_path, "generate", success=False).stderr

def test_cli_accepts_config_from_other_working_directory(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    config = project / "custom.json"
    invoke(tmp_path, "init", "--config", str(config))
    invoke(tmp_path, "generate", "--config", str(config))
    invoke(tmp_path, "migrate", "diff", "initial", "--config", str(config))
    assert (project / "models.py").exists()

@mark.parametrize("arguments, suggestion", [(("genrate",), "generate"), (("migrate", "stats"), "status")])
def test_cli_suggests_misspelled_commands(tmp_path, arguments, suggestion):
    result = invoke(tmp_path, *arguments, success=False)
    assert f"maybe you meant '{suggestion}'" in result.stderr
