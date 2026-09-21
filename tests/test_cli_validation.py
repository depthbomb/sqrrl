from json import dumps
from io import StringIO
from pathlib import Path
from sqrrl import Database
from sqrrl.migrate import diff, write
from pytest import mark, raises, fixture
from sqrrl.cli import _main, _initialize
from sqrrl.schema import Table, Schema, integer


@fixture
def cli_error(monkeypatch):
    stream = StringIO()
    monkeypatch.setattr('sqrrl.cli.stderr', stream)
    return stream


@mark.parametrize(
    'changes, message',
    [
        ({'schema': ''}, 'requires schema'),
        ({'extra': 'value'}, 'requires schema'),
        ({'output': 'models.txt'}, 'Python file'),
        ({'output': 'migrations/models.py'}, 'outside the migration'),
        ({'migrations': '.'}, 'inside the project'),
        ({'schema': 'schema'}, 'module and object'),
        ({'schema': 'math:pi'}, 'must be a sqrrl.schema.Schema'),
    ],
)
async def test_invalid_configuration_reports_actionable_errors(tmp_path, cli_error, changes, message):
    config = tmp_path / 'sqrrl.json'
    config.write_text(dumps({'schema': 'schema:schema', 'output': 'models.py', 'migrations': 'migrations'} | changes))
    assert await _main(['generate', '--config', str(config)]) == 1
    assert message in cli_error.getvalue()
    assert not (tmp_path / 'models.py').exists()


async def test_schema_stdout_is_reported_as_loader_error(tmp_path, cli_error):
    config = tmp_path / 'sqrrl.json'
    _initialize(config)
    schema = tmp_path / 'schema.py'
    schema.write_text(schema.read_text() + '\nprint("debug output")\n')
    assert await _main(['generate', '--config', str(config)]) == 1
    assert 'must not print to stdout' in cli_error.getvalue()


@mark.parametrize(
    'arguments, message',
    [
        (['up'], 'Supply --db'),
        (['diff', 'initial', '--dialect', 'postgresql'], 'PostgreSQL DSN'),
    ],
)
async def test_migration_cli_requires_database_configuration(tmp_path, cli_error, monkeypatch, arguments, message):
    monkeypatch.delenv('SQRRL_DATABASE_URL', raising=False)
    config = tmp_path / 'sqrrl.json'
    _initialize(config)
    assert await _main(['migrate', *arguments, '--config', str(config)]) == 1
    assert message in cli_error.getvalue()


def test_failed_init_removes_partial_files(tmp_path, monkeypatch):
    config = tmp_path / 'sqrrl.json'
    original = Path.open

    def open_file(path, *args, **kwargs):
        if path == config:
            raise PermissionError('config is unwritable')
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', open_file)
    with raises(PermissionError, match='unwritable'):
        _initialize(config)
    assert not (tmp_path / 'schema.py').exists()
    assert not config.exists()


async def test_cli_adoption_refuses_existing_migration_directory(tmp_path, cli_error):
    config = tmp_path / 'sqrrl.json'
    _initialize(config)
    migration = await diff((), Schema((Table('items', 'Item', (integer('id').primary_key(),)),)), 'initial')
    write(tmp_path / 'migrations', migration)
    database_path = tmp_path / 'existing.db'
    async with await Database.create(database_path):
        pass
    assert await _main(['migrate', 'adopt', '--config', str(config), '--db', str(database_path)]) == 1
    assert 'empty Sqrrl migration directory' in cli_error.getvalue()
