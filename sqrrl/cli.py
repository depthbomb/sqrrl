from pathlib import Path
from subprocess import run
from typing import Optional
from json import dumps, loads
from sqrrl.schema import Schema
from dataclasses import dataclass
from sqlite3 import DatabaseError
from sqrrl.runtime import Database
from sys import executable, stderr
from argparse import ArgumentParser
from importlib import import_module
from sqrrl.errors import SqrrlError
from asyncio import run as run_async
from sqrrl.generate import write as write_models
from sqrrl.migrate import adopt, apply, baseline, check, custom, diff, load, status, write

@dataclass(frozen=True)
class Config:
    root: Path
    schema: str
    output: Path
    migrations: Path

_STARTER = """from sqrrl.schema import Schema, Table, boolean, integer, text

schema = Schema(tables=(
    Table("notes", model="Note", fields=(
        integer("id").primary_key(),
        text("title"),
        boolean("done").default("0"),
    )),
))
"""

def _config(path: Path) -> Config:
    data = loads(path.read_text(encoding="utf-8"))
    if (
            not isinstance(data, dict)
            or set(data) != {"schema", "output", "migrations"}
            or not all(isinstance(value, str) and value for value in data.values())
    ):
        raise SqrrlError("Configuration requires schema, output, and migrations strings")

    root = path.resolve().parent
    paths = []
    for name in ("output", "migrations"):
        value = Path(data[name])
        target = (root / value).resolve()
        if value.is_absolute() or not target.is_relative_to(root) or target == root:
            raise SqrrlError(f"{name} must point inside the project")

        paths.append(target)

    if paths[0].suffix != ".py" or paths[0].is_relative_to(paths[1]):
        raise SqrrlError("output must be a Python file outside the migration directory")

    return Config(root, data["schema"], paths[0], paths[1])

def _export_schema(specification: str) -> Schema:
    module_name, separator, attribute = specification.partition(":")
    if not separator or not module_name or not attribute:
        raise SqrrlError("schema must name a module and object, for example schema:schema")

    module = import_module(module_name)
    schema = getattr(module, attribute)
    if not isinstance(schema, Schema):
        raise SqrrlError("Configured schema object must be a sqrrl.schema.Schema")

    return schema.normalize()

def _load_schema(config: Config) -> Schema:
    loader = (
            "from json import dumps; from sqrrl.cli import _export_schema; print(dumps(_export_schema("
            + repr(config.schema)
            + ").to_dict()))"
    )
    result = run(
            [executable, "-c", loader], cwd=config.root, text=True, encoding="utf-8", capture_output=True, check=False
    )
    if result.returncode:
        raise SqrrlError(f"Cannot load schema:\n{result.stderr.strip()}")

    try:
        return Schema.from_dict(loads(result.stdout))
    except (ValueError, TypeError, KeyError) as error:
        raise SqrrlError("Schema loader returned invalid data; schema modules must not print to stdout") from error

def _initialize(path: Path) -> None:
    root = path.resolve().parent
    targets = (root / "schema.py", path.resolve())
    if any(target.exists() for target in targets):
        raise SqrrlError("init refuses to overwrite an existing schema or configuration")

    root.mkdir(parents=True, exist_ok=True)
    created = []
    try:
        configuration = (
                dumps({"schema": "schema:schema", "output": "models.py", "migrations": "migrations"}, indent=2) + "\n"
        )
        for target, content in zip(targets, (_STARTER, configuration), strict=True):
            with target.open("x", encoding="utf-8", newline="\n") as output:
                created.append(target)
                output.write(content)
    except BaseException:
        for target in created:
            target.unlink(missing_ok=True)
        raise

    print("Created schema.py and configuration. Run sqrrl generate, then sqrrl migrate diff initial.")

def _parser() -> ArgumentParser:
    parser = ArgumentParser(
            prog="sqrrl", description="Generated typed access and checked SQLite migrations", suggest_on_error=True
    )
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="Create a starter schema and configuration")
    init.add_argument("--config", type=Path, default=Path("sqrrl.json"))
    generate = commands.add_parser("generate", help="Generate typed Python models and queries")
    generate.add_argument("--config", type=Path, default=Path("sqrrl.json"))
    generate.add_argument("--check", action="store_true", help="Fail if generated code is stale")
    migration = commands.add_parser("migrate", help="Create, verify, and apply migrations", suggest_on_error=True)
    actions = migration.add_subparsers(dest="action", required=True)
    for name in ("diff", "custom", "check", "up", "status", "baseline", 'adopt'):
        action = actions.add_parser(name)
        action.add_argument("--config", type=Path, default=Path("sqrrl.json"))
        if name in ("diff", "custom"):
            action.add_argument("name")

        if name == "diff":
            action.add_argument("--allow-drop", action="store_true")

        if name == "custom":
            action.add_argument("--sql", type=Path, required=True)

        if name in ("up", "status", "baseline", 'adopt'):
            action.add_argument("--db", type=Path, required=True)

        if name == "baseline":
            action.add_argument("--version", type=int, required=True)

        if name == 'adopt':
            action.add_argument('--preserve-sql', type=Path, help='Reviewed CREATE statements for external history objects')

    return parser

async def _main(arguments: Optional[list[str]] = None) -> int:
    args = _parser().parse_args(arguments)
    try:
        if args.command == "init":
            _initialize(args.config)

            return 0

        config = _config(args.config)
        if args.command == "generate":
            output = write_models(config.output, _load_schema(config), check=args.check)
            print(f"Models are up to date: {output}")

            return 0

        history = load(config.migrations)
        if args.action == "diff":
            migration = await diff(history, _load_schema(config), args.name, allow_drop=args.allow_drop)
            if migration is None:
                print("No schema changes.")
            else:
                print(f"Created {write(config.migrations, migration)}")
                print("Review the SQL before applying it.")
        elif args.action == "custom":
            migration = await custom(history, args.name, args.sql.read_text(encoding="utf-8"))
            print(f"Created {write(config.migrations, migration)}")
        elif args.action == "check":
            await check(history, _load_schema(config))
            print("Migration history replays successfully and matches the schema.")
        else:
            opener = Database.create if args.action == "up" else Database.open
            async with await opener(args.db) as database:
                if args.action == "up":
                    await apply(database, history)
                    print("Database is up to date.")
                elif args.action == "baseline":
                    await baseline(database, history, args.version)
                    print(f"Database validated and baselined at version {args.version}.")
                elif args.action == 'adopt':
                    if history:
                        raise SqrrlError('Adoption requires an empty Sqrrl migration directory')
                    preserved = (args.preserve_sql.read_text(encoding='utf-8'),) if args.preserve_sql else ()
                    migration = await adopt(database, _load_schema(config), preserve_sql=preserved)
                    print(f'Created {write(config.migrations, migration)}')
                    print('Review the captured SQL, then run migrate baseline --version 1 with this database.')
                else:
                    for item in await status(database, history):
                        state = "applied" if item.applied else "pending"
                        print(f"{item.version:06d} {item.name} {state}")
    except (SqrrlError, OSError, ValueError, DatabaseError) as error:
        print(f"sqrrl: {error}", file=stderr)

        return 1

    return 0

def main(arguments: Optional[list[str]] = None) -> int:
    return run_async(_main(arguments))
