from uuid import uuid4
from sys import modules
from pytest import fixture
from sqrrl.generate import write
from importlib.util import module_from_spec, spec_from_file_location
from sqrrl.schema import Index, Schema, Table, blob, boolean, integer, real, text

@fixture
def schema():
    return Schema(
            tables=(
                Table(
                        "users",
                        "User",
                        fields=(
                            integer("id").primary_key(),
                            text("name"),
                            text("email").nullable().default("'default@example.com'"),
                            boolean("active").default("1"),
                            text("token").immutable().default("'token'"),
                        ),
                        indexes=(Index("active_names", ("name",), unique=True, where="active = 1"),),
                ),
                Table(
                        "tasks",
                        "Task",
                        fields=(
                            integer("id").primary_key(),
                            integer("owner_id").references("users", "id", on_delete="CASCADE"),
                            text("title"),
                        ),
                ),
                Table(
                        "settings",
                        "Setting",
                        primary_key=("user_id", "key"),
                        fields=(
                            integer("user_id").references("users", "id", on_delete="CASCADE"),
                            text("key"),
                            text("value").nullable(),
                        ),
                ),
                Table(
                        "documents",
                        "Document",
                        fields=(
                            text("id").primary_key(),
                            blob("body"),
                            real("score").nullable(),
                        ),
                ),
            )
    )

@fixture
def models(tmp_path, schema):
    path = write(tmp_path / "models.py", schema)
    name = "generated_" + uuid4().hex
    specification = spec_from_file_location(name, path)
    module = module_from_spec(specification)
    modules[name] = module
    try:
        specification.loader.exec_module(module)
        yield module
    finally:
        modules.pop(name, None)
