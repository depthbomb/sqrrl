from pytest import raises
from sqrrl.migrate import apply, diff
from sqlite3 import IntegrityError, OperationalError
from sqrrl import UNSET, Database, NotFoundError, NotSingularError, ValidationError

async def test_crud_defaults_null_and_composite_keys(tmp_path, schema, models):
    history = ((await diff((), schema, "initial")),)
    async with await Database.create(tmp_path / "data.db") as database:
        (await apply(database, history))
        client = models.Client(database)
        ada = await client.users.create(name="Ada")
        assert ada.email == "default@example.com" and ada.active is True
        assert (await client.users.update(ada.id, email=None)).email is None
        assert (await client.users.update(ada.id, name="Augusta", email=UNSET)).email is None
        assert (await client.users.get(ada.id)).name == "Augusta"
        (await client.settings.create(user_id=ada.id, key="theme", value="dark"))
        (await client.settings.create(user_id=ada.id, key="language", value="en"))
        key = models.SettingKey(user_id=ada.id, key="theme")
        assert (await client.settings.update(key, value=None)).value is None
        assert (await client.settings.query().count()) == 2
        (await client.settings.delete(key))
        assert (await client.settings.query().only()).key == "language"
        document = await client.documents.create(id="hello", body=b"\x00\xff", score=1.5)
        assert (await client.documents.get("hello")) == document
        assert (await client.documents.update("hello", body=b"new")).body == b"new"
        (await client.users.delete(ada.id))
        assert (await client.settings.query().count()) == 0
        with raises(NotFoundError):
            (await client.users.get(ada.id))

async def test_queries_branching_cardinality_and_model_validation(tmp_path, schema, models):
    async with await Database.create(tmp_path / "queries.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = models.Client(database)
        one = await client.users.create(name="one", email=None)
        two = await client.users.create(name="two", active=False)
        query = client.users.query()
        active = query.where(models.UserColumns.active.eq(True))
        assert (await active.only()) == one
        assert (await query.count()) == 2
        assert (await query.order_by(models.UserColumns.id.desc()).first()) == two
        assert (await query.where(models.UserColumns.email.eq(None)).only()) == one
        assert (await query.where(models.UserColumns.email.in_(None, "default@example.com")).count()) == 2
        assert not (await query.where(models.UserColumns.id.in_()).exists())
        assert (await query.where(models.UserColumns.name.eq("x' OR 1=1 --")).count()) == 0
        assert (await query.where(models.UserColumns.id.eq(one.id) | models.UserColumns.id.eq(two.id)).count()) == 2
        assert (await query.where(~models.UserColumns.active.eq(True)).only()) == two
        with raises(NotSingularError):
            (await query.limit(1).only())

        with raises(ValidationError):
            query.where(models.TaskColumns.id.eq(one.id))

        with raises(ValidationError):
            query.order_by(models.TaskColumns.id.asc())

        with raises(ValidationError):
            models.UserColumns.id.eq(1) & models.TaskColumns.id.eq(1)

async def test_runtime_types_and_immutability(tmp_path, schema, models):
    async with await Database.create(tmp_path / "types.db") as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = models.Client(database)
        for value in (None, 3, b"bytes"):
            with raises(ValidationError):
                (await client.users.create(name=value))

        with raises(TypeError):
            (await client.users.create())

        with raises(ValidationError):
            (await client.users.create(name="Ada", active=1))

        for key in (True, 2 ** 63, "1"):
            with raises(ValidationError):
                (await client.users.get(key))

        user = await client.users.create(name="Ada")
        with raises(TypeError):
            (await client.users.update(user.id, token="changed"))

        with raises(ValidationError):
            (await client.documents.create(id="bad", body=b"", score=float("nan")))

        assert (await client.users.query().count()) == 1

async def test_transactions_savepoints_and_decoder_failure(tmp_path, schema, models):
    async with await Database.create(tmp_path / "transactions.db", wal=True, immediate=True) as database:
        (await apply(database, ((await diff((), schema, "initial")),)))
        client = models.Client(database)
        async with client.transaction() as outer:
            first = await outer.users.create(name="first")
            with raises(IntegrityError):
                async with outer.transaction() as inner:
                    (await inner.users.create(name="second"))
                    (await inner.tasks.create(owner_id=999, title="invalid"))

            assert (await outer.users.query().count()) == 1
            (await outer.tasks.create(owner_id=first.id, title="valid"))

        with raises(KeyboardInterrupt):
            async with client.transaction() as transaction:
                (await transaction.users.create(name="third"))
                raise KeyboardInterrupt

        assert (await client.users.query().count()) == 1
        with raises(NotFoundError):
            (await client.users.update(999, name="missing"))

        with raises(NotFoundError):
            (await client.users.delete(999))

        decoder = client.users._decoder

        def fail_decode(row):
            raise ValidationError("decode failed")

        client.users._decoder = fail_decode
        with raises(ValidationError, match="decode failed"):
            (await client.users.create(name="bad"))

        with raises(ValidationError, match="decode failed"):
            (await client.users.update(first.id, name="bad"))

        client.users._decoder = decoder
        assert (await client.users.get(first.id)).name == "first"
        assert not database.connection.in_transaction

async def test_open_does_not_create_or_migrate(tmp_path):
    path = tmp_path / "missing #%.db"
    with raises(OperationalError):
        (await Database.open(path))

    assert not path.exists()
    async with await Database.create(path) as database:
        assert (await (await database.connection.execute("PRAGMA foreign_keys")).fetchone())[0] == 1
        assert (await (await database.connection.execute("PRAGMA synchronous")).fetchone())[0] == 2
        assert (await (await database.connection.execute("PRAGMA busy_timeout")).fetchone())[0] == 5000
        assert (await (await database.connection.execute("SELECT count(*) FROM sqlite_schema")).fetchone())[0] == 0

    async with await Database.open(path):
        pass
