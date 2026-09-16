from asyncio import run
from pathlib import Path
from sqrrl import Database
from sqrrl.migrate import apply, load
from tempfile import TemporaryDirectory
from examples.models import Client, SettingKey, TaskColumns

async def main() -> None:
    with TemporaryDirectory(prefix="sqrrl-example-") as temporary:
        async with await Database.create(Path(temporary) / "app.db") as database:
            (await apply(database, load(Path(__file__).parent / "migrations")))
            client = Client(database)
            async with client.transaction() as transaction:
                user = await transaction.users.create(name="Ada", email="ada@example.com")
                (await transaction.tasks.create(owner_id=user.id, title="Try sqrrl"))
                (await transaction.settings.create(user_id=user.id, key="theme", value="dark"))

            tasks = await client.tasks.query().where(TaskColumns.done.eq(False)).order_by(TaskColumns.id.asc()).all()
            setting = await client.settings.get(SettingKey(user_id=user.id, key="theme"))
            print(f"{user.name}: {tasks[0].title}; theme={setting.value}")

if __name__ == "__main__":
    run(main())
