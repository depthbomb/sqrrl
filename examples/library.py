from asyncio import run
from pathlib import Path
from tempfile import TemporaryDirectory
from examples.library_schema import schema
from sqrrl.migrate import apply, check, diff
from examples.library_types import Binding, Label
from sqrrl import Conflict, Database, Increment, Unloaded
from examples.library_models import BookColumns, BookCreate, BookRelations, Client, ShelfRelations


async def main() -> None:
    """Run with python -m examples.library; all data lives in a temporary directory."""
    with TemporaryDirectory(prefix='sqrrl_library_') as directory:
        async with await Database.create(
            Path(directory) / 'library.db', wal=True, readers=2, immediate=True
        ) as database:
            migration = await diff((), schema, 'initial')
            assert migration is not None
            await check((migration,), schema)
            await apply(database, (migration,))
            client = Client(database)
            await client.shelves.create(room='East', number=1, name='Reference')
            await client.signs.create(room='East', shelf=1, caption='Reference books')
            await client.books.create_many(
                [
                    BookCreate(
                        isbn='field-guide',
                        title='A Field Guide',
                        room='East',
                        shelf=1,
                        copies=2,
                        binding=Binding.CLOTH,
                        metadata={'subjects': ['nature']},
                        label=Label('Reference'),
                    ),
                    BookCreate(isbn='atlas', title='An Atlas', room='East', shelf=1),
                ]
            )
            updated = await client.books.insert(
                BookCreate(isbn='atlas', title='A New Atlas'),
                conflict=Conflict((BookColumns.isbn,), (BookColumns.title,)),
            )
            assert updated is not None
            changed = await client.books.update_where(
                BookColumns.isbn.eq('field-guide') & BookColumns.copies.ge(1), copies=Increment(-1)
            )
            print(f'Checked out {changed} book')
            shelves = await client.shelves.query().load(ShelfRelations.books).all()
            for shelf in shelves:
                assert not isinstance(shelf.books, Unloaded)
                print(f'{shelf.name}: {", ".join(book.title for book in shelf.books)}')

            # Book -> optional location -> optional sign, fetched in three queries.
            # Each then() checks that the next relationship belongs to the target model.
            books = await client.books.query().load(BookRelations.location.then(ShelfRelations.sign)).all()
            for book in books:
                location = book.location
                if isinstance(location, Unloaded) or location is None:
                    continue
                sign = location.sign
                if not isinstance(sign, Unloaded) and sign is not None:
                    print(f'{book.title}: {sign.caption}')


if __name__ == '__main__':
    run(main())
