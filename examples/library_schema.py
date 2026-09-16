from sqrrl.schema import ForeignKey, Index, Relationship, Schema, Table, custom, datetime, enum, integer, json, text

schema = Schema(
    (
        Table(
            'shelves',
            'Shelf',
            (text('room'), integer('number'), text('name')),
            primary_key=('room', 'number'),
            relationships=(
                Relationship('books', 'books', ('room', 'number'), ('room', 'shelf'), many=True),
                Relationship('sign', 'signs', ('room', 'number'), ('room', 'shelf')),
            ),
        ),
        Table(
            'books',
            'Book',
            (
                integer('id').primary_key(),
                text('isbn').unique().immutable(),
                text('title'),
                text('room').nullable(),
                integer('shelf').nullable(),
                integer('copies').default('0'),
                datetime('created').default_factory('examples.library_types:utc_now').immutable(),
                datetime('edited').nullable().on_update('examples.library_types:utc_now'),
                json('metadata').default_factory('examples.library_types:empty_metadata'),
                enum('binding', 'examples.library_types:Binding').default("'PAPER'"),
                custom('label', 'examples.library_types:Label', 'examples.library_types:label_codec').nullable(),
            ),
            foreign_keys=(ForeignKey(('room', 'shelf'), 'shelves', ('room', 'number')),),
            relationships=(Relationship('location', 'shelves', ('room', 'shelf'), ('room', 'number')),),
        ),
        Table(
            'signs',
            'Sign',
            (integer('id').primary_key(), text('room'), integer('shelf'), text('caption')),
            indexes=(Index('sign_location', ('room', 'shelf'), unique=True),),
            foreign_keys=(ForeignKey(('room', 'shelf'), 'shelves', ('room', 'number')),),
        ),
    )
)
