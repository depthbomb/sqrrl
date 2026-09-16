class SqrrlError(Exception):
    """Base error for schema, generation, migration, and data access failures."""

class SchemaError(SqrrlError):
    pass

class MigrationError(SqrrlError):
    pass

class ValidationError(SqrrlError):
    pass

class NotFoundError(SqrrlError):
    pass

class NotSingularError(SqrrlError):
    pass
