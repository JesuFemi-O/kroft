from typing import Any, Callable, Optional


class ColumnDefinition:
    """Metadata and generator for a single table column.

    Args:
        name: Column name as it appears in the database.
        sql_type: SQL type string (e.g. ``"UUID"``, ``"TEXT"``, ``"FLOAT"``).
        generator: Zero-argument callable that returns a new value each call.
        constraints: Optional SQL constraint string appended to the DDL
            (e.g. ``"PRIMARY KEY"```, ``"NOT NULL"``).
        reserved: If ``True``, the column is excluded from the initial schema
            and can be promoted later via schema evolution.
        protected: If ``True``, the column is never chosen for mutation
            (updates/deletes) or schema drops.
    """

    def __init__(
        self,
        name: str,
        sql_type: str,
        generator: Callable[[], Any],
        constraints: Optional[str] = None,
        reserved: bool = False,
        protected: bool = False,
    ):
        self.name = name
        self.sql_type = sql_type
        self.generator = generator
        self.constraints = constraints or ""
        self.reserved = reserved
        self.protected = protected

    def generate(self) -> Any:
        """Return a freshly generated value for this column."""
        return self.generator()

    def ddl(self) -> str:
        """Return the DDL fragment for this column (e.g. ``id UUID PRIMARY KEY``)."""
        parts = [self.name, self.sql_type, self.constraints.strip()]
        return " ".join(p for p in parts if p)