import random
from typing import Dict, List, Optional, Set

from kroft.core.column import ColumnDefinition


class SchemaManager:
    def __init__(
        self,
        conn,
        schema: str,
        table_name: str,
        columns: Dict[str, ColumnDefinition]
    ):
        self.conn = conn
        self.schema = schema
        self.table_name = table_name
        self.columns = columns

        # Only non-reserved columns are added at table creation
        self.active_columns = {
            name: col for name, col in columns.items() if not col.reserved
        }

        self.schema_version = 1
        self.schema_history: List[Set[str]] = [set(self.active_columns.keys())]

    def create_table(self):
        ddl_statements = [
            col.ddl() for _, col in self.active_columns.items()
        ]
        column_defs = ",\n  ".join(ddl_statements)

        ddl = f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
          {column_defs}
        );
        """

        with self.conn.cursor() as cur:
            cur.execute(ddl.strip())
            self.conn.commit()

    def get_create_table_sql(self) -> str:
        ddl_statements = [col.ddl() for col in self.active_columns.values()]
        column_defs = ",\n  ".join(ddl_statements)

        return f"""
        CREATE TABLE IF NOT EXISTS {self.schema}.{self.table_name} (
        {column_defs}
        );
        """.strip()

    def drop_table(self):
        ddl = f"DROP TABLE IF EXISTS {self.schema}.{self.table_name};"
        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

    def get_active_columns(self) -> Dict[str, ColumnDefinition]:
        return self.active_columns

    def add_column(self) -> Optional[str]:
        """
        Promote a reserved column from registry to active schema and evolve the DB.
        """
        available = [
            name for name, col in self.columns.items()
            if col.reserved and name not in self.active_columns
        ]
        if not available:
            return None

        chosen_key = random.choice(available)
        col_def = self.columns[chosen_key]
        ddl = f"ALTER TABLE {self.schema}.{self.table_name} ADD COLUMN {col_def.ddl()};"

        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

        self.active_columns[chosen_key] = col_def
        self._bump_version()
        return chosen_key

    def drop_column(self) -> Optional[str]:
        """
        Drop a random column that is not protected from the physical table 
        and update active schema.
        """
        candidates = [
            name for name, col in self.active_columns.items()
            if not col.protected
        ]
        if not candidates:
            return None

        chosen_key = random.choice(candidates)
        ddl = f"ALTER TABLE {self.schema}.{self.table_name} DROP COLUMN {chosen_key};"

        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

        del self.active_columns[chosen_key]
        self._bump_version()
        return chosen_key

    def register_column(self, name: str, col_def: ColumnDefinition) -> bool:
        """
        Add a new column definition to the registry (without altering DB schema).
        """
        if name in self.columns:
            return False
        self.columns[name] = col_def
        return True

    def _bump_version(self):
        self.schema_version += 1
        self.schema_history.append(set(self.active_columns.keys()))