import random
from typing import Dict, Set

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
        self.active_columns = dict(columns)

    def create_table(self):
        ddl_statements = [
            col.ddl() for col in self.columns.values()
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
        ddl_statements = [col.ddl() for col in self.columns.values()]
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
    
    def add_column(self, registry: Dict[str, ColumnDefinition]) -> str:
        available = [k for k in registry if k not in self.columns]
        if not available:
            return None

        chosen_key = random.choice(available)
        col_def = registry[chosen_key]
        ddl = f"ALTER TABLE {self.schema}.{self.table_name} ADD COLUMN {col_def.ddl()};"

        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

        self.columns[chosen_key] = col_def
        return chosen_key

    def drop_column(self, protected_columns: Set[str]) -> str:
        candidates = [k for k in self.columns if k not in protected_columns]
        if not candidates:
            return None

        chosen_key = random.choice(candidates)
        ddl = f"ALTER TABLE {self.schema}.{self.table_name} DROP COLUMN {chosen_key};"

        with self.conn.cursor() as cur:
            cur.execute(ddl)
            self.conn.commit()

        del self.columns[chosen_key]
        return chosen_key