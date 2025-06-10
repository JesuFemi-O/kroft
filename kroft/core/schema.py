from typing import Dict

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