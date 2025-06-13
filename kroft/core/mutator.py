import random
from typing import Dict, List, Optional, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values

from kroft.core.batch import BatchGenerator


class MutationEngine:
    def __init__(
        self,
        conn,
        schema: str,
        table_name: str,
        primary_key: str = "id",
        update_column: Optional[str] = None,
        generator: Optional[BatchGenerator] = None
    ):
        self.conn = conn
        self.schema = schema
        self.table_name = table_name
        self.primary_key = primary_key
        self.update_column = update_column
        self.generator = generator

        self.total_inserts = 0
        self.total_updates = 0
        self.total_deletes = 0

    def insert_batch(self, rows: List[Dict]) -> List[str]:
        if not rows:
            return []

        inserted_ids = [row[self.primary_key] for row in rows]
        self.total_inserts += len(rows)

        with self.conn.cursor() as cur:
            columns = rows[0].keys()
            query = sql.SQL("INSERT INTO {}.{} ({}) VALUES %s").format(
                sql.Identifier(self.schema),
                sql.Identifier(self.table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns))
            )
            values = [[row[col] for col in columns] for row in rows]
            execute_values(cur, query, values)

            self.conn.commit()

        return inserted_ids

    def maybe_mutate_batch(self, inserted_ids: List[str]) -> Tuple[int, int]:
        if not inserted_ids or random.random() > 0.5:
            return 0, 0

        operation = random.choice(["update", "delete"])
        subset = random.sample(inserted_ids, max(1, len(inserted_ids) // 4))
        print(f"Picking operation next - {operation}")

        if operation == "update":
            print(f"Updating {len(subset)} records: {subset}")
            updated_count = self._update_records(subset)
            self.total_updates += updated_count
            return updated_count, 0
        else:
            deleted_count = self._delete_records(subset)
            self.total_deletes += deleted_count
            return 0, deleted_count

    def _update_records(self, ids: List[str]) -> int:
        if not ids or not self.generator:
            return 0

        modifiable_columns = self.generator.get_modifiable_columns(
            exclude=[self.primary_key]
            )
        
        if not modifiable_columns:
            return 0

        with self.conn.cursor() as cur:
            for row_id in ids:
                col = random.choice(modifiable_columns)
                val = self.generator.generate_value(col)

                if self.update_column:
                    query = sql.SQL(
                        "UPDATE {}.{} SET {} = %s, {} = now() WHERE {} = %s"
                    ).format(
                        sql.Identifier(self.schema),
                        sql.Identifier(self.table_name),
                        sql.Identifier(col),
                        sql.Identifier(self.update_column),
                        sql.Identifier(self.primary_key)
                    )
                    cur.execute(query, (val, row_id))
                else:
                    query = sql.SQL("UPDATE {}.{} SET {} = %s WHERE {} = %s").format(
                        sql.Identifier(self.schema),
                        sql.Identifier(self.table_name),
                        sql.Identifier(col),
                        sql.Identifier(self.primary_key)
                    )
                    cur.execute(query, (val, row_id))

            self.conn.commit()

        return len(ids)

    def _delete_records(self, ids: List[str]) -> int:
        if not ids:
            return 0
        

        # Fetch column type from the generator's schema
        pk_col_def = self.generator.schema.get(self.primary_key)
        pk_type = pk_col_def.sql_type.upper() if pk_col_def else "TEXT"

        # Add cast only if UUID
        cast = "::uuid[]" if pk_type == "UUID" else ""
        query = f'DELETE FROM "{self.schema}"."{self.table_name}" WHERE "{self.primary_key}" = ANY(%s{cast});'  # noqa: E501
        with self.conn.cursor() as cur:

            cur.execute(query, (ids,))
            self.conn.commit()

        return len(ids)

    def get_counters(self) -> Dict[str, int]:
        return {
            "total_inserts": self.total_inserts,
            "total_updates": self.total_updates,
            "total_deletes": self.total_deletes,
        }