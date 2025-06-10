from typing import List, Dict, Tuple
from psycopg2 import sql
from psycopg2.extras import execute_values
from datetime import datetime
import random


class MutationEngine:
    def __init__(self, conn, schema: str, table_name: str):
        self.conn = conn
        self.schema = schema
        self.table_name = table_name
        self.total_inserts = 0
        self.total_updates = 0
        self.total_deletes = 0

    def insert_batch(self, rows: List[Dict]) -> List[str]:
        if not rows:
            return []

        inserted_ids = [row["id"] for row in rows]
        self.total_inserts += len(rows)

        with self.conn.cursor() as cur:
            columns = rows[0].keys()
            query = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(self.schema),
                sql.Identifier(self.table_name),
                sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() * len(columns))
            )

            values = [[row[col] for col in columns] for row in rows]
            execute_values(cur, query, values)

            self.conn.commit()

        return inserted_ids

    def get_insert_count(self) -> int:
        return self.total_inserts
    
    def maybe_mutate_batch(self, inserted_ids: List[str]) -> Tuple[int, int]:
        if not inserted_ids or random.random() > 0.5:
            return (0, 0)

        operation = random.choice(["update", "delete"])
        subset = random.sample(inserted_ids, max(1, len(inserted_ids) // 4))

        if operation == "update":
            return (self._update_records(subset), 0)
        else:
            return (0, self._delete_records(subset))
    
    def _update_records(self, ids: List[str]) -> int:
        if not ids:
            return 0

        updates = []
        for _id in ids:
            updates.append((
                _id,
                datetime.utcnow()  # updated_at
            ))

        with self.conn.cursor() as cur:
            cur.executemany(
                f"""
                UPDATE {self.schema}.{self.table_name}
                SET updated_at = %s
                WHERE id = %s
                """,
                [(u[1], u[0]) for u in updates]  # (updated_at, id)
            )
            self.conn.commit()

        self.total_updates += len(updates)
        return len(updates)

    def _delete_records(self, ids: List[str]) -> int:
        if not ids:
            return 0

        with self.conn.cursor() as cur:
            cur.execute(
                f"""
                DELETE FROM {self.schema}.{self.table_name}
                WHERE id = ANY(%s)
                """,
                (ids,)
            )
            self.conn.commit()

        self.total_deletes += len(ids)
        return len(ids)