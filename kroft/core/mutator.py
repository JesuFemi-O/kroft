import logging
import random
from typing import Any, Dict, List, Optional, Tuple

from psycopg2 import sql
from psycopg2.extras import execute_values

from kroft.core.batch import BatchGenerator

logger = logging.getLogger(__name__)


class MutationEngine:
    """Performs insert, update, and delete operations against a live table.

    Supports four composable simulation scenarios:

    - **Insert only** — call :meth:`insert_batch` and nothing else.
    - **Insert + Update** — call :meth:`insert_batch` then :meth:`update_batch`.
    - **Insert + Update + Delete** — chain all three methods.
    - **Probabilistic mutations** — use :meth:`maybe_mutate_batch` with
      configurable probability and fraction parameters.

    Args:
        conn: A live ``psycopg2`` connection.
        schema: PostgreSQL schema name (e.g. ``"public"``).
        table_name: Target table name.
        primary_key: Name of the primary key column. Defaults to ``"id"``.
        update_column: Optional timestamp column set to ``now()`` on every
            update (e.g. ``"updated_at"``).
        generator: :class:`BatchGenerator` used to produce replacement values
            during updates. Required for update operations.
    """

    def __init__(
        self,
        conn: Any,
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

    def update_batch(
        self,
        ids: List[str],
        fraction: float = 0.2,
        probability: float = 1.0,
    ) -> int:
        """Update a random fraction of the given ids.

        Args:
            ids: Pool of record ids to sample from.
            fraction: Fraction of ids to update (0.0–1.0).
            probability: Chance this call does anything (0.0–1.0).
        """
        if not ids or random.random() > probability:
            return 0
        subset = random.sample(ids, max(1, int(len(ids) * fraction)))
        updated = self._update_records(subset)
        self.total_updates += updated
        return updated

    def delete_batch(
        self,
        ids: List[str],
        fraction: float = 0.1,
        probability: float = 1.0,
    ) -> int:
        """Delete a random fraction of the given ids.

        Args:
            ids: Pool of record ids to sample from.
            fraction: Fraction of ids to delete (0.0–1.0).
            probability: Chance this call does anything (0.0–1.0).
        """
        if not ids or random.random() > probability:
            return 0
        subset = random.sample(ids, max(1, int(len(ids) * fraction)))
        deleted = self._delete_records(subset)
        self.total_deletes += deleted
        return deleted

    def maybe_mutate_batch(
        self,
        inserted_ids: List[str],
        probability: float = 0.5,
        update_fraction: float = 0.2,
        delete_fraction: float = 0.1,
        allow_updates: bool = True,
        allow_deletes: bool = True,
    ) -> Tuple[int, int]:
        """Randomly mutate a subset of the inserted batch.

        Args:
            inserted_ids: Ids from the most recent insert.
            probability: Chance any mutation happens at all (0.0–1.0).
            update_fraction: Fraction of ids to update when updates are chosen.
            delete_fraction: Fraction of ids to delete when deletes are chosen.
            allow_updates: Include updates in the possible operations.
            allow_deletes: Include deletes in the possible operations.
        """
        if not inserted_ids or random.random() > probability:
            return 0, 0

        operations = []
        if allow_updates:
            operations.append("update")
        if allow_deletes:
            operations.append("delete")

        if not operations:
            return 0, 0

        operation = random.choice(operations)
        logger.debug("Picked mutation operation: %s", operation)

        if operation == "update":
            count = max(1, int(len(inserted_ids) * update_fraction))
            subset = random.sample(inserted_ids, count)
            logger.debug("Updating %d records", len(subset))
            updated = self._update_records(subset)
            self.total_updates += updated
            return updated, 0
        else:
            count = max(1, int(len(inserted_ids) * delete_fraction))
            subset = random.sample(inserted_ids, count)
            deleted = self._delete_records(subset)
            self.total_deletes += deleted
            return 0, deleted

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
                    query = sql.SQL(
                        "UPDATE {}.{} SET {} = %s WHERE {} = %s"
                    ).format(
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

        pk_col_def = self.generator.schema.get(self.primary_key)
        pk_type = pk_col_def.sql_type.upper() if pk_col_def else "TEXT"
        cast = "::uuid[]" if pk_type == "UUID" else ""
        query = (
            f'DELETE FROM "{self.schema}"."{self.table_name}"'
            f' WHERE "{self.primary_key}" = ANY(%s{cast});'
        )
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
