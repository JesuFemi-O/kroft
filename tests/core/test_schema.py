import unittest
from unittest.mock import MagicMock

from kroft.core.column import ColumnDefinition
from kroft.core.schema import SchemaManager


class TestSchemaManager(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cursor = MagicMock()
        self.conn.cursor.return_value.__enter__.return_value = self.cursor

        self.columns = {
            "id": ColumnDefinition("id", "UUID", lambda: "uuid", reserved=False),
            "updated_at": ColumnDefinition(
                "updated_at", 
                "TIMESTAMP", 
                lambda: "now()", 
                protected=True
            ),
            "product": ColumnDefinition("product", "TEXT", lambda: "hat"),
            "new_col": ColumnDefinition("new_col", "INT", lambda: 1, reserved=True),
        }

        self.schema_mgr = SchemaManager(
            conn=self.conn,
            schema="public",
            table_name="sales",
            columns=self.columns
        )

    def test_create_table_issues_correct_sql(self):
        self.schema_mgr.create_table()
        self.assertTrue(self.cursor.execute.called)
        executed_sql = self.cursor.execute.call_args[0][0]
        self.assertIn("CREATE TABLE IF NOT EXISTS public.sales", executed_sql)

    def test_get_create_table_sql_returns_expected_string(self):
        ddl = self.schema_mgr.get_create_table_sql()
        self.assertIn("CREATE TABLE IF NOT EXISTS public.sales", ddl)
        self.assertIn("product TEXT", ddl)

    def test_active_columns_excludes_reserved(self):
        active = self.schema_mgr.get_active_columns()
        self.assertIn("product", active)
        self.assertIn("id", active)
        self.assertNotIn("new_col", active)  # reserved

    def test_add_column_adds_from_reserved_only(self):
        added = self.schema_mgr.add_column()
        self.assertEqual(added, "new_col")
        self.assertTrue(self.cursor.execute.called)
        self.assertIn(
            "ALTER TABLE public.sales ADD COLUMN", 
            self.cursor.execute.call_args[0][0]
            )

    def test_drop_column_avoids_protected_fields(self):
        dropped = self.schema_mgr.drop_column()
        self.assertIn(dropped, {"id", "product", "new_col"})
        self.assertNotEqual(dropped, "updated_at")
        self.assertTrue(self.cursor.execute.called)
        self.assertIn("DROP COLUMN", self.cursor.execute.call_args[0][0])

    def test_drop_table_executes_drop_statement(self):
        self.schema_mgr.drop_table()
        self.assertTrue(self.cursor.execute.called)
        executed_sql = self.cursor.execute.call_args[0][0]
        self.assertIn("DROP TABLE IF EXISTS public.sales", executed_sql)


if __name__ == "__main__":
    unittest.main()