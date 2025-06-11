from unittest.mock import MagicMock

from kroft.core.column import ColumnDefinition
from kroft.core.schema import SchemaManager


def test_schema_manager_generates_correct_create_table_sql():
    # Arrange
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc", constraints="PRIMARY KEY"),
        "name": ColumnDefinition("name", "TEXT", lambda: "John Doe")
    }

    # Act
    mgr = SchemaManager(conn, schema="public", table_name="customers", columns=columns)
    mgr.create_table()

    # Assert
    cursor.execute.assert_called_once()
    sql_called_with = cursor.execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS public.customers" in sql_called_with
    assert "id UUID PRIMARY KEY" in sql_called_with
    assert "name TEXT" in sql_called_with


def test_schema_manager_get_create_sql_outputs_expected_ddl():
    columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc", constraints="PRIMARY KEY"),
        "email": ColumnDefinition("email", "TEXT", lambda: "me@example.com")
    }

    mgr = SchemaManager(
        conn=MagicMock(), 
        schema="public", 
        table_name="users", 
        columns=columns)
    ddl = mgr.get_create_table_sql()

    assert "CREATE TABLE IF NOT EXISTS public.users" in ddl
    assert "id UUID PRIMARY KEY" in ddl
    assert "email TEXT" in ddl


def test_schema_manager_generates_drop_table_sql_and_executes():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    mgr = SchemaManager(conn, schema="public", table_name="sessions", columns={})
    mgr.drop_table()

    cursor.execute.assert_called_once()
    sql = cursor.execute.call_args[0][0]
    assert sql == "DROP TABLE IF EXISTS public.sessions;"


def test_schema_manager_tracks_active_columns():
    columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "email": ColumnDefinition("email", "TEXT", lambda: "yo@example.com")
    }
    mgr = SchemaManager(
        conn=MagicMock(), 
        schema="public", 
        table_name="emails", 
        columns=columns)

    active = mgr.get_active_columns()
    assert "id" in active
    assert "email" in active
    assert isinstance(active["email"], ColumnDefinition)

def test_add_column_adds_column_and_updates_registry():
    # Arrange
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    existing_cols = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "name": ColumnDefinition("name", "TEXT", lambda: "John")
    }

    registry = {
        **existing_cols,
        "email": ColumnDefinition("email", "TEXT", lambda: "yo@example.com")
    }

    mgr = SchemaManager(conn, 
                        schema="public", 
                        table_name="users", 
                        columns=existing_cols.copy()
                        )

    # Act
    added = mgr.add_column(registry)

    # Assert
    assert added == "email"
    assert "email" in mgr.columns
    cursor.execute.assert_called_once()
    assert "ADD COLUMN email TEXT" in cursor.execute.call_args[0][0]

def test_drop_column_removes_column_from_registry():
    # Arrange
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    cols = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "email": ColumnDefinition("email", "TEXT", lambda: "yo@example.com")
    }

    protected = {"id"}

    mgr = SchemaManager(conn, schema="public", table_name="users", columns=cols.copy())

    # Act
    dropped = mgr.drop_column(protected_columns=protected)

    # Assert
    assert dropped == "email"
    assert "email" not in mgr.columns
    cursor.execute.assert_called_once()
    assert "DROP COLUMN email" in cursor.execute.call_args[0][0]