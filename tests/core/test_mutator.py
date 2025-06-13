from unittest.mock import MagicMock, patch

from kroft.core.batch import BatchGenerator
from kroft.core.column import ColumnDefinition
from kroft.core.mutator import MutationEngine


@patch("kroft.core.mutator.execute_values")
def test_insert_batch_inserts_rows_and_tracks_count(mock_execute_values):
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    engine = MutationEngine(
        conn, 
        schema="public", 
        table_name="products", 
        primary_key="id"
    )

    rows = [
        {"id": "abc", "name": "Hat"},
        {"id": "def", "name": "Shirt"}
    ]

    inserted_ids = engine.insert_batch(rows)

    mock_execute_values.assert_called_once()
    assert inserted_ids == ["abc", "def"]
    assert engine.total_inserts == 2

@patch("kroft.core.mutator.random.sample", return_value=["id1"])
@patch("kroft.core.mutator.random")
def test_maybe_mutate_batch_calls_update_or_delete(mock_random, mock_sample):
    mock_random.random.return_value = 0.1
    mock_random.choice.side_effect = ["update", "name"]  # operation, column

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "id"),
        "name": ColumnDefinition("name", "TEXT", lambda: "test")
    }
    generator = BatchGenerator(schema)

    engine = MutationEngine(
        conn=conn,
        schema="public",
        table_name="sales",
        primary_key="id",
        update_column="updated_at",
        generator=generator
    )

    inserted_ids = ["id1", "id2", "id3", "id4"]
    updated, deleted = engine.maybe_mutate_batch(inserted_ids)

    assert updated > 0
    assert deleted == 0
    assert engine.total_updates == updated


def test_update_records_with_and_without_update_column():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "id"),
        "name": ColumnDefinition("name", "TEXT", lambda: "john")
    }
    generator = BatchGenerator(schema)

    engine_with_col = MutationEngine(
        conn, 
        "public", 
        "users", 
        "id", 
        "updated_at", 
        generator
        )
    result = engine_with_col._update_records(["id1", "id2"])
    
    # simulate the behavior of maybe_mutate_batch
    engine_with_col.total_deletes += result
    assert result == 2

    engine_without_col = MutationEngine(conn, "public", "users", "id", None, generator)
    result = engine_without_col._update_records(["id3", "id4"])
    # simulate the behavior of maybe_mutate_batch
    engine_without_col.total_deletes += result
    assert result == 2


def test_delete_records_deletes_rows_and_tracks_count():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    # Mock generator with schema containing a UUID column
    mock_generator = MagicMock()
    mock_generator.schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "uuid")
    }

    engine = MutationEngine(
        conn, 
        schema="public", 
        table_name="sales", 
        primary_key="id", 
        generator=mock_generator
    )
    ids = ["id4", "id5"]

    result = engine._delete_records(ids)

    # simulate the behavior of maybe_mutate_batch
    engine.total_deletes += result

    cursor.execute.assert_called_once()
    query, params = cursor.execute.call_args[0]
    assert "DELETE FROM" in str(query)
    assert params == (ids,)
    assert result == 2
    assert engine.total_deletes == 2



@patch("kroft.core.mutator.random")
def test_update_skips_protected_and_reserved_columns(mock_random):
    mock_random.random.return_value = 0.2
    mock_random.choice.side_effect = ["update", "quantity"]
    mock_random.sample.return_value = ["row-123"]

    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "row-123", reserved=True),
        "updated_at": ColumnDefinition(
            "updated_at", "TIMESTAMP", lambda: "now", protected=True
        ),
        "price": ColumnDefinition("price", "FLOAT", lambda: 99.9),
        "quantity": ColumnDefinition("quantity", "INT", lambda: 10),
    }

    generator = BatchGenerator(schema)
    engine = MutationEngine(
        conn=conn,
        schema="public",
        table_name="sales",
        primary_key="id",
        update_column="updated_at",
        generator=generator
    )

    row_ids = ["row-123"]
    assert generator.get_modifiable_columns(exclude=["id"]) == ["price", "quantity"]
    updated, deleted = engine.maybe_mutate_batch(row_ids)

    assert cursor.execute.called, "Expected UPDATE query to be executed"
    assert updated == 1
    assert deleted == 0

