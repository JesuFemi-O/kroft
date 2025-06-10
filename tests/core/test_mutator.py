from unittest.mock import MagicMock, patch
from kroft.core.mutator import MutationEngine
import pytest

@patch("kroft.core.mutator.execute_values")
def test_insert_batch_inserts_rows_and_tracks_count(mock_execute_values):
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    engine = MutationEngine(conn, schema="public", table_name="products")

    rows = [
        {"id": "abc", "name": "Hat"},
        {"id": "def", "name": "Shirt"}
    ]

    inserted_ids = engine.insert_batch(rows)

    mock_execute_values.assert_called_once()
    assert inserted_ids == ["abc", "def"]
    assert engine.total_inserts == 2


@patch("kroft.core.mutator.random")
def test_maybe_mutate_batch_calls_update_or_delete(mock_random):
    # Force mutation path
    mock_random.random.return_value = 0.1  # Below threshold to trigger mutation
    mock_random.choice.return_value = "update"  # Force 'update' operation

    engine = MutationEngine(conn=MagicMock(), schema="public", table_name="sales")

    engine._update_records = MagicMock(return_value=2)
    engine._delete_records = MagicMock(return_value=0)

    inserted_ids = ["id1", "id2", "id3", "id4"]
    updated, deleted = engine.maybe_mutate_batch(inserted_ids)

    engine._update_records.assert_called_once()
    engine._delete_records.assert_not_called()
    assert updated == 2
    assert deleted == 0

def test_update_records_updates_rows_and_tracks_count():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    engine = MutationEngine(conn, schema="public", table_name="sales")
    ids = ["id1", "id2", "id3"]

    result = engine._update_records(ids)

    cursor.executemany.assert_called_once()
    args = cursor.executemany.call_args[0]
    assert f"UPDATE public.sales" in args[0]
    assert len(args[1]) == 3
    assert all(row[1] in ids for row in args[1])
    assert result == 3
    assert engine.total_updates == 3


def test_delete_records_deletes_rows_and_tracks_count():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    engine = MutationEngine(conn, schema="public", table_name="sales")
    ids = ["id4", "id5"]

    result = engine._delete_records(ids)

    cursor.execute.assert_called_once()
    query, params = cursor.execute.call_args[0]
    assert f"DELETE FROM public.sales" in query
    assert params == (ids,)
    assert result == 2
    assert engine.total_deletes == 2