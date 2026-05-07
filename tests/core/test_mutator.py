from unittest.mock import MagicMock, patch

from kroft.core.batch import BatchGenerator
from kroft.core.column import ColumnDefinition
from kroft.core.mutator import MutationEngine


def _make_engine(conn, generator=None, update_column="updated_at"):
    return MutationEngine(
        conn=conn,
        schema="public",
        table_name="sales",
        primary_key="id",
        update_column=update_column,
        generator=generator,
    )


def _make_generator():
    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "id"),
        "name": ColumnDefinition("name", "TEXT", lambda: "john"),
        "price": ColumnDefinition("price", "FLOAT", lambda: 9.99),
    }
    return BatchGenerator(schema)


def _mock_conn():
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    return conn, cursor


@patch("kroft.core.mutator.execute_values")
def test_insert_batch_inserts_rows_and_tracks_count(mock_execute_values):
    conn, _ = _mock_conn()
    engine = _make_engine(conn)

    rows = [{"id": "abc", "name": "Hat"}, {"id": "def", "name": "Shirt"}]
    inserted_ids = engine.insert_batch(rows)

    mock_execute_values.assert_called_once()
    assert inserted_ids == ["abc", "def"]
    assert engine.total_inserts == 2


def test_insert_batch_returns_empty_for_no_rows():
    conn, _ = _mock_conn()
    engine = _make_engine(conn)
    assert engine.insert_batch([]) == []
    assert engine.total_inserts == 0


# --- update_batch ---

def test_update_batch_updates_fraction_of_ids():
    conn, cursor = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    ids = [f"id{i}" for i in range(10)]
    updated = engine.update_batch(ids, fraction=0.5, probability=1.0)

    assert updated == 5
    assert engine.total_updates == 5


def test_update_batch_skips_when_probability_zero():
    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    updated = engine.update_batch(["id1", "id2"], fraction=0.5, probability=0.0)
    assert updated == 0
    assert engine.total_updates == 0


def test_update_batch_skips_empty_ids():
    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())
    assert engine.update_batch([]) == 0


# --- delete_batch ---

def test_delete_batch_deletes_fraction_of_ids():
    conn, cursor = _mock_conn()
    mock_generator = MagicMock()
    mock_generator.schema = {"id": ColumnDefinition("id", "UUID", lambda: "x")}
    engine = _make_engine(conn, generator=mock_generator)

    ids = [f"id{i}" for i in range(10)]
    deleted = engine.delete_batch(ids, fraction=0.3, probability=1.0)

    assert deleted == 3
    assert engine.total_deletes == 3


def test_delete_batch_skips_when_probability_zero():
    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    deleted = engine.delete_batch(["id1", "id2"], fraction=0.5, probability=0.0)
    assert deleted == 0
    assert engine.total_deletes == 0


def test_delete_batch_skips_empty_ids():
    conn, _ = _mock_conn()
    engine = _make_engine(conn)
    assert engine.delete_batch([]) == 0


# --- maybe_mutate_batch ---

@patch("kroft.core.mutator.random.sample", return_value=["id1"])
@patch("kroft.core.mutator.random")
def test_maybe_mutate_batch_update_path(mock_random, mock_sample):
    mock_random.random.return_value = 0.1   # below default probability of 0.5
    mock_random.choice.side_effect = ["update", "name"]

    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    updated, deleted = engine.maybe_mutate_batch(
        ["id1", "id2", "id3", "id4"],
        allow_updates=True,
        allow_deletes=False,
    )

    assert updated > 0
    assert deleted == 0
    assert engine.total_updates == updated


@patch("kroft.core.mutator.random")
def test_maybe_mutate_batch_skips_when_probability_not_met(mock_random):
    mock_random.random.return_value = 0.99  # above probability

    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    updated, deleted = engine.maybe_mutate_batch(
        ["id1", "id2"], probability=0.5
    )
    assert updated == 0
    assert deleted == 0


def test_maybe_mutate_batch_skips_when_no_operations_allowed():
    conn, _ = _mock_conn()
    engine = _make_engine(conn, generator=_make_generator())

    updated, deleted = engine.maybe_mutate_batch(
        ["id1"], allow_updates=False, allow_deletes=False
    )
    assert updated == 0
    assert deleted == 0


def test_maybe_mutate_batch_skips_empty_ids():
    conn, _ = _mock_conn()
    engine = _make_engine(conn)
    assert engine.maybe_mutate_batch([]) == (0, 0)


# --- _update_records ---

def test_update_records_with_and_without_update_column():
    conn, _ = _mock_conn()
    generator = _make_generator()

    engine_with = _make_engine(conn, generator=generator, update_column="updated_at")
    assert engine_with._update_records(["id1", "id2"]) == 2

    engine_without = _make_engine(conn, generator=generator, update_column=None)
    assert engine_without._update_records(["id3", "id4"]) == 2


# --- _delete_records ---

def test_delete_records_deletes_rows():
    conn, cursor = _mock_conn()
    mock_generator = MagicMock()
    mock_generator.schema = {"id": ColumnDefinition("id", "UUID", lambda: "x")}
    engine = _make_engine(conn, generator=mock_generator)

    result = engine._delete_records(["id4", "id5"])
    engine.total_deletes += result

    cursor.execute.assert_called_once()
    query, params = cursor.execute.call_args[0]
    assert "DELETE FROM" in str(query)
    assert params == (["id4", "id5"],)
    assert result == 2
    assert engine.total_deletes == 2


# --- protected/reserved columns ---

@patch("kroft.core.mutator.random")
def test_update_skips_protected_and_reserved_columns(mock_random):
    mock_random.random.return_value = 0.2
    mock_random.choice.side_effect = ["update", "price"]
    mock_random.sample.return_value = ["row-123"]

    conn, cursor = _mock_conn()

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
        conn=conn, schema="public", table_name="sales",
        primary_key="id", update_column="updated_at", generator=generator,
    )

    assert generator.get_modifiable_columns(exclude=["id"]) == ["price", "quantity"]

    updated, deleted = engine.maybe_mutate_batch(
        ["row-123"], allow_updates=True, allow_deletes=False
    )
    assert cursor.execute.called
    assert updated == 1
    assert deleted == 0
