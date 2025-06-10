import pytest

from kroft.core.batch import BatchGenerator
from kroft.core.column import ColumnDefinition


def test_batch_generator_creates_n_rows():
    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc-123"),
        "name": ColumnDefinition("name", "TEXT", lambda: "John Doe")
    }

    generator = BatchGenerator(schema)
    rows = generator.generate_batch(3)

    assert len(rows) == 3
    for row in rows:
        assert row["id"] == "abc-123"
        assert row["name"] == "John Doe"

def test_batch_generator_with_empty_schema():
    generator = BatchGenerator({})
    rows = generator.generate_batch(2)

    assert len(rows) == 2
    for row in rows:
        assert row == {}

def test_batch_generator_uses_fresh_values_per_row():
    import uuid

    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: str(uuid.uuid4()))
    }
    generator = BatchGenerator(schema)
    rows = generator.generate_batch(5)

    ids = [row["id"] for row in rows]
    assert len(set(ids)) == 5

def test_batch_generator_requires_schema_or_registry():
    with pytest.raises(ValueError):
        BatchGenerator()

def test_batch_generator_rejects_invalid_column_type():
    schema = {"bad": "not_a_column"}
    with pytest.raises(TypeError):
        BatchGenerator(schema=schema)