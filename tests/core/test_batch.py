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

def test_generate_value_for_column():
    schema = {
        "name": ColumnDefinition("name", "TEXT", lambda: "John")
    }

    generator = BatchGenerator(schema)
    value = generator._generate_value("name")
    assert value == "John"

    with pytest.raises(ValueError):
        generator._generate_value("nonexistent")


def test_get_modifiable_columns_excludes_reserved_protected_and_explicit():
    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "1", reserved=True),
        "updated_at": ColumnDefinition(
            "updated_at", "TIMESTAMP", lambda: "now", protected=True
        ),
        "price": ColumnDefinition("price", "FLOAT", lambda: 19.99),
        "quantity": ColumnDefinition("quantity", "INT", lambda: 2),
        "internal_flag": ColumnDefinition("internal_flag", "BOOLEAN", lambda: False)
    }

    generator = BatchGenerator(schema)

    # exclude "internal_flag" dynamically
    modifiable = generator.get_modifiable_columns(exclude=["internal_flag"])

    # "id", "updated_at", and "internal_flag" are excluded
    assert set(modifiable) == {"price", "quantity"}  


def test_modifiable_columns_respect_reserved_protected_and_exclude():
    schema = {
        "id": ColumnDefinition("id", "UUID", lambda: "1", reserved=True),
        "updated_at": ColumnDefinition(
            "updated_at", "TIMESTAMP", lambda: "now", protected=True
        ),
        "price": ColumnDefinition("price", "FLOAT", lambda: 19.99),
        "quantity": ColumnDefinition("quantity", "INT", lambda: 2),
        "internal_flag": ColumnDefinition("internal_flag", "BOOLEAN", lambda: False)
    }

    generator = BatchGenerator(schema)
    modifiable = generator.get_modifiable_columns(exclude=["internal_flag"])

    # "id" is reserved, "updated_at" is protected, "internal_flag" is excluded
    assert set(modifiable) == {"price", "quantity"}
