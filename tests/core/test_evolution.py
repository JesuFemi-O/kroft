from unittest.mock import MagicMock

from kroft.core.column import ColumnDefinition
from kroft.core.evolution import EvolutionController
from kroft.core.schema import SchemaManager


def test_schema_evolver_skips_batches_and_limits_evolution(monkeypatch):
    # Mock DB connection + cursor
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    # Schema columns
    columns = {
        "id": ColumnDefinition(
            "id", "UUID", lambda: "uuid", reserved=False, protected=True
        ),
        "name": ColumnDefinition("name", "TEXT", lambda: "John", reserved=False),
        "age": ColumnDefinition("age", "INT", lambda: 30, reserved=True),
        "email": ColumnDefinition("email", "TEXT", lambda: "a@b.com", reserved=True),
    }

    # Patch randomness to always evolve and choose the first option
    monkeypatch.setattr("random.random", lambda: 0.0)
    monkeypatch.setattr("random.choice", lambda x: x[0])

    manager = SchemaManager(conn, "public", "users", columns)
    manager.create_table()

    evolver = EvolutionController(
        manager=manager,
        evolution_interval=5,
        evolution_probability=1.0,
        add_probability=1.0,
        max_additions=2,
        max_drops=1
    )

    # Batch 5: add
    msg1 = evolver.evolve(5)
    assert "[v2] Added column: age" in msg1

    # Batch 10: add
    msg2 = evolver.evolve(10)
    assert "[v3] Added column: email" in msg2

    # Batch 15: drop
    msg3 = evolver.evolve(15)
    assert "[v4] Dropped column: name" in msg3

    # Batch 20: no more actions allowed
    msg4 = evolver.evolve(20)
    assert msg4 == "No evolution possible"

    # Summary check
    assert evolver.summary()["adds"] == 2
    assert evolver.summary()["drops"] == 1
    assert evolver.summary()["schema_version"] == 4