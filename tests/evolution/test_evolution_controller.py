from unittest.mock import MagicMock

from kroft.core.column import ColumnDefinition
from kroft.core.evolution import EvolutionController
from kroft.core.schema import SchemaManager


def test_schema_evolution_controller_add_and_drop(monkeypatch):
    # Fake DB connection + cursor
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor

    # Define base and reserved columns
    columns = {
        "id": ColumnDefinition(
            "id", "UUID", lambda: "uuid", reserved=False, protected=True
            ),
        "name": ColumnDefinition("name", "TEXT", lambda: "John", reserved=False),
        "age": ColumnDefinition("age", "INT", lambda: 30, reserved=True),
        "email": ColumnDefinition("email", "TEXT", lambda: "a@b.com", reserved=True),
    }

    # Predictable randomness
    monkeypatch.setattr("random.choice", lambda x: x[0])  # Always choose first
    monkeypatch.setattr("random.random", lambda: 0.0)     # Always evolve/add

    # Initialize schema and evolution manager
    manager = SchemaManager(conn, "public", "users", columns)
    manager.create_table()
    controller = EvolutionController(
        manager=manager,
        evolution_interval=1,
        evolution_probability=1.0,
        add_probability=1.0,
        max_additions=2,
        max_drops=1
    )

    # Batch 1: Add "age"
    msg1 = controller.evolve(batch_number=1)
    assert "[v2] Added column: age" in msg1

    # Batch 2: Add "email"
    msg2 = controller.evolve(batch_number=2)
    assert "[v3] Added column: email" in msg2

    # Batch 3: Drop "name"
    msg3 = controller.evolve(batch_number=3)
    assert "[v4] Dropped column: name" in msg3

    # Batch 4: Nothing left to evolve
    msg4 = controller.evolve(batch_number=4)
    assert msg4 == "No evolution possible"

    # Assertions
    assert controller.num_additions == 2
    assert controller.num_drops == 1
    assert manager.schema_version == 4
    assert set(manager.get_active_columns().keys()) == {"id", "age", "email"}
    assert controller.evolution_log == [
        {"version": "v2", "action": "add", "column": "age"},
        {"version": "v3", "action": "add", "column": "email"},
        {"version": "v4", "action": "drop", "column": "name"},
    ]