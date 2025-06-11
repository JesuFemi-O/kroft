from unittest.mock import MagicMock
from kroft.core.runner import SimulationRunner
from kroft.core.column import ColumnDefinition


def test_simulation_runner_generates_batches_and_mutates():
    schema_mgr = MagicMock()
    mutator = MagicMock()

    # Simulate 2 active columns
    schema_mgr.columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "name": ColumnDefinition("name", "TEXT", lambda: "John")
    }

    # Mock mutation return
    mutator.insert_batch.return_value = ["id1", "id2", "id3", "id4", "id5"]

    runner = SimulationRunner(
        schema_mgr=schema_mgr,
        mutator=mutator,
        column_registry={},
        total_records=10,
        batch_size=5,
        enable_schema_evolution=False  # skip evolution for now
    )

    runner.run()

    # Should run 2 batches
    assert mutator.insert_batch.call_count == 2
    assert mutator._update_records.call_count == 2
    assert mutator._delete_records.call_count == 2


def test_simulation_runner_triggers_schema_evolution():
    schema_mgr = MagicMock()
    mutator = MagicMock()

    schema_mgr.columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "name": ColumnDefinition("name", "TEXT", lambda: "John")
    }

    # Prevent mutation logic from failing
    mutator.insert_batch.return_value = ["id1", "id2", "id3"]

    runner = SimulationRunner(
        schema_mgr=schema_mgr,
        mutator=mutator,
        column_registry={},
        total_records=5,
        batch_size=1,
        enable_schema_evolution=True,
        evolution_interval=1,
        evolution_probability=1.0,  # always evolve
        add_probability=1.0,        # always add
        protected_columns=None
    )

    runner.run()

    # Should attempt to add a column on each batch
    assert schema_mgr.add_column.call_count == 5


def test_simulation_runner_skips_when_zero_records():
    schema_mgr = MagicMock()
    mutator = MagicMock()
    schema_mgr.columns = {"id": ColumnDefinition("id", "UUID", lambda: "abc")}

    runner = SimulationRunner(
        schema_mgr=schema_mgr,
        mutator=mutator,
        column_registry={},
        total_records=0,
        batch_size=5,
    )

    runner.run()

    mutator.insert_batch.assert_not_called()
    mutator._update_records.assert_not_called()
    mutator._delete_records.assert_not_called()


def test_simulation_runner_handles_empty_insert_batch():
    schema_mgr = MagicMock()
    mutator = MagicMock()
    schema_mgr.columns = {"id": ColumnDefinition("id", "UUID", lambda: "abc")}
    mutator.insert_batch.return_value = []  # Simulate empty insert

    runner = SimulationRunner(
        schema_mgr=schema_mgr,
        mutator=mutator,
        column_registry={},
        total_records=5,
        batch_size=5,
    )

    runner.run()

    mutator._update_records.assert_not_called()
    mutator._delete_records.assert_not_called()

def test_simulation_runner_does_not_drop_protected_columns():
    schema_mgr = MagicMock()
    mutator = MagicMock()
    schema_mgr.columns = {
        "id": ColumnDefinition("id", "UUID", lambda: "abc"),
        "created_at": ColumnDefinition("created_at", "TIMESTAMP", lambda: "now"),
        "customer": ColumnDefinition("customer", "TEXT", lambda: "Alice")
    }

    mutator.insert_batch.return_value = ["a"]

    runner = SimulationRunner(
        schema_mgr=schema_mgr,
        mutator=mutator,
        column_registry={"new_col": ColumnDefinition("new_col", "TEXT", lambda: "hi")},
        total_records=3,
        batch_size=1,
        enable_schema_evolution=True,
        evolution_interval=1,
        evolution_probability=1.0,
        add_probability=0.0,  # always drop
        protected_columns={"id", "created_at"}
    )

    runner.run()

    # Should never attempt to drop a protected column
    args = schema_mgr.drop_column.call_args_list
    for call in args:
        protected = call[0][0]
        assert "id" in protected
        assert "created_at" in protected