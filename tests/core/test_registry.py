from kroft.core.column import ColumnDefinition
from kroft.core.registry import get_registered_columns, register_column


def test_register_column_adds_to_registry():
    @register_column(name="test_col", sql_type="TEXT")
    def fake_gen():
        return "hello"

    registered = get_registered_columns()
    assert "test_col" in registered

    col_def = registered["test_col"]
    assert isinstance(col_def, ColumnDefinition)
    assert col_def.name == "test_col"
    assert col_def.sql_type == "TEXT"
    assert col_def.generate() == "hello"

def test_register_column_with_constraints():
    @register_column(name="id", sql_type="UUID", constraints="PRIMARY KEY")
    def fake_id():
        return "abc-123"

    col_def = get_registered_columns()["id"]
    assert col_def.constraints == "PRIMARY KEY"
    assert col_def.ddl() == "id UUID PRIMARY KEY"

def test_register_column_sets_protected_flag():
    @register_column(name="created_by", sql_type="TEXT", protected=True)
    def gen_user():
        return "system"

    col_def = get_registered_columns()["created_by"]
    assert col_def.protected is True