import re

from kroft.core.column import ColumnDefinition


def test_column_definition_exposes_name_type_constraints():
    col = ColumnDefinition(
        name="price",
        sql_type="FLOAT",
        generator=lambda: 9.99,
        constraints="NOT NULL"
    )

    assert col.name == "price"
    assert col.sql_type == "FLOAT"
    assert col.constraints == "NOT NULL"


def test_column_definition_generates_values():
    col = ColumnDefinition(
        name="discount",
        sql_type="FLOAT",
        generator=lambda: 25.5
    )

    assert col.generate() == 25.5


def test_column_definition_ddl_generation():
    col = ColumnDefinition(
        name="sku",
        sql_type="TEXT",
        generator=lambda: "ABC123",
        constraints="PRIMARY KEY"
    )

    ddl = col.ddl()
    assert ddl == "sku TEXT PRIMARY KEY"
    assert re.match(r"^sku\s+TEXT\s+PRIMARY KEY$", ddl)