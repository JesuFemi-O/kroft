from typing import Any, Dict, List, Optional

from kroft.core.column import ColumnDefinition


class BatchGenerator:
    def __init__(
        self,
        schema: Optional[Dict[str, ColumnDefinition]] = None,
        use_registry: bool = False
    ):
        """
        Initialize a batch generator.

        Args:
            schema: A dictionary of column name -> ColumnDefinition.
            use_registry: If True, loads schema from the registered column registry.

        Raises:
            ValueError: If neither schema nor use_registry is provided.
            TypeError: If the schema is not a dict of ColumnDefinition objects.
        """
        if schema is not None:
            self.schema = schema
        elif use_registry:
            from kroft.core.registry import get_registered_columns
            self.schema = get_registered_columns()
        else:
            raise ValueError("Must provide a schema or set use_registry=True")

        if not isinstance(self.schema, dict):
            raise TypeError("Schema must be a dictionary of ColumnDefinition objects.")

        for name, col in self.schema.items():
            if not isinstance(col, ColumnDefinition):
                raise TypeError(f"Schema entry '{name}' is not a ColumnDefinition.")

    def generate_batch(self, batch_size: int = 1) -> List[Dict[str, Any]]:
        rows = []
        for _ in range(batch_size):
            row = {
                name: col_def.generate()
                for name, col_def in self.schema.items()
            }
            rows.append(row)
        return rows