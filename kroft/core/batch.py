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

    def _generate_value(self, column: str) -> Any:
        """Generate a single value for the given column name."""
        col_def = self.schema.get(column)
        if not col_def:
            raise ValueError(f"No column definition found for column '{column}'")
        return col_def.generate()
    
    def generate_value(self, column: str) -> Any:
        return self._generate_value(column)

    def generate_batch(self, batch_size: int = 1) -> List[Dict[str, Any]]:
        rows = []
        for _ in range(batch_size):
            row = {
                name: self._generate_value(name)
                for name in self.schema
            }
            rows.append(row)
        return rows
    
    def get_modifiable_columns(self, exclude: Optional[List[str]] = None) -> List[str]:
        exclude = set(exclude or [])
        return [
            name for name, col in self.schema.items()
            if not col.reserved and not col.protected and name not in exclude
        ]