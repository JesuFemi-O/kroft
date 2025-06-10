from typing import Callable, Dict, Optional

from kroft.core.column import ColumnDefinition

_COLUMN_REGISTRY: Dict[str, ColumnDefinition] = {}

def register_column(name: str, sql_type: str, constraints: Optional[str] = None):
    def decorator(func: Callable[[], object]):
        col_def = ColumnDefinition(
            name=name,
            sql_type=sql_type,
            generator=func,
            constraints=constraints
        )
        _COLUMN_REGISTRY[name] = col_def
        return func
    return decorator

def get_registered_columns() -> Dict[str, ColumnDefinition]:
    return _COLUMN_REGISTRY.copy()