from typing import Callable, Dict, Optional

from kroft.core.column import ColumnDefinition

_COLUMN_REGISTRY: Dict[str, ColumnDefinition] = {}

def register_column(
    name: str,
    sql_type: str,
    constraints: Optional[str] = None,
    reserved: bool = False,
    protected: bool = False
):
    def decorator(func: Callable[[], object]):
        _COLUMN_REGISTRY[name] = ColumnDefinition(
            name=name,
            sql_type=sql_type,
            generator=func,
            constraints=constraints,
            reserved=reserved,
            protected=protected
        )
        return func
    return decorator

def get_registered_columns() -> Dict[str, ColumnDefinition]:
    return _COLUMN_REGISTRY.copy()