from typing import Any, Callable, Optional


class ColumnDefinition:
    def __init__(
        self,
        name: str,
        sql_type: str,
        generator: Callable[[], Any],
        constraints: Optional[str] = None,
        reserved: bool = False,
        protected: bool = False,
    ):
        self.name = name
        self.sql_type = sql_type
        self.generator = generator
        self.constraints = constraints or ""
        self.reserved = reserved
        self.protected = protected

    def generate(self) -> Any:
        return self.generator()

    def ddl(self) -> str:
        parts = [self.name, self.sql_type, self.constraints.strip()]
        return " ".join(p for p in parts if p)