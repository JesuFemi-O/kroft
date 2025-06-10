from typing import Any, Callable, Optional


class ColumnDefinition:
    def __init__(
        self,
        name: str,
        sql_type: str,
        generator: Callable[[], Any],
        constraints: Optional[str] = None
    ):
        self.name = name
        self.sql_type = sql_type
        self.generator = generator
        self.constraints = constraints or ""

    def generate(self) -> Any:
        return self.generator()

    def ddl(self) -> str:
        return f"{self.name} {self.sql_type} {self.constraints}".strip()