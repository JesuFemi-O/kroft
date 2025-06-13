import random
from typing import Dict, List, Optional

from kroft.core.schema import SchemaManager


class SchemaEvolutionController:
    def __init__(
        self,
        manager: SchemaManager,
        evolution_interval: int = 25,
        evolution_probability: float = 0.2,
        add_probability: float = 0.7,
        max_additions: int = 7,
        max_drops: int = 3
    ):
        self.manager = manager
        self.evolution_interval = evolution_interval
        self.evolution_probability = evolution_probability
        self.add_probability = add_probability
        self.max_additions = max_additions
        self.max_drops = max_drops

        self.num_additions = 0
        self.num_drops = 0
        self.evolution_log: List[Dict[str, str]] = []  # 📝 Paper trail

    def should_evolve(self, batch_number: int) -> bool:
        if batch_number % self.evolution_interval != 0:
            return False
        return random.random() < self.evolution_probability

    def choose_action(self) -> str:
        can_add = (
            self.num_additions < self.max_additions and self.has_reserved_columns()
        )
        can_drop = (
            self.num_drops < self.max_drops and self.has_droppable_columns()
        )

        if not can_add and not can_drop:
            return "none"
        if can_add and not can_drop:
            return "add"
        if not can_add and can_drop:
            return "drop"

        return "add" if random.random() <= self.add_probability else "drop"

    def evolve(self, batch_number: int) -> Optional[str]:
        if not self.should_evolve(batch_number):
            return None

        action = self.choose_action()
        if action == "none":
            return "No evolution possible"

        if action == "add":
            added = self.manager.add_column()
            if added:
                self.num_additions += 1
                self._log_evolution("add", added)
                return f"[v{self.manager.schema_version}] Added column: {added}"

        if action == "drop":
            dropped = self.manager.drop_column()
            if dropped:
                self.num_drops += 1
                self._log_evolution("drop", dropped)
                return f"[v{self.manager.schema_version}] Dropped column: {dropped}"

        return None

    def summary(self) -> Dict:
        return {
            "schema_version": self.manager.schema_version,
            "adds": self.num_additions,
            "drops": self.num_drops,
            "max_adds": self.max_additions,
            "max_drops": self.max_drops,
            "evolution_log": self.evolution_log
        }

    def has_reserved_columns(self) -> bool:
        return any(
            col.reserved and name not in self.manager.get_active_columns()
            for name, col in self.manager.columns.items()
        )

    def has_droppable_columns(self) -> bool:
        return any(
            not col.protected
            for col in self.manager.get_active_columns().values()
        )

    def _log_evolution(self, action: str, column: str):
        self.evolution_log.append({
            "version": f"v{self.manager.schema_version}",
            "action": action,
            "column": column
        })