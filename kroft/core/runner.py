# kroft/core/runner.py

import random
from typing import List, Dict
from kroft.core.schema import SchemaManager
from kroft.core.mutator import MutationEngine
from kroft.core.column import ColumnDefinition

class SimulationRunner:
    def __init__(
        self,
        schema_mgr: SchemaManager,
        mutator: MutationEngine,
        column_registry: Dict[str, ColumnDefinition],
        total_records: int = 10_000,
        batch_size: int = 500,
        enable_schema_evolution: bool = True,
        evolution_interval: int = 5,
        evolution_probability: float = 0.2,
        add_probability: float = 0.7,
        protected_columns: set = None
    ):
        self.schema_mgr = schema_mgr
        self.mutator = mutator
        self.column_registry = column_registry
        self.total_records = total_records
        self.batch_size = batch_size
        self.enable_schema_evolution = enable_schema_evolution
        self.evolution_interval = evolution_interval
        self.evolution_probability = evolution_probability
        self.add_probability = add_probability
        self.protected_columns = protected_columns
        self.total_batches = total_records // batch_size

    def run(self):
        for batch_num in range(1, self.total_batches + 1):
            batch = self._generate_batch()
            inserted_ids = self.mutator.insert_batch(batch)

            self._maybe_mutate(inserted_ids)

            if self.enable_schema_evolution and batch_num % self.evolution_interval == 0:
                self._maybe_evolve_schema()

    def _generate_batch(self) -> List[dict]:
        return [
            {col: col_def.generate() for col, col_def in self.schema_mgr.columns.items()}
            for _ in range(self.batch_size)
        ]

    def _maybe_mutate(self, ids: List[str]):
        if not ids:
            return

        update_count = int(len(ids) * 0.2)
        delete_count = int(len(ids) * 0.1)

        update_ids = random.sample(ids, k=update_count) if update_count else []
        delete_ids = random.sample([i for i in ids if i not in update_ids], k=delete_count) if delete_count else []

        self.mutator._update_records(update_ids)
        self.mutator._delete_records(delete_ids)

    def _maybe_evolve_schema(self):
        if random.random() > self.evolution_probability:
            return

        if random.random() < self.add_probability:
            added = self.schema_mgr.add_column(self.column_registry)
            if added:
                print(f"🟢 Added column: {added}")
        else:
            dropped = self.schema_mgr.drop_column(self.protected_columns or set())
            if dropped:
                print(f"🔴 Dropped column: {dropped}")