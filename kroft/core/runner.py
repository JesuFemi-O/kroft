# kroft/core/runner.py

import logging
import random
from typing import List

from kroft.core.evolution import EvolutionController
from kroft.core.mutator import MutationEngine
from kroft.core.schema import SchemaManager

logger = logging.getLogger(__name__)


class SimulationRunner:
    def __init__(
        self,
        schema_mgr: SchemaManager,
        mutator: MutationEngine,
        evolution_controller: EvolutionController,
        total_records: int = 10_000,
        batch_size: int = 500,
    ):
        self.schema_mgr = schema_mgr
        self.mutator = mutator
        self.evolution_controller = evolution_controller
        self.total_records = total_records
        self.batch_size = batch_size
        self.total_batches = total_records // batch_size

    def run(self):
        for batch_num in range(1, self.total_batches + 1):
            batch = self._generate_batch()
            inserted_ids = self.mutator.insert_batch(batch)

            self._maybe_mutate(inserted_ids)

            result = self.evolution_controller.evolve(batch_num)
            if result:
                logger.info(result)

    def _generate_batch(self) -> List[dict]:
        return [
            {
                col: col_def.generate()
                for col, col_def in self.schema_mgr.columns.items()
            }
            for _ in range(self.batch_size)
        ]

    def _maybe_mutate(self, ids: List[str]):
        if not ids:
            return

        update_count = int(len(ids) * 0.2)
        delete_count = int(len(ids) * 0.1)

        update_ids = random.sample(ids, k=update_count) if update_count else []

        if delete_count:
            eligible = [i for i in ids if i not in update_ids]
            delete_ids = random.sample(eligible, k=delete_count)
        else:
            delete_ids = []

        self.mutator._update_records(update_ids)
        self.mutator._delete_records(delete_ids)
