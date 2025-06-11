import random


class SchemaEvolutionController:
    def __init__(
        self,
        evolution_interval: int = 25,
        evolution_probability: float = 0.2,  # 20% chance to evolve
        add_probability: float = 0.7,        # 70% of evolutions are adds
        max_additions: int = 5,
        max_drops: int = 3
    ):
        self.evolution_interval = evolution_interval
        self.evolution_probability = evolution_probability
        self.add_probability = add_probability
        self.max_additions = max_additions
        self.max_drops = max_drops

        self.num_additions = 0
        self.num_drops = 0

    def should_evolve(self, batch_number: int) -> bool:
        return (
            batch_number % self.evolution_interval == 0
            and random.random() < self.evolution_probability
        )

    def choose_action(self) -> str:
        can_add = self.num_additions < self.max_additions
        can_drop = self.num_drops < self.max_drops

        if not can_add and not can_drop:
            return "none"
        if can_add and not can_drop:
            return "add"
        if not can_add and can_drop:
            return "drop"

        return "add" if random.random() < self.add_probability else "drop"

    def record_action(self, action: str):
        if action == "add":
            self.num_additions += 1
        elif action == "drop":
            self.num_drops += 1

    def summary(self):
        return {
            "adds": self.num_additions,
            "drops": self.num_drops,
            "max_adds": self.max_additions,
            "max_drops": self.max_drops
        }