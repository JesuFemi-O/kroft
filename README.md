# Kroft

A Python library for generating synthetic data and simulating schema evolution — useful for testing ETL pipelines, CDC workflows, and application behaviour under realistic data change scenarios.

## Features

- **Four composable scenarios** — insert only, insert + update, insert + update + delete, schema evolution
- **No forced complexity** — opt into only what your test needs
- **Configurable mutations** — control fractions and probabilities per operation
- **Schema evolution** — add or drop columns at runtime while data keeps flowing
- **Registry pattern** — define reusable column generators with decorators
- **42 tests, 92% coverage** — well-tested core with clear separation of concerns

## Installation

```bash
pip install kroft
```

Or with `uv`:

```bash
uv add kroft
```

## Quick start

```python
import uuid, random, psycopg2
from kroft import ColumnDefinition, SchemaManager, BatchGenerator, MutationEngine

columns = {
    "id": ColumnDefinition("id", "UUID", lambda: str(uuid.uuid4()), constraints="PRIMARY KEY"),
    "item": ColumnDefinition("item", "TEXT", lambda: random.choice(["shoes", "shirt", "hat"])),
    "price": ColumnDefinition("price", "FLOAT", lambda: round(random.uniform(10, 100), 2)),
    "updated_at": ColumnDefinition("updated_at", "TIMESTAMP", lambda: "now()", protected=True),
    "discount": ColumnDefinition("discount", "FLOAT", lambda: 0.0, reserved=True),
}

conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")

manager = SchemaManager(conn, "public", "sales", columns)
manager.drop_table()
manager.create_table()

generator = BatchGenerator(schema=manager.get_active_columns())
engine = MutationEngine(
    conn=conn, schema="public", table_name="sales",
    primary_key="id", update_column="updated_at", generator=generator,
)

for _ in range(10):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)
    engine.update_batch(inserted_ids, fraction=0.2, probability=0.8)

print(engine.get_counters())
```

## Supported scenarios

| Scenario | How |
|----------|-----|
| Insert only | `engine.insert_batch(rows)` |
| Insert + Update | `insert_batch()` + `update_batch(ids, fraction=0.2)` |
| Insert + Update + Delete | `insert_batch()` + `update_batch()` + `delete_batch()` |
| Schema evolution | `EvolutionController.evolve(batch_num)` |

Each scenario is independently composable — you are not forced into mutations or schema evolution unless you opt in.

## Components

| Component | Role |
|-----------|------|
| `ColumnDefinition` | Defines a column: name, SQL type, generator function, and flags (reserved, protected) |
| `SchemaManager` | Manages table schema — CREATE/ALTER/DROP DDL, active vs reserved columns, version history |
| `BatchGenerator` | Generates synthetic rows from a column schema |
| `MutationEngine` | Performs insert, update, and delete operations against a live database |
| `EvolutionController` | Controls when and how the schema evolves during a simulation |
| `SimulationRunner` | Orchestrates a full simulation loop — generate, mutate, evolve |

## Documentation

- [Crash Course](docs/crash-course.md) — walk through all four scenarios step by step
- [Advanced Usage](docs/advanced-usage.md) — registry pattern, custom loops, logging, schema history
- [API Reference](https://jesufemi-o.github.io/kroft/api-reference) — full class and method documentation

## Development

```bash
git clone https://github.com/JesuFemi-O/kroft
cd kroft
uv sync --extra dev
uv run pytest
```

## License

MIT
