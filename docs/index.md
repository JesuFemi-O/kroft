# Kroft

Kroft generates synthetic data and simulates how that data changes over time — new rows, updates, deletes, and even schema changes. It's designed for testing ETL pipelines, CDC workflows, and application behaviour under realistic data change scenarios.

## Features

- **Four composable scenarios** — insert only, insert + update, insert + update + delete, schema evolution
- **No forced complexity** — opt into only what your test needs
- **Schema evolution** — add or drop columns at runtime while data keeps flowing
- **Configurable mutations** — control fractions and probabilities per operation
- **Registry pattern** — define reusable column generators with decorators

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
}

conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")
manager = SchemaManager(conn, "public", "sales", columns)
manager.drop_table()
manager.create_table()

generator = BatchGenerator(schema=manager.get_active_columns())
engine = MutationEngine(conn=conn, schema="public", table_name="sales",
                        primary_key="id", update_column="updated_at", generator=generator)

for _ in range(10):
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)
    engine.update_batch(inserted_ids, fraction=0.2, probability=0.8)

print(engine.get_counters())
```

## Next steps

- [Crash Course](crash-course.md) — walk through all four scenarios step by step
- [Advanced Usage](advanced-usage.md) — registry pattern, custom loops, logging, schema history
- [API Reference](api-reference.md) — full class and method documentation
