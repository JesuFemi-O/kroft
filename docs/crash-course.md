# Kroft Crash Course

Kroft generates synthetic data and simulates how that data changes over time — new rows, updates, deletes, and even schema changes. This guide gets you from zero to a working simulation in minutes.

---

## Prerequisites

- Python 3.10+
- A running PostgreSQL instance
- `pip install kroft psycopg2-binary`

---

## Core idea

Every simulation is built from three things:

1. **A column pool** — what your table looks like, and what values each column produces
2. **A schema** — the live table in your database
3. **A mutation engine** — what happens to the data over time

You compose these yourself, which means you only opt into the complexity you need.

---

## Step 1: Define your columns

A `ColumnDefinition` pairs a column name and SQL type with a generator — a zero-argument function that produces a new value on each call.

```python
import uuid
import random
from kroft import ColumnDefinition

columns = {
    "id": ColumnDefinition(
        name="id",
        sql_type="UUID",
        generator=lambda: str(uuid.uuid4()),
        constraints="PRIMARY KEY",
    ),
    "item": ColumnDefinition(
        name="item",
        sql_type="TEXT",
        generator=lambda: random.choice(["shoes", "shirt", "hat"]),
    ),
    "quantity": ColumnDefinition(
        name="quantity",
        sql_type="INT",
        generator=lambda: random.randint(1, 10),
    ),
    "price": ColumnDefinition(
        name="price",
        sql_type="FLOAT",
        generator=lambda: round(random.uniform(10.0, 100.0), 2),
    ),
    "updated_at": ColumnDefinition(
        name="updated_at",
        sql_type="TIMESTAMP",
        generator=lambda: "now()",
        protected=True,  # never mutated or dropped
    ),
}
```

Two flags control how Kroft treats a column:

| Flag | Effect |
|------|--------|
| `reserved=True` | Excluded from the initial schema; can be added later via schema evolution |
| `protected=True` | Never chosen for updates, deletes, or schema drops |

---

## Step 2: Create the table

`SchemaManager` takes your column pool and issues DDL against a real database. Only non-reserved columns are included at creation time.

```python
import psycopg2
from kroft import SchemaManager

conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")

manager = SchemaManager(conn, schema="public", table_name="sales", columns=columns)
manager.drop_table()   # clean slate
manager.create_table()
```

---

## Step 3: Generate a batch

`BatchGenerator` turns your active columns into rows.

```python
from kroft import BatchGenerator

generator = BatchGenerator(schema=manager.get_active_columns())
rows = generator.generate_batch(batch_size=100)
# [{"id": "...", "item": "shoes", "quantity": 3, "price": 42.5, "updated_at": "now()"}, ...]
```

---

## Step 4: Insert rows

`MutationEngine` writes rows to the database and tracks insert/update/delete counts.

```python
from kroft import MutationEngine

engine = MutationEngine(
    conn=conn,
    schema="public",
    table_name="sales",
    primary_key="id",
    update_column="updated_at",
    generator=generator,
)

inserted_ids = engine.insert_batch(rows)
```

`insert_batch` returns the list of primary key values — you'll need these for updates and deletes.

---

## The four scenarios

### Scenario 1 — Insert only

Keep appending new rows. Nothing else.

```python
for _ in range(20):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    engine.insert_batch(rows)

print(engine.get_counters())
# {"total_inserts": 2000, "total_updates": 0, "total_deletes": 0}
```

---

### Scenario 2 — Insert + Update

Append new rows and update a fraction of previously inserted ones.

```python
all_ids = []

for _ in range(20):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)
    all_ids.extend(inserted_ids)

    # Update 20% of the batch just inserted, always
    engine.update_batch(inserted_ids, fraction=0.2, probability=1.0)

print(engine.get_counters())
# {"total_inserts": 2000, "total_updates": 400, "total_deletes": 0}
```

---

### Scenario 3 — Insert + Update + Delete

Full CRUD — append, update some, delete some. Control the fractions independently.

```python
for _ in range(20):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)

    # Update 20% of this batch
    engine.update_batch(inserted_ids, fraction=0.2, probability=1.0)

    # Delete 10% of this batch (non-overlapping with updates is your responsibility)
    engine.delete_batch(inserted_ids, fraction=0.1, probability=1.0)

print(engine.get_counters())
# {"total_inserts": 2000, "total_updates": 400, "total_deletes": 200}
```

Or let Kroft pick randomly between update and delete on each batch:

```python
# 80% chance something happens; when it does, 70% update / 30% delete
engine.maybe_mutate_batch(
    inserted_ids,
    probability=0.8,
    update_fraction=0.2,
    delete_fraction=0.1,
    allow_updates=True,
    allow_deletes=True,
)
```

---

### Scenario 4 — Schema evolution

Add or drop columns at runtime while data keeps flowing. Mark columns as `reserved=True` upfront — they sit in the pool but are excluded from the initial table. `EvolutionController` decides when to promote or drop them.

```python
columns_with_reserved = {
    **columns,
    "discount": ColumnDefinition("discount", "FLOAT", lambda: 0.0, reserved=True),
    "region": ColumnDefinition("region", "TEXT", lambda: random.choice(["NA", "EU", "ASIA"]), reserved=True),
}

manager = SchemaManager(conn, "public", "sales", columns_with_reserved)
manager.drop_table()
manager.create_table()
```

```python
from kroft import EvolutionController

controller = EvolutionController(
    manager=manager,
    evolution_interval=5,       # consider evolving every 5 batches
    evolution_probability=0.8,  # 80% chance it actually fires
    add_probability=0.7,        # when it fires, 70% chance of adding vs dropping
    max_additions=2,            # add at most 2 columns total
    max_drops=1,                # drop at most 1 column total
)
```

```python
for batch_num in range(1, 21):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)

    engine.update_batch(inserted_ids, fraction=0.2, probability=0.8)

    result = controller.evolve(batch_num)
    if result:
        print(result)  # e.g. "[v2] Added column: discount"

print(controller.summary())
```

Schema evolution and mutations are fully independent — mix and match any combination.

---

## Checking your counters

```python
print(engine.get_counters())
# {"total_inserts": 2000, "total_updates": 320, "total_deletes": 180}

print(controller.summary())
# {"schema_version": 3, "adds": 2, "drops": 1, "max_adds": 2, "max_drops": 1, "evolution_log": [...]}
```
