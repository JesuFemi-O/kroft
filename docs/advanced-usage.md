# Advanced Usage

This guide covers patterns for users who want more control than the crash course provides.

---

## The column registry

Instead of defining a `columns` dict inline, you can register column definitions as decorated functions. This is useful when you want to share column definitions across multiple simulations or modules.

```python
import uuid
import random
from kroft.core.registry import register_column, get_registered_columns

@register_column(name="id", sql_type="UUID", constraints="PRIMARY KEY")
def id_gen():
    return str(uuid.uuid4())

@register_column(name="updated_at", sql_type="TIMESTAMP", protected=True)
def updated_at_gen():
    return "now()"

@register_column(name="product", sql_type="TEXT")
def product_gen():
    return random.choice(["bag", "jacket", "cap"])

@register_column(name="unit_price", sql_type="FLOAT")
def unit_price_gen():
    return round(random.uniform(20.0, 200.0), 2)

# Reserved — excluded from initial schema, available for evolution
@register_column(name="category", sql_type="TEXT", reserved=True)
def category_gen():
    return random.choice(["clothing", "accessory", "footwear"])
```

Build your `SchemaManager` and `BatchGenerator` from the registry:

```python
from kroft import SchemaManager, BatchGenerator
import psycopg2

columns = get_registered_columns()
conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")

manager = SchemaManager(conn, "public", "orders", columns)
manager.drop_table()
manager.create_table()

generator = BatchGenerator(use_registry=True)
```

The registry is global per process. If you run multiple simulations in the same process, call `get_registered_columns()` once and pass the result explicitly to avoid cross-contamination.

---

## Dynamically registering columns at runtime

You can add a new column to a live `SchemaManager` without restarting:

```python
from kroft import ColumnDefinition

new_col = ColumnDefinition("promo_code", "TEXT", lambda: "NONE", reserved=True)
added = manager.register_column("promo_code", new_col)

if added:
    print("promo_code is now available for schema evolution")
```

`register_column` returns `False` if the name already exists — it will not overwrite.

---

## Rolling your own simulation loop

`SimulationRunner` is a convenience wrapper. For full control — custom mutation ratios, conditional evolution, per-batch logging — drive the components directly:

```python
from kroft import SchemaManager, BatchGenerator, MutationEngine, EvolutionController
import psycopg2, uuid, random

conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")

# ... define columns, manager, generator, engine, controller as usual ...

for batch_num in range(1, 51):
    # Always refresh schema in case evolution added a column
    generator.schema = manager.get_active_columns()

    rows = generator.generate_batch(batch_size=200)
    inserted_ids = engine.insert_batch(rows)

    # First 10 batches: insert only
    if batch_num <= 10:
        pass

    # Batches 11–30: insert + update
    elif batch_num <= 30:
        engine.update_batch(inserted_ids, fraction=0.25, probability=0.9)

    # Batches 31+: full CRUD
    else:
        engine.update_batch(inserted_ids, fraction=0.2, probability=0.9)
        engine.delete_batch(inserted_ids, fraction=0.05, probability=0.5)

    # Schema evolution every 10 batches
    if batch_num % 10 == 0:
        result = controller.evolve(batch_num)
        if result:
            print(f"Batch {batch_num}: {result}")

print(engine.get_counters())
print(controller.summary())
```

This pattern is useful when your simulation has distinct phases — e.g. a backfill period followed by steady-state CDC traffic.

---

## Using SimulationRunner

For simpler use cases, `SimulationRunner` handles the loop for you. It calls `insert_batch`, `maybe_mutate_batch`, and `controller.evolve` on every batch.

```python
from kroft import SimulationRunner, EvolutionController

controller = EvolutionController(
    manager=manager,
    evolution_interval=10,
    evolution_probability=0.5,
    max_additions=3,
    max_drops=1,
)

runner = SimulationRunner(
    schema_mgr=manager,
    mutator=engine,
    evolution_controller=controller,
    total_records=10_000,
    batch_size=500,
)

runner.run()
print(engine.get_counters())
```

`SimulationRunner` uses `maybe_mutate_batch` with its defaults — 50% chance of mutation, 20% update fraction, 10% delete fraction, updates and deletes both enabled. To change this behaviour, roll your own loop.

### Reproducible runs

Pass `seed` to get the same data and mutation pattern on every run — useful when you need to reproduce a specific failure or share a scenario with a colleague:

```python
runner = SimulationRunner(
    schema_mgr=manager,
    mutator=engine,
    evolution_controller=controller,
    total_records=10_000,
    batch_size=500,
    seed=42,
)
runner.run()  # identical output every time
```

Omit `seed` (the default) to keep the normal non-deterministic behaviour.

> **Note:** `seed` controls Python's `random` module. If your column generators use `uuid.uuid4()` or `numpy.random`, those are unaffected and will still produce different values each run.

---

## Configuring logging

Kroft uses Python's standard `logging` module under the `kroft` namespace. By default nothing is printed — wire up a handler to see what's happening:

```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
```

To see only Kroft's output without affecting the rest of your application:

```python
import logging

logger = logging.getLogger("kroft")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(handler)
logger.propagate = False
```

Relevant log levels:
- `DEBUG` — per-operation detail (which mutation operation was picked, how many records affected)
- `INFO` — schema evolution events from `SimulationRunner`

---

## Inspecting schema history

`SchemaManager` keeps a full version history of which columns were active at each schema version:

```python
print(manager.schema_version)     # e.g. 3
print(manager.schema_history)
# [{"id", "item", "price", "updated_at"},       # v1 — initial
#  {"id", "item", "price", "updated_at", "discount"},  # v2 — added discount
#  {"id", "item", "updated_at", "discount"}]    # v3 — dropped price
```

This is useful for replaying or auditing what the schema looked like at any point in the simulation.
