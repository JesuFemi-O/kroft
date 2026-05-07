# Kroft

A Python library for generating synthetic data and simulating schema evolution — useful for testing ETL pipelines, CDC workflows, and application behaviour under realistic data change scenarios.

---

## Core Concepts

| Component | Role |
|-----------|------|
| `ColumnDefinition` | Defines a column: name, SQL type, generator function, and flags (reserved, protected) |
| `SchemaManager` | Manages table schema state — CREATE/ALTER/DROP DDL, active vs reserved columns, version history |
| `BatchGenerator` | Generates synthetic rows using column generators |
| `MutationEngine` | Performs insert, update, and delete operations against a live database |
| `EvolutionController` | Controls schema evolution — when and how to add/drop columns |
| `SimulationRunner` | Orchestrates a full simulation: generate batches, mutate data, evolve schema |

---

## Supported Scenarios

Kroft is designed to support four composable data simulation scenarios:

1. **Insert only** — continuously append new rows
2. **Insert + Update** — append rows and update existing ones
3. **Insert + Update + Delete** — full CRUD simulation
4. **Schema evolution** — add or drop columns at runtime while data flows

Each scenario is independently composable. You are not forced into mutations or schema evolution unless you opt in.

---

## Quick Example

```python
import uuid
import random
import psycopg2

from kroft import ColumnDefinition, SchemaManager, BatchGenerator, MutationEngine, EvolutionController

columns = {
    "id": ColumnDefinition("id", "UUID", lambda: str(uuid.uuid4())),
    "updated_at": ColumnDefinition("updated_at", "TIMESTAMP", lambda: "now()", protected=True),
    "item": ColumnDefinition("item", "TEXT", lambda: random.choice(["shoes", "shirt", "hat"])),
    "price": ColumnDefinition("price", "FLOAT", lambda: round(random.uniform(10, 100), 2)),
    "discount": ColumnDefinition("discount", "FLOAT", lambda: 0.0, reserved=True),
}

conn = psycopg2.connect("dbname=kroft_test user=postgres host=localhost")

manager = SchemaManager(conn, "public", "sales", columns)
manager.drop_table()
manager.create_table()

generator = BatchGenerator(schema=manager.get_active_columns())
engine = MutationEngine(conn=conn, schema="public", table_name="sales",
                        primary_key="id", update_column="updated_at", generator=generator)
controller = EvolutionController(manager=manager, evolution_interval=5, max_additions=2)

for batch_num in range(1, 21):
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=100)
    inserted_ids = engine.insert_batch(rows)
    engine.maybe_mutate_batch(inserted_ids)
    result = controller.evolve(batch_num)
```

---

## Production Readiness Assessment

> Last assessed: 2026-05-07

This section tracks the current production readiness of the library. It is updated as work progresses.

### Summary

| Dimension | Status | Notes |
|-----------|--------|-------|
| Public API | **Ready** | Clean `__all__`, consistent imports, no internal leakage |
| Error Handling | **Ready** | Typed exceptions, meaningful messages, no silent failures |
| Test Coverage | **Ready** | 34 passing unit tests, good isolation, edge cases covered |
| CI/CD | **Ready** | Lint (Ruff) + pytest on push/PR; missing coverage reporting |
| Security | **Ready** | Parameterised SQL throughout; no hardcoded secrets in library code |
| Mutation API | **Needs Work** | `maybe_mutate_batch()` has hardcoded probabilities; no per-scenario control |
| Logging | **Needs Work** | Stray `print()` in `mutator.py`; should use `logging` |
| Documentation | **Needs Work** | README minimal; most public classes lack docstrings |
| Packaging | **Blocker** | `pyproject.toml` has placeholder description, empty `dependencies`, no license or classifiers |
| Dependency Management | **Needs Work** | `psycopg2-binary` missing from `pyproject.toml`; no dev/runtime split |

### Detail

**Packaging (Blocker)**
`pyproject.toml` has `description = "Add your description here"` and `dependencies = []`. `psycopg2-binary` is a runtime requirement but is only declared in `requirements.txt`. No license, classifiers, or author metadata. Not publishable to PyPI in current state.

**Dependency Management**
`requirements.txt` mixes runtime and dev dependencies with no separation. `pyproject.toml` should declare `psycopg2-binary>=2.9,<3` under `dependencies` and move `pytest`/`ruff` to a `[dev]` extra.

**Mutation API**
`maybe_mutate_batch()` hardcodes a 50% chance of skipping, a 50/50 update/delete split, and always mutates 25% of the batch. Users cannot opt into insert-only, insert+update, or insert+update+delete scenarios without calling private methods (`_update_records`, `_delete_records`). Needs configurable public methods per operation.

**Logging**
`mutator.py` lines 58 and 61 use `print()`. These leak to stdout and cannot be suppressed by callers. Should use `logging.getLogger(__name__)`.

**Documentation**
Most public classes (`ColumnDefinition`, `SchemaManager`, `MutationEngine`, `EvolutionController`, `SimulationRunner`) have no docstrings. README previously had no usage examples or feature overview.

**CI/CD**
Workflow runs lint and tests on push to `main` and `feat/**` branches and on PRs. Missing: coverage reporting, multi-version Python matrix, and a publish step.

### Roadmap (rough priority order)

- [ ] Fix `MutationEngine` public API — expose per-operation methods with configurable probabilities
- [ ] Replace `print()` with `logging` in `mutator.py`
- [ ] Fix `pyproject.toml` — description, license, classifiers, runtime dependencies
- [ ] Split `requirements.txt` into runtime and dev
- [ ] Add docstrings to all public classes
- [ ] Expand README with full usage examples per scenario
- [ ] Add coverage reporting to CI
- [ ] Consider multi-version Python matrix in CI
