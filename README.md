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

> Last updated: 2026-05-07

This section tracks the current production readiness of the library. It is updated as work progresses.

### Summary

| Dimension | Status | Notes |
|-----------|--------|-------|
| Public API | **Ready** | Clean `__all__`, consistent imports, no internal leakage |
| Error Handling | **Ready** | Typed exceptions, meaningful messages, no silent failures |
| Test Coverage | **Ready** | 34 passing unit tests, good isolation, edge cases covered |
| CI/CD | **Ready** | Lint (Ruff) + pytest on push/PR; missing coverage reporting |
| Security | **Ready** | Parameterised SQL throughout; no hardcoded secrets in library code |
| Mutation API | **Ready** | `update_batch`, `delete_batch`, and configurable `maybe_mutate_batch` — all four scenarios composable |
| Logging | **Ready** | `logging` used throughout; no stray `print()` calls |
| Documentation | **Ready** | Docstrings on all public classes; crash course and advanced usage guides in `docs/` |
| Packaging | **Ready** | `pyproject.toml` has description, license, classifiers, authors, and runtime deps declared |
| Dependency Management | **Ready** | Fully migrated to `uv`; runtime and dev deps split; `uv.lock` committed |

### Detail

**Packaging** ✅
`pyproject.toml` now has proper description, license (MIT), classifiers, authors, keywords, and `psycopg2-binary>=2.9,<3` declared as a runtime dependency. PyPI-publishable.

**Dependency Management** ✅
Fully migrated to `uv`. `requirements.txt` removed. Runtime and dev deps are split — `psycopg2-binary` under `[dependencies]`, `pytest`/`ruff` under `[project.optional-dependencies] dev`. `uv.lock` committed as the canonical lockfile.

**Mutation API** ✅
`MutationEngine` now exposes `update_batch()` and `delete_batch()` with configurable `fraction` and `probability` params. `maybe_mutate_batch()` is fully configurable via `allow_updates`, `allow_deletes`, `update_fraction`, `delete_fraction`, and `probability`. All four scenarios are composable without calling private methods.

**Logging** ✅
All `print()` calls replaced with `logging.getLogger(__name__)` throughout.

**Documentation** ✅
All six public classes have docstrings. `docs/crash-course.md` walks through all four scenarios. `docs/advanced-usage.md` covers the registry, custom loops, `SimulationRunner`, logging setup, and schema history.

**CI/CD**
Migrated to `astral-sh/setup-uv` with `uv sync --extra dev` and `uv run` for lint and tests. Triggers extended to `fix/**` and `chore/**` branches. Missing: coverage reporting, multi-version Python matrix, publish step.

### Roadmap

- [x] Fix `MutationEngine` public API — expose per-operation methods with configurable probabilities
- [x] Replace `print()` with `logging` in `mutator.py`
- [x] Fix `pyproject.toml` — description, license, classifiers, runtime dependencies
- [x] Migrate to `uv`; split runtime and dev deps
- [x] Add docstrings to all public classes
- [x] Add `docs/` with crash course and advanced usage guides
- [ ] Add coverage reporting to CI
- [ ] Multi-version Python matrix in CI (3.10, 3.11, 3.12)
- [ ] Publish to PyPI
