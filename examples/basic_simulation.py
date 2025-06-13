import random
import uuid
import psycopg2

from kroft import ColumnDefinition, SchemaManager, BatchGenerator, MutationEngine
from kroft.core.evolution import EvolutionController


# 1. Define full column pool
columns = {
    "id": ColumnDefinition("id", "UUID", lambda: str(uuid.uuid4()), reserved=False),
    "updated_at": ColumnDefinition("updated_at", "TIMESTAMP", lambda: "now()", protected=True),
    "item": ColumnDefinition("item", "TEXT", lambda: random.choice(["shoes", "shirt", "hat"])),
    "quantity": ColumnDefinition("quantity", "INT", lambda: random.randint(1, 5)),
    "price": ColumnDefinition("price", "FLOAT", lambda: round(random.uniform(10, 100), 2)),

    # Reserved columns (initially excluded from schema)
    "discount": ColumnDefinition("discount", "FLOAT", lambda: 0.0, reserved=True),
    "coupon": ColumnDefinition("coupon", "TEXT", lambda: "NONE", reserved=True),
    "region": ColumnDefinition("region", "TEXT", lambda: random.choice(["NA", "EU", "ASIA"]), reserved=True),
    "delivery_method": ColumnDefinition("delivery_method", "TEXT", lambda: random.choice(["bike", "drone", "pickup"]), reserved=True),
    "refunded": ColumnDefinition("refunded", "BOOLEAN", lambda: False, reserved=True),
}


# 2. Connect to local PostgreSQL
conn = psycopg2.connect("dbname=kroft_test user=postgres password=postgres host=localhost")


# 3. Create fresh schema
manager = SchemaManager(conn, "public", "sales", columns)
manager.drop_table()
manager.create_table()


# 4. Set up generator and mutation engine
generator = BatchGenerator(schema=manager.get_active_columns())
engine = MutationEngine(
    conn=conn,
    schema="public",
    table_name="sales",
    primary_key="id",
    update_column="updated_at",
    generator=generator
)


# 5. Initialize evolution controller
controller = EvolutionController(
    manager=manager,
    evolution_interval=5,
    evolution_probability=1.0,
    add_probability=0.7,
    max_additions=4,
    max_drops=2
)


# 6. Simulate 20 batches with evolution
for batch in range(1, 21):
    print(f"\n📦 Batch {batch}:")

    # Generate and insert batch
    generator.schema = manager.get_active_columns()
    rows = generator.generate_batch(batch_size=5)
    inserted = engine.insert_batch(rows)
    print(f"Inserted: {inserted}")

    # Maybe mutate
    engine.maybe_mutate_batch(inserted)
    print("Mutation stats:", engine.get_counters())

    # Maybe evolve
    result = controller.evolve(batch)
    print("Evolution:", result or "No change")


# 7. Final summary
print("\n📊 Evolution Summary:")
print(controller.summary())

# import random
# import uuid
# import psycopg2

# from kroft import ColumnDefinition, SchemaManager, BatchGenerator, MutationEngine


# # 1. Define schema
# columns = {
#     "id": ColumnDefinition("id", "UUID", lambda: str(uuid.uuid4()), reserved=False),
#     "updated_at": ColumnDefinition("updated_at", "TIMESTAMP", lambda: "now()", protected=True),
#     "item": ColumnDefinition("item", "TEXT", lambda: random.choice(["shoes", "shirt", "hat"])),
#     "quantity": ColumnDefinition("quantity", "INT", lambda: random.randint(1, 5)),
#     "price": ColumnDefinition("price", "FLOAT", lambda: round(random.uniform(10, 100), 2)),

#     # Reserved columns for schema evolution
#     "discount": ColumnDefinition("discount", "FLOAT", lambda: 0.0, reserved=True),
#     "coupon": ColumnDefinition("coupon", "TEXT", lambda: "NONE", reserved=True),
# }


# # 2. Connect to DB
# conn = psycopg2.connect("dbname=kroft_test user=postgres password=postgres host=localhost")


# # 3. Create table from initial schema (excludes reserved columns)
# manager = SchemaManager(conn, "public", "sales", columns)
# manager.drop_table()  # Ensure clean state
# manager.create_table()


# # 4. Generate synthetic data using active columns
# generator = BatchGenerator(schema=manager.get_active_columns())
# engine = MutationEngine(
#     conn=conn,
#     schema="public",
#     table_name="sales",
#     primary_key="id",
#     update_column="updated_at",
#     generator=generator
# )

# # 5. Insert a batch
# batch = generator.generate_batch(batch_size=5)
# inserted_ids = engine.insert_batch(batch)
# print("Inserted IDs:", inserted_ids)

# # 6. Mutate (update/delete) some of them
# engine.maybe_mutate_batch(inserted_ids)
# print(engine.get_counters())

# # 7. Evolve schema (add a column from reserved)
# added_col = manager.add_column()
# print(f"Added column: {added_col}")