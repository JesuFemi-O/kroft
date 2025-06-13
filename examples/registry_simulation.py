import random
import uuid
import psycopg2

from kroft.core.column import ColumnDefinition
from kroft.core.schema import SchemaManager
from kroft.core.mutator import MutationEngine
# naming can be better
from kroft.core.batch import BatchGenerator
from kroft.core.evolution import EvolutionController
# maybe move registry utils to a registry module
from kroft.core.registry import register_column, get_registered_columns


# 1. Register reusable columns (actual decorator signature)
@register_column(name="id", sql_type="UUID", protected=True)
def id_gen():
    return str(uuid.uuid4())

@register_column(name="updated_at", sql_type="TIMESTAMP", protected=True)
def updated_at_gen():
    return "now()"

@register_column(name="product", sql_type="TEXT")
def product_gen():
    return random.choice(["bag", "jacket", "cap"])

@register_column(name="quantity", sql_type="INT")
def quantity_gen():
    return random.randint(1, 10)

@register_column(name="unit_price", sql_type="FLOAT")
def unit_price_gen():
    return round(random.uniform(20, 200), 2)

@register_column(name="category", sql_type="TEXT", reserved=True)
def category_gen():
    return random.choice(["clothing", "accessory", "footwear"])

@register_column(name="discount_rate", sql_type="FLOAT", reserved=True)
def discount_rate_gen():
    return 0.0


# 2. Compose schema from registered columns
columns = get_registered_columns()

# 3. Connect to DB
conn = psycopg2.connect("dbname=kroft_test user=postgres password=postgres host=localhost")

# 4. Schema + table creation
manager = SchemaManager(conn, "public", "orders", columns)
manager.drop_table()
manager.create_table()

# 5. Generator + mutator setup
generator = BatchGenerator(manager.get_active_columns())
engine = MutationEngine(
    conn=conn,
    schema="public",
    table_name="orders",
    primary_key="id",
    update_column="updated_at",
    generator=generator
)

evolution = EvolutionController(
    manager=manager,
    evolution_interval=3,
    evolution_probability=1.0,
    add_probability=1.0,
    max_additions=2,
    max_drops=1
)

# 6. Simulation loop
for i in range(1, 11):
    print(f"\n📦 Batch {i}:")
    batch = generator.generate_batch(batch_size=5)
    inserted = engine.insert_batch(batch)
    print("Inserted:", inserted)

    engine.maybe_mutate_batch(inserted)
    print("Mutation stats:", engine.get_counters())

    evolution_result = evolution.evolve(batch_number=i)
    print("Evolution:", evolution_result)