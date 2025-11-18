# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Test Data for Silver-Only Integration Tests
# MAGIC This notebook generates parquet and JSON test data files for silver-only ingestion testing

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime, timedelta
import json

# Get parameters
uc_volume_path = dbutils.widgets.get("uc_volume_path")
print(f"Generating test data at: {uc_volume_path}/integration_tests/resources/data/silver_direct/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Data (Parquet)

# COMMAND ----------

# Create products test data
products_data = [
    ("P001", "laptop computer", "Electronics", 1200.00, 50, datetime(2024, 1, 1, 10, 0, 0), True),
    ("P002", "desk chair", "Home", 299.99, 100, datetime(2024, 1, 2, 11, 0, 0), True),
    ("P003", "running shoes", "Sports", 89.99, 75, datetime(2024, 1, 3, 12, 0, 0), True),
    ("P004", "python book", "Books", 45.50, 30, datetime(2024, 1, 4, 13, 0, 0), True),
    ("P005", "wireless mouse", "Electronics", 25.99, 200, datetime(2024, 1, 5, 14, 0, 0), True),
    ("P006", "coffee maker", "Home", 79.99, 60, datetime(2024, 1, 6, 15, 0, 0), True),
    ("P007", "yoga mat", "Sports", 29.99, 150, datetime(2024, 1, 7, 16, 0, 0), True),
    ("P008", "novel book", "Books", 19.99, 80, datetime(2024, 1, 8, 17, 0, 0), True),
    # These two should be filtered out
    ("P009", "defective item", "Electronics", -10.00, 5, datetime(2024, 1, 9, 18, 0, 0), True),  # price <= 0
    ("P010", "discontinued item", "Home", 50.00, 0, datetime(2024, 1, 10, 19, 0, 0), False),  # is_active = false
]

products_df = spark.createDataFrame(
    products_data,
    ["product_id", "product_name", "category", "price", "stock_quantity", "last_updated", "is_active"]
)

# Write products as parquet
products_path = f"{uc_volume_path}/integration_tests/resources/data/silver_direct/products"
products_df.write.mode("overwrite").parquet(products_path)
print(f"✓ Generated {products_df.count()} products records at {products_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Orders Data with CDC (Parquet)

# COMMAND ----------

# Create orders test data with CDC operations
orders_data = [
    # Initial inserts
    ("O001", "C001", "P001", 1, 1200.00, datetime(2024, 1, 1, 10, 0, 0), "COMPLETED", "INSERT"),
    ("O002", "C002", "P002", 2, 599.98, datetime(2024, 1, 2, 11, 0, 0), "COMPLETED", "INSERT"),
    ("O003", "C003", "P003", 1, 89.99, datetime(2024, 1, 3, 12, 0, 0), "PENDING", "INSERT"),
    ("O004", "C001", "P004", 3, 136.50, datetime(2024, 1, 4, 13, 0, 0), "COMPLETED", "INSERT"),
    ("O005", "C004", "P005", 2, 51.98, datetime(2024, 1, 5, 14, 0, 0), "COMPLETED", "INSERT"),
    ("O006", "C002", "P006", 1, 79.99, datetime(2024, 1, 6, 15, 0, 0), "PROCESSING", "INSERT"),
    ("O007", "C005", "P007", 2, 59.98, datetime(2024, 1, 7, 16, 0, 0), "COMPLETED", "INSERT"),
    ("O008", "C003", "P008", 1, 19.99, datetime(2024, 1, 8, 17, 0, 0), "COMPLETED", "INSERT"),
    ("O009", "C006", "P001", 1, 1200.00, datetime(2024, 1, 9, 18, 0, 0), "PENDING", "INSERT"),
    ("O010", "C004", "P002", 1, 299.99, datetime(2024, 1, 10, 19, 0, 0), "COMPLETED", "INSERT"),
    # Updates (CDC Type 1 should keep latest)
    ("O003", "C003", "P003", 1, 89.99, datetime(2024, 1, 11, 10, 0, 0), "COMPLETED", "UPDATE"),
    ("O006", "C002", "P006", 1, 79.99, datetime(2024, 1, 12, 11, 0, 0), "COMPLETED", "UPDATE"),
    ("O009", "C006", "P001", 1, 1200.00, datetime(2024, 1, 13, 12, 0, 0), "PROCESSING", "UPDATE"),
    # Additional inserts
    ("O011", "C007", "P003", 2, 179.98, datetime(2024, 1, 14, 13, 0, 0), "COMPLETED", "INSERT"),
    ("O012", "C005", "P005", 5, 129.95, datetime(2024, 1, 15, 14, 0, 0), "COMPLETED", "INSERT"),
    # Deletes (should be removed by CDC)
    ("O001", "C001", "P001", 1, 1200.00, datetime(2024, 1, 16, 15, 0, 0), "CANCELLED", "DELETE"),
    ("O002", "C002", "P002", 2, 599.98, datetime(2024, 1, 17, 16, 0, 0), "CANCELLED", "DELETE"),
]

orders_df = spark.createDataFrame(
    orders_data,
    ["order_id", "customer_id", "product_id", "quantity", "total_amount", "order_date", "status", "operation"]
)

# Write orders as parquet
orders_path = f"{uc_volume_path}/integration_tests/resources/data/silver_direct/orders"
orders_df.write.mode("overwrite").parquet(orders_path)
print(f"✓ Generated {orders_df.count()} orders records (raw) at {orders_path}")
print(f"  After CDC Type 1: 15 unique orders expected (10 inserts + 3 updates + 2 new inserts - 2 deletes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Inventory Data with SCD Type 2 (JSON)

# COMMAND ----------

# Create inventory test data with history
inventory_data = [
    # Initial states
    {"inventory_id": "I001", "product_id": "P001", "warehouse_id": "W001", "quantity": 50, "last_counted": "2024-01-01T10:00:00", "operation": "INSERT"},
    {"inventory_id": "I002", "product_id": "P002", "warehouse_id": "W001", "quantity": 100, "last_counted": "2024-01-01T10:00:00", "operation": "INSERT"},
    {"inventory_id": "I003", "product_id": "P003", "warehouse_id": "W002", "quantity": 75, "last_counted": "2024-01-01T10:00:00", "operation": "INSERT"},
    {"inventory_id": "I004", "product_id": "P004", "warehouse_id": "W002", "quantity": 30, "last_counted": "2024-01-01T10:00:00", "operation": "INSERT"},
    {"inventory_id": "I005", "product_id": "P005", "warehouse_id": "W001", "quantity": 200, "last_counted": "2024-01-01T10:00:00", "operation": "INSERT"},
    # Updates (should create new versions with SCD Type 2)
    {"inventory_id": "I001", "product_id": "P001", "warehouse_id": "W001", "quantity": 45, "last_counted": "2024-01-05T14:00:00", "operation": "UPDATE"},
    {"inventory_id": "I002", "product_id": "P002", "warehouse_id": "W001", "quantity": 98, "last_counted": "2024-01-06T15:00:00", "operation": "UPDATE"},
    {"inventory_id": "I003", "product_id": "P003", "warehouse_id": "W002", "quantity": 74, "last_counted": "2024-01-07T16:00:00", "operation": "UPDATE"},
    # More updates to show multiple versions
    {"inventory_id": "I001", "product_id": "P001", "warehouse_id": "W001", "quantity": 40, "last_counted": "2024-01-10T18:00:00", "operation": "UPDATE"},
    # New inserts
    {"inventory_id": "I006", "product_id": "P006", "warehouse_id": "W003", "quantity": 60, "last_counted": "2024-01-08T17:00:00", "operation": "INSERT"},
]

inventory_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(d) for d in inventory_data]))

# Write inventory as JSON
inventory_path = f"{uc_volume_path}/integration_tests/resources/data/silver_direct/inventory"
inventory_df.write.mode("overwrite").json(inventory_path)
print(f"✓ Generated {inventory_df.count()} inventory records (raw) at {inventory_path}")
print(f"  After CDC Type 2: 12 records expected (6 current + 3 history for I001, 1 history for I002, 1 history for I003)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("\n" + "="*60)
print("Test Data Generation Complete!")
print("="*60)
print(f"\nGenerated files:")
print(f"  1. Products (Parquet): {products_path}")
print(f"     - 10 total records, 8 after filtering")
print(f"  2. Orders (Parquet): {orders_path}")
print(f"     - CDC Type 1 applied, 15 final records")
print(f"  3. Inventory (JSON): {inventory_path}")
print(f"     - CDC Type 2 applied, 12 records with history")
print("\nReady for silver-only integration testing!")

