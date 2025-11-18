# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Notebook for Silver-Only Integration Tests
# MAGIC This notebook validates that the silver-only ingestion completed successfully

# COMMAND ----------

import pandas as pd

run_id = dbutils.widgets.get("run_id")
uc_enabled = eval(dbutils.widgets.get("uc_enabled"))
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
output_file_path = dbutils.widgets.get("output_file_path")
log_list = []

# COMMAND ----------

log_list.append("Starting Silver-Only Integration Test Validation...")
log_list.append(f"Run ID: {run_id}")
log_list.append(f"UC Enabled: {uc_enabled}")
log_list.append(f"Silver Schema: {silver_schema}")

# COMMAND ----------

# Assumption is that Silver pipeline completed successfully
log_list.append("✓ Completed Silver-Only Lakeflow Declarative Pipeline.")

# COMMAND ----------

# Define expected table counts for silver-only ingestion
UC_TABLES = {
    f"{uc_catalog_name}.{silver_schema}.products_silver_direct": 8,  # 10 total, 2 filtered out (price<=0 or is_active=false)
    f"{uc_catalog_name}.{silver_schema}.orders_silver_direct": 15,  # After CDC Type 1 deduplication
    f"{uc_catalog_name}.{silver_schema}.inventory_silver_direct": 12,  # After CDC Type 2 (includes history rows)
}

NON_UC_TABLES = {
    f"{silver_schema}.products_silver_direct": 8,
    f"{silver_schema}.orders_silver_direct": 15,
    f"{silver_schema}.inventory_silver_direct": 12,
}

# COMMAND ----------

log_list.append("Validating Silver-Only Table Counts...")
tables = UC_TABLES if uc_enabled else NON_UC_TABLES

for table, expected_count in tables.items():
    try:
        query = spark.sql(f"SELECT count(*) as cnt FROM {table}")
        actual_count = query.collect()[0].cnt
        
        log_list.append(f"Validating table: {table}")
        log_list.append(f"  Expected: {expected_count}, Actual: {actual_count}")
        
        assert int(actual_count) == expected_count, f"Count mismatch for {table}"
        log_list.append(f"  ✓ PASSED")
    except AssertionError as e:
        log_list.append(f"  ✗ FAILED: {str(e)}")
    except Exception as e:
        log_list.append(f"  ✗ ERROR: {str(e)}")

# COMMAND ----------

# Validate products table has transformations applied
log_list.append("\nValidating transformations on products_silver_direct...")
try:
    products_table = f"{uc_catalog_name}.{silver_schema}.products_silver_direct" if uc_enabled else f"{silver_schema}.products_silver_direct"
    
    # Check that product names are uppercase (transformation was applied)
    query = spark.sql(f"""
        SELECT product_name 
        FROM {products_table} 
        WHERE product_name != UPPER(product_name)
    """)
    non_uppercase_count = query.count()
    
    assert non_uppercase_count == 0, f"Found {non_uppercase_count} product names not uppercase"
    log_list.append("  ✓ Product names are uppercase (transformation applied)")
    
    # Check that price filter was applied (no prices <= 0)
    query = spark.sql(f"SELECT count(*) as cnt FROM {products_table} WHERE price <= 0")
    invalid_price_count = query.collect()[0].cnt
    
    assert invalid_price_count == 0, f"Found {invalid_price_count} products with invalid price"
    log_list.append("  ✓ Price filter applied correctly")
    
    # Check that is_active filter was applied
    query = spark.sql(f"SELECT count(*) as cnt FROM {products_table} WHERE is_active = false")
    inactive_count = query.collect()[0].cnt
    
    assert inactive_count == 0, f"Found {inactive_count} inactive products"
    log_list.append("  ✓ Active filter applied correctly")
    
    # Check metadata columns were included
    query = spark.sql(f"""
        SELECT count(*) as cnt 
        FROM {products_table} 
        WHERE input_file_name IS NOT NULL
    """)
    metadata_count = query.collect()[0].cnt
    
    assert metadata_count > 0, "Metadata columns not found"
    log_list.append(f"  ✓ Metadata columns present ({metadata_count} rows)")
    
except Exception as e:
    log_list.append(f"  ✗ Transformation validation failed: {str(e)}")

# COMMAND ----------

# Validate CDC Type 1 on orders table
log_list.append("\nValidating CDC Type 1 on orders_silver_direct...")
try:
    orders_table = f"{uc_catalog_name}.{silver_schema}.orders_silver_direct" if uc_enabled else f"{silver_schema}.orders_silver_direct"
    
    # Check that operation column is not present (should be in except_column_list)
    columns = spark.sql(f"DESCRIBE {orders_table}").select("col_name").collect()
    column_names = [row.col_name for row in columns]
    
    assert "operation" not in column_names, "Operation column should be excluded"
    log_list.append("  ✓ CDC except_column_list applied correctly")
    
    # Verify no duplicate order_ids (CDC Type 1 keeps latest)
    query = spark.sql(f"""
        SELECT order_id, count(*) as cnt 
        FROM {orders_table} 
        GROUP BY order_id 
        HAVING count(*) > 1
    """)
    duplicate_count = query.count()
    
    assert duplicate_count == 0, f"Found {duplicate_count} duplicate order_ids"
    log_list.append("  ✓ CDC Type 1 deduplication working correctly")
    
except Exception as e:
    log_list.append(f"  ✗ CDC Type 1 validation failed: {str(e)}")

# COMMAND ----------

# Validate CDC Type 2 on inventory table
log_list.append("\nValidating CDC Type 2 on inventory_silver_direct...")
try:
    inventory_table = f"{uc_catalog_name}.{silver_schema}.inventory_silver_direct" if uc_enabled else f"{silver_schema}.inventory_silver_direct"
    
    # Check that SCD Type 2 columns exist
    columns = spark.sql(f"DESCRIBE {inventory_table}").select("col_name").collect()
    column_names = [row.col_name for row in columns]
    
    assert "__START_AT" in column_names, "SCD Type 2 __START_AT column missing"
    assert "__END_AT" in column_names, "SCD Type 2 __END_AT column missing"
    log_list.append("  ✓ SCD Type 2 columns present")
    
    # Check that we have history rows (some inventory_ids should have multiple versions)
    query = spark.sql(f"""
        SELECT inventory_id, count(*) as version_count 
        FROM {inventory_table} 
        GROUP BY inventory_id 
        HAVING count(*) > 1
    """)
    multi_version_count = query.count()
    
    assert multi_version_count > 0, "No history rows found for SCD Type 2"
    log_list.append(f"  ✓ SCD Type 2 history tracking working ({multi_version_count} items with history)")
    
except Exception as e:
    log_list.append(f"  ✗ CDC Type 2 validation failed: {str(e)}")

# COMMAND ----------

# Validate data quality expectations were applied
log_list.append("\nValidating data quality expectations...")
try:
    products_table = f"{uc_catalog_name}.{silver_schema}.products_silver_direct" if uc_enabled else f"{silver_schema}.products_silver_direct"
    
    # All products should have valid product_id (expect_or_fail)
    query = spark.sql(f"SELECT count(*) as cnt FROM {products_table} WHERE product_id IS NULL")
    null_id_count = query.collect()[0].cnt
    
    assert null_id_count == 0, f"Found {null_id_count} products with NULL product_id"
    log_list.append("  ✓ Data quality expectations enforced")
    
except Exception as e:
    log_list.append(f"  ✗ Data quality validation failed: {str(e)}")

# COMMAND ----------

log_list.append("\n" + "="*50)
log_list.append("Silver-Only Integration Test Validation Complete!")
log_list.append("="*50)

# Save results to CSV
pd_df = pd.DataFrame(log_list)
pd_df.to_csv(output_file_path)

print("\n".join(log_list))

