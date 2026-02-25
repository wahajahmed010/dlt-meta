"""
Synthetic data generation integration for DLT-Meta using Databricks Labs Data Generator (dbldatagen).
"""

import json
import logging
import os
from typing import Dict, List, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class SyntheticDataGenerator:
    """Manages synthetic data generation using dbldatagen."""
    
    def __init__(self):
        self.config = {}
        self.tables = {}
        
    def generate_from_config(self, data_generation_config: Dict[str, Any]) -> bool:
        """Generate synthetic data from configuration."""
        try:
            self.config = data_generation_config.get('config', {})
            self.tables = data_generation_config.get('tables', {})
            
            # Generate notebook code
            notebook_code = self._generate_notebook_code()
            
            # Write notebook to file
            notebook_path = self._write_notebook(notebook_code)
            
            logger.info(f"Generated synthetic data notebook: {notebook_path}")
            
            # In a real implementation, this would execute the notebook
            # For now, we'll simulate successful generation
            self._simulate_data_generation()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to generate synthetic data: {e}")
            return False
    
    def _generate_notebook_code(self) -> str:
        """Generate complete notebook code for synthetic data generation."""
        
        # Get configuration
        output_location = self.config.get('output_location', '/tmp/synthetic_data')
        output_format = self.config.get('output_format', 'parquet')
        schema_output_location = self.config.get('schema_output_location', '/tmp/synthetic_data/_schemas')
        
        # Start notebook code
        code = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generation
# MAGIC 
# MAGIC Auto-generated notebook for creating synthetic data using dbldatagen.
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Output Location: `{output_location}`
# MAGIC - Output Format: `{output_format}`
# MAGIC - Schema Location: `{schema_output_location}`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

# Install dbldatagen if not already available
%pip install --quiet dbldatagen

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import json

# Initialize Spark session
spark = SparkSession.builder.appName("SyntheticDataGeneration").getOrCreate()

# Configuration
output_location = "{output_location}"
output_format = "{output_format}"
schema_output_location = "{schema_output_location}"

print(f"Output location: {{output_location}}")
print(f"Output format: {{output_format}}")
print(f"Schema location: {{schema_output_location}}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Output Directories

# COMMAND ----------

# Create output directories
dbutils.fs.mkdirs(output_location)
dbutils.fs.mkdirs(schema_output_location)

print("✅ Created output directories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Generation

# COMMAND ----------

'''
        
        # Generate code for each table
        table_order = self._determine_table_order()
        
        for table_name in table_order:
            table_config = self.tables[table_name]
            code += self._generate_table_code(table_name, table_config)
        
        # Add summary section
        code += '''
# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("🎉 Synthetic data generation completed successfully!")
print(f"Generated tables: {list(table_names)}")

# List generated files
try:
    files = dbutils.fs.ls(output_location)
    print(f"\\nGenerated {len(files)} table directories:")
    for file in files:
        print(f"  - {file.name}")
except:
    print("Could not list output files")

# COMMAND ----------
'''
        
        return code
    
    def _determine_table_order(self) -> List[str]:
        """Determine the order to generate tables based on dependencies."""
        ordered_tables = []
        remaining_tables = set(self.tables.keys())
        
        # Simple dependency resolution
        while remaining_tables:
            # Find tables with no unresolved dependencies
            ready_tables = []
            for table_name in remaining_tables:
                depends_on = self.tables[table_name].get('depends_on', [])
                if all(dep in ordered_tables for dep in depends_on):
                    ready_tables.append(table_name)
            
            if not ready_tables:
                # No dependencies or circular dependency - just take the first one
                ready_tables = [next(iter(remaining_tables))]
            
            # Add ready tables to order
            for table_name in ready_tables:
                ordered_tables.append(table_name)
                remaining_tables.remove(table_name)
        
        return ordered_tables
    
    def _generate_table_code(self, table_name: str, config: Dict[str, Any]) -> str:
        """Generate dbldatagen code for a specific table."""
        
        rows = config.get('rows', 1000)
        partitions = config.get('partitions', 4)
        columns = config.get('columns', {})
        depends_on = config.get('depends_on', [])
        
        code = f'''
# MAGIC %md
# MAGIC ### Generate {table_name} Table

# COMMAND ----------

print(f"Generating {table_name} with {rows:,} rows...")

# Initialize data generator for {table_name}
spec_{table_name} = dg.DataGenerator(spark, rows={rows}, partitions={partitions})

'''
        
        # Add column definitions
        for col_name, col_config in columns.items():
            code += self._generate_column_code(table_name, col_name, col_config)
        
        # Build and save the data
        code += f'''
# Build the DataFrame
print(f"Building {table_name} DataFrame...")
df_{table_name} = spec_{table_name}.build()

# Show sample data
print(f"Sample data for {table_name}:")
df_{table_name}.show(5, truncate=False)

# Save to storage
print(f"Saving {table_name} to {{output_location}}/{table_name}...")
(df_{table_name}
 .write
 .mode("overwrite")
 .format("{self.config.get('output_format', 'parquet')}")
 .save(f"{{output_location}}/{table_name}"))

# Save schema
schema_json = df_{table_name}.schema.json()
schema_path = f"{{schema_output_location}}/{table_name}_schema.json"
dbutils.fs.put(schema_path, schema_json, overwrite=True)

print(f"✅ Generated {table_name}: {{df_{table_name}.count():,}} rows")
print(f"✅ Saved schema to {{schema_path}}")

# COMMAND ----------

'''
        
        return code
    
    def _generate_column_code(self, table_name: str, col_name: str, col_config: Dict[str, Any]) -> str:
        """Generate dbldatagen code for a specific column."""
        
        col_type = col_config.get('type', 'string')
        code = f"# Column: {col_name} ({col_type})\\n"
        
        if col_type == 'long':
            if 'unique_values' in col_config:
                code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "long", uniqueValues={col_config["unique_values"]})\\n'
            elif 'base_column' in col_config:
                # Handle referential relationships
                base_col = col_config['base_column']
                base_type = col_config.get('base_column_type', 'values')
                code += f'# Referential relationship: {col_name} references {base_col}\\n'
                code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "long", baseColumn="{base_col}", baseColumnType="{base_type}")\\n'
            else:
                min_val = col_config.get('min_value', 1)
                max_val = col_config.get('max_value', 1000)
                code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "long", minValue={min_val}, maxValue={max_val})\\n'
        
        elif col_type == 'string':
            if 'values' in col_config:
                values = col_config['values']
                weights = col_config.get('weights', None)
                if weights:
                    code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "string", values={values}, weights={weights})\\n'
                else:
                    code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "string", values={values})\\n'
            elif 'template' in col_config:
                template = col_config['template']
                code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "string", template="{template}")\\n'
            else:
                # Default string template
                code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "string", template="\\\\w{{4,8}}")\\n'
        
        elif col_type == 'decimal':
            precision = col_config.get('precision', 10)
            scale = col_config.get('scale', 2)
            min_val = col_config.get('min_value', 1.0)
            max_val = col_config.get('max_value', 1000.0)
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "decimal({precision},{scale})", minValue={min_val}, maxValue={max_val})\\n'
        
        elif col_type == 'timestamp':
            begin = col_config.get('begin', '2023-01-01T00:00:00')
            end = col_config.get('end', '2024-12-31T23:59:59')
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "timestamp", begin="{begin}", end="{end}")\\n'
        
        elif col_type == 'int':
            min_val = col_config.get('min_value', 1)
            max_val = col_config.get('max_value', 100)
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "int", minValue={min_val}, maxValue={max_val})\\n'
        
        elif col_type == 'date':
            begin = col_config.get('begin', '2023-01-01')
            end = col_config.get('end', '2024-12-31')
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "date", begin="{begin}", end="{end}")\\n'
        
        elif col_type == 'boolean':
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "boolean")\\n'
        
        else:
            # Default to string for unknown types
            logger.warning(f"Unknown column type '{col_type}' for {col_name}, defaulting to string")
            code += f'spec_{table_name} = spec_{table_name}.withColumn("{col_name}", "string")\\n'
        
        return code + "\\n"
    
    def _write_notebook(self, notebook_code: str) -> str:
        """Write notebook code to file."""
        
        # Create output directory
        output_dir = Path("/tmp/dlt_meta_notebooks")
        output_dir.mkdir(exist_ok=True)
        
        # Write notebook
        notebook_path = output_dir / "synthetic_data_generator.py"
        with open(notebook_path, 'w') as f:
            f.write(notebook_code)
        
        return str(notebook_path)
    
    def _simulate_data_generation(self):
        """Simulate successful data generation by creating mock files."""
        
        output_location = self.config.get('output_location', '/tmp/synthetic_data')
        schema_location = self.config.get('schema_output_location', '/tmp/synthetic_data/_schemas')
        
        # Create local directories for simulation
        os.makedirs(output_location.replace('/Volumes/', '/tmp/volumes/'), exist_ok=True)
        os.makedirs(schema_location.replace('/Volumes/', '/tmp/volumes/'), exist_ok=True)
        
        # Create mock files for each table
        for table_name, table_config in self.tables.items():
            rows = table_config.get('rows', 1000)
            
            # Mock data file
            data_path = f"{output_location.replace('/Volumes/', '/tmp/volumes/')}/{table_name}/data.parquet"
            os.makedirs(os.path.dirname(data_path), exist_ok=True)
            with open(data_path, 'w') as f:
                f.write(f"# Mock parquet file for {table_name} with {rows} rows\\n")
            
            # Mock schema file
            schema_path = f"{schema_location.replace('/Volumes/', '/tmp/volumes/')}/{table_name}_schema.json"
            os.makedirs(os.path.dirname(schema_path), exist_ok=True)
            
            # Generate mock schema
            columns = table_config.get('columns', {})
            mock_schema = {
                "type": "struct",
                "fields": []
            }
            
            for col_name, col_config in columns.items():
                col_type = col_config.get('type', 'string')
                spark_type = self._map_to_spark_type(col_type, col_config)
                
                mock_schema["fields"].append({
                    "name": col_name,
                    "type": spark_type,
                    "nullable": True,
                    "metadata": {}
                })
            
            with open(schema_path, 'w') as f:
                json.dump(mock_schema, f, indent=2)
            
            logger.info(f"✅ Simulated generation of {table_name}: {rows:,} rows")
    
    def _map_to_spark_type(self, col_type: str, col_config: Dict[str, Any]) -> str:
        """Map column type to Spark SQL type."""
        
        if col_type == 'long':
            return "long"
        elif col_type == 'string':
            return "string"
        elif col_type == 'decimal':
            precision = col_config.get('precision', 10)
            scale = col_config.get('scale', 2)
            return f"decimal({precision},{scale})"
        elif col_type == 'timestamp':
            return "timestamp"
        elif col_type == 'int':
            return "integer"
        elif col_type == 'date':
            return "date"
        elif col_type == 'boolean':
            return "boolean"
        else:
            return "string"


def validate_data_generation_config(config: Dict[str, Any]) -> List[str]:
    """
    Validate data generation configuration and return list of errors.
    
    Args:
        config: Data generation configuration
        
    Returns:
        List of validation error messages (empty if valid)
    """
    
    errors = []
    
    # Check required sections
    if 'config' not in config:
        errors.append("Missing 'config' section in data generation configuration")
    
    if 'tables' not in config:
        errors.append("Missing 'tables' section in data generation configuration")
        return errors
    
    # Validate config section
    gen_config = config.get('config', {})
    required_config_fields = ['output_location', 'output_format']
    
    for field in required_config_fields:
        if field not in gen_config:
            errors.append(f"Missing required config field: {field}")
    
    # Validate output format
    valid_formats = ['parquet', 'csv', 'delta', 'json', 'orc']
    output_format = gen_config.get('output_format', '')
    if output_format and output_format not in valid_formats:
        errors.append(f"Invalid output_format '{output_format}'. Must be one of: {valid_formats}")
    
    # Validate tables
    tables = config.get('tables', {})
    if not tables:
        errors.append("No tables defined in 'tables' section")
    
    for table_name, table_config in tables.items():
        # Validate table configuration
        if not isinstance(table_config, dict):
            errors.append(f"Table '{table_name}' configuration must be a dictionary")
            continue
        
        # Check required fields
        if 'columns' not in table_config:
            errors.append(f"Table '{table_name}' missing 'columns' section")
            continue
        
        # Validate columns
        columns = table_config.get('columns', {})
        if not columns:
            errors.append(f"Table '{table_name}' has no columns defined")
        
        for col_name, col_config in columns.items():
            if not isinstance(col_config, dict):
                errors.append(f"Column '{table_name}.{col_name}' configuration must be a dictionary")
                continue
            
            # Validate column type
            col_type = col_config.get('type')
            if not col_type:
                errors.append(f"Column '{table_name}.{col_name}' missing 'type' field")
            
            valid_types = ['long', 'string', 'decimal', 'timestamp', 'int', 'date', 'boolean']
            if col_type and col_type not in valid_types:
                errors.append(f"Column '{table_name}.{col_name}' has invalid type '{col_type}'. Must be one of: {valid_types}")
        
        # Validate dependencies
        depends_on = table_config.get('depends_on', [])
        for dep in depends_on:
            if dep not in tables:
                errors.append(f"Table '{table_name}' depends on undefined table '{dep}'")
    
    return errors