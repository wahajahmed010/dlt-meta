#!/usr/bin/env python3
"""
Demo script showing the enhanced DLT-Meta CLI functionality.
"""

import json
import logging
import os
import sys
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from enhanced_cli import EnhancedDLTMetaCLI

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_demo_config():
    """Create demonstration configuration files."""
    
    # Synthetic data configuration (from the document)
    synthetic_config = {
        'variables': {
            'uc_catalog_name': 'dev_catalog',
            'bronze_schema': 'synthetic_bronze',
            'silver_schema': 'synthetic_silver',
            'uc_volume_path': '/Volumes/dev_catalog/dltmeta/dltmeta'
        },
        'resources': {
            'data_generation': {
                'config': {
                    'output_location': '{uc_volume_path}/synthetic_data',
                    'output_format': 'parquet',
                    'schema_output_location': '{uc_volume_path}/synthetic_data/schemas'
                },
                'tables': {
                    'orders': {
                        'rows': 10000,
                        'partitions': 4,
                        'columns': {
                            'order_id': {
                                'type': 'long',
                                'unique_values': 10000
                            },
                            'customer_id': {
                                'type': 'long',
                                'min_value': 1,
                                'max_value': 1000
                            },
                            'order_date': {
                                'type': 'timestamp',
                                'begin': '2023-01-01T00:00:00',
                                'end': '2024-12-31T23:59:59'
                            },
                            'order_amount': {
                                'type': 'decimal',
                                'precision': 10,
                                'scale': 2,
                                'min_value': 10.00,
                                'max_value': 5000.00
                            }
                        }
                    },
                    'order_details': {
                        'rows': 25000,
                        'partitions': 4,
                        'depends_on': ['orders'],
                        'columns': {
                            'order_id': {
                                'type': 'long',
                                'base_column': 'order_id',
                                'base_column_type': 'values'
                            },
                            'product_name': {
                                'type': 'string',
                                'values': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
                                'weights': [30, 20, 20, 20, 10]
                            },
                            'quantity': {
                                'type': 'int',
                                'min_value': 1,
                                'max_value': 5
                            },
                            'unit_price': {
                                'type': 'decimal',
                                'precision': 8,
                                'scale': 2,
                                'min_value': 5.00,
                                'max_value': 2000.00
                            }
                        }
                    }
                }
            }
        },
        'dataflows': [
            {
                'data_flow_id': '100',
                'data_flow_group': 'A1',
                'source_format': 'cloudFiles',
                'source_details': {
                    'source_table': 'orders',
                    'source_path_dev': '{uc_volume_path}/synthetic_data/orders'
                },
                'bronze_catalog_dev': '{uc_catalog_name}',
                'bronze_database_dev': '{bronze_schema}',
                'bronze_table': 'orders',
                'bronze_table_path_dev': '{uc_volume_path}/data/bronze/orders',
                'bronze_reader_options': {
                    'cloudFiles.format': 'parquet',
                    'cloudFiles.schemaLocation': '{uc_volume_path}/synthetic_data/_schemas'
                },
                'bronze_database_quarantine_dev': '{uc_catalog_name}.{bronze_schema}',
                'bronze_quarantine_table': 'orders_quarantine',
                'bronze_quarantine_table_path_dev': '{uc_volume_path}/data/bronze/orders_quarantine',
                'silver_catalog_dev': '{uc_catalog_name}',
                'silver_database_dev': '{silver_schema}',
                'silver_table': 'orders_clean',
                'silver_table_path_dev': '{uc_volume_path}/data/silver/orders_clean',
                'silver_transformation_yaml_dev': '{uc_volume_path}/demo/conf/silver_transformations.yaml'
            },
            {
                'data_flow_id': '101',
                'data_flow_group': 'A1',
                'source_format': 'cloudFiles',
                'source_details': {
                    'source_table': 'order_details',
                    'source_path_dev': '{uc_volume_path}/synthetic_data/order_details'
                },
                'bronze_catalog_dev': '{uc_catalog_name}',
                'bronze_database_dev': '{bronze_schema}',
                'bronze_table': 'order_details',
                'bronze_table_path_dev': '{uc_volume_path}/data/bronze/order_details',
                'bronze_reader_options': {
                    'cloudFiles.format': 'parquet',
                    'cloudFiles.schemaLocation': '{uc_volume_path}/synthetic_data/_schemas'
                },
                'bronze_database_quarantine_dev': '{uc_catalog_name}.{bronze_schema}',
                'bronze_quarantine_table': 'order_details_quarantine',
                'bronze_quarantine_table_path_dev': '{uc_volume_path}/data/bronze/order_details_quarantine',
                'silver_catalog_dev': '{uc_catalog_name}',
                'silver_database_dev': '{silver_schema}',
                'silver_table': 'order_details_clean',
                'silver_table_path_dev': '{uc_volume_path}/data/silver/order_details_clean',
                'silver_transformation_yaml_dev': '{uc_volume_path}/demo/conf/silver_transformations.yaml'
            }
        ],
        'transformations': [
            {
                'target_table': 'orders',
                'select_exp': [
                    'order_id',
                    'customer_id',
                    'order_date',
                    'order_amount',
                    "date_format(order_date, 'yyyy-MM') as order_month",
                    "case when order_amount > 1000 then 'High' else 'Standard' end as order_tier",
                    '_rescued_data'
                ],
                'where_clause': [
                    'order_id IS NOT NULL',
                    'order_amount > 0'
                ]
            },
            {
                'target_table': 'order_details',
                'select_exp': [
                    'order_id',
                    'product_name',
                    'quantity',
                    'unit_price',
                    'quantity * unit_price as line_total',
                    'upper(product_name) as product_category',
                    '_rescued_data'
                ],
                'where_clause': [
                    'order_id IS NOT NULL',
                    'quantity > 0',
                    'unit_price > 0'
                ]
            }
        ]
    }
    
    # Lakeflow Connect configuration (from the document)
    lakeflow_config = {
        'variables': {
            'uc_catalog_name': 'dev_catalog',
            'bronze_schema': 'lakeflow_bronze',
            'silver_schema': 'lakeflow_silver',
            'staging_schema': 'lakeflow_staging',
            'uc_volume_path': '/Volumes/dev_catalog/dltmeta/dltmeta'
        },
        'resources': {
            'connections': {
                'sqlserver-connection': {
                    'name': 'prod_sqlserver_db',
                    'connection_type': 'SQLSERVER',
                    'options': {
                        'host': 'sqlserver.company.com',
                        'port': '1433',
                        'user': '{db_username}',
                        'password': '{db_password}'
                    }
                }
            },
            'pipelines': {
                'gateway': {
                    'name': 'sqlserver-gateway',
                    'gateway_definition': {
                        'connection_name': 'prod_sqlserver_db',
                        'gateway_storage_catalog': '{uc_catalog_name}',
                        'gateway_storage_schema': '{staging_schema}',
                        'gateway_storage_name': 'sqlserver-gateway'
                    },
                    'target': '{staging_schema}',
                    'catalog': '{uc_catalog_name}'
                },
                'pipeline_sqlserver': {
                    'name': 'sqlserver-ingestion-pipeline',
                    'ingestion_definition': {
                        'ingestion_gateway_id': '{gateway_pipeline_id}',
                        'objects': [
                            {
                                'table': {
                                    'source_catalog': 'test',
                                    'source_schema': 'dbo',
                                    'source_table': 'customers',
                                    'destination_catalog': '{uc_catalog_name}',
                                    'destination_schema': '{staging_schema}'
                                }
                            },
                            {
                                'schema': {
                                    'source_catalog': 'test',
                                    'source_schema': 'sales',
                                    'destination_catalog': '{uc_catalog_name}',
                                    'destination_schema': '{staging_schema}'
                                }
                            }
                        ]
                    },
                    'target': '{staging_schema}',
                    'catalog': '{uc_catalog_name}'
                }
            }
        },
        'dataflows': [
            {
                'data_flow_id': '200',
                'data_flow_group': 'A1',
                'source_format': 'lakeflow_connect',
                'source_details': {
                    'source_table': 'customers',
                    'source_path_dev': '{uc_catalog_name}.{staging_schema}.customers'
                },
                'bronze_catalog_dev': '{uc_catalog_name}',
                'bronze_database_dev': '{bronze_schema}',
                'bronze_table': 'customers_from_sqlserver',
                'bronze_table_path_dev': '{uc_volume_path}/data/bronze/customers_from_sqlserver',
                'bronze_reader_options': {
                    'format': 'delta'
                },
                'bronze_database_quarantine_dev': '{uc_catalog_name}.{bronze_schema}',
                'bronze_quarantine_table': 'customers_quarantine',
                'bronze_quarantine_table_path_dev': '{uc_volume_path}/data/bronze/customers_quarantine',
                'silver_catalog_dev': '{uc_catalog_name}',
                'silver_database_dev': '{silver_schema}',
                'silver_table': 'customers_clean',
                'silver_table_path_dev': '{uc_volume_path}/data/silver/customers_clean',
                'silver_transformation_yaml_dev': '{uc_volume_path}/demo/conf/silver_transformations.yaml'
            }
        ]
    }
    
    return synthetic_config, lakeflow_config


def demo_synthetic_data():
    """Demonstrate synthetic data configuration processing."""
    logger.info("🎯 Demonstrating Synthetic Data Configuration Processing")
    logger.info("=" * 60)
    
    synthetic_config, _ = create_demo_config()
    
    # Write configuration file
    config_file = "/tmp/demo_synthetic_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(synthetic_config, f, default_flow_style=False)
    
    logger.info(f"📝 Created configuration file: {config_file}")
    
    # Process with enhanced CLI
    cli = EnhancedDLTMetaCLI()
    cli.load_config(config_file)
    
    cli_variables = {
        'uc_catalog_name': 'demo_catalog',
        'bronze_schema': 'demo_bronze',
        'silver_schema': 'demo_silver'
    }
    
    # Generate synthetic data
    logger.info("🔄 Processing synthetic data generation...")
    cli.generate_synthetic_data(cli_variables)
    
    # Create transformation files
    logger.info("🔄 Creating transformation files...")
    transformation_files = cli.create_transformation_files(cli_variables)
    
    # Create onboarding file
    logger.info("🔄 Creating onboarding file...")
    onboarding_file = cli.create_onboarding_file(cli_variables)
    
    # Show generated files
    logger.info("\\n📋 Generated Files:")
    
    if os.path.exists(onboarding_file):
        logger.info(f"✅ Onboarding file: {onboarding_file}")
        with open(onboarding_file, 'r') as f:
            content = f.read()
            logger.info(f"Content preview (first 500 chars):\\n{content[:500]}...")
    
    for tf in transformation_files:
        if os.path.exists(tf):
            logger.info(f"✅ Transformation file: {tf}")
            with open(tf, 'r') as f:
                content = f.read()
                logger.info(f"Content preview (first 300 chars):\\n{content[:300]}...")
    
    # Show generated notebook
    notebook_path = "/tmp/dlt_meta_notebooks/synthetic_data_generator.py"
    if os.path.exists(notebook_path):
        logger.info(f"✅ Generated notebook: {notebook_path}")
        with open(notebook_path, 'r') as f:
            lines = f.readlines()
            logger.info(f"Notebook has {len(lines)} lines")
            logger.info("First 10 lines:")
            for i, line in enumerate(lines[:10]):
                logger.info(f"  {i+1:2d}: {line.rstrip()}")


def demo_lakeflow_connect():
    """Demonstrate Lakeflow Connect configuration processing."""
    logger.info("\\n🎯 Demonstrating Lakeflow Connect Configuration Processing")
    logger.info("=" * 60)
    
    _, lakeflow_config = create_demo_config()
    
    # Write configuration file
    config_file = "/tmp/demo_lakeflow_config.yaml"
    with open(config_file, 'w') as f:
        yaml.dump(lakeflow_config, f, default_flow_style=False)
    
    logger.info(f"📝 Created configuration file: {config_file}")
    
    # Process with enhanced CLI
    cli = EnhancedDLTMetaCLI()
    cli.load_config(config_file)
    
    cli_variables = {
        'uc_catalog_name': 'demo_catalog',
        'bronze_schema': 'demo_bronze',
        'silver_schema': 'demo_silver',
        'staging_schema': 'demo_staging',
        'db_username': 'demo_user',
        'db_password': 'demo_password'
    }
    
    # Setup Lakeflow Connect
    logger.info("🔄 Processing Lakeflow Connect setup...")
    lfc_resources = cli.setup_lakeflow_connect(cli_variables)
    
    # Create onboarding file
    logger.info("🔄 Creating onboarding file...")
    onboarding_file = cli.create_onboarding_file(cli_variables)
    
    # Show results
    logger.info("\\n📋 Lakeflow Connect Resources:")
    for resource_name, resource_id in lfc_resources.items():
        logger.info(f"✅ {resource_name}: {resource_id}")
    
    if os.path.exists(onboarding_file):
        logger.info(f"\\n✅ Onboarding file: {onboarding_file}")
        with open(onboarding_file, 'r') as f:
            content = f.read()
            logger.info(f"Content preview (first 500 chars):\\n{content[:500]}...")


def demo_cli_commands():
    """Show CLI command examples."""
    logger.info("\\n🎯 CLI Command Examples")
    logger.info("=" * 60)
    
    synthetic_cmd = '''# Enhanced CLI for Synthetic Data
dlt-meta onboard-enhanced \\
  --config_file complete_config.yaml \\
  --uc_catalog_name dev_catalog \\
  --bronze_schema synthetic_bronze \\
  --silver_schema synthetic_silver
# Creates: Synthetic Data → Bronze Tables → Silver Tables'''
    
    lakeflow_cmd = '''# Enhanced CLI for Lakeflow Connect
dlt-meta onboard-enhanced \\
  --config_file complete_lakeflow_config.yaml \\
  --uc_catalog_name dev_catalog \\
  --bronze_schema lakeflow_bronze \\
  --silver_schema lakeflow_silver \\
  --staging_schema lakeflow_staging
# Creates: UC Connection → Gateway Pipeline → Ingestion Pipeline → DLT Pipeline'''
    
    logger.info("📋 Synthetic Data Command:")
    logger.info(synthetic_cmd)
    
    logger.info("\\n📋 Lakeflow Connect Command:")
    logger.info(lakeflow_cmd)


def main():
    """Run the demonstration."""
    logger.info("🚀 Enhanced DLT-Meta CLI Demonstration")
    logger.info("=" * 60)
    logger.info("This demo shows the new multi-section YAML capabilities")
    logger.info("for synthetic data generation and Lakeflow Connect integration.")
    logger.info("")
    
    try:
        # Demo synthetic data processing
        demo_synthetic_data()
        
        # Demo Lakeflow Connect processing
        demo_lakeflow_connect()
        
        # Show CLI commands
        demo_cli_commands()
        
        logger.info("\\n🎉 Demonstration completed successfully!")
        logger.info("\\n📁 Generated files are available in /tmp/ for inspection")
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())