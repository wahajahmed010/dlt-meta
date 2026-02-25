#!/usr/bin/env python3
"""
Test script for enhanced DLT-Meta CLI implementation.
"""

import json
import logging
import os
import sys
import tempfile
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from enhanced_cli import EnhancedDLTMetaCLI
from synthetic_data import SyntheticDataGenerator, validate_data_generation_config
from archive.lakeflow_connect_specs import create_lakeflow_connect_specs

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_synthetic_data_config():
    """Test synthetic data configuration validation and generation."""
    logger.info("🧪 Testing synthetic data configuration...")
    
    # Test configuration from the document
    config = {
        'config': {
            'output_location': '/tmp/test_synthetic_data',
            'output_format': 'parquet',
            'schema_output_location': '/tmp/test_synthetic_data/_schemas'
        },
        'tables': {
            'orders': {
                'rows': 1000,
                'partitions': 2,
                'columns': {
                    'order_id': {
                        'type': 'long',
                        'unique_values': 1000
                    },
                    'customer_id': {
                        'type': 'long',
                        'min_value': 1,
                        'max_value': 100
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
                'rows': 2500,
                'partitions': 2,
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
    
    # Validate configuration
    errors = validate_data_generation_config(config)
    if errors:
        logger.error(f"Configuration validation failed: {errors}")
        return False
    
    logger.info("✅ Configuration validation passed")
    
    # Test generation
    generator = SyntheticDataGenerator()
    success = generator.generate_from_config(config)
    
    if success:
        logger.info("✅ Synthetic data generation test passed")
        return True
    else:
        logger.error("❌ Synthetic data generation test failed")
        return False


def test_lakeflow_connect_specs():
    """Test Lakeflow Connect specification generation."""
    logger.info("🧪 Testing Lakeflow Connect specifications...")
    
    # Test configuration from the document
    config = {
        'connection_name': 'prod_sqlserver_db',
        'gateway_storage_catalog': 'dev_catalog',
        'gateway_storage_schema': 'lakeflow_staging',
        'pipeline_mode': 'cdc',
        'ingestion_objects': [
            {
                'table': {
                    'source_catalog': 'test',
                    'source_schema': 'dbo',
                    'source_table': 'customers',
                    'destination_catalog': 'dev_catalog',
                    'destination_schema': 'lakeflow_staging'
                }
            },
            {
                'schema': {
                    'source_catalog': 'test',
                    'source_schema': 'sales',
                    'destination_catalog': 'dev_catalog',
                    'destination_schema': 'lakeflow_staging'
                }
            }
        ]
    }
    
    try:
        gateway_spec, ingestion_spec = create_lakeflow_connect_specs(config)
        
        logger.info("Gateway specification:")
        logger.info(json.dumps(gateway_spec, indent=2))
        
        logger.info("Ingestion specification:")
        logger.info(json.dumps(ingestion_spec, indent=2))
        
        # Validate specs have required fields
        if gateway_spec and 'gateway_definition' in gateway_spec:
            logger.info("✅ Gateway specification generated successfully")
        else:
            logger.error("❌ Invalid gateway specification")
            return False
        
        if ingestion_spec and 'ingestion_definition' in ingestion_spec:
            logger.info("✅ Ingestion specification generated successfully")
        else:
            logger.error("❌ Invalid ingestion specification")
            return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Lakeflow Connect specification generation failed: {e}")
        return False


def test_multi_section_yaml():
    """Test multi-section YAML parsing."""
    logger.info("🧪 Testing multi-section YAML parsing...")
    
    # Create test YAML configuration
    test_config = {
        'variables': {
            'uc_catalog_name': 'test_catalog',
            'bronze_schema': 'test_bronze',
            'silver_schema': 'test_silver',
            'uc_volume_path': '/tmp/test_volumes'
        },
        'resources': {
            'data_generation': {
                'config': {
                    'output_location': '{uc_volume_path}/synthetic_data',
                    'output_format': 'parquet'
                },
                'tables': {
                    'test_table': {
                        'rows': 100,
                        'columns': {
                            'id': {'type': 'long', 'unique_values': 100},
                            'name': {'type': 'string', 'template': '\\\\w{5,10}'}
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
                    'source_table': 'test_table',
                    'source_path_dev': '{uc_volume_path}/synthetic_data/test_table'
                },
                'bronze_catalog_dev': '{uc_catalog_name}',
                'bronze_database_dev': '{bronze_schema}',
                'bronze_table': 'test_table'
            }
        ]
    }
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(test_config, f)
        config_file = f.name
    
    try:
        # Test parsing
        cli = EnhancedDLTMetaCLI()
        loaded_config = cli.load_config(config_file)
        
        # Validate sections
        if cli.variables.get('uc_catalog_name') != 'test_catalog':
            logger.error("❌ Variables section not parsed correctly")
            return False
        
        if 'data_generation' not in cli.resources:
            logger.error("❌ Resources section not parsed correctly")
            return False
        
        if len(cli.dataflows) != 1:
            logger.error("❌ Dataflows section not parsed correctly")
            return False
        
        # Test variable substitution
        cli_variables = {
            'uc_catalog_name': 'override_catalog',
            'bronze_schema': 'override_bronze'
        }
        
        substituted = cli.substitute_variables(cli.dataflows[0], cli_variables)
        
        if substituted['bronze_catalog_dev'] != 'override_catalog':
            logger.error("❌ Variable substitution failed")
            return False
        
        logger.info("✅ Multi-section YAML parsing test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Multi-section YAML parsing test failed: {e}")
        return False
        
    finally:
        # Cleanup
        os.unlink(config_file)


def test_complete_workflow():
    """Test complete enhanced CLI workflow."""
    logger.info("🧪 Testing complete enhanced CLI workflow...")
    
    # Create complete test configuration
    complete_config = {
        'variables': {
            'uc_catalog_name': 'test_catalog',
            'bronze_schema': 'test_bronze',
            'silver_schema': 'test_silver',
            'uc_volume_path': '/tmp/test_volumes'
        },
        'resources': {
            'data_generation': {
                'config': {
                    'output_location': '{uc_volume_path}/synthetic_data',
                    'output_format': 'parquet',
                    'schema_output_location': '{uc_volume_path}/synthetic_data/_schemas'
                },
                'tables': {
                    'customers': {
                        'rows': 500,
                        'partitions': 2,
                        'columns': {
                            'customer_id': {'type': 'long', 'unique_values': 500},
                            'name': {'type': 'string', 'template': '\\\\w{5,15}'},
                            'email': {'type': 'string', 'template': '\\\\w+@\\\\w+\\\\.com'},
                            'created_date': {'type': 'timestamp', 'begin': '2023-01-01T00:00:00', 'end': '2024-12-31T23:59:59'}
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
                    'source_table': 'customers',
                    'source_path_dev': '{uc_volume_path}/synthetic_data/customers'
                },
                'bronze_catalog_dev': '{uc_catalog_name}',
                'bronze_database_dev': '{bronze_schema}',
                'bronze_table': 'customers',
                'bronze_table_path_dev': '{uc_volume_path}/data/bronze/customers',
                'bronze_reader_options': {
                    'cloudFiles.format': 'parquet',
                    'cloudFiles.schemaLocation': '{uc_volume_path}/synthetic_data/_schemas'
                },
                'silver_catalog_dev': '{uc_catalog_name}',
                'silver_database_dev': '{silver_schema}',
                'silver_table': 'customers_clean',
                'silver_table_path_dev': '{uc_volume_path}/data/silver/customers_clean'
            }
        ],
        'transformations': [
            {
                'target_table': 'customers',
                'select_exp': [
                    'customer_id',
                    'name',
                    'email',
                    'created_date',
                    'upper(name) as name_upper'
                ],
                'where_clause': [
                    'customer_id IS NOT NULL',
                    'email IS NOT NULL'
                ]
            }
        ]
    }
    
    # Write to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(complete_config, f)
        config_file = f.name
    
    try:
        # Create mock CLI arguments
        class MockArgs:
            def __init__(self):
                self.config_file = config_file
                self.uc_catalog_name = 'test_catalog'
                self.bronze_schema = 'test_bronze'
                self.silver_schema = 'test_silver'
        
        args = MockArgs()
        
        # Test enhanced CLI
        cli = EnhancedDLTMetaCLI()
        
        # Load and validate configuration
        config = cli.load_config(args.config_file)
        
        cli_variables = {
            'uc_catalog_name': args.uc_catalog_name,
            'bronze_schema': args.bronze_schema,
            'silver_schema': args.silver_schema,
        }
        
        # Test synthetic data generation
        if not cli.generate_synthetic_data(cli_variables):
            logger.error("❌ Synthetic data generation failed")
            return False
        
        # Test transformation file creation
        transformation_files = cli.create_transformation_files(cli_variables)
        if not transformation_files:
            logger.error("❌ Transformation file creation failed")
            return False
        
        # Test onboarding file creation
        onboarding_file = cli.create_onboarding_file(cli_variables)
        if not onboarding_file:
            logger.error("❌ Onboarding file creation failed")
            return False
        
        # Verify files were created
        if not os.path.exists(onboarding_file):
            logger.error(f"❌ Onboarding file not created: {onboarding_file}")
            return False
        
        if transformation_files and not os.path.exists(transformation_files[0]):
            logger.error(f"❌ Transformation file not created: {transformation_files[0]}")
            return False
        
        logger.info("✅ Complete workflow test passed")
        return True
        
    except Exception as e:
        logger.error(f"❌ Complete workflow test failed: {e}")
        return False
        
    finally:
        # Cleanup
        os.unlink(config_file)


def main():
    """Run all tests."""
    logger.info("🚀 Starting enhanced DLT-Meta CLI tests...")
    
    tests = [
        ("Synthetic Data Configuration", test_synthetic_data_config),
        ("Lakeflow Connect Specifications", test_lakeflow_connect_specs),
        ("Multi-Section YAML Parsing", test_multi_section_yaml),
        ("Complete Workflow", test_complete_workflow),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        logger.info(f"\\n{'='*60}")
        logger.info(f"Running test: {test_name}")
        logger.info('='*60)
        
        try:
            if test_func():
                logger.info(f"✅ {test_name} PASSED")
                passed += 1
            else:
                logger.error(f"❌ {test_name} FAILED")
                failed += 1
        except Exception as e:
            logger.error(f"❌ {test_name} FAILED with exception: {e}")
            failed += 1
    
    # Summary
    logger.info(f"\\n{'='*60}")
    logger.info(f"TEST SUMMARY")
    logger.info('='*60)
    logger.info(f"Total tests: {passed + failed}")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    
    if failed == 0:
        logger.info("🎉 ALL TESTS PASSED!")
        return 0
    else:
        logger.error(f"💥 {failed} TESTS FAILED!")
        return 1


if __name__ == '__main__':
    sys.exit(main())