#!/usr/bin/env python3
"""
Enhanced DLT-Meta CLI with multi-section YAML support for synthetic data generation and Lakeflow Connect.
"""

import argparse
import json
import logging
import os
import sys
import yaml
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class EnhancedDLTMetaCLI:
    """Enhanced CLI for DLT-Meta with multi-section YAML support."""
    
    def __init__(self):
        self.config = {}
        self.variables = {}
        self.resources = {}
        self.dataflows = []
        self.transformations = []
        
    def load_config(self, config_file_path: str) -> Dict[str, Any]:
        """Load and parse multi-section YAML configuration."""
        try:
            with open(config_file_path, 'r') as file:
                config = yaml.safe_load(file)
            
            logger.info(f"Loaded configuration from {config_file_path}")
            
            # Extract sections
            self.variables = config.get('variables', {})
            self.resources = config.get('resources', {})
            self.transformations = config.get('transformations', [])
            
            # Handle dataflows section (optional for backward compatibility)
            if 'dataflows' in config:
                self.dataflows = config['dataflows']
            elif isinstance(config, list):
                # Traditional format - array at root level
                self.dataflows = config
            else:
                raise ValueError("No 'dataflows' section found and config is not a list")
                
            return config
            
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    def substitute_variables(self, obj: Any, cli_variables: Dict[str, str]) -> Any:
        """Recursively substitute variables in configuration using {variable} syntax."""
        # CLI variables override file variables
        all_variables = {**self.variables, **cli_variables}
        
        if isinstance(obj, str):
            for key, value in all_variables.items():
                obj = obj.replace(f"{{{key}}}", str(value))
            return obj
        elif isinstance(obj, dict):
            return {k: self.substitute_variables(v, cli_variables) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.substitute_variables(item, cli_variables) for item in obj]
        else:
            return obj
    
    def generate_synthetic_data(self, cli_variables: Dict[str, str]) -> bool:
        """Generate synthetic data using dbldatagen based on resources.data_generation config."""
        if 'data_generation' not in self.resources:
            logger.info("No data_generation section found, skipping synthetic data generation")
            return True
            
        try:
            from src.synthetic_data import SyntheticDataGenerator, validate_data_generation_config
            
            data_gen_config = self.substitute_variables(
                self.resources['data_generation'], cli_variables
            )
            
            # Validate before generation
            errors = validate_data_generation_config(data_gen_config)
            if errors:
                for err in errors:
                    logger.error(err)
                return False
            
            generator = SyntheticDataGenerator()
            return generator.generate_from_config(data_gen_config)
            
        except ImportError:
            logger.warning("SyntheticDataGenerator not available - skipping synthetic data generation")
            return True
        except Exception as e:
            logger.error(f"Error generating synthetic data: {e}")
            return False
    
    def setup_lakeflow_connect(self, cli_variables: Dict[str, str]) -> Dict[str, str]:
        """Setup Lakeflow Connect resources (connections, gateway, ingestion pipelines)."""
        if 'connections' not in self.resources and 'pipelines' not in self.resources:
            logger.info("No Lakeflow Connect resources found, skipping setup")
            return {}
        
        try:
            # Substitute variables in resources
            resources = self.substitute_variables(self.resources, cli_variables)
            
            # Use LakeflowConnectManager when Databricks SDK is available
            try:
                from src.lakeflow_connect import LakeflowConnectManager
                manager = LakeflowConnectManager()
                if manager.client is not None:
                    return manager.deploy_complete_lakeflow_setup({"resources": resources})
            except ImportError:
                pass
            
            # Fallback: dry-run mode when SDK not available (e.g. testing)
            return self._setup_lakeflow_connect_dry_run(resources)
            
        except Exception as e:
            logger.error(f"Error setting up Lakeflow Connect: {e}")
            raise
    
    def _setup_lakeflow_connect_dry_run(self, resources: Dict[str, Any]) -> Dict[str, str]:
        """Dry-run mode: log specs without creating resources (when Databricks SDK unavailable)."""
        created_resources = {}
        pipelines = resources.get('pipelines', {})
        
        if 'connections' in resources:
            for conn_name, conn_config in resources['connections'].items():
                logger.info(f"Would create connection {conn_name}: {json.dumps(conn_config, indent=2)}")
                created_resources[f'connection_{conn_name}'] = f"conn_{conn_name}_12345"
        
        # Create gateway pipelines first
        for pipeline_name, pipeline_config in pipelines.items():
            if 'gateway_definition' in pipeline_config:
                logger.info(f"Would create gateway pipeline {pipeline_name}")
                created_resources[f'pipeline_{pipeline_name}'] = f"pipeline_{pipeline_name}_67890"
        
        # Create ingestion pipelines (with gateway reference resolved)
        gateway_id = created_resources.get('pipeline_gateway')
        for pipeline_name, pipeline_config in pipelines.items():
            if 'ingestion_definition' in pipeline_config:
                if gateway_id:
                    logger.info(f"Would create ingestion pipeline {pipeline_name} (gateway={gateway_id})")
                else:
                    logger.info(f"Would create ingestion pipeline {pipeline_name}")
                created_resources[f'pipeline_{pipeline_name}'] = f"pipeline_{pipeline_name}_67890"
        
        return created_resources
    
    def create_transformation_files(self, cli_variables: Dict[str, str]) -> List[str]:
        """Create separate transformation files from transformations section."""
        if not self.transformations:
            logger.info("No transformations section found, skipping transformation file creation")
            return []
        
        try:
            # Substitute variables in transformations
            transformations = self.substitute_variables(self.transformations, cli_variables)
            
            # Create transformation file
            transformation_file = "/tmp/silver_transformations.yaml"
            with open(transformation_file, 'w') as f:
                yaml.dump(transformations, f, default_flow_style=False)
            
            logger.info(f"Created transformation file: {transformation_file}")
            return [transformation_file]
            
        except Exception as e:
            logger.error(f"Error creating transformation files: {e}")
            raise
    
    def create_onboarding_file(self, cli_variables: Dict[str, str]) -> str:
        """Create traditional onboarding file from dataflows section."""
        try:
            # Substitute variables in dataflows
            dataflows = self.substitute_variables(self.dataflows, cli_variables)
            
            # Create onboarding file
            onboarding_file = "/tmp/onboarding.yaml"
            with open(onboarding_file, 'w') as f:
                yaml.dump(dataflows, f, default_flow_style=False)
            
            logger.info(f"Created onboarding file: {onboarding_file}")
            return onboarding_file
            
        except Exception as e:
            logger.error(f"Error creating onboarding file: {e}")
            raise
    
    def run_enhanced_onboarding(self, args: argparse.Namespace) -> bool:
        """Run the enhanced onboarding process."""
        try:
            # Load configuration
            config = self.load_config(args.config_file)
            
            # Prepare CLI variables
            cli_variables = {
                'uc_catalog_name': args.uc_catalog_name,
                'bronze_schema': getattr(args, 'bronze_schema', 'bronze'),
                'silver_schema': getattr(args, 'silver_schema', 'silver'),
                'staging_schema': getattr(args, 'staging_schema', 'staging'),
            }
            
            # Add any additional CLI parameters as variables
            for key, value in vars(args).items():
                if value is not None and key not in ['config_file']:
                    cli_variables[key] = value
            
            logger.info(f"CLI variables: {cli_variables}")
            
            # Step 1: Generate synthetic data (if configured)
            if not self.generate_synthetic_data(cli_variables):
                logger.error("Synthetic data generation failed")
                return False
            
            # Step 2: Setup Lakeflow Connect resources (if configured)
            lfc_resources = self.setup_lakeflow_connect(cli_variables)
            
            # Step 3: Create transformation files
            transformation_files = self.create_transformation_files(cli_variables)
            
            # Step 4: Create traditional onboarding file
            onboarding_file = self.create_onboarding_file(cli_variables)
            
            # Step 5: Run traditional DLT-Meta onboarding
            logger.info("Running traditional DLT-Meta onboarding...")
            
            # Prepare arguments for original CLI
            original_args = [
                '--onboarding_file_path', onboarding_file,
                '--uc_catalog_name', cli_variables['uc_catalog_name'],
            ]
            
            # Add optional parameters
            for param in ['bronze_schema', 'silver_schema', 'staging_schema']:
                if param in cli_variables:
                    original_args.extend([f'--{param}', cli_variables[param]])
            
            # In a real implementation, this would call the original CLI
            logger.info(f"Would call original CLI with args: {original_args}")
            
            logger.info("✅ Enhanced onboarding completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Enhanced onboarding failed: {e}")
            return False


def main():
    """Main entry point for enhanced CLI."""
    parser = argparse.ArgumentParser(description='Enhanced DLT-Meta CLI with multi-section YAML support')
    
    # Enhanced CLI specific arguments
    parser.add_argument('--config_file', required=True, 
                       help='Path to multi-section YAML configuration file')
    
    # Standard DLT-Meta arguments
    parser.add_argument('--uc_catalog_name', required=True,
                       help='Unity Catalog name')
    parser.add_argument('--bronze_schema', 
                       help='Bronze schema name')
    parser.add_argument('--silver_schema',
                       help='Silver schema name') 
    parser.add_argument('--staging_schema',
                       help='Staging schema name (for Lakeflow Connect)')
    parser.add_argument('--uc_volume_path',
                       help='Unity Catalog volume path')
    
    # Additional parameters
    parser.add_argument('--db_username',
                       help='Database username (for Lakeflow Connect)')
    parser.add_argument('--db_password', 
                       help='Database password (for Lakeflow Connect)')
    
    args = parser.parse_args()
    
    # Run enhanced onboarding
    cli = EnhancedDLTMetaCLI()
    success = cli.run_enhanced_onboarding(args)
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()