"""
Generate a Databricks notebook for synthetic data generation.
Redundant wrapper around SyntheticDataGenerator.generate_from_config().

ARCHIVED: Never called; use SyntheticDataGenerator directly.
"""

from typing import Dict, Any


def generate_synthetic_data_notebook(config: Dict[str, Any]) -> str:
    """
    Generate a Databricks notebook for synthetic data generation.
    
    Args:
        config: Data generation configuration with 'config' and 'tables' sections
        
    Returns:
        Path to the generated notebook file
    """
    
    from src.synthetic_data import SyntheticDataGenerator
    
    generator = SyntheticDataGenerator()
    success = generator.generate_from_config(config)
    
    if success:
        return "/tmp/dlt_meta_notebooks/synthetic_data_generator.py"
    else:
        raise Exception("Failed to generate synthetic data notebook")
