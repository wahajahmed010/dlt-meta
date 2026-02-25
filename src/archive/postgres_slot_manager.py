"""
PostgreSQL replication slot and publication management for Lakeflow Connect CDC.
Based on reference implementation from lfcddemo-one-click-notebooks.

ARCHIVED: Not documented in docs/dlt-meta-dab.md; not wired into enhanced_cli.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Optional imports for testing
try:
    import pandas as pd
    import sqlalchemy as sa
    from sqlalchemy import create_engine, text
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    logger.warning("SQLAlchemy/pandas not available - running in test mode")
    pd = None
    sa = None
    create_engine = None
    text = None
    SQLAlchemyError = Exception


class PostgreSQLSlotManager:
    """Manages PostgreSQL replication slots and publications for CDC."""
    
    def __init__(self, connection_config: Dict[str, Any]):
        """Initialize with PostgreSQL connection configuration."""
        self.connection_config = connection_config
        self.engine = None
        self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine from connection configuration."""
        try:
            options = self.connection_config.get('options', {})
            
            # Build connection URL
            host = options.get('host')
            port = options.get('port', '5432')
            user = options.get('user')
            password = options.get('password')
            database = options.get('database', 'postgres')
            
            if not all([host, user, password]):
                raise ValueError("Missing required PostgreSQL connection parameters")
            
            connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
            
            self.engine = create_engine(connection_url)
            logger.info(f"Created PostgreSQL engine for {host}:{port}/{database}")
            
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL engine: {e}")
            raise
    
    def create_replication_slot_and_publication(self, target_schema: str, 
                                              source_schema: str = "lfcddemo",
                                              tables: Optional[List[str]] = None) -> bool:
        """
        Create PostgreSQL replication slot and publication for CDC.
        
        Args:
            target_schema: Target schema name (used as slot name)
            source_schema: Source schema containing tables
            tables: List of tables to include in publication (defaults to intpk, dtix)
            
        Returns:
            True if successful, False otherwise
        """
        
        if tables is None:
            tables = ["intpk", "dtix"]
        
        slot_name = target_schema
        publication_name = f"{target_schema}_pub"
        
        try:
            with self.engine.connect() as conn:
                # Create publication
                table_list = ", ".join([f"{source_schema}.{table}" for table in tables])
                publication_sql = f"CREATE PUBLICATION {publication_name} FOR TABLE {table_list}"
                
                logger.info(f"Creating publication: {publication_name}")
                logger.debug(f"Publication SQL: {publication_sql}")
                
                try:
                    conn.execute(text(publication_sql))
                    logger.info(f"✅ Created publication: {publication_name}")
                except SQLAlchemyError as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Publication {publication_name} already exists")
                    else:
                        logger.error(f"Failed to create publication: {e}")
                        return False
                
                # Create replication slot
                slot_sql = f"SELECT 'init' FROM pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
                
                logger.info(f"Creating replication slot: {slot_name}")
                logger.debug(f"Slot SQL: {slot_sql}")
                
                try:
                    conn.execute(text(slot_sql))
                    logger.info(f"✅ Created replication slot: {slot_name}")
                except SQLAlchemyError as e:
                    if "already exists" in str(e).lower():
                        logger.info(f"Replication slot {slot_name} already exists")
                    else:
                        logger.error(f"Failed to create replication slot: {e}")
                        return False
                
                # Commit changes
                conn.commit()
                
                # Verify creation
                self._verify_replication_setup(conn, slot_name, publication_name)
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to create replication slot and publication: {e}")
            return False
    
    def _verify_replication_setup(self, conn, slot_name: str, publication_name: str):
        """Verify that replication slot and publication were created successfully."""
        
        try:
            # Check replication slots
            slots_query = text("SELECT * FROM pg_replication_slots ORDER BY slot_name")
            slots_result = conn.execute(slots_query)
            slots_df = pd.DataFrame(slots_result.fetchall(), columns=slots_result.keys())
            
            logger.info("Current replication slots:")
            if not slots_df.empty:
                logger.info(f"\n{slots_df.to_string(index=False)}")
            else:
                logger.info("No replication slots found")
            
            # Check publications
            pubs_query = text("SELECT * FROM pg_publication ORDER BY pubname")
            pubs_result = conn.execute(pubs_query)
            pubs_df = pd.DataFrame(pubs_result.fetchall(), columns=pubs_result.keys())
            
            logger.info("Current publications:")
            if not pubs_df.empty:
                logger.info(f"\n{pubs_df.to_string(index=False)}")
            else:
                logger.info("No publications found")
            
            # Verify our specific slot and publication exist
            slot_exists = slot_name in slots_df['slot_name'].values if not slots_df.empty else False
            pub_exists = publication_name in pubs_df['pubname'].values if not pubs_df.empty else False
            
            if slot_exists and pub_exists:
                logger.info(f"✅ Verified replication setup: slot='{slot_name}', publication='{publication_name}'")
            else:
                logger.warning(f"⚠️  Incomplete setup: slot_exists={slot_exists}, pub_exists={pub_exists}")
                
        except Exception as e:
            logger.error(f"Failed to verify replication setup: {e}")
    
    def cleanup_replication_slot_and_publication(self, target_schema: str) -> bool:
        """
        Clean up PostgreSQL replication slot and publication.
        
        Args:
            target_schema: Target schema name (used as slot name)
            
        Returns:
            True if successful, False otherwise
        """
        
        slot_name = target_schema
        publication_name = f"{target_schema}_pub"
        
        try:
            with self.engine.connect() as conn:
                # Drop publication
                pub_sql = f"DROP PUBLICATION IF EXISTS {publication_name} CASCADE"
                logger.info(f"Dropping publication: {publication_name}")
                
                try:
                    conn.execute(text(pub_sql))
                    logger.info(f"✅ Dropped publication: {publication_name}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to drop publication: {e}")
                
                # Drop replication slot
                slot_sql = f"""
                SELECT pg_drop_replication_slot('{slot_name}') 
                WHERE EXISTS (
                    SELECT 1 FROM pg_replication_slots 
                    WHERE slot_name = '{slot_name}'
                )
                """
                logger.info(f"Dropping replication slot: {slot_name}")
                
                try:
                    conn.execute(text(slot_sql))
                    logger.info(f"✅ Dropped replication slot: {slot_name}")
                except SQLAlchemyError as e:
                    logger.error(f"Failed to drop replication slot: {e}")
                
                # Commit changes
                conn.commit()
                
                return True
                
        except Exception as e:
            logger.error(f"Failed to cleanup replication slot and publication: {e}")
            return False
    
    def get_table_info(self, schema_name: str = "lfcddemo") -> Tuple:
        """
        Get information about tables, columns, and sample data.
        
        Args:
            schema_name: Schema to query
            
        Returns:
            Tuple of (tables_df, columns_df, sample_data_df)
        """
        
        try:
            with self.engine.connect() as conn:
                # Get tables
                tables_query = text(f"""
                    SELECT * FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_SCHEMA='{schema_name}'
                """)
                tables_result = conn.execute(tables_query)
                tables_df = pd.DataFrame(
                    tables_result.fetchall(), 
                    columns=[key.upper() for key in tables_result.keys()]
                )
                
                columns_df = pd.DataFrame()
                sample_data_df = pd.DataFrame()
                
                if not tables_df.empty:
                    first_table = tables_df["TABLE_NAME"].iloc[0]
                    
                    # Get columns
                    try:
                        columns_query = text(f"""
                            SELECT * FROM INFORMATION_SCHEMA.COLUMNS 
                            WHERE TABLE_SCHEMA='{schema_name}' 
                            AND TABLE_NAME='{first_table}'
                        """)
                        columns_result = conn.execute(columns_query)
                        columns_df = pd.DataFrame(
                            columns_result.fetchall(), 
                            columns=columns_result.keys()
                        )
                    except Exception as e:
                        logger.warning(f"Could not get columns info: {e}")
                    
                    # Get sample data
                    try:
                        sample_query = text(f"""
                            SELECT * FROM {schema_name}.{first_table} 
                            WHERE DT = (SELECT MIN(DT) FROM {schema_name}.{first_table})
                        """)
                        sample_result = conn.execute(sample_query)
                        sample_data_df = pd.DataFrame(
                            sample_result.fetchall(), 
                            columns=sample_result.keys()
                        )
                    except Exception as e:
                        logger.warning(f"Could not get sample data: {e}")
                
                return tables_df, columns_df, sample_data_df
                
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                logger.info(f"✅ PostgreSQL connection successful: {version}")
                return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            return False
    
    def close(self):
        """Close database engine."""
        if self.engine:
            self.engine.dispose()
            logger.info("Closed PostgreSQL engine")


def setup_postgres_cdc(connection_config: Dict[str, Any], target_schema: str, 
                      source_schema: str = "lfcddemo", 
                      tables: Optional[List[str]] = None) -> bool:
    """
    Setup PostgreSQL CDC prerequisites (replication slot and publication).
    
    Args:
        connection_config: PostgreSQL connection configuration
        target_schema: Target schema name (used as slot name)
        source_schema: Source schema containing tables
        tables: List of tables to include in publication
        
    Returns:
        True if successful, False otherwise
    """
    
    manager = PostgreSQLSlotManager(connection_config)
    
    try:
        # Test connection
        if not manager.test_connection():
            return False
        
        # Create replication slot and publication
        success = manager.create_replication_slot_and_publication(
            target_schema, source_schema, tables
        )
        
        return success
        
    finally:
        manager.close()


def cleanup_postgres_cdc(connection_config: Dict[str, Any], target_schema: str) -> bool:
    """
    Cleanup PostgreSQL CDC resources (replication slot and publication).
    
    Args:
        connection_config: PostgreSQL connection configuration
        target_schema: Target schema name (used as slot name)
        
    Returns:
        True if successful, False otherwise
    """
    
    manager = PostgreSQLSlotManager(connection_config)
    
    try:
        success = manager.cleanup_replication_slot_and_publication(target_schema)
        return success
        
    finally:
        manager.close()


def get_postgres_table_info(connection_config: Dict[str, Any], 
                           schema_name: str = "lfcddemo") -> Dict:
    """
    Get PostgreSQL table information for CDC setup.
    
    Args:
        connection_config: PostgreSQL connection configuration
        schema_name: Schema to query
        
    Returns:
        Dictionary with 'tables', 'columns', 'sample_data' DataFrames
    """
    
    manager = PostgreSQLSlotManager(connection_config)
    
    try:
        tables_df, columns_df, sample_data_df = manager.get_table_info(schema_name)
        
        return {
            'tables': tables_df,
            'columns': columns_df,
            'sample_data': sample_data_df
        }
        
    finally:
        manager.close()
