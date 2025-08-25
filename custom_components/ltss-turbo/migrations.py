
"""Database migrations for LTSS Turbo with mandatory PostGIS location support."""

import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from sqlalchemy import text, inspect, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, OperationalError

_LOGGER = logging.getLogger(__name__)

# Current schema version
SCHEMA_VERSION = 4  # Increment for mandatory location change

# Migration registry
MIGRATIONS: Dict[int, Callable] = {}

def migration(version: int):
    """Decorator to register a migration function."""
    def decorator(func):
        MIGRATIONS[version] = func
        return func
    return decorator

def run_startup_migrations(engine: Engine, table_name: str = "ltss_turbo", 
                         enable_timescale: bool = True,
                         enable_compression: bool = True,
                         chunk_time_interval: int = 86400000000,
                         compression_after: int = 7,
                         retention_days: Optional[int] = None) -> bool:
    """
    Run all necessary migrations with mandatory PostGIS location support.
    
    Returns True if successful, False otherwise.
    """
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # Ensure metadata table exists
            _ensure_metadata_table(conn, table_name)
            
            # Get current schema version
            current_version = _get_schema_version(conn, table_name)
            
            if current_version is None:
                # Fresh install - run initial setup with mandatory location
                _LOGGER.info("Fresh LTSS Turbo installation detected, running initial setup with mandatory PostGIS...")
                _run_initial_setup_mandatory_location(conn, engine, table_name, enable_timescale, 
                                                    chunk_time_interval)
                current_version = 0
            
            # Run any pending migrations
            migrations_run = 0
            for version in sorted(MIGRATIONS.keys()):
                if version > current_version:
                    _LOGGER.info(f"Running migration v{version}...")
                    try:
                        MIGRATIONS[version](conn, table_name, engine)
                        _set_schema_version(conn, table_name, version)
                        migrations_run += 1
                        current_version = version
                    except Exception as e:
                        _LOGGER.error(f"Migration v{version} failed: {e}")
            
            if migrations_run > 0:
                _LOGGER.info(f"Completed {migrations_run} migrations, schema now at v{current_version}")
            else:
                _LOGGER.debug(f"Schema up-to-date at v{current_version}, no migrations needed")
            
            # Apply runtime configurations
            if enable_timescale:
                _configure_timescaledb_policies(conn, table_name, enable_compression, 
                                              compression_after, retention_days)
            
            return True
            
    except Exception as e:
        _LOGGER.error(f"Migration system error: {e}")
        return False

def _ensure_metadata_table(conn, table_name: str):
    """Create metadata table if it doesn't exist."""
    meta_table = f"{table_name}_meta"
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {meta_table} (
            key VARCHAR(50) PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """))

def _get_schema_version(conn, table_name: str) -> Optional[int]:
    """Get current schema version from metadata."""
    meta_table = f"{table_name}_meta"
    try:
        result = conn.execute(text(f"""
            SELECT value FROM {meta_table} WHERE key = 'schema_version'
        """))
        row = result.fetchone()
        return int(row.value) if row else None
    except Exception:
        return None

def _set_schema_version(conn, table_name: str, version: int):
    """Update schema version in metadata."""
    meta_table = f"{table_name}_meta"
    conn.execute(text(f"""
        INSERT INTO {meta_table} (key, value, updated_at)
        VALUES ('schema_version', :version, NOW())
        ON CONFLICT (key) DO UPDATE 
        SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
    """), {"version": str(version)})

def _run_initial_setup_mandatory_location(conn, engine: Engine, table_name: str,
                                        enable_timescale: bool, chunk_time_interval: int):
    """Run initial setup with mandatory PostGIS location support."""
    
    inspector = inspect(engine)
    
    # Check for available extensions
    extensions = _get_available_extensions(conn)

    # TimescaleDB setup
    if enable_timescale and "timescaledb" in extensions:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
            _LOGGER.info("TimescaleDB extension enabled")
        except Exception as e:
            _LOGGER.warning(f"Could not enable TimescaleDB: {e}")

    # PostGIS setup - MANDATORY now
    if "postgis" in extensions:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis CASCADE"))
            _LOGGER.info("PostGIS extension enabled (mandatory)")
        except Exception as e:
            _LOGGER.error(f"Could not enable PostGIS (required for location support): {e}")
            raise Exception("PostGIS is required for LTSS Turbo - location support is mandatory")
    else:
        raise Exception("PostGIS extension not available - location support is mandatory")

    # Import model and ensure location is activated
    from .models import Base, make_ltss_model
    Model = make_ltss_model(table_name)
    
    # Location is now always activated
    # Note: The model now always includes the location column

    # Create table if missing
    if not inspector.has_table(table_name):
        _LOGGER.info(f"Creating table '{table_name}' with mandatory location column...")
        Base.metadata.create_all(engine, tables=[Base.metadata.tables[table_name]])

        # Verify table creation
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            raise RuntimeError(f"Failed to create table '{table_name}'")
            
        # Verify location column exists
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if 'location' not in columns:
            raise RuntimeError(f"Location column not created in table '{table_name}' - PostGIS may not be properly installed")
            
        _LOGGER.info(f"Table '{table_name}' created successfully with location column")
    else:
        _LOGGER.info(f"Table '{table_name}' already exists")
        
        # Ensure location column exists for existing tables
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        if 'location' not in columns:
            _LOGGER.info("Adding mandatory location column to existing table...")
            try:
                conn.execute(text(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN location GEOMETRY(POINT, 4326)
                """))
                _LOGGER.info("Location column added successfully")
            except Exception as e:
                _LOGGER.error(f"Failed to add location column: {e}")
                raise

    # Convert to hypertable if TimescaleDB is available
    if enable_timescale and "timescaledb" in extensions:
        try:
            conn.execute(text(f"""
                SELECT create_hypertable(
                    '{table_name}',
                    'time',
                    chunk_time_interval => INTERVAL '{chunk_time_interval // 1_000_000} seconds',
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                )
            """))
            _LOGGER.info(f"Hypertable created for '{table_name}'")
        except Exception as e:
            _LOGGER.warning(f"Could not create hypertable: {e}")

def _get_available_extensions(conn) -> set:
    """Get set of available PostgreSQL extensions."""
    try:
        result = conn.execute(text("""
            SELECT name FROM pg_available_extensions
        """))
        return {row.name for row in result}
    except Exception:
        return set()

def _configure_timescaledb_policies(conn, table_name: str, enable_compression: bool,
                                   compression_after: int, retention_days: Optional[int]):
    """Configure TimescaleDB compression and retention policies."""
    
    # Check if table is a hypertable
    try:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_name = '{table_name}'
        """))
        if result.fetchone().count == 0:
            return  # Not a hypertable, skip policies
    except Exception:
        return  # TimescaleDB not available

    # Configure compression
    if enable_compression:
        try:
            # Enable compression on the hypertable
            conn.execute(text(f"""
                ALTER TABLE {table_name} SET (
                    timescaledb.compress,
                    timescaledb.compress_segmentby = 'entity_id',
                    timescaledb.compress_orderby = 'time DESC'
                )
            """))
            
            # Add compression policy
            conn.execute(text(f"""
                SELECT add_compression_policy(
                    '{table_name}',
                    INTERVAL '{compression_after} days',
                    if_not_exists => TRUE
                )
            """))
            
            _LOGGER.debug(f"Compression policy configured: compress after {compression_after} days")
        except Exception as e:
            _LOGGER.debug(f"Compression policy already configured: {e}")
    
    # Configure retention
    if retention_days:
        try:
            conn.execute(text(f"""
                SELECT add_retention_policy(
                    '{table_name}',
                    INTERVAL '{retention_days} days',
                    if_not_exists => TRUE
                )
            """))
            _LOGGER.debug(f"Retention policy configured: {retention_days} days")
        except Exception as e:
            _LOGGER.debug(f"Retention policy already configured: {e}")

# =============================================================================
# UPDATED MIGRATIONS - ADD LOCATION COLUMN MIGRATION
# =============================================================================

@migration(1)
def _v1_add_core_indexes(conn, table_name: str, engine: Engine):
    """Version 1: Add core performance indexes including location."""
    
    indexes = [
        # Core indexes
        {
            "name": f"ix_{table_name}_entity_time",
            "columns": "entity_id, time DESC",
            "where": None
        },
        {
            "name": f"ix_{table_name}_domain_time",
            "columns": "domain, time DESC",
            "where": None
        },
        {
            "name": f"ix_{table_name}_device_class",
            "columns": "device_class, time DESC",
            "where": "device_class IS NOT NULL"
        },
        {
            "name": f"ix_{table_name}_state_numeric",
            "columns": "entity_id, state_numeric, time DESC",
            "where": "state_numeric IS NOT NULL"
        },
        # Location index for PostGIS queries
        {
            "name": f"ix_{table_name}_location_gist",
            "columns": "location",
            "where": "location IS NOT NULL",
            "using": "GIST"
        },
    ]
    
    for idx in indexes:
        where_clause = f"WHERE {idx['where']}" if idx['where'] else ""
        using_clause = f"USING {idx['using']}" if idx.get('using') else ""
        
        try:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {idx['name']}
                ON {table_name} {using_clause} ({idx['columns']})
                {where_clause}
            """))
            _LOGGER.debug(f"Index {idx['name']} ensured")
        except Exception as e:
            _LOGGER.warning(f"Could not create index {idx['name']}: {e}")

@migration(2)
def _v2_add_jsonb_index(conn, table_name: str, engine: Engine):
    """Version 2: Add GIN index for JSONB attributes."""
    try:
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{table_name}_attributes_gin
            ON {table_name} USING gin(attributes)
            WHERE attributes IS NOT NULL
        """))
        _LOGGER.info("JSONB GIN index added for attributes")
    except Exception as e:
        _LOGGER.warning(f"Could not create JSONB index: {e}")

@migration(3)  
def _v3_optimize_time_indexes(conn, table_name: str, engine: Engine):
    """Version 3: Add covering indexes for common query patterns."""
    try:
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{table_name}_entity_time_covering
            ON {table_name} (entity_id, time DESC)
            INCLUDE (state, state_numeric, friendly_name, unit_of_measurement)
            WHERE state_numeric IS NOT NULL
        """))
        _LOGGER.info("Covering index added for common queries")
    except Exception as e:
        _LOGGER.debug(f"Could not create covering index (PostgreSQL 11+ required): {e}")

@migration(4)
def _v4_ensure_mandatory_location(conn, table_name: str, engine: Engine):
    """Version 4: Ensure location column exists (migration to mandatory location)."""
    try:
        # Check if location column exists
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND column_name = 'location'
        """))
        
        if result.fetchone().count == 0:
            # Add location column
            conn.execute(text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN location GEOMETRY(POINT, 4326)
            """))
            
            # Add spatial index
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS ix_{table_name}_location_gist
                ON {table_name} USING GIST (location)
                WHERE location IS NOT NULL
            """))
            
            _LOGGER.info(f"Added mandatory location column and index to {table_name}")
        else:
            _LOGGER.debug(f"Location column already exists in {table_name}")
            
    except Exception as e:
        _LOGGER.error(f"Failed to ensure location column: {e}")
