"""Database migrations for LTSS Turbo - Versioned and Idempotent."""

import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from sqlalchemy import text, inspect, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, OperationalError

_LOGGER = logging.getLogger(__name__)

# Current schema version - increment when adding new migrations
SCHEMA_VERSION = 3

# Migration registry
MIGRATIONS: Dict[int, Callable] = {}


def migration(version: int):
    """Decorator to register a migration function."""
    def decorator(func):
        MIGRATIONS[version] = func
        return func
    return decorator


def run_startup_migrations(engine: Engine, table_name: str = "ltss", 
                         enable_timescale: bool = True,
                         enable_compression: bool = True,
                         enable_location: bool = False,
                         chunk_time_interval: int = 86400000000,
                         compression_after: int = 7,
                         retention_days: Optional[int] = None) -> bool:
    """
    Run all necessary migrations on startup.
    
    Returns True if successful, False otherwise.
    Fast path: If schema is up-to-date, this only runs a few SELECTs.
    """
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            
            # Ensure metadata table exists
            _ensure_metadata_table(conn, table_name)
            
            # Get current schema version
            current_version = _get_schema_version(conn, table_name)
            
            if current_version is None:
                # Fresh install - run initial setup
                _LOGGER.info("Fresh LTSS Turbo installation detected, running initial setup...")
                _run_initial_setup(conn, engine, table_name, enable_timescale, 
                                 enable_location, chunk_time_interval)
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
                        # Continue with other migrations
            
            if migrations_run > 0:
                _LOGGER.info(f"Completed {migrations_run} migrations, schema now at v{current_version}")
            else:
                _LOGGER.debug(f"Schema up-to-date at v{current_version}, no migrations needed")
            
            # Apply runtime configurations if TimescaleDB is available
            if enable_timescale:
                _configure_timescaledb_policies(conn, table_name, enable_compression, 
                                              compression_after, retention_days)
            
            return True
            
    except Exception as e:
        _LOGGER.error(f"Migration system error: {e}")
        return False


def _ensure_metadata_table(conn, table_name: str):
    """Create metadata table if it doesn't exist (per-table)."""
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


def _run_initial_setup(conn, engine: Engine, table_name: str, 
                      enable_timescale: bool, enable_location: bool,
                      chunk_time_interval: int):
    """Run initial setup for fresh installation."""
    
    inspector = inspect(engine)
    
    # Check for available extensions
    extensions = _get_available_extensions(conn)
    
    # Enable TimescaleDB if available and requested
    if enable_timescale and "timescaledb" in extensions:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
            _LOGGER.info("TimescaleDB extension enabled")
        except Exception as e:
            _LOGGER.warning(f"Could not enable TimescaleDB: {e}")
    
    # Enable PostGIS if available and requested
    if enable_location and "postgis" in extensions:
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis CASCADE"))
            _LOGGER.info("PostGIS extension enabled")
            # This should trigger the model to add location column
            from .models import LTSS
            LTSS.activate_location_extraction()
        except Exception as e:
            _LOGGER.warning(f"Could not enable PostGIS: {e}")
    
        # Create table if it doesn't exist
        if not inspector.has_table(table_name):
            _LOGGER.info(f"Creating table '{table_name}'...")
            from .models import Base
            Base.metadata.create_all(engine, tables=[Base.metadata.tables[table_name]])
        else:
            _LOGGER.info(f"Table '{table_name}' already exists — skipping creation")
        # Convert to hypertable if TimescaleDB is available
        if enable_timescale and "timescaledb" in extensions:
            try:
                conn.execute(text(f"""
                    SELECT create_hypertable(
                        '{table_name}',
                        'time',
                        chunk_time_interval => INTERVAL '{chunk_time_interval // 1000000} seconds',
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
        return  # TimescaleDB not available or table not found
    
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
            
            # Add or update compression policy
            conn.execute(text(f"""
                SELECT add_compression_policy(
                    '{table_name}',
                    INTERVAL '{compression_after} days',
                    if_not_exists => TRUE
                )
            """))
            
            _LOGGER.debug(f"Compression policy configured: compress after {compression_after} days")
        except Exception as e:
            _LOGGER.debug(f"Compression policy already configured or not available: {e}")
    
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
            _LOGGER.debug(f"Retention policy already configured or not available: {e}")


# ============================================================================
# MIGRATION DEFINITIONS
# ============================================================================

@migration(1)
def _v1_add_core_indexes(conn, table_name: str, engine: Engine):
    """Version 1: Add core performance indexes."""
    
    indexes = [
        # Primary lookup pattern - entity + time
        {
            "name": f"ix_{table_name}_entity_time",
            "columns": "entity_id, time DESC",
            "where": None
        },
        # Domain queries
        {
            "name": f"ix_{table_name}_domain_time",
            "columns": "domain, time DESC",
            "where": None
        },
        # Friendly name for UI queries
        {
            "name": f"ix_{table_name}_friendly_name",
            "columns": "friendly_name",
            "where": "friendly_name IS NOT NULL"
        },
        # Device class queries
        {
            "name": f"ix_{table_name}_device_class",
            "columns": "device_class, time DESC",
            "where": "device_class IS NOT NULL"
        },
        # Numeric state for aggregations
        {
            "name": f"ix_{table_name}_state_numeric",
            "columns": "entity_id, state_numeric, time DESC",
            "where": "state_numeric IS NOT NULL"
        },
        # Quality flags
        {
            "name": f"ix_{table_name}_quality",
            "columns": "is_unavailable, is_unknown, time DESC",
            "where": None
        }
    ]
    
    for idx in indexes:
        where_clause = f"WHERE {idx['where']}" if idx['where'] else ""
        try:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {idx['name']}
                ON {table_name} ({idx['columns']})
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
    
    # Check if this is a hypertable
    is_hypertable = False
    try:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_name = '{table_name}'
        """))
        is_hypertable = result.fetchone().count > 0
    except Exception:
        pass
    
    if not is_hypertable:
        # For non-hypertables, add a covering index for the most common query pattern
        try:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS ix_{table_name}_entity_time_covering
                ON {table_name} (entity_id, time DESC)
                INCLUDE (state, state_numeric, friendly_name, unit_of_measurement)
                WHERE state_numeric IS NOT NULL
            """))
            _LOGGER.info("Covering index added for common queries")
        except Exception as e:
            # PostgreSQL < 11 doesn't support INCLUDE
            _LOGGER.debug(f"Could not create covering index (may need PostgreSQL 11+): {e}")


# Future migrations can be added here:
# @migration(4)
# def _v4_your_migration(conn, table_name: str, engine: Engine):
#     """Version 4: Description of changes."""
#     pass

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def add_column_if_not_exists(conn, table_name: str, column_name: str, 
                            column_type: str, default: Optional[str] = None):
    """Helper to add a column if it doesn't exist."""
    try:
        # Check if column exists
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            AND column_name = '{column_name}'
        """))
        
        if result.fetchone().count == 0:
            default_clause = f"DEFAULT {default}" if default else ""
            conn.execute(text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN {column_name} {column_type} {default_clause}
            """))
            _LOGGER.info(f"Added column {column_name} to {table_name}")
            return True
    except Exception as e:
        _LOGGER.warning(f"Could not add column {column_name}: {e}")
    return False


def create_index_concurrently(conn, table_name: str, index_name: str,
                            columns: str, where_clause: Optional[str] = None):
    """
    Create an index concurrently (non-blocking).
    Note: Cannot be run in a transaction.
    """
    where = f"WHERE {where_clause}" if where_clause else ""
    try:
        conn.execute(text(f"""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
            ON {table_name} ({columns})
            {where}
        """))
        _LOGGER.info(f"Index {index_name} created concurrently")
        return True
    except Exception as e:
        _LOGGER.warning(f"Could not create index {index_name} concurrently: {e}")
        return False


def get_table_stats(conn, table_name: str) -> Dict[str, Any]:
    """Get statistics about the table for monitoring."""
    stats = {}
    
    try:
        # Basic stats
        result = conn.execute(text(f"""
            SELECT 
                pg_size_pretty(pg_total_relation_size('{table_name}')) as total_size,
                pg_size_pretty(pg_relation_size('{table_name}')) as table_size,
                pg_size_pretty(pg_indexes_size('{table_name}')) as indexes_size,
                COUNT(*) as row_count,
                MIN(time) as oldest_record,
                MAX(time) as newest_record
            FROM {table_name}
        """))
        row = result.fetchone()
        stats.update({
            "total_size": row.total_size,
            "table_size": row.table_size, 
            "indexes_size": row.indexes_size,
            "row_count": row.row_count,
            "oldest_record": row.oldest_record,
            "newest_record": row.newest_record
        })
        
        # TimescaleDB stats if available
        try:
            result = conn.execute(text(f"""
                SELECT 
                    num_chunks,
                    compression_enabled,
                    retention_period
                FROM timescaledb_information.hypertables
                WHERE hypertable_name = '{table_name}'
            """))
            row = result.fetchone()
            if row:
                stats.update({
                    "chunks": row.num_chunks,
                    "compression_enabled": row.compression_enabled,
                    "retention_period": row.retention_period
                })
        except Exception:
            pass  # TimescaleDB not available
            
    except Exception as e:
        _LOGGER.warning(f"Could not get table stats: {e}")
    
    return stats