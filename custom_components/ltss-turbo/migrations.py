"""Database migrations for LTSS Turbo with metadata table for version tracking."""

import logging
from typing import Optional, Dict, Any, Callable
from datetime import datetime
import time

from sqlalchemy import text, inspect, MetaData, Table, Column, Integer, String, DateTime
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError, OperationalError

_LOGGER = logging.getLogger(__name__)

# Current schema version - increment when adding migrations
SCHEMA_VERSION = 5  # Added performance indexes and metadata improvements

# Migration registry
MIGRATIONS: Dict[int, Callable] = {}

def migration(version: int):
    """Decorator to register a migration function."""
    def decorator(func):
        MIGRATIONS[version] = func
        return func
    return decorator


class MetadataManager:
    """Manager for schema metadata and version tracking."""
    
    def __init__(self, conn, table_name: str):
        self.conn = conn
        self.meta_table = f"{table_name}_meta"
        self._ensure_table()
    
    def _ensure_table(self):
        """Create metadata table if it doesn't exist."""
        try:
            self.conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {self.meta_table} (
                    key VARCHAR(50) PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            
            # Create index for faster lookups
            self.conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS ix_{self.meta_table}_updated 
                ON {self.meta_table}(updated_at DESC)
            """))
            
            _LOGGER.debug(f"Metadata table {self.meta_table} ensured")
        except Exception as e:
            _LOGGER.error(f"Failed to create metadata table: {e}")
            raise
    
    def get(self, key: str, default: Any = None) -> Optional[str]:
        """Get metadata value by key."""
        try:
            result = self.conn.execute(text(f"""
                SELECT value FROM {self.meta_table} WHERE key = :key
            """), {"key": key})
            row = result.fetchone()
            return row.value if row else default
        except Exception as e:
            _LOGGER.debug(f"Could not get metadata for key '{key}': {e}")
            return default
    
    def set(self, key: str, value: Any):
        """Set metadata value."""
        try:
            self.conn.execute(text(f"""
                INSERT INTO {self.meta_table} (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE 
                SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
            """), {"key": key, "value": str(value)})
            
            _LOGGER.debug(f"Metadata set: {key} = {value}")
        except Exception as e:
            _LOGGER.error(f"Failed to set metadata {key}: {e}")
            raise
    
    def get_all(self) -> Dict[str, str]:
        """Get all metadata."""
        try:
            result = self.conn.execute(text(f"""
                SELECT key, value FROM {self.meta_table}
                ORDER BY key
            """))
            return {row.key: row.value for row in result}
        except Exception as e:
            _LOGGER.warning(f"Could not get all metadata: {e}")
            return {}
    
    def get_schema_version(self) -> Optional[int]:
        """Get current schema version."""
        version_str = self.get("schema_version")
        return int(version_str) if version_str else None
    
    def set_schema_version(self, version: int):
        """Set schema version."""
        self.set("schema_version", version)
        self.set(f"migration_v{version}_completed", datetime.utcnow().isoformat())
    
    def record_migration_time(self, version: int, duration_ms: float):
        """Record migration execution time."""
        self.set(f"migration_v{version}_duration_ms", round(duration_ms, 2))


def run_startup_migrations(
    engine: Engine, 
    table_name: str = "ltss_turbo",
    enable_timescale: bool = True,
    enable_compression: bool = True,
    chunk_time_interval: int = 86400000000,
    compression_after: int = 7,
    retention_days: Optional[int] = None
) -> bool:
    """
    Run all necessary migrations with comprehensive version tracking.
    
    Returns True if successful, False otherwise.
    """
    try:
        with engine.connect() as conn:
            # Initialize metadata manager
            metadata_mgr = MetadataManager(conn, table_name)
            
            # Get current schema version
            current_version = metadata_mgr.get_schema_version()
            
            # Record startup info
            metadata_mgr.set("last_startup", datetime.utcnow().isoformat())
            metadata_mgr.set("enable_timescale", str(enable_timescale))
            metadata_mgr.set("enable_compression", str(enable_compression))
            metadata_mgr.set("chunk_time_interval", str(chunk_time_interval))
            metadata_mgr.set("compression_after_days", str(compression_after))
            metadata_mgr.set("retention_days", str(retention_days) if retention_days else "unlimited")
            
            if current_version is None:
                # Fresh install - run initial setup
                _LOGGER.info("Fresh LTSS Turbo installation detected, running initial setup...")
                
                start_time = time.time()
                success = _run_initial_setup(conn, engine, table_name, enable_timescale, 
                                           chunk_time_interval, metadata_mgr)
                duration_ms = (time.time() - start_time) * 1000
                
                if not success:
                    _LOGGER.error("Initial setup failed")
                    return False
                
                metadata_mgr.set_schema_version(0)
                metadata_mgr.record_migration_time(0, duration_ms)
                current_version = 0
                
                _LOGGER.info(f"Initial setup completed in {duration_ms:.1f}ms")
            
            # Run any pending migrations
            migrations_run = 0
            total_migration_time = 0
            
            for version in sorted(MIGRATIONS.keys()):
                if version > current_version:
                    _LOGGER.info(f"Running migration v{version}: {MIGRATIONS[version].__doc__}")
                    
                    start_time = time.time()
                    try:
                        MIGRATIONS[version](conn, table_name, engine, metadata_mgr)
                        duration_ms = (time.time() - start_time) * 1000
                        
                        metadata_mgr.set_schema_version(version)
                        metadata_mgr.record_migration_time(version, duration_ms)
                        
                        migrations_run += 1
                        total_migration_time += duration_ms
                        current_version = version
                        
                        _LOGGER.info(f"Migration v{version} completed in {duration_ms:.1f}ms")
                    except Exception as e:
                        _LOGGER.error(f"Migration v{version} failed: {e}", exc_info=True)
                        metadata_mgr.set(f"migration_v{version}_error", str(e))
                        return False
            
            if migrations_run > 0:
                _LOGGER.info(
                    f"Completed {migrations_run} migrations in {total_migration_time:.1f}ms total, "
                    f"schema now at v{current_version}"
                )
            else:
                _LOGGER.debug(f"Schema up-to-date at v{current_version}, no migrations needed")
            
            # Apply runtime configurations
            if enable_timescale:
                _configure_timescaledb_policies(conn, table_name, enable_compression,
                                              compression_after, retention_days, metadata_mgr)
            
            # Log final metadata state
            all_metadata = metadata_mgr.get_all()
            _LOGGER.debug(f"Metadata state: {all_metadata}")
            
            return True
            
    except Exception as e:
        _LOGGER.error(f"Migration system error: {e}", exc_info=True)
        return False


def _run_initial_setup(
    conn, 
    engine: Engine, 
    table_name: str,
    enable_timescale: bool, 
    chunk_time_interval: int,
    metadata_mgr: 'MetadataManager'
) -> bool:
    """Run initial setup with enhanced error handling and metadata tracking."""
    
    try:
        inspector = inspect(engine)
        
        # Check for available extensions
        extensions = _get_available_extensions(conn)
        metadata_mgr.set("available_extensions", ",".join(sorted(extensions)))
        _LOGGER.info(f"Available extensions: {sorted(extensions)}")

        # TimescaleDB setup (optional but recommended)
        timescaledb_available = False
        if enable_timescale and "timescaledb" in extensions:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE"))
                timescaledb_available = True
                metadata_mgr.set("timescaledb_enabled", "true")
                
                # Get TimescaleDB version
                result = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"))
                tsdb_version = result.fetchone()
                if tsdb_version:
                    metadata_mgr.set("timescaledb_version", tsdb_version.extversion)
                    _LOGGER.info(f"TimescaleDB {tsdb_version.extversion} enabled")
            except Exception as e:
                metadata_mgr.set("timescaledb_enabled", "false")
                metadata_mgr.set("timescaledb_error", str(e))
                _LOGGER.warning(f"Could not enable TimescaleDB: {e}")

        # PostGIS setup - try to enable but make it optional
        postgis_available = False
        if "postgis" in extensions:
            try:
                conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis CASCADE"))
                postgis_available = True
                metadata_mgr.set("postgis_enabled", "true")
                
                # Get PostGIS version
                result = conn.execute(text("SELECT postgis_version()"))
                postgis_version = result.fetchone()
                if postgis_version:
                    metadata_mgr.set("postgis_version", postgis_version[0])
                    _LOGGER.info(f"PostGIS {postgis_version[0]} enabled")
            except Exception as e:
                metadata_mgr.set("postgis_enabled", "false")
                metadata_mgr.set("postgis_error", str(e))
                _LOGGER.warning(f"Could not enable PostGIS: {e}")
        else:
            metadata_mgr.set("postgis_enabled", "false")
            _LOGGER.warning("PostGIS extension not available - location support will be limited")

        # Import model and create table
        try:
            from .models import Base, make_ltss_model
            Model = make_ltss_model(table_name)
            
            # Create table if missing
            if not inspector.has_table(table_name):
                _LOGGER.info(f"Creating table '{table_name}'...")
                
                # Create the table
                Base.metadata.create_all(engine, tables=[Base.metadata.tables[table_name]])

                # Verify table creation
                inspector = inspect(engine)
                if not inspector.has_table(table_name):
                    raise RuntimeError(f"Failed to create table '{table_name}'")
                    
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                metadata_mgr.set("table_columns", ",".join(sorted(columns)))
                _LOGGER.info(f"Table '{table_name}' created with columns: {sorted(columns)}")
                
                # Check if location column exists
                if 'location' in columns:
                    metadata_mgr.set("location_column", "true")
                    _LOGGER.info("Location column created successfully")
                else:
                    metadata_mgr.set("location_column", "false")
                    _LOGGER.warning("Location column not created - PostGIS may not be available")
                    
            else:
                _LOGGER.info(f"Table '{table_name}' already exists")
                
                # For existing tables, check columns
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                metadata_mgr.set("table_columns", ",".join(sorted(columns)))
                
                # Try to add location column if missing and PostGIS is available
                if 'location' not in columns and postgis_available:
                    _LOGGER.info("Adding location column to existing table...")
                    try:
                        conn.execute(text(f"""
                            ALTER TABLE {table_name} 
                            ADD COLUMN location GEOMETRY(POINT, 4326)
                        """))
                        metadata_mgr.set("location_column", "added")
                        _LOGGER.info("Location column added successfully")
                    except Exception as e:
                        metadata_mgr.set("location_column", "failed")
                        _LOGGER.warning(f"Could not add location column: {e}")

        except Exception as e:
            _LOGGER.error(f"Failed to create/update table: {e}")
            metadata_mgr.set("table_creation_error", str(e))
            return False

        # Convert to hypertable if TimescaleDB is available
        if timescaledb_available:
            try:
                # Check if already a hypertable
                result = conn.execute(text(f"""
                    SELECT COUNT(*) as count 
                    FROM timescaledb_information.hypertables 
                    WHERE hypertable_name = '{table_name}'
                """))
                
                if result.fetchone().count == 0:
                    conn.execute(text(f"""
                        SELECT create_hypertable(
                            '{table_name}',
                            'time',
                            chunk_time_interval => INTERVAL '{chunk_time_interval // 1_000_000} seconds',
                            if_not_exists => TRUE,
                            migrate_data => TRUE
                        )
                    """))
                    metadata_mgr.set("hypertable_created", datetime.utcnow().isoformat())
                    _LOGGER.info(f"Hypertable created for '{table_name}'")
                else:
                    metadata_mgr.set("hypertable_status", "already_exists")
                    _LOGGER.info(f"Table '{table_name}' is already a hypertable")
            except Exception as e:
                metadata_mgr.set("hypertable_error", str(e))
                _LOGGER.warning(f"Could not create hypertable: {e}")

        return True
        
    except Exception as e:
        _LOGGER.error(f"Initial setup failed: {e}", exc_info=True)
        metadata_mgr.set("setup_error", str(e))
        return False


def _get_available_extensions(conn) -> set:
    """Get set of available PostgreSQL extensions."""
    try:
        result = conn.execute(text("""
            SELECT name FROM pg_available_extensions
        """))
        extensions = {row.name for row in result}
        return extensions
    except Exception as e:
        _LOGGER.warning(f"Could not query available extensions: {e}")
        return set()


def _configure_timescaledb_policies(
    conn, 
    table_name: str, 
    enable_compression: bool,
    compression_after: int, 
    retention_days: Optional[int],
    metadata_mgr: 'MetadataManager'
):
    """Configure TimescaleDB compression and retention policies."""
    
    # Check if table is a hypertable
    try:
        result = conn.execute(text(f"""
            SELECT COUNT(*) as count 
            FROM timescaledb_information.hypertables 
            WHERE hypertable_name = '{table_name}'
        """))
        if result.fetchone().count == 0:
            _LOGGER.debug(f"Table {table_name} is not a hypertable, skipping TimescaleDB policies")
            metadata_mgr.set("timescaledb_policies", "skipped_not_hypertable")
            return
    except Exception as e:
        _LOGGER.debug(f"Could not check hypertable status: {e}")
        metadata_mgr.set("timescaledb_policies", "error_checking")
        return

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
            
            metadata_mgr.set("compression_enabled", "true")
            metadata_mgr.set("compression_after_days", str(compression_after))
            _LOGGER.info(f"Compression policy configured: compress after {compression_after} days")
        except Exception as e:
            metadata_mgr.set("compression_error", str(e))
            _LOGGER.info(f"Compression policy setup: {e}")
    
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
            metadata_mgr.set("retention_days", str(retention_days))
            _LOGGER.info(f"Retention policy configured: {retention_days} days")
        except Exception as e:
            metadata_mgr.set("retention_error", str(e))
            _LOGGER.info(f"Retention policy setup: {e}")


# =============================================================================
# MIGRATIONS
# =============================================================================

@migration(1)
def _v1_add_core_indexes(conn, table_name: str, engine: Engine, metadata_mgr: 'MetadataManager'):
    """Add core performance indexes."""
    
    indexes_created = []
    indexes_failed = []
    
    # Check if PostGIS is available
    postgis_enabled = metadata_mgr.get("postgis_enabled") == "true"
    
    indexes = [
        # Core indexes for common queries
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
    ]
    
    # Add spatial index only if PostGIS is available
    if postgis_enabled:
        indexes.append({
            "name": f"ix_{table_name}_location_gist",
            "columns": "location",
            "where": "location IS NOT NULL",
            "using": "GIST"
        })
    
    for idx in indexes:
        where_clause = f"WHERE {idx['where']}" if idx['where'] else ""
        using_clause = f"USING {idx['using']}" if idx.get('using') else ""
        
        try:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {idx['name']}
                ON {table_name} {using_clause} ({idx['columns']})
                {where_clause}
            """))
            indexes_created.append(idx['name'])
            _LOGGER.debug(f"Index {idx['name']} ensured")
        except Exception as e:
            indexes_failed.append(idx['name'])
            _LOGGER.warning(f"Could not create index {idx['name']}: {e}")
    
    metadata_mgr.set("indexes_v1_created", ",".join(indexes_created))
    if indexes_failed:
        metadata_mgr.set("indexes_v1_failed", ",".join(indexes_failed))


@migration(2)
def _v2_add_jsonb_index(conn, table_name: str, engine: Engine, metadata_mgr: 'MetadataManager'):
    """Add GIN index for JSONB attributes."""
    try:
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{table_name}_attributes_gin
            ON {table_name} USING gin(attributes)
            WHERE attributes IS NOT NULL
        """))
        metadata_mgr.set("jsonb_index", "created")
        _LOGGER.info("JSONB GIN index added for attributes")
    except Exception as e:
        metadata_mgr.set("jsonb_index", f"failed: {e}")
        _LOGGER.warning(f"Could not create JSONB index: {e}")


@migration(3)  
def _v3_optimize_time_indexes(conn, table_name: str, engine: Engine, metadata_mgr: 'MetadataManager'):
    """Add covering indexes for common query patterns."""
    try:
        # Check PostgreSQL version for covering index support (11+)
        result = conn.execute(text("SELECT version()"))
        pg_version = result.fetchone()[0]
        metadata_mgr.set("pg_version", pg_version)
        
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{table_name}_entity_time_covering
            ON {table_name} (entity_id, time DESC)
            INCLUDE (state, state_numeric, friendly_name, unit_of_measurement)
            WHERE state_numeric IS NOT NULL
        """))
        metadata_mgr.set("covering_index", "created")
        _LOGGER.info("Covering index added for common queries")
    except Exception as e:
        metadata_mgr.set("covering_index", f"failed: {e}")
        _LOGGER.debug(f"Could not create covering index (PostgreSQL 11+ required): {e}")


@migration(4)
def _v4_ensure_location_support(conn, table_name: str, engine: Engine, metadata_mgr: 'MetadataManager'):
    """Ensure location column and spatial index exist."""
    
    def _split_schema_and_table(name: str):
        if "." in name:
            schema, tbl = name.split(".", 1)
            return schema.strip('"'), tbl.strip('"')
        return None, name.strip('"')

    def _qid(name: str) -> str:
        return conn.execute(text("SELECT quote_ident(:n)"), {"n": name}).scalar_one()

    schema, tbl = _split_schema_and_table(table_name)
    q_schema = _qid(schema) + "." if schema else ""
    q_table = _qid(tbl)
    fqtn = f"{q_schema}{q_table}"

    # Check if PostGIS is available
    ext = conn.execute(text("""
        SELECT extname FROM pg_extension WHERE extname = 'postgis'
    """)).fetchone()
    
    if not ext:
        metadata_mgr.set("location_support_v4", "skipped_no_postgis")
        _LOGGER.warning("PostGIS not available, skipping location support migration")
        return

    # Check if column exists
    col_exists = conn.execute(text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = COALESCE(:schema, current_schema())
          AND table_name   = :table
          AND column_name  = 'location'
        LIMIT 1
    """), {"schema": schema, "table": tbl}).fetchone() is not None

    if not col_exists:
        try:
            conn.execute(text(f"""
                ALTER TABLE {fqtn}
                ADD COLUMN location GEOMETRY(POINT, 4326)
            """))
            metadata_mgr.set("location_column_v4", "added")
            _LOGGER.info(f"Added location GEOMETRY(POINT,4326) to {fqtn}")
        except Exception as e:
            metadata_mgr.set("location_column_v4", f"failed: {e}")
            _LOGGER.warning(f"Could not add location column: {e}")
            return

    # Ensure spatial index exists
    idx_base = f"ix_{tbl}_location_gist"
    idx_name = f"{schema}_{idx_base}" if schema else idx_base
    q_idx = _qid(idx_name)

    idx_exists = conn.execute(text("""
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = COALESCE(:schema, current_schema())
          AND tablename  = :table
          AND indexname  = :index
        LIMIT 1
    """), {"schema": schema, "table": tbl, "index": idx_name}).fetchone() is not None

    if not idx_exists:
        try:
            conn.execute(text(f"""
                CREATE INDEX {q_idx}
                ON {fqtn} USING GIST (location)
                WHERE location IS NOT NULL
            """))
            metadata_mgr.set("location_index_v4", "created")
            _LOGGER.info(f"Created GiST index {idx_name} on {fqtn}(location)")
        except Exception as e:
            metadata_mgr.set("location_index_v4", f"failed: {e}")
            _LOGGER.warning(f"Could not create spatial index: {e}")


@migration(5)
def _v5_add_performance_indexes(conn, table_name: str, engine: Engine, metadata_mgr: 'MetadataManager'):
    """Add additional performance indexes for Grafana queries."""
    
    performance_indexes = [
        # Index for availability analysis
        {
            "name": f"ix_{table_name}_availability",
            "columns": "entity_id, time DESC",
            "where": "is_unavailable = true OR is_unknown = true"
        },
        # Index for state changes
        {
            "name": f"ix_{table_name}_state_changes",
            "columns": "entity_id, last_changed DESC",
            "where": "last_changed IS NOT NULL"
        },
        # Index for numeric aggregations by device class
        {
            "name": f"ix_{table_name}_device_numeric",
            "columns": "device_class, time DESC, state_numeric",
            "where": "device_class IS NOT NULL AND state_numeric IS NOT NULL"
        },
        # Composite index for domain filtering with numeric values
        {
            "name": f"ix_{table_name}_domain_numeric",
            "columns": "domain, entity_id, time DESC",
            "where": "state_numeric IS NOT NULL"
        },
    ]
    
    indexes_created = []
    indexes_failed = []
    
    for idx in performance_indexes:
        where_clause = f"WHERE {idx['where']}" if idx['where'] else ""
        
        try:
            conn.execute(text(f"""
                CREATE INDEX IF NOT EXISTS {idx['name']}
                ON {table_name} ({idx['columns']})
                {where_clause}
            """))
            indexes_created.append(idx['name'])
            _LOGGER.debug(f"Performance index {idx['name']} created")
        except Exception as e:
            indexes_failed.append(idx['name'])
            _LOGGER.warning(f"Could not create index {idx['name']}: {e}")
    
    metadata_mgr.set("performance_indexes_v5_created", ",".join(indexes_created))
    if indexes_failed:
        metadata_mgr.set("performance_indexes_v5_failed", ",".join(indexes_failed))
    
    # Analyze table to update statistics for query planner
    try:
        conn.execute(text(f"ANALYZE {table_name}"))
        metadata_mgr.set("analyze_v5", datetime.utcnow().isoformat())
        _LOGGER.info("Table analyzed for query planner optimization")
    except Exception as e:
        _LOGGER.warning(f"Could not analyze table: {e}")