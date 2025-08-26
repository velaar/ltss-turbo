"""Long Time State Storage Turbo - Optimized for TimescaleDB and Grafana."""

# Standard library
import asyncio
import concurrent.futures
import struct
import io
import json
import logging
import queue
import threading
import time
import csv
from collections import OrderedDict
from typing import Any, Callable, Iterable, List, Optional, Dict
from datetime import datetime

# Third-party libraries
import voluptuous as vol
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool

# Home Assistant core
from homeassistant.components import persistent_notification
from homeassistant.const import (
    ATTR_ENTITY_ID,
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
    STATE_UNKNOWN,
)
from homeassistant.core import CoreState, HomeAssistant, callback
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entityfilter import (
    INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA,
    convert_include_exclude_filter,
)
from homeassistant.helpers.typing import ConfigType
import homeassistant.util.dt as dt_util
from homeassistant.helpers.json import JSONEncoder

# Local packages
from .migrations import run_startup_migrations
from .models import make_ltss_model
#from .services import setup_services

_LOGGER = logging.getLogger(__name__)

DOMAIN = "ltss_turbo"

# Configuration constants
CONF_DB_URL = "db_url"
CONF_CHUNK_TIME_INTERVAL = "chunk_time_interval"
CONF_BATCH_SIZE = "batch_size"
CONF_BATCH_TIMEOUT_MS = "batch_timeout_ms"
CONF_POLL_INTERVAL_MS = "poll_interval_ms"
CONF_COMPRESSION_AFTER = "compression_after"
CONF_RETENTION_DAYS = "retention_days"
CONF_POOL_SIZE = "pool_size"
CONF_MAX_OVERFLOW = "max_overflow"
CONF_ENABLE_COMPRESSION = "enable_compression"
CONF_TABLE_NAME = "table_name"

CONNECT_RETRY_WAIT = 3
DEFAULT_CHUNK_INTERVAL = 86400000000  # 1 day in microseconds (optimal for compression)
DEFAULT_BATCH_SIZE = 500  # Larger batches for better throughput
DEFAULT_BATCH_TIMEOUT_MS = 1000
DEFAULT_POLL_INTERVAL_MS = 100
DEFAULT_COMPRESSION_AFTER = 7  # Compress chunks older than 7 days by default
DEFAULT_POOL_SIZE = 5
DEFAULT_MAX_OVERFLOW = 10
DEFAULT_TABLE_NAME = "ltss_turbo"

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA.extend(
            {
                vol.Required(CONF_DB_URL): cv.string,
                vol.Optional(
                    CONF_CHUNK_TIME_INTERVAL, default=DEFAULT_CHUNK_INTERVAL
                ): cv.positive_int,
                vol.Optional(CONF_BATCH_SIZE, default=DEFAULT_BATCH_SIZE): vol.Range(
                    min=10, max=5000
                ),
                vol.Optional(
                    CONF_BATCH_TIMEOUT_MS, default=DEFAULT_BATCH_TIMEOUT_MS
                ): vol.Range(min=100, max=10000),
                vol.Optional(
                    CONF_POLL_INTERVAL_MS, default=DEFAULT_POLL_INTERVAL_MS
                ): vol.Range(min=10, max=1000),
                vol.Optional(
                    CONF_COMPRESSION_AFTER, default=DEFAULT_COMPRESSION_AFTER
                ): vol.Range(min=1, max=365),
                vol.Optional(CONF_RETENTION_DAYS): vol.Range(min=1, max=36500),
                vol.Optional(CONF_POOL_SIZE, default=DEFAULT_POOL_SIZE): vol.Range(
                    min=1, max=20
                ),
                vol.Optional(
                    CONF_MAX_OVERFLOW, default=DEFAULT_MAX_OVERFLOW
                ): vol.Range(min=0, max=50),
                vol.Optional(CONF_ENABLE_COMPRESSION, default=True): cv.boolean,
                vol.Optional(CONF_TABLE_NAME, default=DEFAULT_TABLE_NAME): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up LTSS Turbo component."""
    conf = config[DOMAIN]

    db_url = conf.get(CONF_DB_URL)
    chunk_time_interval = conf.get(CONF_CHUNK_TIME_INTERVAL)
    batch_size = conf.get(CONF_BATCH_SIZE)
    batch_timeout_ms = conf.get(CONF_BATCH_TIMEOUT_MS)
    poll_interval_ms = conf.get(CONF_POLL_INTERVAL_MS)
    compression_after = conf.get(CONF_COMPRESSION_AFTER)
    retention_days = conf.get(CONF_RETENTION_DAYS)
    pool_size = conf.get(CONF_POOL_SIZE)
    max_overflow = conf.get(CONF_MAX_OVERFLOW)
    enable_compression = conf.get(CONF_ENABLE_COMPRESSION)
    table_name = conf.get(CONF_TABLE_NAME)
    entity_filter = convert_include_exclude_filter(conf)

    instance = LTSS_DB(
        hass=hass,
        uri=db_url,
        chunk_time_interval=chunk_time_interval,
        entity_filter=entity_filter,
        batch_size=batch_size,
        batch_timeout_ms=batch_timeout_ms,
        poll_interval_ms=poll_interval_ms,
        compression_after=compression_after,
        retention_days=retention_days,
        pool_size=pool_size,
        max_overflow=max_overflow,
        enable_compression=enable_compression,
        table_name=table_name,
    )

    instance.async_initialize()
    instance.start()

    # Wait for database to be ready
    db_ready = await instance.async_db_ready
    
    # Set up services if database is ready
    if db_ready:
        #setup_services(hass, instance)
        _LOGGER.info("LTSS Turbo services registered")
    
    return db_ready


class LRUStringCache:
    """LRU cache for strings with better memory management."""
    
    def __init__(self, max_size: int = 10000):
        self._cache = OrderedDict()
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def get(self, key: str) -> Optional[bytes]:
        """Get cached string, updating LRU order."""
        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            return None
    
    def put(self, key: str, value: bytes):
        """Store string in cache with LRU eviction."""
        with self._lock:
            if key in self._cache:
                # Update and move to end
                self._cache[key] = value
                self._cache.move_to_end(key)
            else:
                # Add new entry
                if len(self._cache) >= self._max_size:
                    # Remove least recently used
                    self._cache.popitem(last=False)
                self._cache[key] = value
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(hit_rate, 2),
                "size": len(self._cache),
                "max_size": self._max_size
            }


class BinaryRowEncoder:
    """High-performance binary encoder for PostgreSQL COPY operations."""
    
    def __init__(self):
        # Pre-compiled format strings for common types
        self._header_format = struct.Struct('>11sii')  # signature, flags, header extension
        self._int16_format = struct.Struct('>h')
        self._int32_format = struct.Struct('>i')
        self._int64_format = struct.Struct('>q') 
        self._float64_format = struct.Struct('>d')
        self._bool_format = struct.Struct('>?')
        
        # LRU String cache for better memory management
        self._string_cache = LRUStringCache(max_size=10000)
        
        # Pre-encode common strings
        common_strings = [
            "on", "off", "unavailable", "unknown", "true", "false",
            "°C", "°F", "%", "W", "kWh", "sensor", "binary_sensor",
            "switch", "light", "climate", "cover", "device_tracker"
        ]
        for s in common_strings:
            self._string_cache.put(s, s.encode('utf-8'))
    
    def _encode_string(self, value: str) -> bytes:
        """Encode string with LRU caching."""
        if value is None:
            return b''
        
        # Check cache first
        cached = self._string_cache.get(value)
        if cached is not None:
            return cached
        
        # Encode and cache
        encoded = value.encode('utf-8')
        if len(value) <= 100:  # Only cache reasonable sized strings
            self._string_cache.put(value, encoded)
        
        return encoded
    
    def _write_field(self, buffer: io.BytesIO, value: Any, field_type: str):
        """Write a single field in binary format."""
        if value is None:
            # NULL field
            buffer.write(self._int32_format.pack(-1))
            return
        
        if field_type == 'text':
            data = self._encode_string(str(value))
            buffer.write(self._int32_format.pack(len(data)))
            buffer.write(data)
            
        elif field_type == 'timestamptz':
            # PostgreSQL timestamp: microseconds since 2000-01-01
            if hasattr(value, 'timestamp'):
                # Optimized conversion
                pg_epoch_diff = 946684800  # Seconds between 1970 and 2000
                pg_timestamp = int((value.timestamp() - pg_epoch_diff) * 1_000_000)
                buffer.write(self._int32_format.pack(8))  # 8 bytes
                buffer.write(self._int64_format.pack(pg_timestamp))
            else:
                buffer.write(self._int32_format.pack(-1))  # NULL
                
        elif field_type == 'jsonb':
            if value:
                # JSONB format: version byte + JSON data
                json_str = json.dumps(value, separators=(',', ':'), ensure_ascii=False, default=str)
                json_data = self._encode_string(json_str)
                buffer.write(self._int32_format.pack(len(json_data) + 1))
                buffer.write(b'\x01')  # JSONB version
                buffer.write(json_data)
            else:
                buffer.write(self._int32_format.pack(-1))  # NULL
                
        elif field_type == 'float8':
            if value is not None:
                buffer.write(self._int32_format.pack(8))  # 8 bytes
                buffer.write(self._float64_format.pack(float(value)))
            else:
                buffer.write(self._int32_format.pack(-1))  # NULL
                
        elif field_type == 'bool':
            buffer.write(self._int32_format.pack(1))  # 1 byte
            buffer.write(self._bool_format.pack(bool(value)))
            
        elif field_type == 'geometry':
            if value:
                ewkb = self._encode_point_ewkb(str(value))
                buffer.write(self._int32_format.pack(len(ewkb)))
                buffer.write(ewkb)
            else:
                buffer.write(self._int32_format.pack(-1))

    def _encode_point_ewkb(self, value: str) -> bytes:
        """
        Optimized EWKB encoding for POINT geometry.
        Accepts 'SRID=4326;POINT(lon lat)' or 'POINT(lon lat)'
        """
        srid = 4326
        txt = value.strip()

        # Split optional SRID
        if txt.upper().startswith("SRID="):
            head, _, rest = txt.partition(";")
            try:
                srid = int(head.split("=", 1)[1])
            except Exception:
                srid = 4326
            wkt = rest
        else:
            wkt = txt

        # Extract lon/lat from WKT
        start = wkt.find("(")
        end = wkt.find(")", start + 1)
        if start == -1 or end == -1:
            raise ValueError(f"Invalid WKT for POINT: {value}")
        
        parts = wkt[start + 1 : end].strip().split()
        if len(parts) != 2:
            raise ValueError(f"Expected 'lon lat' in POINT: {value}")

        lon = float(parts[0])
        lat = float(parts[1])

        # EWKB (little-endian) - pre-computed constants
        WKB_POINT_WITH_SRID = 0x20000001  # Point type with SRID flag
        
        return struct.pack(
            "<BIIdd",  # Single pack for efficiency
            1,  # Little endian
            WKB_POINT_WITH_SRID,
            srid,
            lon,
            lat
        )

    def encode_rows(self, rows: List[Any]) -> bytes:
        """Encode multiple rows in binary format with better memory management."""
        if not rows:
            return b''
        
        # Pre-allocate buffer with estimated size to avoid resizing
        estimated_size = len(rows) * 200  # Estimate ~200 bytes per row
        buffer = io.BytesIO()
        buffer.seek(0)
        
        # PostgreSQL binary format header
        buffer.write(self._header_format.pack(b'PGCOPY\n\xff\r\n\x00', 0, 0))
        
        # Field definitions (column types in order) 
        field_types = [
            'timestamptz',  # time
            'text',         # entity_id
            'text',         # state
            'jsonb',        # attributes
            'text',         # friendly_name
            'text',         # unit_of_measurement
            'text',         # device_class
            'text',         # icon
            'text',         # domain
            'float8',       # state_numeric
            'timestamptz',  # last_changed
            'timestamptz',  # last_updated
            'bool',         # is_unavailable
            'bool',         # is_unknown
            'geometry',     # location
        ]
        
        column_count = 15  # Fixed count
        
        # Encode each row
        for row in rows:
            # Field count
            buffer.write(self._int16_format.pack(column_count))
            
            # Field values - using getattr with defaults
            row_values = [
                getattr(row, "time", None),
                getattr(row, "entity_id", None),
                getattr(row, "state", None),
                getattr(row, "attributes", None),
                getattr(row, "friendly_name", None),
                getattr(row, "unit_of_measurement", None),
                getattr(row, "device_class", None),
                getattr(row, "icon", None),
                getattr(row, "domain", None),
                getattr(row, "state_numeric", None),
                getattr(row, "last_changed", None),
                getattr(row, "last_updated", None),
                getattr(row, "is_unavailable", False),
                getattr(row, "is_unknown", False),
                getattr(row, "location", None),
            ]
            
            # Write each field
            for value, field_type in zip(row_values, field_types):
                self._write_field(buffer, value, field_type)
        
        # EOF marker
        buffer.write(self._int16_format.pack(-1))
        
        return buffer.getvalue()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get encoder cache statistics."""
        return self._string_cache.get_stats()


class ConnectionManager:
    """Enhanced connection management with better pgbouncer support."""
    
    def __init__(self, engine, is_pgbouncer: bool = False):
        self.engine = engine
        self.is_pgbouncer = is_pgbouncer
        self._copy_connection = None
        self._connection_lock = threading.RLock()
        
        # Statistics
        self.stats = {
            "copy_operations": 0,
            "connection_reuses": 0,
            "connection_failures": 0,
            "last_operation": None,
        }
    
    def get_copy_connection(self):
        """Get or create connection for COPY operations."""
        with self._connection_lock:
            # Reuse existing connection if still valid
            if self._copy_connection and not self._copy_connection.closed:
                self.stats["connection_reuses"] += 1
                return self._copy_connection
            
            # Create new connection
            try:
                self._copy_connection = self.engine.raw_connection()
                
                # Only set session parameters for direct connections
                if not self.is_pgbouncer:
                    try:
                        with self._copy_connection.cursor() as cur:
                            cur.execute("SET synchronous_commit = OFF")
                            cur.execute("SET statement_timeout = 30000")
                            cur.execute("SET work_mem = '64MB'")  # Help with sorting
                    except Exception as e:
                        # Ensure we clear any aborted txn before reuse
                        try:
                            self._copy_connection.rollback()
                        except Exception:
                            pass
                        _LOGGER.debug(f"Connection tuning skipped: {e}")
                        
                return self._copy_connection
                
            except Exception as e:
                self.stats["connection_failures"] += 1
                _LOGGER.error(f"Failed to create COPY connection: {e}")
                raise
    
    def close_copy_connection(self):
        """Close the COPY connection."""
        with self._connection_lock:
            if self._copy_connection and not self._copy_connection.closed:
                self._copy_connection.close()
                self._copy_connection = None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics."""
        with self._connection_lock:
            return self.stats.copy()


class LTSS_DB(threading.Thread):
    """Optimized threaded LTSS database handler."""

    def __init__(
        self,
        hass: HomeAssistant,
        uri: str,
        chunk_time_interval: int,
        entity_filter: Callable[[str], bool],
        batch_size: int = DEFAULT_BATCH_SIZE,
        batch_timeout_ms: int = DEFAULT_BATCH_TIMEOUT_MS,
        poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS,
        compression_after: int = DEFAULT_COMPRESSION_AFTER,
        retention_days: Optional[int] = None,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        enable_compression: bool = True,
        table_name: str = DEFAULT_TABLE_NAME,
    ) -> None:
        """Initialize the LTSS database handler."""
        threading.Thread.__init__(self, name="LTSS-Turbo")

        self.hass = hass
        self.queue: queue.Queue = queue.Queue(maxsize=batch_size * 10)
        self.recording_start = dt_util.utcnow()
        self.db_url = uri
        self.table_name = table_name
        self.chunk_time_interval = chunk_time_interval
        self.async_db_ready = asyncio.Future()
        self.engine: Optional[Engine] = None
        self._conn_manager = None
        self._binary_encoder = None

        # Configuration
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.poll_interval_ms = poll_interval_ms
        self.compression_after = compression_after
        self.retention_days = retention_days
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.enable_compression = enable_compression

        self.entity_filter = entity_filter
        self.get_session = None
        
        # Statistics with more detail
        self.stats = {
            "events_processed": 0,
            "events_dropped": 0,
            "batches_processed": 0,
            "last_batch_time": None,
            "avg_batch_size": 0,
            "total_processing_time_ms": 0,
        }

        self.LTSS = make_ltss_model(self.table_name)

    @callback
    def async_initialize(self):
        """Initialize the LTSS event listener."""
        self.hass.bus.async_listen(EVENT_STATE_CHANGED, self.event_listener)

    def run(self):
        """Start processing events to save."""
        tries = 1
        connected = False

        while not connected and tries <= 10:
            if tries != 1:
                time.sleep(CONNECT_RETRY_WAIT)
            try:
                self._setup_connection()
                connected = True
                _LOGGER.info("Connected to LTSS database (attempt %d)", tries)
            except Exception as err:
                _LOGGER.error(
                    "Connection setup failed: %s (retry %d/10 in %ds)",
                    err,
                    tries,
                    CONNECT_RETRY_WAIT,
                )
                tries += 1

        if not connected:
            @callback
            def connection_failed():
                """Handle connection failure."""
                self.async_db_ready.set_result(False)
                persistent_notification.async_create(
                    self.hass,
                    "LTSS Turbo could not connect to database. Please check configuration and logs.",
                    "LTSS Turbo Error",
                    "ltss_connection_error",
                )

            self.hass.add_job(connection_failed)
            return

        shutdown_task = object()
        hass_started = concurrent.futures.Future()

        @callback
        def register():
            """Register shutdown handler after connection success."""
            self.async_db_ready.set_result(True)

            def shutdown(event):
                """Shut down the LTSS handler."""
                if not hass_started.done():
                    hass_started.set_result(shutdown_task)
                self.queue.put(None)
                self.join()

            self.hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, shutdown)

            if self.hass.state == CoreState.running:
                hass_started.set_result(None)
            else:
                @callback
                def notify_hass_started(event):
                    """Notify that Home Assistant has started."""
                    hass_started.set_result(None)

                self.hass.bus.async_listen_once(
                    EVENT_HOMEASSISTANT_START, notify_hass_started
                )

        self.hass.add_job(register)
        result = hass_started.result()

        if result is shutdown_task:
            return

        _LOGGER.info(
            "LTSS Turbo started (batch=%d, timeout=%dms, compression=%s)",
            self.batch_size,
            self.batch_timeout_ms,
            "enabled" if self.enable_compression else "disabled",
        )

        # Main processing loop
        while True:
            batch = self._collect_batch()

            if not batch:
                continue

            # Check for shutdown signal
            if None in batch:
                actual_events = [event for event in batch if event is not None]
                if actual_events:
                    _LOGGER.info("Processing final batch of %d events", len(actual_events))
                    self._process_batch(actual_events)

                self._close_connection()
                self._log_final_stats()
                return

            # Process normal batch
            self._process_batch(batch)

    def _collect_batch(self) -> List:
        """Collect events into batch with timeout."""
        batch = []
        batch_start_time = None
        poll_timeout = self.poll_interval_ms / 1000.0

        while len(batch) < self.batch_size:
            try:
                # Try to get event with timeout
                event = self.queue.get(timeout=poll_timeout)

                if event is None:  # Shutdown signal
                    batch.append(None)
                    self.queue.task_done()
                    return batch

                if not batch_start_time:
                    batch_start_time = time.time() * 1000

                batch.append(event)

            except queue.Empty:
                # Check timeout
                if batch and batch_start_time:
                    elapsed = (time.time() * 1000) - batch_start_time
                    if elapsed >= self.batch_timeout_ms:
                        break

        return batch

    def _process_batch(self, batch: List) -> None:
        """Batch processing using binary COPY format."""
        if not batch:
            return

        start_time = time.time()
        
        # Pre-allocate list for better memory performance
        rows_data = [None] * len(batch)
        valid_rows = 0
        
        # Convert events to LTSS records in single pass
        for i, event in enumerate(batch):
            try:
                row = self.LTSS.from_event(event)
                if row:
                    rows_data[valid_rows] = row
                    valid_rows += 1
            except Exception as e:
                _LOGGER.warning("Failed to process event: %s", e)
                self.stats["events_dropped"] += 1

        if valid_rows == 0:
            # Mark all events as processed even if no data was inserted
            for _ in batch:
                self.queue.task_done()
            return
            
        # Trim the list to actual valid rows
        rows_data = rows_data[:valid_rows]

        # Use binary COPY for bulk insert
        tries = 1
        inserted = False
        
        while not inserted and tries <= 3:
            try:
                self._bulk_insert(rows_data)
                inserted = True
                
                # Update statistics
                processing_time = (time.time() - start_time) * 1000
                self._update_stats(len(batch), valid_rows, processing_time)
                
                _LOGGER.debug(
                    "Binary batch processed: %d events -> %d rows in %.2fms (%.1f rows/sec)",
                    len(batch),
                    valid_rows,
                    processing_time,
                    valid_rows / (processing_time / 1000) if processing_time > 0 else 0,
                )
                
            except Exception as e:
                _LOGGER.error("Binary batch insert failed (attempt %d/3): %s", tries, e)
                tries += 1
                if tries <= 3:
                    time.sleep(1)

        if not inserted:
            _LOGGER.error("Failed to insert batch after 3 attempts, dropping %d events", len(batch))
            self.stats["events_dropped"] += len(batch)

        # Mark all events as processed
        for _ in batch:
            self.queue.task_done()

    def _bulk_insert(self, rows: List[Any]) -> None:
        """
        High-performance binary COPY implementation.
        """
        if not rows:
            return
        
        try:
            binary_data = self._binary_encoder.encode_rows(rows)
            conn = self._conn_manager.get_copy_connection()
            columns = [
                "time", "entity_id", "state", "attributes",
                "friendly_name", "unit_of_measurement", "device_class", "icon",
                "domain", "state_numeric", "last_changed", "last_updated",
                "is_unavailable", "is_unknown", "location"
            ]
            cols_sql = ", ".join(columns)
            copy_sql = f"COPY {self.table_name} ({cols_sql}) FROM STDIN WITH (FORMAT binary)"
            
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, io.BytesIO(binary_data))
            conn.commit()
            
            self._conn_manager.stats["copy_operations"] += 1
            self._conn_manager.stats["last_operation"] = time.time()
            
        except Exception as e:
            _LOGGER.error("Binary COPY failed: %s -- falling back to text COPY", e)
            try:
                self._bulk_insert_text(rows)
            except Exception as e2:
                _LOGGER.error("Text COPY also failed: %s", e2)
                try:
                    conn.rollback()
                except Exception:
                    pass
                finally:
                    self._conn_manager.close_copy_connection()
                raise

    def _bulk_insert_text(self, rows):
        """Fallback text-based COPY for compatibility."""
        if not rows:
            return
            
        conn = self._conn_manager.get_copy_connection()
        cols = [
            "time","entity_id","state","attributes","friendly_name","unit_of_measurement",
            "device_class","icon","domain","state_numeric","last_changed","last_updated",
            "is_unavailable","is_unknown","location"
        ]
        cols_sql = ", ".join(cols)
        copy_sql = f"COPY {self.table_name} ({cols_sql}) FROM STDIN WITH (FORMAT csv, HEADER false)"
        csv_payload = self._rows_to_csv_lines(rows)
        
        with conn.cursor() as cur:
            cur.copy_expert(copy_sql, io.StringIO(csv_payload))
        conn.commit()

    def _rows_to_csv_lines(self, rows):
        """Generate CSV lines for text COPY."""
        buf = io.StringIO()
        w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
        
        for r in rows:
            line = [
                getattr(r, "time", None),
                getattr(r, "entity_id", None),
                getattr(r, "state", None),
                json.dumps(getattr(r, "attributes", None), separators=(",", ":"), ensure_ascii=False) 
                    if getattr(r, "attributes", None) is not None else None,
                getattr(r, "friendly_name", None),
                getattr(r, "unit_of_measurement", None),
                getattr(r, "device_class", None),
                getattr(r, "icon", None),
                getattr(r, "domain", None),
                getattr(r, "state_numeric", None),
                getattr(r, "last_changed", None),
                getattr(r, "last_updated", None),
                bool(getattr(r, "is_unavailable", False)),
                bool(getattr(r, "is_unknown", False)),
                getattr(r, "location", None),
            ]
            w.writerow(['' if v is None else v for v in line])
            
        return buf.getvalue()

    @callback
    def event_listener(self, event):
        """Listen for state changes and queue them for processing."""
        entity_id = event.data.get(ATTR_ENTITY_ID)
        state = event.data.get("new_state")

        if entity_id and state and state.state != STATE_UNKNOWN:
            if self.entity_filter(entity_id):
                try:
                    self.queue.put_nowait(event)
                except queue.Full:
                    _LOGGER.warning(
                        "Event queue full, dropping event for %s. Consider increasing batch_size.",
                        entity_id
                    )
                    self.stats["events_dropped"] += 1

    def _detect_pgbouncer(self, db_url: str) -> bool:
        """Detect if we're connecting through pgbouncer."""
        # Common pgbouncer ports
        pgbouncer_ports = ['6432', '5433']
        
        # Check for common pgbouncer ports
        for port in pgbouncer_ports:
            if f':{port}' in db_url:
                return True
                
        # Check for common pgbouncer hostnames
        pgbouncer_patterns = ['pgbouncer', 'bouncer', 'pool']
        for pattern in pgbouncer_patterns:
            if pattern in db_url.lower():
                return True
                
        return False

    def _get_connect_args(self) -> dict:
        """Get connection arguments optimized for the connection type."""
        is_pgbouncer = self._detect_pgbouncer(self.db_url)
        
        base_args = {
            "connect_timeout": 10,
            "application_name": "ltss_turbo",
        }
        
        if not is_pgbouncer:
            # Direct PostgreSQL connection - we can use startup parameters
            base_args["options"] = "-c statement_timeout=30000"
            _LOGGER.debug("Using direct PostgreSQL connection args")
        else:
            # pgbouncer connection - avoid startup parameters
            _LOGGER.info("Detected pgbouncer connection, using compatible connection args")
            
        return base_args

    def _setup_connection(self):
        """Set up database connection and initialize components."""
        if self.engine is not None:
            self.engine.dispose()

        # Get appropriate connection arguments
        connect_args = self._get_connect_args()
        is_pgbouncer = self._detect_pgbouncer(self.db_url)

        # Optimized connection settings
        self.engine = create_engine(
            self.db_url,
            echo=False,
            poolclass=QueuePool,
            
            # Adjust pool size based on connection type
            pool_size=2 if is_pgbouncer else self.pool_size,
            max_overflow=5 if is_pgbouncer else self.max_overflow,
            pool_pre_ping=not is_pgbouncer,  # pgbouncer handles health checks
            pool_recycle=-1 if is_pgbouncer else 3600,  # Recycle direct connections hourly
            
            connect_args=connect_args,
            json_serializer=lambda obj: json.dumps(obj, cls=JSONEncoder, separators=(',', ':')),
        )

        # Initialize connection manager
        self._conn_manager = ConnectionManager(self.engine, is_pgbouncer)
        
        # Initialize binary encoder
        self._binary_encoder = BinaryRowEncoder()

        # Run migrations
        migrations_ok = run_startup_migrations(
            self.engine,
            self.table_name,
            enable_timescale=True,
            enable_compression=self.enable_compression,
            chunk_time_interval=self.chunk_time_interval,
            compression_after=self.compression_after,
            retention_days=self.retention_days
        )
        
        if not migrations_ok:
            raise Exception("Failed to run database migrations")
        
        # Set up session factory
        self.get_session = scoped_session(sessionmaker(bind=self.engine))
        
        _LOGGER.info(
            "LTSS Turbo connection initialized: table=%s, compression=%s, pgbouncer_mode=%s",
            self.table_name,
            "enabled" if self.enable_compression else "disabled",
            "detected" if is_pgbouncer else "direct"
        )

    def _close_connection(self):
        """Close all database connections."""
        if hasattr(self, '_conn_manager'):
            self._conn_manager.close_copy_connection()
            _LOGGER.info("Connection manager stats: %s", self._conn_manager.get_stats())
            
        if self.get_session:
            self.get_session.remove()
            self.get_session = None
            
        if self.engine:
            self.engine.dispose()
            self.engine = None
            
        _LOGGER.info("Database connections closed")

    def _update_stats(self, batch_size: int, valid_rows: int, processing_time_ms: float):
        """Update processing statistics."""
        self.stats["events_processed"] += valid_rows
        self.stats["batches_processed"] += 1
        self.stats["last_batch_time"] = time.time()
        self.stats["total_processing_time_ms"] += processing_time_ms
        
        # Calculate running average
        if self.stats["batches_processed"] > 0:
            self.stats["avg_batch_size"] = (
                self.stats["events_processed"] / self.stats["batches_processed"]
            )

    def _log_final_stats(self):
        """Log comprehensive statistics on shutdown."""
        # Get cache stats from encoder
        encoder_stats = self._binary_encoder.get_cache_stats() if self._binary_encoder else {}
        
        # Get model performance stats
        model_stats = self.LTSS.get_performance_stats() if hasattr(self.LTSS, 'get_performance_stats') else {}
        
        final_stats = {
            "processing": self.stats,
            "encoder_cache": encoder_stats,
            "model_caches": model_stats,
            "connection": self._conn_manager.get_stats() if self._conn_manager else {},
        }
        
        _LOGGER.info(
            "LTSS Turbo shutdown complete. Final statistics:\n%s",
            json.dumps(final_stats, indent=2)
        )