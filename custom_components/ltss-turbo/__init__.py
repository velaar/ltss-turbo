"""Long Time State Storage Turbo - Optimized for TimescaleDB and Grafana."""

import asyncio
import concurrent.futures
from contextlib import contextmanager
from datetime import datetime, timedelta
import logging
import queue
import threading
import time
import json
import csv
from typing import Any, Dict, Optional, Callable, List
from io import StringIO, BytesIO

import voluptuous as vol
from sqlalchemy import exc, create_engine, inspect, text, pool
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool

import psycopg2
from psycopg2.extras import execute_values

from .models import Base, make_ltss_model
from .migrations import run_startup_migrations

from homeassistant.const import (
    ATTR_ENTITY_ID,
    CONF_DOMAINS,
    CONF_ENTITIES,
    CONF_EXCLUDE,
    CONF_INCLUDE,
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
    STATE_UNKNOWN,
)
from homeassistant.components import persistent_notification
from homeassistant.core import CoreState, HomeAssistant, callback
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entityfilter import (
    convert_include_exclude_filter,
    INCLUDE_EXCLUDE_BASE_FILTER_SCHEMA,
)
from homeassistant.helpers.typing import ConfigType
import homeassistant.util.dt as dt_util
from homeassistant.helpers.json import JSONEncoder

#from .services import setup_services

_LOGGER = logging.getLogger(__name__)

DOMAIN = "ltss"

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
CONF_ENABLE_LOCATION = "enable_location"
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
                vol.Optional(CONF_ENABLE_LOCATION, default=False): cv.boolean,
                vol.Optional(CONF_TABLE_NAME, default=DEFAULT_TABLE_NAME): cv.string,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass: HomeAssistant, config: ConfigType) -> bool:
    """Set up LTSS Turbo."""
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
    enable_location = conf.get(CONF_ENABLE_LOCATION)
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
        enable_location=enable_location,
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
        enable_location: bool = False,
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
        
        # Reusable connection for COPY operations
        self._copy_connection = None

        # Configuration
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.poll_interval_ms = poll_interval_ms
        self.compression_after = compression_after
        self.retention_days = retention_days
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.enable_compression = enable_compression
        self.enable_location = enable_location

        self.entity_filter = entity_filter
        self.get_session = None
        
        # Statistics
        self.stats = {
            "events_processed": 0,
            "events_dropped": 0,
            "batches_processed": 0,
            "last_batch_time": None,
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
                    self._process_batch_optimized(actual_events)

                self._close_connection()
                _LOGGER.info(
                    "LTSS Turbo shutdown complete. Stats: %s", 
                    json.dumps(self.stats, indent=2)
                )
                return

            # Process normal batch
            self._process_batch_optimized(batch)

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

    def _get_copy_connection(self):
        """Get or create a dedicated connection for COPY operations."""
        if self._copy_connection is None or self._copy_connection.closed:
            self._copy_connection = self.engine.raw_connection()
        return self._copy_connection

    def _close_copy_connection(self):
        """Close the dedicated COPY connection."""
        if self._copy_connection and not self._copy_connection.closed:
            self._copy_connection.close()
            self._copy_connection = None

    def _process_batch_optimized(self, batch: List) -> None:
        """Process batch using optimized COPY command for bulk insert."""
        if not batch:
            return

        start_time = time.time()
        
        # Pre-allocate list for better memory performance
        rows_data = [None] * len(batch)
        valid_rows = 0
        
        # Convert events to LTSS records
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

        # Use COPY for bulk insert (much faster than individual INSERTs)
        tries = 1
        inserted = False
        
        while not inserted and tries <= 3:
            try:
                self._bulk_insert_copy(rows_data)
                inserted = True
                
                # Update statistics
                self.stats["events_processed"] += valid_rows
                self.stats["batches_processed"] += 1
                self.stats["last_batch_time"] = time.time()
                
                processing_time = (time.time() - start_time) * 1000
                _LOGGER.debug(
                    "Batch processed: %d events -> %d rows in %.2fms (%.1f rows/sec)",
                    len(batch),
                    valid_rows,
                    processing_time,
                    valid_rows / (processing_time / 1000) if processing_time > 0 else 0,
                )
                
            except Exception as e:
                _LOGGER.error("Batch insert failed (attempt %d/3): %s", tries, e)
                tries += 1
                if tries <= 3:
                    time.sleep(1)

        if not inserted:
            _LOGGER.error("Failed to insert batch after 3 attempts, dropping %d events", len(batch))
            self.stats["events_dropped"] += len(batch)

        # Mark all events as processed
        for _ in batch:
            self.queue.task_done()

    def _bulk_insert_copy(self, rows: List[Any]) -> None:
        """Use PostgreSQL COPY for efficient bulk insert with optimized formatting."""
        if not rows:
            return
            
        # Use CSV writer for more efficient and safer formatting
        buffer = StringIO()
        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL, 
                           escapechar='\\', lineterminator='\n')
            
        for row in rows:
            # Pre-format common values to avoid repeated operations
            row_data = [
                row.time.isoformat() if row.time else "\\N",
                row.entity_id or "\\N",
                row.state or "\\N",  # CSV writer handles escaping
                json.dumps(row.attributes, cls=JSONEncoder) if row.attributes else "\\N",
                row.friendly_name or "\\N",
                row.unit_of_measurement or "\\N",
                row.device_class or "\\N",
                row.icon or "\\N",
                row.domain or "\\N",
                str(row.state_numeric) if row.state_numeric is not None else "\\N",
                row.last_changed.isoformat() if row.last_changed else "\\N",
                row.last_updated.isoformat() if row.last_updated else "\\N",
                "true" if row.is_unavailable else "false",
                "true" if row.is_unknown else "false",
            ]
            
            # Add location if enabled in config
            if self.enable_location:
                row_data.append(getattr(row, "location", None) or "\\N")
            
            writer.writerow(row_data)
        
        buffer.seek(0)
        
        # Use dedicated connection for COPY operations (reuses connection)
        conn = self._get_copy_connection()
        try:
            with conn.cursor() as cursor:
                # Define columns once
                columns = [
                    "time", "entity_id", "state", "attributes", "friendly_name",
                    "unit_of_measurement", "device_class", "icon", "domain",
                    "state_numeric", "last_changed", "last_updated",
                    "is_unavailable", "is_unknown"
                ]
                
                if self.enable_location:
                    columns.append("location")
                
                cursor.copy_from(
                    buffer,
                    self.table_name,
                    columns=columns,
                    null="\\N", 
                    sep="\t"
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            # Force reconnection on error
            self._close_copy_connection()
            raise e
        finally:
            buffer.close()

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

    def _setup_connection(self):
        """Set up database connection using migration system."""
        if self.engine is not None:
            self.engine.dispose()

        # Use connection pooling for better performance
        self.engine = create_engine(
            self.db_url,
            echo=False,
            poolclass=QueuePool,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,  # Recycle connections after 1 hour
            json_serializer=lambda obj: json.dumps(obj, cls=JSONEncoder),
        )

        # Set table name before running migrations
        self.LTSS = make_ltss_model(self.table_name)
        # Run migrations - this handles all schema setup idempotently
        migrations_ok = run_startup_migrations(
            self.engine,
            self.table_name,
            enable_timescale=True,  # Always try to enable if available
            enable_compression=self.enable_compression,
            enable_location=self.enable_location,
            chunk_time_interval=self.chunk_time_interval,
            compression_after=self.compression_after,
            retention_days=self.retention_days
        )
        
        if not migrations_ok:
            raise Exception("Failed to run database migrations")
        
        # Set up session factory
        self.get_session = scoped_session(sessionmaker(bind=self.engine))
        
        _LOGGER.info(
            "LTSS Turbo initialized: table=%s, compression=%s after %d days",
            self.table_name,
            "enabled" if self.enable_compression else "disabled",
            self.compression_after
        )

    def _close_connection(self):
        """Close database connections cleanly."""
        # Close dedicated COPY connection
        self._close_copy_connection()
        
        if self.get_session:
            self.get_session.remove()
            self.get_session = None
            
        if self.engine:
            self.engine.dispose()
            self.engine = None
            
        _LOGGER.info("Database connections closed")