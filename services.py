"""Service implementations for LTSS Turbo."""

import csv
import logging
from datetime import datetime, timedelta
from typing import Optional
import os

from sqlalchemy import text
import voluptuous as vol

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util

_LOGGER = logging.getLogger(__name__)

DOMAIN = "ltss"

# Service names
SERVICE_COMPRESS_CHUNKS = "compress_chunks"
SERVICE_GET_STATISTICS = "get_statistics"
SERVICE_VACUUM_ANALYZE = "vacuum_analyze"
SERVICE_EXPORT_DATA = "export_data"

# Service schemas
COMPRESS_CHUNKS_SCHEMA = vol.Schema({
    vol.Optional("older_than_days", default=7): cv.positive_int,
})

EXPORT_DATA_SCHEMA = vol.Schema({
    vol.Optional("entity_id"): cv.string,
    vol.Optional("start_date"): cv.datetime,
    vol.Optional("end_date"): cv.datetime,
    vol.Required("output_path"): cv.string,
})


def setup_services(hass: HomeAssistant, ltss_instance):
    """Set up LTSS services."""
    
    async def handle_compress_chunks(call: ServiceCall) -> None:
        """Handle compress_chunks service call."""
        older_than_days = call.data.get("older_than_days", 7)
        
        def compress():
            try:
                with ltss_instance.engine.connect() as con:
                    con = con.execution_options(isolation_level="AUTOCOMMIT")
                    
                    # Get chunks to compress
                    result = con.execute(
                        text(f"""
                            SELECT compress_chunk(c, if_not_exists => true)
                            FROM show_chunks('ltss', older_than => INTERVAL '{older_than_days} days') c
                        """)
                    )
                    
                    chunks_compressed = result.rowcount
                    _LOGGER.info(f"Compressed {chunks_compressed} chunks older than {older_than_days} days")
                    
                    hass.components.persistent_notification.async_create(
                        f"Successfully compressed {chunks_compressed} TimescaleDB chunks",
                        "LTSS Turbo Compression",
                        "ltss_compression_success"
                    )
                    
            except Exception as e:
                _LOGGER.error(f"Failed to compress chunks: {e}")
                hass.components.persistent_notification.async_create(
                    f"Failed to compress chunks: {str(e)}",
                    "LTSS Turbo Error",
                    "ltss_compression_error"
                )
        
        await hass.async_add_executor_job(compress)
    
    async def handle_get_statistics(call: ServiceCall) -> None:
        """Handle get_statistics service call."""
        
        def get_stats():
            try:
                stats = {}
                with ltss_instance.engine.connect() as con:
                    # Table size
                    result = con.execute(
                        text("""
                            SELECT 
                                pg_size_pretty(pg_total_relation_size('ltss')) as table_size,
                                COUNT(*) as row_count,
                                MIN(time) as oldest_record,
                                MAX(time) as newest_record
                            FROM ltss
                        """)
                    )
                    row = result.fetchone()
                    stats.update({
                        "table_size": row.table_size,
                        "row_count": row.row_count,
                        "oldest_record": row.oldest_record.isoformat() if row.oldest_record else None,
                        "newest_record": row.newest_record.isoformat() if row.newest_record else None,
                    })
                    
                    # Check if TimescaleDB is enabled
                    result = con.execute(
                        text("""
                            SELECT COUNT(*) as enabled
                            FROM timescaledb_information.hypertables
                            WHERE hypertable_name = 'ltss'
                        """)
                    )
                    timescale_enabled = result.fetchone().enabled > 0
                    
                    if timescale_enabled:
                        # Compression stats
                        result = con.execute(
                            text("""
                                SELECT 
                                    COUNT(*) FILTER (WHERE compression_status = 'Compressed') as compressed_chunks,
                                    COUNT(*) FILTER (WHERE compression_status != 'Compressed') as uncompressed_chunks,
                                    pg_size_pretty(SUM(uncompressed_total_bytes)) as uncompressed_size,
                                    pg_size_pretty(SUM(compressed_total_bytes)) as compressed_size
                                FROM timescaledb_information.chunks
                                WHERE hypertable_name = 'ltss'
                            """)
                        )
                        row = result.fetchone()
                        stats.update({
                            "timescaledb_enabled": True,
                            "compressed_chunks": row.compressed_chunks or 0,
                            "uncompressed_chunks": row.uncompressed_chunks or 0,
                            "uncompressed_size": row.uncompressed_size or "0 bytes",
                            "compressed_size": row.compressed_size or "0 bytes",
                        })
                    else:
                        stats["timescaledb_enabled"] = False
                    
                    # Runtime statistics
                    stats.update(ltss_instance.stats)
                    
                    # Format as notification
                    message = f"""
LTSS Turbo Statistics:
• Table Size: {stats['table_size']}
• Total Rows: {stats['row_count']:,}
• Date Range: {stats['oldest_record']} to {stats['newest_record']}
• Events Processed: {stats['events_processed']:,}
• Events Dropped: {stats['events_dropped']:,}
• Batches Processed: {stats['batches_processed']:,}
"""
                    
                    if stats['timescaledb_enabled']:
                        message += f"""
• TimescaleDB: Enabled
• Compressed Chunks: {stats['compressed_chunks']}
• Uncompressed Chunks: {stats['uncompressed_chunks']}
• Compressed Size: {stats['compressed_size']}
"""
                    
                    hass.components.persistent_notification.async_create(
                        message,
                        "LTSS Turbo Statistics",
                        "ltss_statistics"
                    )
                    
                    _LOGGER.info(f"LTSS Statistics: {stats}")
                    
            except Exception as e:
                _LOGGER.error(f"Failed to get statistics: {e}")
                hass.components.persistent_notification.async_create(
                    f"Failed to get statistics: {str(e)}",
                    "LTSS Turbo Error",
                    "ltss_statistics_error"
                )
        
        await hass.async_add_executor_job(get_stats)
    
    async def handle_vacuum_analyze(call: ServiceCall) -> None:
        """Handle vacuum_analyze service call."""
        
        def vacuum():
            try:
                with ltss_instance.engine.connect() as con:
                    con = con.execution_options(isolation_level="AUTOCOMMIT")
                    con.execute(text("VACUUM ANALYZE ltss"))
                    
                    _LOGGER.info("Successfully ran VACUUM ANALYZE on ltss table")
                    
                    hass.components.persistent_notification.async_create(
                        "Successfully optimized LTSS table (VACUUM ANALYZE complete)",
                        "LTSS Turbo Maintenance",
                        "ltss_vacuum_success"
                    )
                    
            except Exception as e:
                _LOGGER.error(f"Failed to vacuum analyze: {e}")
                hass.components.persistent_notification.async_create(
                    f"Failed to vacuum analyze: {str(e)}",
                    "LTSS Turbo Error",
                    "ltss_vacuum_error"
                )
        
        await hass.async_add_executor_job(vacuum)
    
    async def handle_export_data(call: ServiceCall) -> None:
        """Handle export_data service call."""
        entity_id = call.data.get("entity_id")
        start_date = call.data.get("start_date", datetime.now() - timedelta(days=7))
        end_date = call.data.get("end_date", datetime.now())
        output_path = call.data.get("output_path")
        
        # Ensure path is within config directory for security
        if not output_path.startswith("/config/"):
            output_path = f"/config/{output_path}"
        
        def export():
            try:
                with ltss_instance.engine.connect() as con:
                    # Build query
                    if entity_id:
                        query = text("""
                            SELECT 
                                time, entity_id, state, friendly_name,
                                unit_of_measurement, device_class, domain,
                                state_numeric, attributes
                            FROM ltss
                            WHERE entity_id = :entity_id
                              AND time >= :start_date
                              AND time <= :end_date
                            ORDER BY time
                        """)
                        params = {"entity_id": entity_id, "start_date": start_date, "end_date": end_date}
                    else:
                        query = text("""
                            SELECT 
                                time, entity_id, state, friendly_name,
                                unit_of_measurement, device_class, domain,
                                state_numeric, attributes
                            FROM ltss
                            WHERE time >= :start_date
                              AND time <= :end_date
                            ORDER BY time
                        """)
                        params = {"start_date": start_date, "end_date": end_date}
                    
                    # Export to CSV
                    result = con.execute(query, params)
                    
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    
                    with open(output_path, 'w', newline='') as csvfile:
                        writer = csv.writer(csvfile)
                        
                        # Write header
                        writer.writerow([
                            "time", "entity_id", "state", "friendly_name",
                            "unit_of_measurement", "device_class", "domain",
                            "state_numeric", "attributes"
                        ])
                        
                        # Write data in chunks
                        row_count = 0
                        for row in result:
                            writer.writerow([
                                row.time.isoformat() if row.time else "",
                                row.entity_id or "",
                                row.state or "",
                                row.friendly_name or "",
                                row.unit_of_measurement or "",
                                row.device_class or "",
                                row.domain or "",
                                row.state_numeric if row.state_numeric is not None else "",
                                json.dumps(row.attributes) if row.attributes else ""
                            ])
                            row_count += 1
                    
                    _LOGGER.info(f"Exported {row_count} rows to {output_path}")
                    
                    hass.components.persistent_notification.async_create(
                        f"Successfully exported {row_count:,} rows to {output_path}",
                        "LTSS Turbo Export",
                        "ltss_export_success"
                    )
                    
            except Exception as e:
                _LOGGER.error(f"Failed to export data: {e}")
                hass.components.persistent_notification.async_create(
                    f"Failed to export data: {str(e)}",
                    "LTSS Turbo Error",
                    "ltss_export_error"
                )
        
        await hass.async_add_executor_job(export)
    
    # Register services
    hass.services.async_register(
        DOMAIN,
        SERVICE_COMPRESS_CHUNKS,
        handle_compress_chunks,
        schema=COMPRESS_CHUNKS_SCHEMA,
    )
    
    hass.services.async_register(
        DOMAIN,
        SERVICE_GET_STATISTICS,
        handle_get_statistics,
    )
    
    hass.services.async_register(
        DOMAIN,
        SERVICE_VACUUM_ANALYZE,
        handle_vacuum_analyze,
    )
    
    hass.services.async_register(
        DOMAIN,
        SERVICE_EXPORT_DATA,
        handle_export_data,
        schema=EXPORT_DATA_SCHEMA,
    )
    
    _LOGGER.info("LTSS Turbo services registered")