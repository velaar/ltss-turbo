# LTSS Turbo - Long Time State Storage for Home Assistant

[![GitHub Release](https://img.shields.io/github/release/velaar/ltss-turbo.svg?style=flat-square)](https://github.com/velaar/ltss-turbo/releases)
[![License](https://img.shields.io/github/license/velaar/ltss-turbo.svg?style=flat-square)](LICENSE)
[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg?style=flat-square)](https://github.com/hacs/integration)

High-performance time-series storage for Home Assistant using PostgreSQL with TimescaleDB. Designed for efficient long-term data retention with automatic compression and optimized Grafana integration.

## Key Features

- **Optimized Bulk Processing**: PostgreSQL COPY operations for efficient batch inserts
- **Smart Numeric Conversion**: Intelligent parsing of Home Assistant states to numeric values
- **TimescaleDB Integration**: Automatic compression and chunk management
- **Configurable Table Names**: Support for multiple instances and custom table naming
- **Connection Pooling**: Efficient database connection management with configurable pools
- **Comprehensive Filtering**: Include/exclude entities and domains with pattern matching
- **PostGIS Support**: Optional GPS/location tracking for device trackers
- **Production Ready**: Robust error handling, statistics tracking, and monitoring

## Requirements

- Home Assistant 2023.1 or newer
- PostgreSQL 12+ 
- TimescaleDB extension (recommended for compression and performance)
- Python 3.9+

### Optional PostgreSQL Extensions

- **TimescaleDB** (strongly recommended for compression and hypertables)
- **PostGIS** (optional, enables GPS/location tracking)

## Installation

### HACS Installation (Recommended)

1. Add this repository to HACS as a custom repository:
   - URL: `https://github.com/velaar/ltss-turbo`
   - Category: Integration
2. Install "LTSS Turbo" through HACS
3. Restart Home Assistant

### Manual Installation

1. Copy the `ltss` folder to your `custom_components` directory
2. Restart Home Assistant

### Database Setup

```sql
-- Create database and user
CREATE DATABASE homeassistant;
CREATE USER ltss WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE homeassistant TO ltss;

-- Connect to the database
\c homeassistant

-- Enable TimescaleDB (strongly recommended)
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Enable PostGIS (optional, for location tracking)
CREATE EXTENSION IF NOT EXISTS postgis CASCADE;
```

## Configuration

### Basic Configuration

```yaml
ltss:
  db_url: postgresql://ltss:password@localhost:5432/homeassistant
```

### Advanced Configuration

```yaml
ltss:
  # Database connection
  db_url: postgresql://ltss:password@localhost:5432/homeassistant
  table_name: ltss                    # Custom table name (default: ltss)
  
  # Performance tuning
  batch_size: 500                     # Events per batch (10-5000, default: 500)
  batch_timeout_ms: 1000              # Max wait before batch flush (100-10000ms, default: 1000)
  poll_interval_ms: 100               # Queue polling interval (10-1000ms, default: 100)
  
  # Connection pooling
  pool_size: 5                        # Base connection pool size (1-20, default: 5)
  max_overflow: 10                    # Additional connections (0-50, default: 10)
  
  # TimescaleDB features
  enable_compression: true            # Enable automatic compression (default: true)
  compression_after: 7                # Compress chunks after N days (1-365, default: 7)
  chunk_time_interval: 86400000000    # Chunk size in microseconds (default: 1 day)
  
  # Data lifecycle
  retention_days: 365                 # Auto-delete data after N days (optional)
  
  # Features
  enable_location: false              # Enable PostGIS location tracking (default: false)
  
  # Entity filtering
  include:
    domains:
      - sensor
      - binary_sensor
      - switch
      - light
      - climate
    entities:
      - input_boolean.important_flag
  exclude:
    entity_globs:
      - sensor.*_linkquality
      - sensor.*_last_seen
      - sensor.*_rssi
    entities:
      - sensor.debug_counter
```

### Configuration Options Reference

| Option | Type | Default | Range | Description |
|--------|------|---------|--------|-------------|
| `db_url` | string | **Required** | - | PostgreSQL connection string |
| `table_name` | string | `ltss` | - | Custom table name for multi-instance support |
| `batch_size` | int | 500 | 10-5000 | Events processed per batch |
| `batch_timeout_ms` | int | 1000 | 100-10000 | Maximum milliseconds before batch flush |
| `poll_interval_ms` | int | 100 | 10-1000 | Queue polling interval |
| `chunk_time_interval` | int | 86400000000 | - | TimescaleDB chunk size (microseconds) |
| `pool_size` | int | 5 | 1-20 | Base database connection pool size |
| `max_overflow` | int | 10 | 0-50 | Additional connections beyond pool |
| `enable_compression` | bool | true | - | Enable TimescaleDB compression |
| `compression_after` | int | 7 | 1-365 | Days before chunk compression |
| `retention_days` | int | None | 1-36500 | Automatic data deletion after N days |
| `enable_location` | bool | false | - | Enable PostGIS location extraction |

## Database Schema

LTSS Turbo creates an optimized table structure:

```sql
CREATE TABLE ltss (
    time TIMESTAMPTZ NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    attributes JSONB,
    friendly_name VARCHAR(255),
    unit_of_measurement VARCHAR(50),
    device_class VARCHAR(50),
    icon VARCHAR(100),
    domain VARCHAR(50) NOT NULL,
    state_numeric FLOAT,
    last_changed TIMESTAMPTZ,
    last_updated TIMESTAMPTZ,
    is_unavailable BOOLEAN NOT NULL DEFAULT FALSE,
    is_unknown BOOLEAN NOT NULL DEFAULT FALSE,
    location GEOMETRY(POINT, 4326), -- Optional with PostGIS
    PRIMARY KEY (time, entity_id)
);
```

### Optimized Indexes

The system automatically creates indexes for common query patterns:
- Entity + Time (primary queries)
- Domain + Time (domain-wide analysis)
- Device Class + Time (sensor type queries)
- Numeric State queries (aggregations)
- JSONB attributes (flexible querying)
- Quality flags (availability analysis)

## Grafana Integration

### Basic Queries

**Temperature Over Time**
```sql
SELECT 
  time,
  state_numeric AS temperature,
  friendly_name
FROM ltss
WHERE 
  entity_id = 'sensor.living_room_temperature'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
ORDER BY time;
```

**All Temperature Sensors**
```sql
SELECT 
  time,
  entity_id,
  state_numeric AS temperature,
  unit_of_measurement
FROM ltss
WHERE 
  device_class = 'temperature'
  AND domain = 'sensor'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
ORDER BY time;
```

### TimescaleDB Aggregations

**Hourly Averages**
```sql
SELECT 
  time_bucket('1 hour', time) AS time,
  entity_id,
  AVG(state_numeric) AS avg_value,
  MAX(state_numeric) AS max_value,
  MIN(state_numeric) AS min_value
FROM ltss
WHERE 
  device_class = 'temperature'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 hour', time), entity_id
ORDER BY time;
```

**Daily Energy Consumption**
```sql
SELECT 
  time_bucket('1 day', time) AS time,
  entity_id,
  MAX(state_numeric) - MIN(state_numeric) AS daily_consumption
FROM ltss
WHERE 
  device_class = 'energy'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 day', time), entity_id
HAVING MAX(state_numeric) - MIN(state_numeric) > 0
ORDER BY time;
```

**Motion Events**
```sql
SELECT 
  time,
  friendly_name,
  entity_id
FROM ltss
WHERE 
  device_class = 'motion'
  AND state = 'on'
  AND $__timeFilter(time)
ORDER BY time DESC;
```

## Numeric State Conversion

LTSS Turbo intelligently converts Home Assistant states to numeric values for efficient aggregations:

### Binary States
| Original State | Numeric Value | Domains |
|---------------|---------------|---------|
| on/off | 1.0/0.0 | switch, light, binary_sensor |
| true/false | 1.0/0.0 | input_boolean |
| home/away | 1.0/0.0 | person, device_tracker |
| locked/unlocked | 1.0/0.0 | lock |
| open/closed | 1.0/0.0 | cover, binary_sensor |

### Climate States
| State | Numeric Value |
|-------|---------------|
| off | 0.0 |
| heat | 1.0 |
| cool | 2.0 |
| auto | 3.0 |
| heat_cool | 4.0 |
| dry | 5.0 |
| fan_only | 6.0 |
| eco | 7.0 |

### Special States
| State | Numeric Value |
|-------|---------------|
| unavailable | -1.0 |
| unknown | -2.0 |
| none | -3.0 |
| error | -4.0 |
| fault | -5.0 |

### Advanced Parsing
- **Numeric extraction**: `"23.5°C"` → `23.5`
- **Percentage handling**: `"85%"` → `85.0`
- **Timestamp conversion**: ISO/Unix timestamps → epoch seconds
- **Categorical options**: Array index for `input_select` options
- **Level states**: low/medium/high → 0/1/2

## Performance Optimization

### PostgreSQL Configuration

Add to `postgresql.conf`:
```ini
# Memory settings (adjust based on available RAM)
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
work_mem = 10MB

# Checkpoint and WAL settings
checkpoint_completion_target = 0.9
wal_buffers = 16MB
max_wal_size = 2GB

# Query planner
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200

# Parallel processing
max_worker_processes = 8
max_parallel_workers_per_gather = 4
max_parallel_workers = 8
```

### LTSS Turbo Tuning

For high-volume systems:
```yaml
ltss:
  batch_size: 1000              # Larger batches for high throughput
  batch_timeout_ms: 500         # Faster flushing
  pool_size: 10                 # More connections
  max_overflow: 20              # Higher burst capacity
```

For low-resource systems:
```yaml
ltss:
  batch_size: 100               # Smaller batches
  batch_timeout_ms: 2000        # Less frequent flushing
  pool_size: 2                  # Fewer connections
  max_overflow: 5               # Lower overhead
```

## Monitoring and Maintenance

### Check System Status

Enable debug logging:
```yaml
logger:
  default: info
  logs:
    custom_components.ltss: debug
```

### Database Maintenance

**Check TimescaleDB Status**
```sql
SELECT 
  hypertable_name,
  num_chunks,
  table_bytes,
  index_bytes,
  compressed_total_bytes,
  uncompressed_total_bytes
FROM timescaledb_information.hypertable 
WHERE hypertable_name = 'ltss';
```

**Monitor Compression**
```sql
SELECT 
  chunk_name,
  compression_status,
  before_compression_total_bytes,
  after_compression_total_bytes,
  pg_size_pretty(before_compression_total_bytes) AS before_size,
  pg_size_pretty(after_compression_total_bytes) AS after_size
FROM timescaledb_information.chunks
WHERE hypertable_name = 'ltss'
ORDER BY range_start DESC
LIMIT 10;
```

**Check Index Usage**
```sql
SELECT 
  schemaname,
  tablename,
  indexname,
  idx_tup_read,
  idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE tablename = 'ltss'
ORDER BY idx_tup_read DESC;
```

## Troubleshooting

### Common Issues

**Queue Full Errors**
```
Event queue full, dropping event for sensor.temperature
```
- Increase `batch_size` or reduce `batch_timeout_ms`
- Check database connection performance
- Monitor system resources

**Connection Pool Exhausted**
```
Connection pool exhausted
```
- Increase `pool_size` and `max_overflow`
- Check for long-running queries
- Monitor connection usage

**High Memory Usage**
- Reduce `batch_size`
- Enable compression earlier
- Check PostgreSQL memory settings

**Slow Queries**
- Verify indexes exist: `\d+ ltss`
- Run `ANALYZE ltss;` to update statistics
- Check query plans with `EXPLAIN ANALYZE`

### Manual Compression

Force compression of older chunks:
```sql
SELECT compress_chunk(c) 
FROM show_chunks('ltss', older_than => INTERVAL '7 days') c;
```

### Recovery Procedures

**Recreate Indexes**
```sql
REINDEX TABLE ltss;
```

**Reset Statistics**
```sql
SELECT reset_stats();
```

## Multi-Instance Setup

LTSS Turbo supports multiple Home Assistant instances using different table names:

**Instance 1 Configuration**
```yaml
ltss:
  db_url: postgresql://ltss:password@localhost/homeassistant
  table_name: ltss_main
```

**Instance 2 Configuration**
```yaml
ltss:
  db_url: postgresql://ltss:password@localhost/homeassistant
  table_name: ltss_remote
```

This allows:
- Separate data streams in the same database
- Independent compression and retention policies
- Isolated monitoring and maintenance

## Migration Guide

### From Standard LTSS

1. **Backup existing data**
2. **Install LTSS Turbo** (can run alongside temporarily)
3. **Configure with different table name**:
   ```yaml
   ltss:
     table_name: ltss_turbo  # Avoid conflict with existing table
   ```
4. **Test thoroughly before switching**
5. **Optional: Migrate historical data**

### Schema Differences

LTSS Turbo uses an optimized schema that's not directly compatible with standard LTSS:
- Separate columns for metadata (friendly_name, device_class, etc.)
- Numeric state column for efficient aggregations
- Quality flags for data validation
- Optional PostGIS location support

## Performance Expectations

Typical performance characteristics:
- **Insert Rate**: 1,000-10,000 events/second (depending on hardware)
- **Storage Efficiency**: 40-90% reduction with compression
- **Query Performance**: Sub-second response for most Grafana queries
- **Memory Usage**: 50-200MB baseline (scales with batch size)

Performance varies based on:
- Database server specifications
- Network latency
- Entity count and update frequency
- Compression settings
- Query complexity

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Credits

**Original LTSS**: Created by [@freol35241](https://github.com/freol35241) (2019)
- Original concept and Home Assistant integration
- MIT License: Copyright (c) 2019 freol35241
- Repository: https://github.com/freol35241/ltss

**LTSS Turbo**: Substantial rewrite and optimization by [@velaar](https://github.com/velaar) (2024)
- Complete performance rewrite with TimescaleDB focus
- Batch processing inspiration from original `feat/batch-processing` PR
- Optimized numeric parsing and database schema

**Acknowledgments**:
- Home Assistant recorder component authors for foundational code patterns
- TimescaleDB team for excellent time-series database technology
- PostgreSQL community for robust database foundation

## License

This project is licensed under the MIT License.

**Original LTSS License** (portions of code):
```
MIT License
Copyright (c) 2019 freol35241

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
```

**LTSS Turbo** (substantial rewrite):
MIT License - Copyright (c) 2024 velaar - see LICENSE file for details.

## Support

- [Report Issues](https://github.com/velaar/ltss-turbo/issues)
- [Discussions](https://github.com/velaar/ltss-turbo/discussions)
- [Home Assistant Community](https://community.home-assistant.io/)

---

**Note**: This is a substantial rewrite focused on performance and scalability. While inspired by the original LTSS concept and maintaining compatibility with Home Assistant's state model, LTSS Turbo uses an optimized schema and is not directly compatible with existing LTSS database installations.