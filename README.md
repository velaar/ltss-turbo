# LTSS Turbo - Long Time State Storage for Home Assistant

[![GitHub Release](https://img.shields.io/github/release/velaar/ltss-turbo.svg?style=flat-square)](https://github.com/velaar/ltss-turbo/releases)
[![License](https://img.shields.io/github/license/velaar/ltss-turbo.svg?style=flat-square)](LICENSE)
[![Tests](https://github.com/velaar/ltss-turbo/actions/workflows/ci.yml/badge.svg)](https://github.com/velaar/ltss-turbo/actions)
[![codecov](https://codecov.io/gh/velaar/ltss-turbo/branch/main/graph/badge.svg)](https://codecov.io/gh/velaar/ltss-turbo)
[![hacs_badge](https://img.shields.io/badge/HACS-Custom-41BDF5.svg?style=flat-square)](https://github.com/hacs/integration)

High-performance time-series storage for Home Assistant using PostgreSQL with TimescaleDB. Designed for efficient long-term data retention with automatic compression and optimized Grafana integration.

## ⚠️ Important Disclaimer

**This is a hobby project created for fun and learning purposes.** 

- **NO SUPPORT GUARANTEES**: This project is maintained in spare time with no commitments
- **NO WARRANTIES**: Use at your own risk - there are no guarantees of functionality or fitness for any purpose
- **NO SLA**: No response time commitments for issues or feature requests
- **EXPECT BUGS**: This is experimental software that may have issues
- **DATA LOSS POSSIBLE**: Always maintain backups of your data

That said, the project aims for quality and includes comprehensive testing within GitHub's free tier limits. Contributions and feedback are welcome!

## 🚀 Key Features

- **Optimized Bulk Processing**: PostgreSQL COPY operations with binary encoding for 3x faster inserts
- **Smart Numeric Conversion**: Intelligent parsing of Home Assistant states to `state_numeric` for aggregations
- **Advanced Caching**: LRU caches with automatic eviction for optimal memory usage
- **TimescaleDB Integration**: Automatic compression and chunk management
- **Metadata Tracking**: Schema versioning and migration history in `{table_name}_meta` table
- **Connection Pooling**: Efficient database connection management with pgbouncer support
- **Comprehensive Filtering**: Include/exclude entities and domains with pattern matching
- **PostGIS Support**: Optional GPS/location tracking for device trackers
- **Production Ready**: Robust error handling, statistics tracking, and performance monitoring

## 📊 Performance Metrics

Typical performance characteristics with optimizations:
- **Insert Rate**: 1,000-10,000 events/second (with binary COPY)
- **Storage Efficiency**: 40-90% reduction with TimescaleDB compression
- **Memory Usage**: 50-200MB baseline with LRU caching
- **Query Performance**: Sub-second response for most Grafana queries
- **Cache Hit Rate**: 80-95% for common state conversions

## 🔧 Requirements

- Home Assistant 2023.1 or newer
- PostgreSQL 12+ 
- TimescaleDB extension (strongly recommended)
- Python 3.9+

### Optional PostgreSQL Extensions

- **TimescaleDB** (strongly recommended for compression and hypertables)
- **PostGIS** (optional, enables GPS/location tracking)

## 📦 Installation

### HACS Installation (Recommended)

1. Add this repository to HACS as a custom repository:
   - URL: `https://github.com/velaar/ltss-turbo`
   - Category: Integration
2. Install "LTSS Turbo" through HACS
3. Restart Home Assistant

### Manual Installation

1. Copy the `ltss_turbo` folder to your `custom_components` directory
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

## ⚙️ Configuration

### Basic Configuration

```yaml
ltss_turbo:
  db_url: postgresql://ltss:password@localhost:5432/homeassistant
```

### Advanced Configuration

```yaml
ltss_turbo:
  # Database connection
  db_url: postgresql://ltss:password@localhost:5432/homeassistant
  table_name: ltss_turbo              # Custom table name (default: ltss_turbo)
  
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

## 📊 Database Schema

LTSS Turbo creates an optimized table structure with the `state_numeric` column for efficient aggregations:

```sql
CREATE TABLE ltss_turbo (
    time TIMESTAMPTZ NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL,
    attributes JSONB,
    friendly_name VARCHAR(255),
    unit_of_measurement VARCHAR(50),
    device_class VARCHAR(50),
    icon VARCHAR(100),
    domain VARCHAR(50) NOT NULL,
    state_numeric FLOAT,              -- ← Key column for aggregations
    last_changed TIMESTAMPTZ,
    last_updated TIMESTAMPTZ,
    is_unavailable BOOLEAN NOT NULL DEFAULT FALSE,
    is_unknown BOOLEAN NOT NULL DEFAULT FALSE,
    location GEOMETRY(POINT, 4326),    -- Optional with PostGIS
    PRIMARY KEY (time, entity_id)
);
```

### Metadata Table

Schema version and migration history are tracked in `{table_name}_meta`:

```sql
CREATE TABLE ltss_turbo_meta (
    key VARCHAR(50) PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

## 📈 Grafana Integration - state_numeric Examples

The `state_numeric` column is the key to efficient aggregations in Grafana. Here are comprehensive examples:

### Basic Time Series Queries

#### Temperature Over Time
```sql
SELECT 
  time,
  state_numeric AS temperature,
  friendly_name
FROM ltss_turbo
WHERE 
  entity_id = 'sensor.living_room_temperature'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
ORDER BY time;
```

#### Multiple Sensors on Same Graph
```sql
SELECT 
  time,
  entity_id AS metric,
  state_numeric AS value
FROM ltss_turbo
WHERE 
  entity_id IN ('sensor.bedroom_temp', 'sensor.living_room_temp', 'sensor.outside_temp')
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
ORDER BY time;
```

### Device Class Aggregations

#### All Temperature Sensors
```sql
SELECT 
  time,
  entity_id,
  state_numeric AS temperature,
  unit_of_measurement
FROM ltss_turbo
WHERE 
  device_class = 'temperature'
  AND domain = 'sensor'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
ORDER BY time;
```

#### Humidity Distribution Heatmap
```sql
SELECT 
  time_bucket('1 hour', time) AS time,
  entity_id,
  AVG(state_numeric) AS humidity
FROM ltss_turbo
WHERE 
  device_class = 'humidity'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 hour', time), entity_id
ORDER BY time;
```

### TimescaleDB Time Bucketing

#### Hourly Average with Min/Max
```sql
SELECT 
  time_bucket('1 hour', time) AS time,
  AVG(state_numeric) AS avg_temp,
  MAX(state_numeric) AS max_temp,
  MIN(state_numeric) AS min_temp,
  STDDEV(state_numeric) AS stddev_temp
FROM ltss_turbo
WHERE 
  entity_id = 'sensor.living_room_temperature'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 hour', time)
ORDER BY time;
```

#### Daily Energy Consumption
```sql
SELECT 
  time_bucket('1 day', time) AS time,
  entity_id,
  MAX(state_numeric) - MIN(state_numeric) AS daily_kwh,
  MAX(state_numeric) AS end_reading,
  MIN(state_numeric) AS start_reading
FROM ltss_turbo
WHERE 
  device_class = 'energy'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 day', time), entity_id
HAVING MAX(state_numeric) - MIN(state_numeric) > 0
ORDER BY time;
```

### Binary State Analysis

#### Switch Usage Pattern
```sql
-- state_numeric: 1.0 = on, 0.0 = off
SELECT 
  time_bucket('1 hour', time) AS time,
  entity_id,
  SUM(state_numeric) / COUNT(*) * 100 AS on_percentage,
  COUNT(*) FILTER (WHERE state_numeric = 1.0) AS on_count,
  COUNT(*) FILTER (WHERE state_numeric = 0.0) AS off_count
FROM ltss_turbo
WHERE 
  domain = 'switch'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 hour', time), entity_id
ORDER BY time;
```

#### Motion Detection Frequency
```sql
-- Count motion events (state_numeric = 1.0 when detected)
SELECT 
  time_bucket('15 minutes', time) AS time,
  entity_id,
  SUM(CASE WHEN state_numeric = 1.0 AND LAG(state_numeric) OVER (PARTITION BY entity_id ORDER BY time) = 0.0 
           THEN 1 ELSE 0 END) AS motion_events
FROM ltss_turbo
WHERE 
  device_class = 'motion'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('15 minutes', time), entity_id
ORDER BY time;
```

### Climate Analysis

#### HVAC Mode Distribution
```sql
-- state_numeric: 0=off, 1=heat, 2=cool, 3=auto, 4=heat_cool
SELECT 
  time_bucket('1 day', time) AS time,
  CASE state_numeric
    WHEN 0 THEN 'off'
    WHEN 1 THEN 'heat'
    WHEN 2 THEN 'cool'
    WHEN 3 THEN 'auto'
    WHEN 4 THEN 'heat_cool'
    ELSE 'other'
  END AS mode,
  COUNT(*) AS mode_count,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY time_bucket('1 day', time)) AS percentage
FROM ltss_turbo
WHERE 
  domain = 'climate'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY time_bucket('1 day', time), state_numeric
ORDER BY time, mode;
```

### Advanced Analytics

#### Correlation Between Temperature and Humidity
```sql
WITH sensor_data AS (
  SELECT 
    time_bucket('1 hour', time) AS hour,
    AVG(CASE WHEN device_class = 'temperature' THEN state_numeric END) AS avg_temp,
    AVG(CASE WHEN device_class = 'humidity' THEN state_numeric END) AS avg_humidity
  FROM ltss_turbo
  WHERE 
    entity_id IN ('sensor.living_room_temp', 'sensor.living_room_humidity')
    AND $__timeFilter(time)
    AND state_numeric IS NOT NULL
  GROUP BY hour
)
SELECT 
  hour AS time,
  avg_temp,
  avg_humidity,
  CORR(avg_temp, avg_humidity) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS correlation
FROM sensor_data
WHERE avg_temp IS NOT NULL AND avg_humidity IS NOT NULL
ORDER BY hour;
```

#### Power Usage vs Time of Day
```sql
SELECT 
  EXTRACT(HOUR FROM time) AS hour_of_day,
  AVG(state_numeric) AS avg_power_w,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY state_numeric) AS median_power_w,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY state_numeric) AS p95_power_w,
  COUNT(*) AS samples
FROM ltss_turbo
WHERE 
  device_class = 'power'
  AND $__timeFilter(time)
  AND state_numeric IS NOT NULL
GROUP BY EXTRACT(HOUR FROM time)
ORDER BY hour_of_day;
```

#### Availability Monitoring
```sql
-- Track sensor availability (using quality flags)
SELECT 
  time_bucket('1 hour', time) AS time,
  entity_id,
  COUNT(*) FILTER (WHERE NOT is_unavailable AND NOT is_unknown) AS available_count,
  COUNT(*) AS total_count,
  (COUNT(*) FILTER (WHERE NOT is_unavailable AND NOT is_unknown))::float / COUNT(*) * 100 AS availability_pct
FROM ltss_turbo
WHERE 
  domain = 'sensor'
  AND $__timeFilter(time)
GROUP BY time_bucket('1 hour', time), entity_id
HAVING COUNT(*) > 0
ORDER BY time, availability_pct DESC;
```

### Performance Queries

#### Most Frequently Updated Entities
```sql
SELECT 
  entity_id,
  friendly_name,
  COUNT(*) AS update_count,
  COUNT(*) / EXTRACT(EPOCH FROM (MAX(time) - MIN(time))) * 60 AS updates_per_minute,
  MIN(time) AS first_seen,
  MAX(time) AS last_seen
FROM ltss_turbo
WHERE 
  $__timeFilter(time)
GROUP BY entity_id, friendly_name
ORDER BY update_count DESC
LIMIT 20;
```

#### Storage Usage by Entity
```sql
SELECT 
  entity_id,
  COUNT(*) AS row_count,
  pg_size_pretty(
    COUNT(*) * 100::bigint  -- Approximate row size
  ) AS estimated_size,
  MIN(time) AS oldest_data,
  MAX(time) AS newest_data
FROM ltss_turbo
WHERE $__timeFilter(time)
GROUP BY entity_id
ORDER BY row_count DESC
LIMIT 50;
```

### Grafana Variable Queries

#### Dynamic Entity List
```sql
SELECT DISTINCT entity_id AS __text, entity_id AS __value
FROM ltss_turbo
WHERE 
  domain = '$domain'
  AND time > NOW() - INTERVAL '7 days'
ORDER BY entity_id;
```

#### Device Classes
```sql
SELECT DISTINCT device_class AS __text, device_class AS __value
FROM ltss_turbo
WHERE 
  device_class IS NOT NULL
  AND time > NOW() - INTERVAL '7 days'
ORDER BY device_class;
```

## 🔢 Numeric State Conversion Reference

The `state_numeric` column automatically converts states for efficient aggregation:

### Binary States (0.0 or 1.0)
| State | Numeric | Domains |
|-------|---------|---------|
| on/off | 1.0/0.0 | switch, light, binary_sensor |
| true/false | 1.0/0.0 | input_boolean |
| home/away | 1.0/0.0 | person, device_tracker |
| locked/unlocked | 1.0/0.0 | lock |
| open/closed | 1.0/0.0 | cover, binary_sensor |
| detected/clear | 1.0/0.0 | binary_sensor |
| connected/disconnected | 1.0/0.0 | binary_sensor |

### Climate States
| State | Numeric |
|-------|---------|
| off | 0.0 |
| heat | 1.0 |
| cool | 2.0 |
| auto | 3.0 |
| heat_cool | 4.0 |
| dry | 5.0 |
| fan_only | 6.0 |
| eco | 7.0 |

### Level States
| State | Numeric |
|-------|---------|
| low | 0.0 |
| medium | 1.0 |
| high | 2.0 |
| quiet | 0.0 |
| normal | 1.0 |
| boost | 2.0 |

### Special States
| State | Numeric |
|-------|---------|
| unavailable | -1.0 |
| unknown | -2.0 |
| none | -3.0 |
| error | -4.0 |

## 🔍 Monitoring and Maintenance

### Check System Status

```yaml
logger:
  default: info
  logs:
    custom_components.ltss_turbo: debug
```

### View Metadata

```sql
-- Check schema version and migration history
SELECT * FROM ltss_turbo_meta ORDER BY key;

-- View compression status
SELECT 
  hypertable_name,
  num_chunks,
  compressed_total_bytes,
  uncompressed_total_bytes,
  pg_size_pretty(compressed_total_bytes) AS compressed_size,
  pg_size_pretty(uncompressed_total_bytes) AS uncompressed_size,
  100 - (compressed_total_bytes::float / NULLIF(uncompressed_total_bytes, 0) * 100) AS compression_ratio
FROM timescaledb_information.hypertable
WHERE hypertable_name = 'ltss_turbo';
```

### Performance Monitoring

```sql
-- Check batch processing performance
SELECT 
  key, 
  value 
FROM ltss_turbo_meta 
WHERE key LIKE 'migration_v%_duration_ms'
ORDER BY key;

-- Monitor table growth
SELECT 
  time_bucket('1 day', time) AS day,
  COUNT(*) AS events_per_day,
  COUNT(DISTINCT entity_id) AS unique_entities,
  pg_size_pretty(COUNT(*)::bigint * 100) AS estimated_size
FROM ltss_turbo
WHERE time > NOW() - INTERVAL '30 days'
GROUP BY day
ORDER BY day DESC;
```

## 🚀 Performance Optimization

### For High-Volume Systems

```yaml
ltss_turbo:
  batch_size: 1000              # Larger batches
  batch_timeout_ms: 500         # Faster flushing
  pool_size: 10                 # More connections
  max_overflow: 20              # Higher burst capacity
```

### For Low-Resource Systems

```yaml
ltss_turbo:
  batch_size: 100               # Smaller batches
  batch_timeout_ms: 2000        # Less frequent flushing
  pool_size: 2                  # Fewer connections
  max_overflow: 5               # Lower overhead
```

### PostgreSQL Tuning

```ini
# postgresql.conf optimizations
shared_buffers = 1GB
effective_cache_size = 3GB
maintenance_work_mem = 256MB
work_mem = 10MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
max_wal_size = 2GB
random_page_cost = 1.1
effective_io_concurrency = 200
max_worker_processes = 8
max_parallel_workers_per_gather = 4
```

## 🔄 Migration from Standard LTSS

LTSS Turbo uses an optimized schema focusing on the `state_numeric` column. To migrate:

1. **Install LTSS Turbo** alongside existing LTSS
2. **Use different table name** to avoid conflicts
3. **Run both temporarily** to verify data
4. **Switch Grafana queries** to use new table and `state_numeric`
5. **Remove old LTSS** after validation

## 📊 Multi-Instance Support

Run multiple Home Assistant instances with separate tables:

```yaml
# Instance 1
ltss_turbo:
  db_url: postgresql://ltss:password@localhost/homeassistant
  table_name: ltss_main

# Instance 2  
ltss_turbo:
  db_url: postgresql://ltss:password@localhost/homeassistant
  table_name: ltss_remote
```

## 🧪 Testing & Quality Assurance

### Automated Testing

This project uses GitHub Actions for CI/CD (free tier):

- **Unit Tests**: Core functionality testing with >70% coverage target
- **Integration Tests**: Testing with real PostgreSQL/TimescaleDB
- **Home Assistant Compatibility**: Testing against multiple HA versions
- **Security Scanning**: Automated vulnerability detection
- **Code Quality**: Black, isort, flake8, pylint, mypy

[![Tests](https://github.com/velaar/ltss-turbo/actions/workflows/ci.yml/badge.svg)](https://github.com/velaar/ltss-turbo/actions)
[![codecov](https://codecov.io/gh/velaar/ltss-turbo/branch/main/graph/badge.svg)](https://codecov.io/gh/velaar/ltss-turbo)

### Running Tests Locally

```bash
# Install test dependencies
pip install -r requirements-test.txt

# Run tests with coverage
pytest tests/ --cov=custom_components/ltss_turbo

# Run specific test file
pytest tests/test_models.py -v

# Run with markers
pytest -m "not slow"  # Skip slow tests
pytest -m unit        # Only unit tests
```

### Test Database Setup

```bash
# Using Docker (recommended)
docker run -d \
  --name ltss-test \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=test_db \
  -p 5432:5432 \
  timescale/timescaledb-ha:pg14-latest

# Run tests
DATABASE_URL=postgresql://postgres:test@localhost:5432/test_db pytest
```

### Pre-commit Hooks

```bash
# Install pre-commit
pip install pre-commit

# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

### Code Coverage

- **Target**: >70% code coverage
- **Reports**: XML (Codecov), HTML (local), Terminal
- **Exclusions**: Test files, type checking blocks

View coverage locally:
```bash
pytest --cov-report=html
open htmlcov/index.html
```

## 🤝 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Remember**: This is a hobby project with no support guarantees. However, quality contributions that improve the project for everyone are appreciated!

## 📜 Credits

**Original LTSS**: Created by [@freol35241](https://github.com/freol35241) (2019)
- Original concept and Home Assistant integration
- Repository: https://github.com/freol35241/ltss

**LTSS Turbo**: Complete performance rewrite by [@velaar](https://github.com/velaar) (2024)
- Binary COPY protocol implementation
- LRU caching system
- Smart numeric state parsing
- TimescaleDB optimization
- Metadata table for migration tracking

## 📄 License

MIT License - See LICENSE file for details

## 🆘 Support

- [Report Issues](https://github.com/velaar/ltss-turbo/issues)
- [Discussions](https://github.com/velaar/ltss-turbo/discussions)
- [Home Assistant Community](https://community.home-assistant.io/)

---

**Note**: This is a complete rewrite focused on performance. The `state_numeric` column is the key feature enabling efficient Grafana aggregations and TimescaleDB optimizations.