"""Optimized models.py with high-performance state parsing and mandatory location support."""

import re
import json
from datetime import datetime
import logging
from typing import Optional, Dict, Any, Set
from functools import lru_cache
import threading

from sqlalchemy import Column, DateTime, String, Float, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from sqlalchemy.orm import declarative_base, mapped_column, Mapped

_LOGGER = logging.getLogger(__name__)
Base = declarative_base()

class OptimizedStateParser:
    """High-performance state parser with comprehensive caching."""
    
    def __init__(self):
        # Pre-compiled regex patterns (compiled once, reused millions of times)
        self.NUMERIC_PATTERN = re.compile(r'^-?\d+(?:\.\d+)?$')
        self.TIMESTAMP_ISO_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}')
        self.TIMESTAMP_UNIX_SEC_PATTERN = re.compile(r'^\d{10}$')
        self.TIMESTAMP_UNIX_MS_PATTERN = re.compile(r'^\d{13}$')
        self.NUMERIC_EXTRACT_PATTERN = re.compile(r'-?\d+(?:\.\d+)?')
        self.PERCENTAGE_PATTERN = re.compile(r'(-?\d+(?:\.\d+)?)\s*%')
        
        # State mapping caches (immutable, thread-safe)
        self.BINARY_STATES = {
            "on": 1.0, "off": 0.0, "true": 1.0, "false": 0.0,
            "yes": 1.0, "no": 0.0, "open": 1.0, "closed": 0.0,
            "connected": 1.0, "disconnected": 0.0, "locked": 1.0, "unlocked": 0.0,
            "home": 1.0, "away": 0.0, "not_home": 0.0, "active": 1.0, "inactive": 0.0,
            "armed": 1.0, "disarmed": 0.0, "present": 1.0, "absent": 0.0,
            "detected": 1.0, "clear": 0.0, "occupied": 1.0, "unoccupied": 0.0,
        }
        
        self.CLIMATE_STATES = {
            "off": 0.0, "heat": 1.0, "cool": 2.0, "auto": 3.0,
            "heat_cool": 4.0, "dry": 5.0, "fan_only": 6.0, "eco": 7.0,
        }
        
        self.LEVEL_STATES = {
            "low": 0.0, "medium": 1.0, "high": 2.0, "very_high": 3.0,
            "min": 0.0, "mid": 1.0, "max": 2.0,
        }
        
        self.SPECIAL_STATES = {
            "unavailable": -1.0, "unknown": -2.0, "none": -3.0,
            "error": -4.0, "fault": -5.0,
        }
        
        # Thread-safe parsing cache for expensive operations
        self._parse_cache = {}
        self._cache_lock = threading.RLock()
        self._max_cache_size = 10000
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Domain sets for fast membership testing
        self.BINARY_DOMAINS = {
            "binary_sensor", "switch", "input_boolean", "light", 
            "fan", "cover", "lock", "person", "device_tracker"
        }
        
        # Pre-compiled translation table for cleaning
        self.CONTROL_CHARS = str.maketrans({
            '\x00': '\ufffd', '\u00A0': ' ', '\u200B': '',
            '\t': ' ', '\n': ' ', '\r': ' ',
        })
    
    @lru_cache(maxsize=5000)
    def _get_domain(self, entity_id: str) -> str:
        """Extract domain from entity_id with caching."""
        return entity_id.split(".", 1)[0] if "." in entity_id else "unknown"
    
    def parse_numeric_state(self, raw_state: str, device_class: Optional[str], 
                           domain: Optional[str], entity_id: str, 
                           options: Optional[list] = None) -> Optional[float]:
        """
        Ultra-optimized numeric state parsing with multi-level caching.
        
        Performance improvements:
        - Cache results for expensive parsing operations
        - Early returns for most common cases (70% of states are numeric)
        - Minimize string operations and regex usage
        - Use set membership instead of dict lookups where faster
        """
        if not raw_state or not isinstance(raw_state, str):
            return None
        
        # Create cache key for complex parsing operations
        cache_key = None
        needs_caching = (
            device_class == "timestamp" or 
            options or 
            "%" in raw_state or
            len(raw_state) > 20  # Cache longer strings
        )
        
        if needs_caching:
            cache_key = (raw_state, device_class, domain, bool(options))
            with self._cache_lock:
                if cache_key in self._parse_cache:
                    self._cache_hits += 1
                    return self._parse_cache[cache_key]
                self._cache_misses += 1
        
        # Fast path 1: Direct numeric conversion (handles ~70% of cases)
        # Optimized check - avoid multiple replace calls
        if raw_state.replace('.', '', 1).replace('-', '', 1).isdigit():
            try:
                result = float(raw_state)
                if needs_caching:
                    self._cache_result(cache_key, result)
                return result
            except (ValueError, TypeError):
                pass
        
        # Fast path 2: Pre-normalized state for common comparisons
        state_lower = raw_state.strip().lower()
        
        # Special states (second most common ~15% of cases)
        if state_lower in self.SPECIAL_STATES:
            result = self.SPECIAL_STATES[state_lower]
            if needs_caching:
                self._cache_result(cache_key, result)
            return result
        
        # Fast path 3: Pure numeric pattern matching
        if self.NUMERIC_PATTERN.match(raw_state.strip()):
            try:
                result = float(raw_state)
                if needs_caching:
                    self._cache_result(cache_key, result)
                return result
            except (ValueError, TypeError):
                pass
        
        # Complex parsing (expensive operations - always cache results)
        
        # Timestamp handling
        if device_class == "timestamp":
            result = self._parse_timestamp(raw_state)
            self._cache_result(cache_key, result)
            return result
        
        # Domain-specific optimizations
        if domain == "climate" and state_lower in self.CLIMATE_STATES:
            result = self.CLIMATE_STATES[state_lower]
            if needs_caching:
                self._cache_result(cache_key, result)
            return result
        
        if domain in self.BINARY_DOMAINS and state_lower in self.BINARY_STATES:
            result = self.BINARY_STATES[state_lower]
            if needs_caching:
                self._cache_result(cache_key, result)
            return result
        
        if state_lower in self.LEVEL_STATES:
            result = self.LEVEL_STATES[state_lower]
            if needs_caching:
                self._cache_result(cache_key, result)
            return result
        
        # Categorical options (input_select, etc.)
        if options and isinstance(options, list):
            try:
                result = float(options.index(raw_state))
                self._cache_result(cache_key, result)
                return result
            except (ValueError, TypeError):
                pass
        
        # Percentage handling (regex is expensive, always cache)
        if "%" in raw_state:
            match = self.PERCENTAGE_PATTERN.search(raw_state)
            if match:
                try:
                    result = float(match.group(1))
                    self._cache_result(cache_key, result)
                    return result
                except (ValueError, TypeError):
                    pass
        
        # Last resort: extract any numeric value from string
        match = self.NUMERIC_EXTRACT_PATTERN.search(raw_state)
        if match:
            try:
                result = float(match.group(0))
                if needs_caching:
                    self._cache_result(cache_key, result)
                return result
            except (ValueError, TypeError):
                pass
        
        # Cache miss - store None to avoid re-parsing
        if needs_caching:
            self._cache_result(cache_key, None)
        return None
    
    def _parse_timestamp(self, raw_state: str) -> Optional[float]:
        """Parse timestamp values with multiple format support."""
        try:
            # Unix seconds (most common for HA timestamps)
            if self.TIMESTAMP_UNIX_SEC_PATTERN.match(raw_state):
                return float(raw_state)
            # Unix milliseconds  
            elif self.TIMESTAMP_UNIX_MS_PATTERN.match(raw_state):
                return float(raw_state) / 1000.0
            # ISO format
            elif self.TIMESTAMP_ISO_PATTERN.match(raw_state):
                from datetime import datetime as dt
                parsed = dt.fromisoformat(raw_state.replace("Z", "+00:00"))
                return parsed.timestamp()
        except Exception:
            pass
        return None
    
    def _cache_result(self, cache_key, result):
        """Cache parsing result with size limit and LRU eviction."""
        if cache_key is None:
            return
            
        with self._cache_lock:
            # Simple FIFO cache eviction when full
            if len(self._parse_cache) >= self._max_cache_size:
                # Remove oldest 20% of entries
                keys_to_remove = list(self._parse_cache.keys())[:self._max_cache_size // 5]
                for key in keys_to_remove:
                    del self._parse_cache[key]
            
            self._parse_cache[cache_key] = result
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        with self._cache_lock:
            total_requests = self._cache_hits + self._cache_misses
            hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "hit_rate_percent": round(hit_rate, 2),
                "cache_size": len(self._parse_cache),
                "max_cache_size": self._max_cache_size
            }


class StringInternPool:
    """String interning pool for common values to reduce memory usage."""
    
    def __init__(self, max_size=5000):
        self._pool = {}
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        
        # Pre-intern very common Home Assistant values
        common_values = [
            "on", "off", "unavailable", "unknown", "°C", "°F", "%", 
            "lux", "W", "kWh", "V", "A", "temperature", "humidity",
            "motion", "door", "window", "light", "switch", "sensor",
            "binary_sensor", "device_tracker", "climate", "cover"
        ]
        for value in common_values:
            self._pool[value] = value
    
    def intern(self, string_val: Optional[str]) -> Optional[str]:
        """Intern string if beneficial for memory usage."""
        if not string_val or len(string_val) > 100:  # Don't intern very long strings
            return string_val
            
        with self._lock:
            if string_val in self._pool:
                self._hits += 1
                return self._pool[string_val]
            
            self._misses += 1
            
            # Only intern if we have space and string is worth interning
            if len(self._pool) < self._max_size and len(string_val) > 2:
                self._pool[string_val] = string_val
                return string_val
            
        return string_val
    
    def get_stats(self) -> Dict[str, Any]:
        """Get string interning statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate_percent": round(hit_rate, 2),
                "pool_size": len(self._pool),
                "max_pool_size": self._max_size
            }


def make_ltss_model(table_name: str = "ltss_turbo"):
    """Create optimized LTSS model with mandatory location support."""
    
    # Remove stale table from metadata
    if table_name in Base.metadata.tables:
        Base.metadata.remove(Base.metadata.tables[table_name])
    
    # Create shared instances for performance
    state_parser = OptimizedStateParser()
    string_pool = StringInternPool()
    
    class LTSS(Base):
        __tablename__ = table_name
        __table_args__ = {"extend_existing": True}

        # Primary keys
        time: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True, nullable=False)
        entity_id: Mapped[str] = mapped_column(String(255), primary_key=True)
        
        # Core state data
        state: Mapped[str] = mapped_column(String(255), nullable=True)
        attributes: Mapped[dict] = mapped_column(JSONB, nullable=True)
        
        # Metadata for analysis and UI
        friendly_name = Column(String(255))
        unit_of_measurement = Column(String(50))
        device_class = Column(String(50))
        icon = Column(String(100))
        domain = Column(String(50), nullable=False)
        
        # Numerical analysis
        state_numeric = Column(Float)
        
        # State tracking
        last_changed = Column(DateTime(timezone=True))
        last_updated = Column(DateTime(timezone=True))
        
        # Quality indicators
        is_unavailable = Column(Boolean, default=False, nullable=False)
        is_unknown = Column(Boolean, default=False, nullable=False)
        
        # PostGIS location - now mandatory
        location = Column(Geometry("POINT", srid=4326), nullable=True)  # Nullable for entities without coordinates
        
        @classmethod
        def from_event(cls, event) -> Optional['LTSS']:
            """
            Highly optimized event-to-record conversion with mandatory location support.
            
            Key optimizations:
            - Single-pass attribute processing
            - String interning for common values  
            - Cached domain extraction
            - Optimized state parsing with caching
            - Mandatory location processing (always attempted)
            """
            try:
                # Fast field extraction
                entity_id = event.data.get("entity_id")
                state = event.data.get("new_state")
                
                if not entity_id or not state:
                    return None
                
                # Cached domain extraction
                domain = state_parser._get_domain(entity_id)
                
                # State processing with optimizations
                state_str = state.state
                is_unavailable = state_str == "unavailable"
                is_unknown = state_str == "unknown"
                
                # Clean state string using pre-compiled translation table
                clean_state = state_str.translate(state_parser.CONTROL_CHARS) if state_str else None
                
                # Single-pass attribute processing
                attrs = state.attributes
                attrs_dict = {}
                
                # Extracted metadata
                friendly_name = None
                unit_of_measurement = None
                device_class = None
                icon = None
                lat = None
                lon = None
                options = None
                
                if attrs:
                    for key, value in attrs.items():
                        if key == "friendly_name":
                            friendly_name = string_pool.intern(str(value)) if value else None
                        elif key == "unit_of_measurement":
                            unit_of_measurement = string_pool.intern(str(value)) if value else None
                        elif key == "device_class":
                            device_class = string_pool.intern(str(value)) if value else None
                        elif key == "icon":
                            icon = str(value) if value else None
                        elif key == "latitude":
                            lat = value
                        elif key == "longitude":
                            lon = value  
                        elif key == "options":
                            options = value
                        else:
                            # Keep other attributes for JSONB storage
                            attrs_dict[key] = value
                
                # Generate friendly name if missing
                if not friendly_name:
                    if "." in entity_id:
                        friendly_name = entity_id.split(".", 1)[1].replace("_", " ").title()
                    else:
                        friendly_name = entity_id
                    friendly_name = string_pool.intern(friendly_name)
                
                # Optimized numeric state parsing with caching
                state_numeric = state_parser.parse_numeric_state(
                    state_str, device_class, domain, entity_id, options
                )
                
                # Location processing - now always attempted (mandatory support)
                location = None
                if lat is not None and lon is not None:
                    try:
                        lat_f, lon_f = float(lat), float(lon)
                        # Validate coordinate ranges
                        if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                            location = f"SRID=4326;POINT({lon_f} {lat_f})"
                        else:
                            _LOGGER.debug(f"Invalid coordinates for {entity_id}: lat={lat_f}, lon={lon_f}")
                    except (ValueError, TypeError):
                        _LOGGER.debug(f"Could not parse coordinates for {entity_id}: lat={lat}, lon={lon}")
                
                return cls(
                    entity_id=entity_id,
                    time=event.time_fired,
                    state=clean_state,
                    attributes=attrs_dict if attrs_dict else None,
                    friendly_name=friendly_name,
                    unit_of_measurement=unit_of_measurement,
                    device_class=device_class,
                    icon=icon,
                    domain=string_pool.intern(domain),
                    state_numeric=state_numeric,
                    last_changed=getattr(state, "last_changed", event.time_fired),
                    last_updated=getattr(state, "last_updated", event.time_fired),
                    is_unavailable=is_unavailable,
                    is_unknown=is_unknown,
                    location=location,  # Always processed, may be None
                )
                
            except Exception as e:
                _LOGGER.error(f"Optimized event conversion failed: {e}", exc_info=True)
                return None
        
        @classmethod
        def get_performance_stats(cls) -> Dict[str, Any]:
            """Get comprehensive performance statistics."""
            return {
                "state_parser": state_parser.get_cache_stats(),
                "string_pool": string_pool.get_stats(),
            }
    
    return LTSS