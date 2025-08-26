"""Optimized models.py with LRU cache and enhanced memory management."""

import re
import json
from datetime import datetime
import logging
from typing import Optional, Dict, Any, Set, Tuple
from functools import lru_cache
import threading
from collections import OrderedDict

from sqlalchemy import Column, DateTime, String, Float, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, mapped_column, Mapped
from geoalchemy2 import Geometry

_LOGGER = logging.getLogger(__name__)

Base = declarative_base()


class LRUCache:
    """Thread-safe LRU cache implementation."""
    
    def __init__(self, max_size: int = 10000):
        self._cache = OrderedDict()
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
    
    def get(self, key: Any) -> Any:
        """Get value from cache, updating LRU order."""
        with self._lock:
            if key in self._cache:
                # Move to end (most recently used)
                self._cache.move_to_end(key)
                self._hits += 1
                return self._cache[key]
            self._misses += 1
            return None
    
    def put(self, key: Any, value: Any):
        """Store value in cache with LRU eviction."""
        with self._lock:
            if key in self._cache:
                # Update and move to end
                self._cache[key] = value
                self._cache.move_to_end(key)
            else:
                # Add new entry
                if len(self._cache) >= self._max_size:
                    # Remove least recently used (10% to avoid frequent evictions)
                    evict_count = max(1, self._max_size // 10)
                    for _ in range(evict_count):
                        self._cache.popitem(last=False)
                self._cache[key] = value
    
    def clear(self):
        """Clear the cache."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
    
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


class OptimizedStateParser:
    """High-performance state parser with LRU caching."""
    
    def __init__(self):
        # Pre-compiled regex patterns
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
            "above": 1.0, "below": 0.0, "triggered": 1.0, "untriggered": 0.0,
            "enabled": 1.0, "disabled": 0.0, "running": 1.0, "stopped": 0.0,
        }
        
        self.CLIMATE_STATES = {
            "off": 0.0, "heat": 1.0, "cool": 2.0, "auto": 3.0,
            "heat_cool": 4.0, "dry": 5.0, "fan_only": 6.0, "eco": 7.0,
        }
        
        self.LEVEL_STATES = {
            "low": 0.0, "medium": 1.0, "high": 2.0, "very_high": 3.0,
            "min": 0.0, "mid": 1.0, "max": 2.0, "auto": -1.0,
            "quiet": 0.0, "normal": 1.0, "boost": 2.0,
        }
        
        self.SPECIAL_STATES = {
            "unavailable": -1.0, "unknown": -2.0, "none": -3.0,
            "error": -4.0, "fault": -5.0, "problem": -6.0,
        }
        
        # Combine all static mappings for O(1) lookup
        self.ALL_STATIC_STATES = {
            **self.BINARY_STATES,
            **self.CLIMATE_STATES,
            **self.LEVEL_STATES,
            **self.SPECIAL_STATES,
        }
        
        # LRU cache for expensive parsing operations
        self._parse_cache = LRUCache(max_size=10000)
        
        # Domain sets for fast membership testing
        self.BINARY_DOMAINS = frozenset([
            "binary_sensor", "switch", "input_boolean", "light", 
            "fan", "cover", "lock", "person", "device_tracker", "group"
        ])
        
        self.NUMERIC_DOMAINS = frozenset([
            "sensor", "number", "input_number", "counter"
        ])
        
        # Pre-compiled translation table for cleaning
        self.CONTROL_CHARS = str.maketrans({
            '\x00': '\ufffd', '\u00A0': ' ', '\u200B': '',
            '\t': ' ', '\n': ' ', '\r': ' ',
        })
        
        # Cache for domain extraction
        self._domain_cache = {}
        self._domain_cache_lock = threading.RLock()
    
    def get_domain(self, entity_id: str) -> str:
        """Extract domain from entity_id with caching."""
        with self._domain_cache_lock:
            if entity_id in self._domain_cache:
                return self._domain_cache[entity_id]
            
            domain = entity_id.split(".", 1)[0] if "." in entity_id else "unknown"
            
            # Cache if not too large
            if len(self._domain_cache) < 5000:
                self._domain_cache[entity_id] = domain
            
            return domain
    
    def parse_numeric_state(
        self, 
        raw_state: str, 
        device_class: Optional[str], 
        domain: Optional[str], 
        entity_id: str, 
        options: Optional[list] = None
    ) -> Optional[float]:
        """
        Ultra-optimized numeric state parsing with LRU caching.
        
        Performance improvements:
        - LRU cache for expensive operations
        - Early returns for common cases
        - Combined static state lookup
        - Minimized string operations
        """
        if not raw_state or not isinstance(raw_state, str):
            return None
        
        # Fast path 1: Check static mappings first (O(1) lookup)
        state_lower = raw_state.strip().lower()
        if state_lower in self.ALL_STATIC_STATES:
            return self.ALL_STATIC_STATES[state_lower]
        
        # Fast path 2: Direct numeric conversion (most common case ~60%)
        # Optimized check - single pass character validation
        clean_state = raw_state.strip()
        if clean_state:
            # Quick numeric check
            is_numeric = True
            decimal_count = 0
            minus_count = 0
            
            for i, char in enumerate(clean_state):
                if char == '.':
                    decimal_count += 1
                    if decimal_count > 1:
                        is_numeric = False
                        break
                elif char == '-':
                    minus_count += 1
                    if minus_count > 1 or i > 0:
                        is_numeric = False
                        break
                elif not char.isdigit():
                    is_numeric = False
                    break
            
            if is_numeric:
                try:
                    return float(clean_state)
                except (ValueError, TypeError):
                    pass
        
        # Check cache for expensive operations
        cache_key = (raw_state, device_class, domain, bool(options))
        cached_result = self._parse_cache.get(cache_key)
        if cached_result is not None:
            return cached_result if cached_result != "NULL" else None
        
        # Complex parsing (expensive operations)
        result = self._parse_complex_state(
            raw_state, device_class, domain, entity_id, options, state_lower
        )
        
        # Cache the result (use "NULL" marker for None to differentiate from cache miss)
        self._parse_cache.put(cache_key, result if result is not None else "NULL")
        
        return result
    
    def _parse_complex_state(
        self,
        raw_state: str,
        device_class: Optional[str],
        domain: Optional[str],
        entity_id: str,
        options: Optional[list],
        state_lower: str
    ) -> Optional[float]:
        """Handle complex state parsing that requires regex or special logic."""
        
        # Timestamp handling
        if device_class == "timestamp":
            return self._parse_timestamp(raw_state)
        
        # Categorical options (input_select, etc.)
        if options and isinstance(options, list):
            try:
                return float(options.index(raw_state))
            except (ValueError, TypeError):
                pass
        
        # Percentage handling
        if "%" in raw_state:
            match = self.PERCENTAGE_PATTERN.search(raw_state)
            if match:
                try:
                    return float(match.group(1))
                except (ValueError, TypeError):
                    pass
        
        # Try pure numeric pattern
        if self.NUMERIC_PATTERN.match(raw_state.strip()):
            try:
                return float(raw_state)
            except (ValueError, TypeError):
                pass
        
        # Last resort: extract any numeric value from string
        match = self.NUMERIC_EXTRACT_PATTERN.search(raw_state)
        if match:
            try:
                return float(match.group(0))
            except (ValueError, TypeError):
                pass
        
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
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        return self._parse_cache.get_stats()
    
    def clear_cache(self):
        """Clear all caches."""
        self._parse_cache.clear()
        with self._domain_cache_lock:
            self._domain_cache.clear()


class StringInternPool:
    """Optimized string interning pool with LRU eviction."""
    
    def __init__(self, max_size=5000):
        self._pool = OrderedDict()
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        
        # Pre-intern very common Home Assistant values
        common_values = [
            "on", "off", "unavailable", "unknown", "°C", "°F", "%", 
            "lux", "W", "kWh", "V", "A", "temperature", "humidity",
            "motion", "door", "window", "light", "switch", "sensor",
            "binary_sensor", "device_tracker", "climate", "cover",
            "true", "false", "open", "closed", "home", "away",
            "locked", "unlocked", "triggered", "clear", "detected",
        ]
        for value in common_values:
            self._pool[value] = value
    
    def intern(self, string_val: Optional[str]) -> Optional[str]:
        """Intern string with LRU eviction."""
        if not string_val:
            return string_val
            
        # Don't intern very long strings or URLs
        if len(string_val) > 100 or '://' in string_val:
            return string_val
            
        with self._lock:
            if string_val in self._pool:
                # Move to end (most recently used)
                self._pool.move_to_end(string_val)
                self._hits += 1
                return self._pool[string_val]
            
            self._misses += 1
            
            # Add to pool with LRU eviction
            if len(self._pool) >= self._max_size:
                # Remove least recently used (10% for efficiency)
                evict_count = max(1, self._max_size // 10)
                for _ in range(evict_count):
                    self._pool.popitem(last=False)
            
            self._pool[string_val] = string_val
            return string_val
    
    def get_stats(self) -> Dict[str, Any]:
        """Get string interning statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": round(hit_rate, 2),
                "pool_size": len(self._pool),
                "max_pool_size": self._max_size
            }
    
    def clear(self):
        """Clear the intern pool."""
        with self._lock:
            # Keep common values
            common = ["on", "off", "unavailable", "unknown", "true", "false"]
            temp = {k: v for k, v in self._pool.items() if k in common}
            self._pool.clear()
            self._pool.update(temp)
            self._hits = 0
            self._misses = 0


def make_ltss_model(table_name: str = "ltss_turbo"):
    """Create optimized LTSS model with enhanced performance features."""
    
    # Remove stale table from metadata
    if table_name in Base.metadata.tables:
        Base.metadata.remove(Base.metadata.tables[table_name])
    
    # Create shared instances for performance (singleton pattern)
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
        
        # Optional location column (PostGIS)
        location = Column(Geometry("POINT", srid=4326), nullable=True)
        
        @classmethod
        def from_event(cls, event) -> Optional['LTSS']:
            """
            Highly optimized event-to-record conversion.
            
            Key optimizations:
            - Single-pass attribute processing
            - String interning with LRU
            - Cached domain extraction
            - LRU cached state parsing
            - Efficient location processing
            """
            try:
                # Fast field extraction
                entity_id = event.data.get("entity_id")
                state = event.data.get("new_state")
                
                if not entity_id or not state:
                    return None
                
                # Get domain with caching
                domain = state_parser.get_domain(entity_id)
                
                # State processing
                state_str = state.state
                is_unavailable = state_str == "unavailable"
                is_unknown = state_str == "unknown"
                
                # Clean state string (only if needed)
                if state_str and any(ord(c) < 32 for c in state_str):
                    clean_state = state_str.translate(state_parser.CONTROL_CHARS)
                else:
                    clean_state = state_str
                
                # Single-pass attribute processing with early extraction
                attrs = state.attributes
                attrs_dict = {}
                
                # Pre-initialize to avoid repeated checks
                friendly_name = None
                unit_of_measurement = None
                device_class = None
                icon = None
                lat = None
                lon = None
                options = None
                
                if attrs:
                    # Process attributes in priority order
                    for key, value in attrs.items():
                        if key == "friendly_name":
                            friendly_name = string_pool.intern(str(value)[:255]) if value else None
                        elif key == "unit_of_measurement":
                            unit_of_measurement = string_pool.intern(str(value)[:50]) if value else None
                        elif key == "device_class":
                            device_class = string_pool.intern(str(value)[:50]) if value else None
                        elif key == "icon":
                            icon = str(value)[:100] if value else None
                        elif key == "latitude":
                            lat = value
                        elif key == "longitude":
                            lon = value
                        elif key == "options":
                            options = value
                        else:
                            # Store other attributes (limit size to save memory)
                            if len(str(value)) < 1000:  # Skip very large attributes
                                attrs_dict[key] = value
                
                # Generate friendly name if missing
                if not friendly_name:
                    if "." in entity_id:
                        friendly_name = entity_id.split(".", 1)[1].replace("_", " ").title()
                    else:
                        friendly_name = entity_id
                    friendly_name = string_pool.intern(friendly_name[:255])
                
                # Optimized numeric state parsing with LRU cache
                state_numeric = state_parser.parse_numeric_state(
                    state_str, device_class, domain, entity_id, options
                )
                
                # Location processing (optional)
                location = None
                if lat is not None and lon is not None:
                    try:
                        lat_f, lon_f = float(lat), float(lon)
                        # Validate coordinate ranges
                        if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                            # Round to 6 decimal places (11cm precision)
                            lat_f = round(lat_f, 6)
                            lon_f = round(lon_f, 6)
                            location = f"SRID=4326;POINT({lon_f} {lat_f})"
                    except (ValueError, TypeError):
                        pass  # Skip invalid coordinates
                
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
                    location=location,
                )
                
            except Exception as e:
                _LOGGER.error(f"Event conversion failed for {entity_id}: {e}")
                return None
        
        @classmethod
        def get_performance_stats(cls) -> Dict[str, Any]:
            """Get comprehensive performance statistics."""
            return {
                "state_parser": state_parser.get_cache_stats(),
                "string_pool": string_pool.get_stats(),
            }
        
        @classmethod
        def clear_caches(cls):
            """Clear all caches to free memory."""
            state_parser.clear_cache()
            string_pool.clear()
            _LOGGER.info("LTSS model caches cleared")
    
    return LTSS