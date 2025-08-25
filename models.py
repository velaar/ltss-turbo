"""Models for SQLAlchemy - Optimized LTSS Schema."""

import re
import json
from datetime import datetime
import logging
from typing import Optional, Dict, Any

from sqlalchemy import (
    Column,
    DateTime,
    String,
    Float,
    Boolean,
    text,
)
from sqlalchemy.schema import Index
from sqlalchemy.dialects.postgresql import JSONB
from geoalchemy2 import Geometry
from sqlalchemy.orm import column_property, declarative_base

# SQLAlchemy Schema
Base = declarative_base()

_LOGGER = logging.getLogger(__name__)

# Compiled regex patterns for better performance
NUMERIC_PATTERN = re.compile(r'^-?\d+(?:\.\d+)?$')
TIMESTAMP_ISO_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}')
TIMESTAMP_UNIX_SEC_PATTERN = re.compile(r'^\d{10}$')
TIMESTAMP_UNIX_MS_PATTERN = re.compile(r'^\d{13}$')
NUMERIC_EXTRACT_PATTERN = re.compile(r'-?\d+(?:\.\d+)?')

# Domain-specific state mappings cached for performance
BINARY_STATES = {
    "on": 1.0, "off": 0.0,
    "true": 1.0, "false": 0.0,
    "yes": 1.0, "no": 0.0,
    "open": 1.0, "closed": 0.0,
    "connected": 1.0, "disconnected": 0.0,
    "locked": 1.0, "unlocked": 0.0,
    "home": 1.0, "away": 0.0, "not_home": 0.0,
    "active": 1.0, "inactive": 0.0,
    "armed": 1.0, "disarmed": 0.0,
    "present": 1.0, "absent": 0.0,
    "detected": 1.0, "clear": 0.0,
    "occupied": 1.0, "unoccupied": 0.0,
}

CLIMATE_STATES = {
    "off": 0.0, "heat": 1.0, "cool": 2.0, "auto": 3.0,
    "heat_cool": 4.0, "dry": 5.0, "fan_only": 6.0, "eco": 7.0,
}

LEVEL_STATES = {
    "low": 0.0, "medium": 1.0, "high": 2.0, "very_high": 3.0,
    "min": 0.0, "mid": 1.0, "max": 2.0,
}

SPECIAL_STATES = {
    "unavailable": -1.0,
    "unknown": -2.0,
    "none": -3.0,
    "error": -4.0,
    "fault": -5.0,
}


class LTSS(Base):
    """Optimized state change history for time-series storage."""

    __tablename__ = "ltss"
    
    # Primary keys - optimized for TimescaleDB partitioning
    time = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    entity_id = Column(String(255), primary_key=True, nullable=False)
    
    # Core state data
    state = Column(String(255), nullable=False)
    attributes = Column(JSONB)
    
    # Essential metadata for analysis
    friendly_name = Column(String(255))
    unit_of_measurement = Column(String(50))
    device_class = Column(String(50))
    icon = Column(String(100))
    domain = Column(String(50), nullable=False)
    
    # Numerical state for efficient aggregations
    state_numeric = Column(Float)
    
    # State tracking
    last_changed = Column(DateTime(timezone=True))
    last_updated = Column(DateTime(timezone=True))
    
    # Quality indicators
    is_unavailable = Column(Boolean, default=False, nullable=False)
    is_unknown = Column(Boolean, default=False, nullable=False)
    
    # Optional location (activated via PostGIS)
    location = None

    @classmethod
    def activate_location_extraction(cls):
        """Enable PostGIS location support."""
        cls.location = column_property(Column(Geometry("POINT", srid=4326)))

    @classmethod
    def parse_numeric_state(cls, raw_state: str, device_class: Optional[str], 
                          domain: Optional[str], entity_id: str, 
                          options: Optional[list] = None) -> Optional[float]:
        """
        Optimized numeric state parsing with caching and early returns.
        """
        if not raw_state or not isinstance(raw_state, str):
            return None
            
        state_lower = raw_state.strip().lower()
        
        # 1. Check special states first (most common)
        if state_lower in SPECIAL_STATES:
            return SPECIAL_STATES[state_lower]
        
        # 2. Try pure numeric conversion (fast path)
        if NUMERIC_PATTERN.match(raw_state.strip()):
            try:
                return float(raw_state)
            except (ValueError, TypeError):
                pass
        
        # 3. Handle timestamps
        if device_class == "timestamp":
            try:
                # Unix seconds
                if TIMESTAMP_UNIX_SEC_PATTERN.match(raw_state):
                    return float(raw_state)
                # Unix milliseconds
                elif TIMESTAMP_UNIX_MS_PATTERN.match(raw_state):
                    return float(raw_state) / 1000.0
                # ISO format
                elif TIMESTAMP_ISO_PATTERN.match(raw_state):
                    from datetime import datetime as dt
                    parsed = dt.fromisoformat(raw_state.replace("Z", "+00:00"))
                    return parsed.timestamp()
            except Exception:
                pass
        
        # 4. Domain-specific mappings
        if domain == "climate" and state_lower in CLIMATE_STATES:
            return CLIMATE_STATES[state_lower]
        
        if domain in {"binary_sensor", "switch", "input_boolean", "light", 
                     "fan", "cover", "lock", "person", "device_tracker"}:
            if state_lower in BINARY_STATES:
                return BINARY_STATES[state_lower]
        
        if state_lower in LEVEL_STATES:
            return LEVEL_STATES[state_lower]
        
        # 5. Categorical options (e.g., input_select)
        if options and isinstance(options, list) and raw_state in options:
            try:
                return float(options.index(raw_state))
            except (ValueError, TypeError):
                pass
        
        # 6. Extract numeric from string (last resort)
        # Clean non-breaking spaces and zero-width spaces
        cleaned = raw_state.replace("\u00A0", " ").replace("\u200B", "")
        match = NUMERIC_EXTRACT_PATTERN.search(cleaned)
        if match:
            try:
                return float(match.group(0))
            except (ValueError, TypeError):
                pass
        
        return None

    @classmethod
    def from_event(cls, event) -> Optional['LTSS']:
        """Create LTSS record from Home Assistant state_changed event."""
        try:
            entity_id = event.data.get("entity_id")
            state = event.data.get("new_state")
            
            if not entity_id or not state:
                return None
            
            # Extract domain efficiently
            domain = entity_id.split(".", 1)[0] if "." in entity_id else "unknown"
            
            # Get attributes safely
            attrs = dict(state.attributes) if state.attributes else {}
            
            # Extract metadata
            friendly_name = attrs.get("friendly_name")
            if not friendly_name or not friendly_name.strip():
                # Generate friendly name from entity_id
                if "." in entity_id:
                    friendly_name = entity_id.split(".", 1)[1].replace("_", " ").title()
                else:
                    friendly_name = entity_id
            
            unit_of_measurement = attrs.get("unit_of_measurement")
            device_class = attrs.get("device_class")
            icon = attrs.get("icon")
            
            # Parse numeric state
            state_numeric = cls.parse_numeric_state(
                state.state,
                device_class,
                domain,
                entity_id,
                attrs.get("options")
            )
            
            # Quality flags
            state_lower = state.state.lower() if state.state else ""
            is_unavailable = state_lower == "unavailable"
            is_unknown = state_lower == "unknown"
            
            # Handle location if PostGIS is enabled
            location = None
            if cls.location is not None:
                lat = attrs.pop("latitude", None)
                lon = attrs.pop("longitude", None)
                
                if lat is not None and lon is not None:
                    try:
                        lat_f = float(lat)
                        lon_f = float(lon)
                        if -90 <= lat_f <= 90 and -180 <= lon_f <= 180:
                            location = f"SRID=4326;POINT({lon_f} {lat_f})"
                    except (ValueError, TypeError):
                        _LOGGER.debug(f"Invalid coordinates for {entity_id}: lat={lat}, lon={lon}")
            
            # Remove extracted fields from attributes to save space
            for key in ["friendly_name", "unit_of_measurement", "device_class", 
                       "icon", "latitude", "longitude"]:
                attrs.pop(key, None)
            
            # Clean state string (remove null bytes)
            clean_state = state.state.replace("\x00", "\ufffd") if state.state else None
            
            return cls(
                entity_id=entity_id,
                time=event.time_fired,
                state=clean_state,
                attributes=attrs if attrs else None,  # Don't store empty dicts
                friendly_name=friendly_name,
                unit_of_measurement=unit_of_measurement,
                device_class=device_class,
                icon=icon,
                domain=domain,
                state_numeric=state_numeric,
                last_changed=getattr(state, "last_changed", event.time_fired),
                last_updated=getattr(state, "last_updated", event.time_fired),
                is_unavailable=is_unavailable,
                is_unknown=is_unknown,
                location=location,
            )
            
        except Exception as e:
            _LOGGER.error(f"Error creating LTSS record from event: {e}", exc_info=True)
            return None


# Optimized indexes for common query patterns
# Primary index for entity queries (most common in Grafana)
LTSS_entityid_time_idx = Index(
    "ltss_entityid_time_idx", 
    LTSS.entity_id, 
    LTSS.time.desc()
)

# Domain queries (e.g., all sensors)
LTSS_domain_time_idx = Index(
    "ltss_domain_time_idx", 
    LTSS.domain, 
    LTSS.time.desc()
)

# Device class queries (e.g., all temperature sensors)
LTSS_device_class_time_idx = Index(
    "ltss_device_class_time_idx", 
    LTSS.device_class, 
    LTSS.time.desc(),
    postgresql_where=LTSS.device_class.isnot(None)
)

# Numeric state queries for aggregations
LTSS_numeric_state_idx = Index(
    "ltss_numeric_state_idx", 
    LTSS.entity_id, 
    LTSS.state_numeric,
    LTSS.time.desc(),
    postgresql_where=LTSS.state_numeric.isnot(None)
)

# JSONB GIN index for attribute queries
LTSS_attributes_idx = Index(
    "ltss_attributes_idx", 
    LTSS.attributes, 
    postgresql_using="gin",
    postgresql_where=LTSS.attributes.isnot(None)
)

# Quality/availability queries
LTSS_quality_idx = Index(
    "ltss_quality_idx",
    LTSS.is_unavailable,
    LTSS.is_unknown,
    LTSS.time.desc()
)