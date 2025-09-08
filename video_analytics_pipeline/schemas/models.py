"""
Data models for video analytics events using Pydantic for validation.
"""

from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field, validator


class EventType(str, Enum):
    """Enumeration of supported video analytics event types."""
    MOTION_DETECTED = "motion_detected"
    PERSON_DETECTED = "person_detected"
    VEHICLE_DETECTED = "vehicle_detected"
    ANOMALY_DETECTED = "anomaly_detected"
    PERFORMANCE_METRIC = "performance_metric"
    SYSTEM_EVENT = "system_event"


class Location(BaseModel):
    """Geographic location information."""
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None


class VideoSource(BaseModel):
    """Video source information."""
    source_id: str = Field(..., description="Unique identifier for the video source")
    camera_id: str = Field(..., description="Camera or device identifier")
    location: Optional[Location] = None


class BoundingBox(BaseModel):
    """Bounding box coordinates for detected objects."""
    x: float = Field(..., ge=0)
    y: float = Field(..., ge=0)
    width: float = Field(..., gt=0)
    height: float = Field(..., gt=0)


class EventData(BaseModel):
    """Event-specific data payload."""
    confidence: Optional[float] = Field(None, ge=0, le=1)
    bounding_box: Optional[BoundingBox] = None
    metrics: Optional[Dict[str, float]] = None
    attributes: Optional[Dict[str, Any]] = None


class ProcessingMetadata(BaseModel):
    """Metadata about the processing pipeline."""
    pipeline_version: Optional[str] = None
    processing_time: Optional[datetime] = None
    model_version: Optional[str] = None


class VideoAnalyticsEvent(BaseModel):
    """Main video analytics event model."""
    event_id: str = Field(..., description="Unique identifier for the event")
    timestamp: datetime = Field(..., description="When the event occurred")
    video_source: VideoSource
    event_type: EventType
    data: EventData
    processing_metadata: Optional[ProcessingMetadata] = None
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        """Parse timestamp from various formats."""
        if isinstance(v, str):
            try:
                return datetime.fromisoformat(v.replace('Z', '+00:00'))
            except ValueError:
                return datetime.fromisoformat(v)
        return v
    
    class Config:
        """Pydantic configuration."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
        use_enum_values = True