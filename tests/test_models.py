"""
Tests for video analytics data models and validation.
"""

import pytest
from datetime import datetime
from pydantic import ValidationError

from video_analytics_pipeline.schemas.models import (
    VideoAnalyticsEvent, VideoSource, EventData, EventType,
    BoundingBox, Location, ProcessingMetadata
)
from video_analytics_pipeline.utils.data_quality import DataQualityChecker, ValidationResult


class TestVideoAnalyticsModels:
    """Test video analytics data models."""
    
    def test_video_source_creation(self):
        """Test creating a video source."""
        source = VideoSource(
            source_id="camera_001",
            camera_id="cam_123"
        )
        assert source.source_id == "camera_001"
        assert source.camera_id == "cam_123"
        assert source.location is None
    
    def test_video_source_with_location(self):
        """Test creating a video source with location."""
        location = Location(
            latitude=37.4419,
            longitude=-122.1430,
            address="123 Main St, Palo Alto, CA"
        )
        source = VideoSource(
            source_id="camera_001",
            camera_id="cam_123",
            location=location
        )
        assert source.location.latitude == 37.4419
        assert source.location.longitude == -122.1430
    
    def test_bounding_box_validation(self):
        """Test bounding box validation."""
        # Valid bounding box
        bbox = BoundingBox(x=10, y=20, width=100, height=80)
        assert bbox.x == 10
        assert bbox.y == 20
        assert bbox.width == 100
        assert bbox.height == 80
        
        # Invalid bounding box (negative width)
        with pytest.raises(ValidationError):
            BoundingBox(x=10, y=20, width=-100, height=80)
    
    def test_event_data_creation(self):
        """Test creating event data."""
        bbox = BoundingBox(x=10, y=20, width=100, height=80)
        data = EventData(
            confidence=0.95,
            bounding_box=bbox,
            attributes={"model": "yolo_v5"}
        )
        assert data.confidence == 0.95
        assert data.bounding_box == bbox
        assert data.attributes["model"] == "yolo_v5"
    
    def test_video_analytics_event_creation(self):
        """Test creating a complete video analytics event."""
        source = VideoSource(source_id="camera_001", camera_id="cam_123")
        data = EventData(confidence=0.95)
        
        event = VideoAnalyticsEvent(
            event_id="test_event_001",
            timestamp=datetime.utcnow(),
            video_source=source,
            event_type=EventType.PERSON_DETECTED,
            data=data
        )
        
        assert event.event_id == "test_event_001"
        assert event.event_type == EventType.PERSON_DETECTED
        assert event.data.confidence == 0.95
    
    def test_event_type_enum(self):
        """Test event type enumeration."""
        assert EventType.PERSON_DETECTED == "person_detected"
        assert EventType.VEHICLE_DETECTED == "vehicle_detected"
        assert EventType.ANOMALY_DETECTED == "anomaly_detected"
    
    def test_timestamp_parsing(self):
        """Test timestamp parsing from various formats."""
        source = VideoSource(source_id="camera_001", camera_id="cam_123")
        data = EventData()
        
        # Test ISO format string
        event = VideoAnalyticsEvent(
            event_id="test_event_001",
            timestamp="2023-12-01T10:30:00Z",
            video_source=source,
            event_type=EventType.MOTION_DETECTED,
            data=data
        )
        
        assert isinstance(event.timestamp, datetime)
    
    def test_confidence_validation(self):
        """Test confidence score validation."""
        # Valid confidence
        data = EventData(confidence=0.95)
        assert data.confidence == 0.95
        
        # Invalid confidence (too high)
        with pytest.raises(ValidationError):
            EventData(confidence=1.5)
        
        # Invalid confidence (negative)
        with pytest.raises(ValidationError):
            EventData(confidence=-0.1)


class TestDataQualityChecker:
    """Test data quality validation functionality."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.checker = DataQualityChecker()
        self.valid_event = VideoAnalyticsEvent(
            event_id="test_event_001",
            timestamp=datetime.utcnow(),
            video_source=VideoSource(source_id="camera_001", camera_id="cam_123"),
            event_type=EventType.PERSON_DETECTED,
            data=EventData(
                confidence=0.95,
                bounding_box=BoundingBox(x=10, y=20, width=100, height=80)
            )
        )
    
    def test_valid_event_validation(self):
        """Test validation of a valid event."""
        result = self.checker.validate_event(self.valid_event)
        
        assert isinstance(result, ValidationResult)
        assert result.is_valid
        assert len(result.errors) == 0
        assert result.score > 0.8
    
    def test_confidence_validation(self):
        """Test confidence validation rules."""
        # High confidence event
        high_conf_event = self.valid_event.copy()
        high_conf_event.data.confidence = 0.99
        result = self.checker.validate_event(high_conf_event)
        assert result.is_valid
        
        # Low confidence event (should generate warning)
        low_conf_event = self.valid_event.copy()
        low_conf_event.data.confidence = 0.1
        result = self.checker.validate_event(low_conf_event)
        assert len(result.warnings) > 0
    
    def test_bounding_box_validation(self):
        """Test bounding box validation rules."""
        # Event with very small bounding box
        small_bbox_event = self.valid_event.copy()
        small_bbox_event.data.bounding_box = BoundingBox(x=10, y=20, width=5, height=5)
        result = self.checker.validate_event(small_bbox_event)
        assert len(result.warnings) > 0
    
    def test_event_consistency_validation(self):
        """Test event consistency validation."""
        # Detection event without confidence
        inconsistent_event = VideoAnalyticsEvent(
            event_id="test_event_002",
            timestamp=datetime.utcnow(),
            video_source=VideoSource(source_id="camera_001", camera_id="cam_123"),
            event_type=EventType.PERSON_DETECTED,
            data=EventData()  # No confidence score
        )
        
        result = self.checker.validate_event(inconsistent_event)
        assert len(result.warnings) > 0
    
    def test_empty_source_id_validation(self):
        """Test validation of empty source IDs."""
        invalid_event = self.valid_event.copy()
        invalid_event.video_source.source_id = ""
        
        result = self.checker.validate_event(invalid_event)
        assert not result.is_valid
        assert len(result.errors) > 0


class TestEventSerialization:
    """Test event serialization and deserialization."""
    
    def test_event_to_dict(self):
        """Test converting event to dictionary."""
        source = VideoSource(source_id="camera_001", camera_id="cam_123")
        data = EventData(confidence=0.95)
        
        event = VideoAnalyticsEvent(
            event_id="test_event_001",
            timestamp=datetime.utcnow(),
            video_source=source,
            event_type=EventType.PERSON_DETECTED,
            data=data
        )
        
        event_dict = event.dict()
        
        assert event_dict["event_id"] == "test_event_001"
        assert event_dict["event_type"] == "person_detected"
        assert event_dict["data"]["confidence"] == 0.95
    
    def test_event_from_dict(self):
        """Test creating event from dictionary."""
        event_dict = {
            "event_id": "test_event_001",
            "timestamp": "2023-12-01T10:30:00Z",
            "video_source": {
                "source_id": "camera_001",
                "camera_id": "cam_123"
            },
            "event_type": "person_detected",
            "data": {
                "confidence": 0.95
            }
        }
        
        event = VideoAnalyticsEvent(**event_dict)
        
        assert event.event_id == "test_event_001"
        assert event.event_type == EventType.PERSON_DETECTED
        assert event.data.confidence == 0.95
    
    def test_event_json_serialization(self):
        """Test JSON serialization with custom encoder."""
        import json
        
        source = VideoSource(source_id="camera_001", camera_id="cam_123")
        data = EventData(confidence=0.95)
        
        event = VideoAnalyticsEvent(
            event_id="test_event_001",
            timestamp=datetime.utcnow(),
            video_source=source,
            event_type=EventType.PERSON_DETECTED,
            data=data
        )
        
        # Test that JSON serialization works
        json_str = json.dumps(event.dict(), default=str)
        assert "test_event_001" in json_str
        assert "person_detected" in json_str


if __name__ == "__main__":
    pytest.main([__file__])