"""
Data quality validation utilities for video analytics events.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from jsonschema import validate, ValidationError as JsonSchemaValidationError
from pydantic import ValidationError

from ..schemas.models import VideoAnalyticsEvent


@dataclass
class ValidationResult:
    """Result of data quality validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    score: float  # Quality score from 0.0 to 1.0


class DataQualityChecker:
    """Comprehensive data quality checker for video analytics events."""
    
    def __init__(self):
        self.schema = self._load_json_schema()
        self.validation_rules = self._initialize_validation_rules()
    
    def _load_json_schema(self) -> Dict[str, Any]:
        """Load the JSON schema for video events."""
        # In a real implementation, this would load from the schema file
        # For now, return a basic schema structure
        return {
            "type": "object",
            "required": ["event_id", "timestamp", "video_source", "event_type", "data"],
            "properties": {
                "event_id": {"type": "string", "minLength": 1},
                "timestamp": {"type": "string", "format": "date-time"},
                "video_source": {
                    "type": "object",
                    "required": ["source_id", "camera_id"],
                    "properties": {
                        "source_id": {"type": "string", "minLength": 1},
                        "camera_id": {"type": "string", "minLength": 1}
                    }
                },
                "event_type": {
                    "type": "string",
                    "enum": [
                        "motion_detected", "person_detected", "vehicle_detected",
                        "anomaly_detected", "performance_metric", "system_event"
                    ]
                },
                "data": {"type": "object"}
            }
        }
    
    def _initialize_validation_rules(self) -> Dict[str, callable]:
        """Initialize custom validation rules."""
        return {
            "timestamp_validity": self._validate_timestamp,
            "confidence_range": self._validate_confidence,
            "bounding_box_validity": self._validate_bounding_box,
            "event_consistency": self._validate_event_consistency,
            "source_integrity": self._validate_source_integrity,
        }
    
    def validate_event(self, event: VideoAnalyticsEvent) -> ValidationResult:
        """
        Perform comprehensive validation of a video analytics event.
        
        Args:
            event: Video analytics event to validate
            
        Returns:
            ValidationResult: Detailed validation result
        """
        errors = []
        warnings = []
        quality_score = 1.0
        
        try:
            # Convert event to dict for JSON schema validation
            event_dict = event.dict()
            
            # JSON Schema validation
            try:
                validate(instance=event_dict, schema=self.schema)
            except JsonSchemaValidationError as e:
                errors.append(f"Schema validation failed: {e.message}")
                quality_score -= 0.3
            
            # Custom validation rules
            for rule_name, rule_func in self.validation_rules.items():
                try:
                    rule_result = rule_func(event)
                    if rule_result["errors"]:
                        errors.extend(rule_result["errors"])
                        quality_score -= rule_result["severity"] * 0.2
                    if rule_result["warnings"]:
                        warnings.extend(rule_result["warnings"])
                        quality_score -= rule_result["severity"] * 0.1
                except Exception as e:
                    errors.append(f"Validation rule '{rule_name}' failed: {str(e)}")
                    quality_score -= 0.1
            
            # Ensure quality score doesn't go below 0
            quality_score = max(0.0, quality_score)
            
            is_valid = len(errors) == 0
            
        except Exception as e:
            errors.append(f"Unexpected validation error: {str(e)}")
            quality_score = 0.0
            is_valid = False
        
        return ValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            score=quality_score
        )
    
    def _validate_timestamp(self, event: VideoAnalyticsEvent) -> Dict[str, Any]:
        """Validate timestamp fields."""
        errors = []
        warnings = []
        severity = 0.8  # High severity for timestamp issues
        
        try:
            # Check if timestamp is not too far in the future
            now = datetime.utcnow()
            if event.timestamp > now + timedelta(minutes=5):
                errors.append("Timestamp is too far in the future")
            
            # Check if timestamp is not too old (e.g., more than 7 days)
            if event.timestamp < now - timedelta(days=7):
                warnings.append("Timestamp is quite old (more than 7 days)")
            
            # Check processing metadata timestamp
            if (event.processing_metadata and 
                event.processing_metadata.processing_time and
                event.processing_metadata.processing_time < event.timestamp):
                warnings.append("Processing time is before event timestamp")
                
        except Exception as e:
            errors.append(f"Timestamp validation error: {str(e)}")
        
        return {"errors": errors, "warnings": warnings, "severity": severity}
    
    def _validate_confidence(self, event: VideoAnalyticsEvent) -> Dict[str, Any]:
        """Validate confidence scores."""
        errors = []
        warnings = []
        severity = 0.5  # Medium severity
        
        try:
            if event.data.confidence is not None:
                # Check confidence range
                if not (0.0 <= event.data.confidence <= 1.0):
                    errors.append(f"Confidence score {event.data.confidence} is out of range [0,1]")
                
                # Warn about very low confidence for detection events
                if (event.event_type in ["person_detected", "vehicle_detected"] and
                    event.data.confidence < 0.3):
                    warnings.append("Very low confidence for detection event")
                
        except Exception as e:
            errors.append(f"Confidence validation error: {str(e)}")
        
        return {"errors": errors, "warnings": warnings, "severity": severity}
    
    def _validate_bounding_box(self, event: VideoAnalyticsEvent) -> Dict[str, Any]:
        """Validate bounding box coordinates."""
        errors = []
        warnings = []
        severity = 0.6  # Medium-high severity
        
        try:
            if event.data.bounding_box is not None:
                bbox = event.data.bounding_box
                
                # Check for negative coordinates
                if bbox.x < 0 or bbox.y < 0:
                    errors.append("Bounding box has negative coordinates")
                
                # Check for zero or negative dimensions
                if bbox.width <= 0 or bbox.height <= 0:
                    errors.append("Bounding box has invalid dimensions")
                
                # Warn about very small bounding boxes
                if bbox.width < 10 or bbox.height < 10:
                    warnings.append("Bounding box is very small")
                
                # Warn about very large bounding boxes (assuming video resolution limits)
                if bbox.width > 3840 or bbox.height > 2160:
                    warnings.append("Bounding box dimensions exceed typical video resolution")
                
        except Exception as e:
            errors.append(f"Bounding box validation error: {str(e)}")
        
        return {"errors": errors, "warnings": warnings, "severity": severity}
    
    def _validate_event_consistency(self, event: VideoAnalyticsEvent) -> Dict[str, Any]:
        """Validate consistency between event type and data."""
        errors = []
        warnings = []
        severity = 0.7  # High severity for consistency issues
        
        try:
            # Detection events should have confidence scores
            if (event.event_type in ["person_detected", "vehicle_detected", "motion_detected"] and
                event.data.confidence is None):
                warnings.append(f"Detection event '{event.event_type}' missing confidence score")
            
            # Detection events should have bounding boxes for visual detections
            if (event.event_type in ["person_detected", "vehicle_detected"] and
                event.data.bounding_box is None):
                warnings.append(f"Visual detection event '{event.event_type}' missing bounding box")
            
            # Performance metrics should have metrics data
            if (event.event_type == "performance_metric" and
                (not event.data.metrics or len(event.data.metrics) == 0)):
                errors.append("Performance metric event missing metrics data")
            
        except Exception as e:
            errors.append(f"Event consistency validation error: {str(e)}")
        
        return {"errors": errors, "warnings": warnings, "severity": severity}
    
    def _validate_source_integrity(self, event: VideoAnalyticsEvent) -> Dict[str, Any]:
        """Validate video source information integrity."""
        errors = []
        warnings = []
        severity = 0.4  # Lower severity
        
        try:
            # Check for empty source IDs
            if not event.video_source.source_id.strip():
                errors.append("Empty video source ID")
            
            if not event.video_source.camera_id.strip():
                errors.append("Empty camera ID")
            
            # Validate location if present
            if event.video_source.location:
                loc = event.video_source.location
                if loc.latitude is not None and not (-90 <= loc.latitude <= 90):
                    errors.append("Invalid latitude value")
                if loc.longitude is not None and not (-180 <= loc.longitude <= 180):
                    errors.append("Invalid longitude value")
            
        except Exception as e:
            errors.append(f"Source integrity validation error: {str(e)}")
        
        return {"errors": errors, "warnings": warnings, "severity": severity}


class DataQualityMonitor:
    """Monitor data quality metrics over time."""
    
    def __init__(self):
        self.quality_history = []
        self.error_counts = {}
        self.warning_counts = {}
    
    def record_validation_result(self, result: ValidationResult, event_id: str):
        """Record validation result for monitoring."""
        self.quality_history.append({
            "timestamp": datetime.utcnow(),
            "event_id": event_id,
            "quality_score": result.score,
            "is_valid": result.is_valid,
            "error_count": len(result.errors),
            "warning_count": len(result.warnings)
        })
        
        # Track error types
        for error in result.errors:
            self.error_counts[error] = self.error_counts.get(error, 0) + 1
        
        for warning in result.warnings:
            self.warning_counts[warning] = self.warning_counts.get(warning, 0) + 1
    
    def get_quality_metrics(self, window_minutes: int = 60) -> Dict[str, Any]:
        """Get data quality metrics for the specified time window."""
        cutoff_time = datetime.utcnow() - timedelta(minutes=window_minutes)
        
        recent_records = [
            record for record in self.quality_history
            if record["timestamp"] > cutoff_time
        ]
        
        if not recent_records:
            return {"no_data": True}
        
        total_events = len(recent_records)
        valid_events = sum(1 for record in recent_records if record["is_valid"])
        avg_quality_score = sum(record["quality_score"] for record in recent_records) / total_events
        
        return {
            "window_minutes": window_minutes,
            "total_events": total_events,
            "valid_events": valid_events,
            "validity_rate": valid_events / total_events,
            "average_quality_score": avg_quality_score,
            "total_errors": sum(record["error_count"] for record in recent_records),
            "total_warnings": sum(record["warning_count"] for record in recent_records),
            "top_errors": sorted(self.error_counts.items(), key=lambda x: x[1], reverse=True)[:5],
            "top_warnings": sorted(self.warning_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        }