"""
Apache Beam transforms for video analytics data processing.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.options.pipeline_options import PipelineOptions
from pydantic import ValidationError

from ..schemas.models import VideoAnalyticsEvent
from ..utils.data_quality import DataQualityChecker
from ..monitoring.metrics import MetricsCollector


class ParseVideoEvent(beam.DoFn):
    """Parse and validate incoming video analytics events."""
    
    def __init__(self):
        self.metrics_collector = None
        self.data_quality_checker = None
    
    def setup(self):
        """Initialize components needed for processing."""
        self.metrics_collector = MetricsCollector()
        self.data_quality_checker = DataQualityChecker()
    
    def process(self, element: bytes) -> Iterable[VideoAnalyticsEvent]:
        """
        Parse and validate a video analytics event.
        
        Args:
            element: Raw message bytes from Pub/Sub
            
        Yields:
            VideoAnalyticsEvent: Validated event object
        """
        try:
            # Parse JSON
            event_data = json.loads(element.decode('utf-8'))
            
            # Validate against schema
            event = VideoAnalyticsEvent(**event_data)
            
            # Perform data quality checks
            quality_result = self.data_quality_checker.validate_event(event)
            
            if quality_result.is_valid:
                # Add processing metadata
                if not event.processing_metadata:
                    event.processing_metadata = {}
                
                event.processing_metadata.processing_time = datetime.utcnow()
                event.processing_metadata.pipeline_version = "1.0.0"
                
                # Record successful processing metric
                self.metrics_collector.increment_counter("events_processed_success")
                
                yield event
            else:
                # Log data quality issues
                logging.warning(f"Data quality check failed for event {event.event_id}: {quality_result.errors}")
                self.metrics_collector.increment_counter("events_data_quality_failed")
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON: {e}")
            self.metrics_collector.increment_counter("events_parse_failed")
            
        except ValidationError as e:
            logging.error(f"Schema validation failed: {e}")
            self.metrics_collector.increment_counter("events_validation_failed")
            
        except Exception as e:
            logging.error(f"Unexpected error processing event: {e}")
            self.metrics_collector.increment_counter("events_processing_error")


class EnrichEvent(beam.DoFn):
    """Enrich video analytics events with additional context."""
    
    def __init__(self):
        self.metrics_collector = None
    
    def setup(self):
        """Initialize components needed for processing."""
        self.metrics_collector = MetricsCollector()
    
    def process(self, event: VideoAnalyticsEvent) -> Iterable[VideoAnalyticsEvent]:
        """
        Enrich event with additional context and derived metrics.
        
        Args:
            event: Validated video analytics event
            
        Yields:
            VideoAnalyticsEvent: Enriched event
        """
        try:
            # Add derived fields based on event type
            if event.event_type in ["person_detected", "vehicle_detected"]:
                if event.data.confidence and event.data.confidence > 0.9:
                    if not event.data.attributes:
                        event.data.attributes = {}
                    event.data.attributes["high_confidence"] = True
            
            # Add geographical context if location is available
            if event.video_source.location:
                if not event.data.attributes:
                    event.data.attributes = {}
                event.data.attributes["has_location"] = True
            
            self.metrics_collector.increment_counter("events_enriched")
            yield event
            
        except Exception as e:
            logging.error(f"Error enriching event {event.event_id}: {e}")
            self.metrics_collector.increment_counter("events_enrichment_failed")
            # Still yield the original event
            yield event


class FilterAnomalies(beam.DoFn):
    """Filter and flag anomalous events for special handling."""
    
    def __init__(self, confidence_threshold: float = 0.95):
        self.confidence_threshold = confidence_threshold
        self.metrics_collector = None
    
    def setup(self):
        """Initialize components needed for processing."""
        self.metrics_collector = MetricsCollector()
    
    def process(self, event: VideoAnalyticsEvent) -> Iterable[Dict[str, Any]]:
        """
        Filter events and tag anomalies for priority processing.
        
        Args:
            event: Video analytics event
            
        Yields:
            Dict containing event and priority flag
        """
        try:
            is_anomaly = False
            
            # Check for anomaly conditions
            if event.event_type == "anomaly_detected":
                is_anomaly = True
            elif (event.data.confidence and 
                  event.data.confidence > self.confidence_threshold and
                  event.event_type in ["person_detected", "vehicle_detected"]):
                is_anomaly = True
            
            result = {
                "event": event.dict(),
                "is_anomaly": is_anomaly,
                "processing_priority": "high" if is_anomaly else "normal",
                "processed_at": datetime.utcnow().isoformat()
            }
            
            if is_anomaly:
                self.metrics_collector.increment_counter("anomalies_detected")
            
            self.metrics_collector.increment_counter("events_filtered")
            yield result
            
        except Exception as e:
            logging.error(f"Error filtering event {event.event_id}: {e}")
            self.metrics_collector.increment_counter("events_filtering_failed")


class WindowedAggregation(beam.DoFn):
    """Aggregate events within time windows for analytics."""
    
    def __init__(self):
        self.metrics_collector = None
    
    def setup(self):
        """Initialize components needed for processing."""
        self.metrics_collector = MetricsCollector()
    
    def process(self, element, window=beam.DoFn.WindowParam) -> Iterable[Dict[str, Any]]:
        """
        Aggregate events within a time window.
        
        Args:
            element: Tuple of (key, events) from GroupBy operation
            window: Beam window information
            
        Yields:
            Dict containing aggregated metrics
        """
        try:
            key, events = element
            events_list = list(events)
            
            # Calculate aggregations
            total_events = len(events_list)
            event_types = {}
            confidence_scores = []
            
            for event_dict in events_list:
                event_type = event_dict["event"]["event_type"]
                event_types[event_type] = event_types.get(event_type, 0) + 1
                
                if event_dict["event"]["data"].get("confidence"):
                    confidence_scores.append(event_dict["event"]["data"]["confidence"])
            
            avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0
            
            aggregation = {
                "window_start": window.start.to_utc_datetime().isoformat(),
                "window_end": window.end.to_utc_datetime().isoformat(),
                "source_id": key,
                "total_events": total_events,
                "event_type_counts": event_types,
                "average_confidence": avg_confidence,
                "high_confidence_events": len([s for s in confidence_scores if s > 0.9]),
                "aggregated_at": datetime.utcnow().isoformat()
            }
            
            self.metrics_collector.increment_counter("windows_aggregated")
            yield aggregation
            
        except Exception as e:
            logging.error(f"Error aggregating window: {e}")
            self.metrics_collector.increment_counter("aggregation_failed")