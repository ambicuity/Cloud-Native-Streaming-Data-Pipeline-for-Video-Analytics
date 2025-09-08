"""
Example script to generate and publish sample video analytics events.
"""

import json
import random
import time
from datetime import datetime, timedelta
from typing import List

from google.cloud import pubsub_v1

from video_analytics_pipeline.schemas.models import (
    VideoAnalyticsEvent, VideoSource, EventData, EventType, 
    BoundingBox, Location, ProcessingMetadata
)


def generate_sample_event(event_type: EventType = None) -> VideoAnalyticsEvent:
    """Generate a sample video analytics event."""
    
    if event_type is None:
        event_type = random.choice(list(EventType))
    
    # Generate random source
    source = VideoSource(
        source_id=f"camera_{random.randint(1, 10):03d}",
        camera_id=f"cam_{random.randint(100, 999)}",
        location=Location(
            latitude=random.uniform(37.4, 37.5),  # San Francisco area
            longitude=random.uniform(-122.2, -122.1),
            address=f"{random.randint(1, 999)} Main St, San Francisco, CA"
        )
    )
    
    # Generate event data based on type
    data = EventData()
    
    if event_type in [EventType.PERSON_DETECTED, EventType.VEHICLE_DETECTED]:
        data.confidence = random.uniform(0.6, 0.99)
        data.bounding_box = BoundingBox(
            x=random.uniform(0, 800),
            y=random.uniform(0, 600),
            width=random.uniform(50, 200),
            height=random.uniform(80, 300)
        )
        data.attributes = {
            "detection_model": "yolo_v5",
            "processing_time_ms": random.uniform(10, 50)
        }
    elif event_type == EventType.MOTION_DETECTED:
        data.confidence = random.uniform(0.7, 0.95)
        data.attributes = {
            "motion_intensity": random.uniform(0.1, 1.0),
            "area_percentage": random.uniform(5, 30)
        }
    elif event_type == EventType.ANOMALY_DETECTED:
        data.confidence = random.uniform(0.8, 0.99)
        data.attributes = {
            "anomaly_type": random.choice(["unusual_behavior", "object_left_behind", "crowd_formation"]),
            "severity": random.choice(["low", "medium", "high"])
        }
    elif event_type == EventType.PERFORMANCE_METRIC:
        data.metrics = {
            "fps": random.uniform(25, 30),
            "cpu_usage": random.uniform(30, 80),
            "memory_usage": random.uniform(40, 75),
            "processing_latency_ms": random.uniform(5, 25)
        }
    
    # Create event
    event = VideoAnalyticsEvent(
        event_id=f"evt_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        timestamp=datetime.utcnow() - timedelta(seconds=random.randint(0, 300)),
        video_source=source,
        event_type=event_type,
        data=data,
        processing_metadata=ProcessingMetadata(
            pipeline_version="1.0.0",
            model_version="v2.1"
        )
    )
    
    return event


def publish_sample_events(project_id: str, topic_name: str, num_events: int = 10):
    """Publish sample events to a Pub/Sub topic."""
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    
    print(f"Publishing {num_events} sample events to {topic_path}")
    
    for i in range(num_events):
        # Generate event with some bias towards detection events
        if random.random() < 0.7:
            event_type = random.choice([EventType.PERSON_DETECTED, EventType.VEHICLE_DETECTED])
        else:
            event_type = random.choice(list(EventType))
        
        event = generate_sample_event(event_type)
        
        # Convert to JSON
        message_data = json.dumps(event.dict(), default=str).encode('utf-8')
        
        # Add attributes
        attributes = {
            "event_type": event.event_type,
            "source_id": event.video_source.source_id,
            "timestamp": event.timestamp.isoformat()
        }
        
        # Publish message
        future = publisher.publish(topic_path, data=message_data, **attributes)
        message_id = future.result()
        
        print(f"Published event {i+1}/{num_events}: {event.event_id} (Message ID: {message_id})")
        
        # Add some delay to simulate real-time streaming
        time.sleep(random.uniform(0.1, 2.0))
    
    print(f"Successfully published {num_events} events")


def generate_event_batch(num_events: int = 100) -> List[dict]:
    """Generate a batch of sample events as dictionaries."""
    events = []
    
    for _ in range(num_events):
        event = generate_sample_event()
        events.append(event.dict())
    
    return events


def save_sample_events_to_file(filename: str = "sample_events.json", num_events: int = 20):
    """Save sample events to a JSON file."""
    events = generate_event_batch(num_events)
    
    with open(filename, 'w') as f:
        json.dump(events, f, indent=2, default=str)
    
    print(f"Saved {num_events} sample events to {filename}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate sample video analytics events")
    parser.add_argument("--project", required=True, help="Google Cloud project ID")
    parser.add_argument("--topic", default="video-analytics-input", help="Pub/Sub topic name")
    parser.add_argument("--num-events", type=int, default=10, help="Number of events to generate")
    parser.add_argument("--save-to-file", help="Save events to file instead of publishing")
    
    args = parser.parse_args()
    
    if args.save_to_file:
        save_sample_events_to_file(args.save_to_file, args.num_events)
    else:
        publish_sample_events(args.project, args.topic, args.num_events)