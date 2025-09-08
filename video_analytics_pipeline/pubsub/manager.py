"""
Google Cloud Pub/Sub utilities for the video analytics pipeline.
"""

import json
import logging
from typing import Dict, Any, Optional, Callable, List
from concurrent.futures import ThreadPoolExecutor
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import DeadLetterPolicy, RetryPolicy
from google.api_core import retry

from ..config.settings import PubSubConfig
from ..schemas.models import VideoAnalyticsEvent


class PubSubManager:
    """Manager for Pub/Sub topics, subscriptions, and message handling."""
    
    def __init__(self, config: PubSubConfig):
        """
        Initialize Pub/Sub manager.
        
        Args:
            config: Pub/Sub configuration
        """
        self.config = config
        self.publisher = pubsub_v1.PublisherClient()
        self.subscriber = pubsub_v1.SubscriberClient()
        self.project_path = self.publisher.common_project_path(config.project_id)
    
    def create_topics_and_subscriptions(self):
        """Create all required topics and subscriptions."""
        topics_to_create = [
            self.config.input_topic,
            self.config.output_topic,
            self.config.anomaly_topic,
            self.config.analytics_topic,
            self.config.dead_letter_topic
        ]
        
        # Create topics
        for topic_name in topics_to_create:
            self._create_topic_if_not_exists(topic_name)
        
        # Create input subscription with dead letter queue
        self._create_subscription_with_dlq(
            self.config.input_subscription,
            self.config.input_topic,
            self.config.dead_letter_topic
        )
    
    def _create_topic_if_not_exists(self, topic_name: str):
        """Create a topic if it doesn't exist."""
        topic_path = self.publisher.topic_path(self.config.project_id, topic_name)
        
        try:
            self.publisher.create_topic(request={"name": topic_path})
            logging.info(f"Created topic: {topic_path}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.info(f"Topic already exists: {topic_path}")
            else:
                logging.error(f"Failed to create topic {topic_path}: {e}")
                raise
    
    def _create_subscription_with_dlq(self, subscription_name: str, topic_name: str, dlq_topic_name: str):
        """Create a subscription with dead letter queue configuration."""
        subscription_path = self.subscriber.subscription_path(
            self.config.project_id, subscription_name
        )
        topic_path = self.publisher.topic_path(self.config.project_id, topic_name)
        dlq_topic_path = self.publisher.topic_path(self.config.project_id, dlq_topic_name)
        
        # Dead letter policy
        dead_letter_policy = DeadLetterPolicy(
            dead_letter_topic=dlq_topic_path,
            max_delivery_attempts=self.config.max_delivery_attempts
        )
        
        # Retry policy
        retry_policy = RetryPolicy(
            minimum_backoff={"seconds": 10},
            maximum_backoff={"seconds": 600}
        )
        
        try:
            self.subscriber.create_subscription(
                request={
                    "name": subscription_path,
                    "topic": topic_path,
                    "message_retention_duration": {"seconds": self.config.message_retention_duration},
                    "ack_deadline_seconds": self.config.ack_deadline_seconds,
                    "dead_letter_policy": dead_letter_policy,
                    "retry_policy": retry_policy,
                    "enable_message_ordering": False
                }
            )
            logging.info(f"Created subscription: {subscription_path}")
        except Exception as e:
            if "already exists" in str(e).lower():
                logging.info(f"Subscription already exists: {subscription_path}")
            else:
                logging.error(f"Failed to create subscription {subscription_path}: {e}")
                raise
    
    def publish_event(self, topic_name: str, event: VideoAnalyticsEvent, attributes: Optional[Dict[str, str]] = None) -> str:
        """
        Publish a video analytics event to a topic.
        
        Args:
            topic_name: Name of the topic
            event: Video analytics event to publish
            attributes: Optional message attributes
            
        Returns:
            Message ID of the published message
        """
        topic_path = self.publisher.topic_path(self.config.project_id, topic_name)
        
        # Convert event to JSON
        message_data = json.dumps(event.dict(), default=str).encode('utf-8')
        
        # Default attributes
        if attributes is None:
            attributes = {}
        
        attributes.update({
            "event_type": event.event_type,
            "source_id": event.video_source.source_id,
            "timestamp": event.timestamp.isoformat()
        })
        
        # Publish message
        future = self.publisher.publish(
            topic_path, 
            data=message_data, 
            **attributes
        )
        
        message_id = future.result()
        logging.debug(f"Published message {message_id} to {topic_path}")
        return message_id
    
    def publish_json(self, topic_name: str, data: Dict[str, Any], attributes: Optional[Dict[str, str]] = None) -> str:
        """
        Publish JSON data to a topic.
        
        Args:
            topic_name: Name of the topic
            data: JSON-serializable data
            attributes: Optional message attributes
            
        Returns:
            Message ID of the published message
        """
        topic_path = self.publisher.topic_path(self.config.project_id, topic_name)
        
        # Convert data to JSON
        message_data = json.dumps(data, default=str).encode('utf-8')
        
        if attributes is None:
            attributes = {}
        
        # Publish message
        future = self.publisher.publish(
            topic_path,
            data=message_data,
            **attributes
        )
        
        message_id = future.result()
        logging.debug(f"Published JSON message {message_id} to {topic_path}")
        return message_id


class MessageProcessor:
    """Process messages from Pub/Sub subscriptions."""
    
    def __init__(self, config: PubSubConfig, message_handler: Callable[[Dict[str, Any]], bool]):
        """
        Initialize message processor.
        
        Args:
            config: Pub/Sub configuration
            message_handler: Function to handle received messages
        """
        self.config = config
        self.message_handler = message_handler
        self.subscriber = pubsub_v1.SubscriberClient()
        self.executor = ThreadPoolExecutor(max_workers=10)
        self.active_futures = []
    
    def start_processing(self, subscription_name: str):
        """
        Start processing messages from a subscription.
        
        Args:
            subscription_name: Name of the subscription to process
        """
        subscription_path = self.subscriber.subscription_path(
            self.config.project_id, subscription_name
        )
        
        logging.info(f"Starting message processing from {subscription_path}")
        
        # Configure flow control
        flow_control = pubsub_v1.types.FlowControl(max_messages=100, max_bytes=1024*1024*10)  # 10MB
        
        def callback(message):
            """Callback function to handle received messages."""
            try:
                # Parse message data
                message_data = json.loads(message.data.decode('utf-8'))
                
                # Add message metadata
                message_data['_pubsub_metadata'] = {
                    'message_id': message.message_id,
                    'publish_time': message.publish_time.isoformat(),
                    'attributes': dict(message.attributes)
                }
                
                # Process message
                success = self.message_handler(message_data)
                
                if success:
                    message.ack()
                    logging.debug(f"Successfully processed message {message.message_id}")
                else:
                    message.nack()
                    logging.warning(f"Failed to process message {message.message_id}, will retry")
                    
            except Exception as e:
                logging.error(f"Error processing message {message.message_id}: {e}")
                message.nack()
        
        # Start pulling messages
        streaming_pull_future = self.subscriber.subscribe(
            subscription_path,
            callback=callback,
            flow_control=flow_control
        )
        
        self.active_futures.append(streaming_pull_future)
        
        logging.info(f"Listening for messages on {subscription_path}")
        
        try:
            # Keep the main thread running
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            logging.info("Message processing stopped")
    
    def stop_processing(self):
        """Stop processing messages."""
        for future in self.active_futures:
            future.cancel()
        self.active_futures.clear()
        self.executor.shutdown(wait=True)
        logging.info("Message processing stopped")


class EventPublisher:
    """Utility class for publishing video analytics events."""
    
    def __init__(self, pubsub_manager: PubSubManager):
        """
        Initialize event publisher.
        
        Args:
            pubsub_manager: PubSubManager instance
        """
        self.pubsub_manager = pubsub_manager
    
    def publish_normal_event(self, event: VideoAnalyticsEvent) -> str:
        """Publish a normal video analytics event."""
        return self.pubsub_manager.publish_event(
            self.pubsub_manager.config.output_topic,
            event,
            {"priority": "normal"}
        )
    
    def publish_anomaly_event(self, event: VideoAnalyticsEvent) -> str:
        """Publish an anomaly event for priority processing."""
        return self.pubsub_manager.publish_event(
            self.pubsub_manager.config.anomaly_topic,
            event,
            {"priority": "high", "anomaly": "true"}
        )
    
    def publish_analytics_data(self, analytics_data: Dict[str, Any]) -> str:
        """Publish aggregated analytics data."""
        return self.pubsub_manager.publish_json(
            self.pubsub_manager.config.analytics_topic,
            analytics_data,
            {"data_type": "analytics"}
        )
    
    def publish_dead_letter(self, failed_message: Dict[str, Any], error_reason: str) -> str:
        """Publish a failed message to the dead letter queue."""
        dead_letter_data = {
            "original_message": failed_message,
            "error_reason": error_reason,
            "failed_at": json.dumps(str()),  # Current timestamp
            "retry_count": failed_message.get("_retry_count", 0) + 1
        }
        
        return self.pubsub_manager.publish_json(
            self.pubsub_manager.config.dead_letter_topic,
            dead_letter_data,
            {"message_type": "dead_letter"}
        )


class SubscriptionMonitor:
    """Monitor Pub/Sub subscription metrics and health."""
    
    def __init__(self, config: PubSubConfig):
        """
        Initialize subscription monitor.
        
        Args:
            config: Pub/Sub configuration
        """
        self.config = config
        self.subscriber = pubsub_v1.SubscriberClient()
    
    def get_subscription_stats(self, subscription_name: str) -> Dict[str, Any]:
        """
        Get statistics for a subscription.
        
        Args:
            subscription_name: Name of the subscription
            
        Returns:
            Dictionary containing subscription statistics
        """
        subscription_path = self.subscriber.subscription_path(
            self.config.project_id, subscription_name
        )
        
        try:
            # Get subscription info
            subscription = self.subscriber.get_subscription(
                request={"subscription": subscription_path}
            )
            
            # In a real implementation, you would use the Monitoring API
            # to get detailed metrics like message backlog, delivery rate, etc.
            stats = {
                "subscription_name": subscription_name,
                "topic": subscription.topic,
                "ack_deadline_seconds": subscription.ack_deadline_seconds,
                "message_retention_duration": subscription.message_retention_duration.total_seconds(),
                "has_dead_letter_policy": bool(subscription.dead_letter_policy),
                "state": "active"  # This would come from monitoring metrics
            }
            
            if subscription.dead_letter_policy:
                stats["dead_letter_topic"] = subscription.dead_letter_policy.dead_letter_topic
                stats["max_delivery_attempts"] = subscription.dead_letter_policy.max_delivery_attempts
            
            return stats
            
        except Exception as e:
            logging.error(f"Failed to get subscription stats for {subscription_name}: {e}")
            return {"error": str(e)}
    
    def check_subscription_health(self, subscription_name: str) -> Dict[str, Any]:
        """
        Check the health of a subscription.
        
        Args:
            subscription_name: Name of the subscription
            
        Returns:
            Dictionary containing health status
        """
        stats = self.get_subscription_stats(subscription_name)
        
        if "error" in stats:
            return {
                "subscription": subscription_name,
                "status": "error",
                "details": stats
            }
        
        # Basic health checks
        health_status = "healthy"
        issues = []
        
        # Check if subscription exists and is accessible
        if not stats.get("state"):
            health_status = "unhealthy"
            issues.append("Subscription state unknown")
        
        return {
            "subscription": subscription_name,
            "status": health_status,
            "issues": issues,
            "stats": stats
        }