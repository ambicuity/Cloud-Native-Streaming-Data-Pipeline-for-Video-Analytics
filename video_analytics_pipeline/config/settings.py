"""
Configuration settings for the video analytics pipeline.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class PipelineConfig:
    """Configuration for the video analytics pipeline."""
    
    # Google Cloud Platform settings
    gcp_project: str
    gcp_region: str = "us-central1"
    
    # Pipeline settings
    job_name: str = "video-analytics-pipeline"
    runner: str = "DataflowRunner"  # Can be "DirectRunner" for local testing
    
    # Pub/Sub settings
    input_subscription: str = ""
    output_topic: str = ""
    anomaly_topic: str = ""
    analytics_topic: str = ""
    dead_letter_topic: str = ""
    
    # Storage settings
    staging_location: str = ""
    temp_location: str = ""
    
    # Processing settings
    window_size_seconds: int = 60
    anomaly_confidence_threshold: float = 0.95
    max_num_workers: int = 10
    
    # Monitoring settings
    enable_monitoring: bool = True
    metrics_export_interval: int = 30
    
    # Security settings
    use_public_ips: bool = False
    network: Optional[str] = None
    subnetwork: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "PipelineConfig":
        """Create configuration from environment variables."""
        return cls(
            gcp_project=os.getenv("GCP_PROJECT", ""),
            gcp_region=os.getenv("GCP_REGION", "us-central1"),
            job_name=os.getenv("JOB_NAME", "video-analytics-pipeline"),
            runner=os.getenv("RUNNER", "DataflowRunner"),
            input_subscription=os.getenv("INPUT_SUBSCRIPTION", ""),
            output_topic=os.getenv("OUTPUT_TOPIC", ""),
            anomaly_topic=os.getenv("ANOMALY_TOPIC", ""),
            analytics_topic=os.getenv("ANALYTICS_TOPIC", ""),
            dead_letter_topic=os.getenv("DEAD_LETTER_TOPIC", ""),
            staging_location=os.getenv("STAGING_LOCATION", ""),
            temp_location=os.getenv("TEMP_LOCATION", ""),
            window_size_seconds=int(os.getenv("WINDOW_SIZE_SECONDS", "60")),
            anomaly_confidence_threshold=float(os.getenv("ANOMALY_THRESHOLD", "0.95")),
            max_num_workers=int(os.getenv("MAX_NUM_WORKERS", "10")),
            enable_monitoring=os.getenv("ENABLE_MONITORING", "true").lower() == "true",
            metrics_export_interval=int(os.getenv("METRICS_EXPORT_INTERVAL", "30")),
            use_public_ips=os.getenv("USE_PUBLIC_IPS", "false").lower() == "true",
            network=os.getenv("NETWORK"),
            subnetwork=os.getenv("SUBNETWORK"),
        )
    
    def validate(self) -> bool:
        """Validate the configuration."""
        required_fields = [
            "gcp_project",
            "input_subscription", 
            "output_topic",
            "staging_location",
            "temp_location"
        ]
        
        for field in required_fields:
            if not getattr(self, field):
                raise ValueError(f"Required configuration field '{field}' is missing")
        
        # Validate GCS paths
        if not self.staging_location.startswith("gs://"):
            raise ValueError("staging_location must be a GCS path (gs://...)")
        
        if not self.temp_location.startswith("gs://"):
            raise ValueError("temp_location must be a GCS path (gs://...)")
        
        return True


@dataclass
class PubSubConfig:
    """Configuration for Pub/Sub topics and subscriptions."""
    
    project_id: str
    
    # Topic configurations
    input_topic: str = "video-analytics-input"
    output_topic: str = "video-analytics-output"
    anomaly_topic: str = "video-analytics-anomalies"
    analytics_topic: str = "video-analytics-analytics"
    dead_letter_topic: str = "video-analytics-dead-letter"
    
    # Subscription configurations
    input_subscription: str = "video-analytics-input-sub"
    
    # Message settings
    message_retention_duration: int = 604800  # 7 days in seconds
    ack_deadline_seconds: int = 60
    max_delivery_attempts: int = 5
    
    def get_full_topic_name(self, topic_name: str) -> str:
        """Get full Pub/Sub topic name."""
        return f"projects/{self.project_id}/topics/{topic_name}"
    
    def get_full_subscription_name(self, subscription_name: str) -> str:
        """Get full Pub/Sub subscription name."""
        return f"projects/{self.project_id}/subscriptions/{subscription_name}"


@dataclass
class MonitoringConfig:
    """Configuration for monitoring and observability."""
    
    project_id: str
    
    # Metrics
    enable_custom_metrics: bool = True
    metrics_prefix: str = "video_analytics"
    
    # Logging
    log_level: str = "INFO"
    structured_logging: bool = True
    
    # Alerting
    enable_alerting: bool = True
    alert_channels: list = None
    
    # Health checks
    health_check_interval: int = 30
    health_check_timeout: int = 10
    
    def __post_init__(self):
        """Initialize default values."""
        if self.alert_channels is None:
            self.alert_channels = []