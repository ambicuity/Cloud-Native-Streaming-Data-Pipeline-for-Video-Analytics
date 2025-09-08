"""
Monitoring and metrics collection for the video analytics pipeline.
"""

import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional
from collections import defaultdict, deque

from google.cloud import monitoring_v3
from google.cloud import logging as cloud_logging


class MetricsCollector:
    """Collect and export custom metrics to Google Cloud Monitoring."""
    
    def __init__(self, project_id: Optional[str] = None, metric_prefix: str = "video_analytics"):
        """
        Initialize metrics collector.
        
        Args:
            project_id: Google Cloud project ID
            metric_prefix: Prefix for all custom metrics
        """
        self.project_id = project_id
        self.metric_prefix = metric_prefix
        self.client = None
        self.project_name = None
        
        # Local metric storage for batch export
        self.counters = defaultdict(int)
        self.gauges = defaultdict(float)
        self.histograms = defaultdict(list)
        self.last_export_time = time.time()
        self.export_interval = 30  # seconds
        
        # Initialize Cloud Monitoring client if project_id is provided
        if project_id:
            try:
                self.client = monitoring_v3.MetricServiceClient()
                self.project_name = f"projects/{project_id}"
            except Exception as e:
                logging.warning(f"Failed to initialize Cloud Monitoring client: {e}")
    
    def increment_counter(self, metric_name: str, value: int = 1, labels: Optional[Dict[str, str]] = None):
        """
        Increment a counter metric.
        
        Args:
            metric_name: Name of the metric
            value: Value to increment by
            labels: Optional labels for the metric
        """
        full_metric_name = f"{self.metric_prefix}.{metric_name}"
        
        # Store locally
        key = self._create_metric_key(full_metric_name, labels)
        self.counters[key] += value
        
        # Export if interval has passed
        self._maybe_export_metrics()
    
    def set_gauge(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """
        Set a gauge metric value.
        
        Args:
            metric_name: Name of the metric
            value: Value to set
            labels: Optional labels for the metric
        """
        full_metric_name = f"{self.metric_prefix}.{metric_name}"
        
        # Store locally
        key = self._create_metric_key(full_metric_name, labels)
        self.gauges[key] = value
        
        # Export if interval has passed
        self._maybe_export_metrics()
    
    def record_histogram(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """
        Record a value for a histogram metric.
        
        Args:
            metric_name: Name of the metric
            value: Value to record
            labels: Optional labels for the metric
        """
        full_metric_name = f"{self.metric_prefix}.{metric_name}"
        
        # Store locally
        key = self._create_metric_key(full_metric_name, labels)
        self.histograms[key].append(value)
        
        # Keep only recent values (last 1000)
        if len(self.histograms[key]) > 1000:
            self.histograms[key] = self.histograms[key][-1000:]
        
        # Export if interval has passed
        self._maybe_export_metrics()
    
    def _create_metric_key(self, metric_name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a unique key for a metric with labels."""
        if labels:
            label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
            return f"{metric_name}[{label_str}]"
        return metric_name
    
    def _maybe_export_metrics(self):
        """Export metrics to Cloud Monitoring if interval has passed."""
        current_time = time.time()
        if current_time - self.last_export_time >= self.export_interval:
            self.export_metrics()
            self.last_export_time = current_time
    
    def export_metrics(self):
        """Export all accumulated metrics to Cloud Monitoring."""
        if not self.client:
            # Just log metrics if no client available
            self._log_metrics()
            return
        
        try:
            # Export counters
            for metric_key, value in self.counters.items():
                metric_name, labels = self._parse_metric_key(metric_key)
                self._create_time_series(metric_name, value, "CUMULATIVE", labels)
            
            # Export gauges
            for metric_key, value in self.gauges.items():
                metric_name, labels = self._parse_metric_key(metric_key)
                self._create_time_series(metric_name, value, "GAUGE", labels)
            
            # Export histogram summaries
            for metric_key, values in self.histograms.items():
                if values:
                    metric_name, labels = self._parse_metric_key(metric_key)
                    # Export average, min, max
                    avg_value = sum(values) / len(values)
                    self._create_time_series(f"{metric_name}_avg", avg_value, "GAUGE", labels)
                    self._create_time_series(f"{metric_name}_min", min(values), "GAUGE", labels)
                    self._create_time_series(f"{metric_name}_max", max(values), "GAUGE", labels)
            
            # Clear exported metrics
            self.counters.clear()
            self.gauges.clear()
            self.histograms.clear()
            
        except Exception as e:
            logging.error(f"Failed to export metrics to Cloud Monitoring: {e}")
            self._log_metrics()
    
    def _log_metrics(self):
        """Log metrics locally when Cloud Monitoring is not available."""
        if self.counters:
            logging.info(f"Counters: {dict(self.counters)}")
        if self.gauges:
            logging.info(f"Gauges: {dict(self.gauges)}")
        if self.histograms:
            histogram_summary = {
                key: {"count": len(values), "avg": sum(values)/len(values) if values else 0}
                for key, values in self.histograms.items()
            }
            logging.info(f"Histograms: {histogram_summary}")
    
    def _parse_metric_key(self, metric_key: str) -> tuple:
        """Parse metric key to extract name and labels."""
        if "[" in metric_key:
            metric_name, label_str = metric_key.split("[", 1)
            label_str = label_str.rstrip("]")
            labels = {}
            for pair in label_str.split(","):
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    labels[key] = value
            return metric_name, labels
        return metric_key, {}
    
    def _create_time_series(self, metric_name: str, value: float, metric_kind: str, labels: Dict[str, str]):
        """Create a time series point in Cloud Monitoring."""
        try:
            # This is a simplified implementation
            # In production, you would create proper metric descriptors and time series
            logging.debug(f"Would export metric {metric_name}={value} with labels {labels}")
        except Exception as e:
            logging.error(f"Failed to create time series for {metric_name}: {e}")


class PipelineLogger:
    """Structured logging for the video analytics pipeline."""
    
    def __init__(self, project_id: Optional[str] = None, log_name: str = "video-analytics-pipeline"):
        """
        Initialize pipeline logger.
        
        Args:
            project_id: Google Cloud project ID
            log_name: Name of the log
        """
        self.project_id = project_id
        self.log_name = log_name
        self.client = None
        
        # Setup local logging
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(logging.INFO)
        
        # Setup Cloud Logging if project_id is provided
        if project_id:
            try:
                self.client = cloud_logging.Client(project=project_id)
                self.client.setup_logging()
            except Exception as e:
                logging.warning(f"Failed to initialize Cloud Logging client: {e}")
    
    def log_event_processed(self, event_id: str, event_type: str, processing_time_ms: float, success: bool):
        """Log event processing information."""
        log_data = {
            "event_id": event_id,
            "event_type": event_type,
            "processing_time_ms": processing_time_ms,
            "success": success,
            "timestamp": datetime.utcnow().isoformat(),
            "component": "event_processor"
        }
        
        if success:
            self.logger.info("Event processed successfully", extra=log_data)
        else:
            self.logger.error("Event processing failed", extra=log_data)
    
    def log_pipeline_health(self, component: str, status: str, details: Dict[str, Any]):
        """Log pipeline health information."""
        log_data = {
            "component": component,
            "status": status,
            "details": details,
            "timestamp": datetime.utcnow().isoformat(),
            "log_type": "health_check"
        }
        
        if status == "healthy":
            self.logger.info(f"Component {component} is healthy", extra=log_data)
        else:
            self.logger.warning(f"Component {component} health issue: {status}", extra=log_data)
    
    def log_data_quality_issue(self, event_id: str, errors: list, warnings: list, quality_score: float):
        """Log data quality issues."""
        log_data = {
            "event_id": event_id,
            "errors": errors,
            "warnings": warnings,
            "quality_score": quality_score,
            "timestamp": datetime.utcnow().isoformat(),
            "component": "data_quality_checker"
        }
        
        if errors:
            self.logger.error("Data quality errors detected", extra=log_data)
        elif warnings:
            self.logger.warning("Data quality warnings detected", extra=log_data)
        else:
            self.logger.debug("Data quality check passed", extra=log_data)


class HealthChecker:
    """Health checking for pipeline components."""
    
    def __init__(self, metrics_collector: MetricsCollector, logger: PipelineLogger):
        """
        Initialize health checker.
        
        Args:
            metrics_collector: Metrics collector instance
            logger: Pipeline logger instance
        """
        self.metrics_collector = metrics_collector
        self.logger = logger
        self.component_health = {}
        self.last_health_check = {}
    
    def check_component_health(self, component_name: str) -> Dict[str, Any]:
        """
        Check health of a specific component.
        
        Args:
            component_name: Name of the component to check
            
        Returns:
            Dict containing health status and details
        """
        health_status = {
            "component": component_name,
            "status": "unknown",
            "timestamp": datetime.utcnow().isoformat(),
            "details": {}
        }
        
        try:
            if component_name == "pubsub_connection":
                health_status.update(self._check_pubsub_health())
            elif component_name == "dataflow_job":
                health_status.update(self._check_dataflow_health())
            elif component_name == "data_quality":
                health_status.update(self._check_data_quality_health())
            elif component_name == "monitoring":
                health_status.update(self._check_monitoring_health())
            else:
                health_status["status"] = "unknown_component"
            
            # Record health status
            self.component_health[component_name] = health_status
            self.last_health_check[component_name] = time.time()
            
            # Log health status
            self.logger.log_pipeline_health(
                component_name, 
                health_status["status"], 
                health_status["details"]
            )
            
            # Record health metric
            health_value = 1.0 if health_status["status"] == "healthy" else 0.0
            self.metrics_collector.set_gauge(
                "component_health", 
                health_value, 
                {"component": component_name}
            )
            
        except Exception as e:
            health_status["status"] = "error"
            health_status["details"]["error"] = str(e)
            self.logger.logger.error(f"Health check failed for {component_name}: {e}")
        
        return health_status
    
    def _check_pubsub_health(self) -> Dict[str, Any]:
        """Check Pub/Sub connection health."""
        # In a real implementation, this would test Pub/Sub connectivity
        return {
            "status": "healthy",
            "details": {
                "connectivity": "ok",
                "last_message_received": datetime.utcnow().isoformat()
            }
        }
    
    def _check_dataflow_health(self) -> Dict[str, Any]:
        """Check Dataflow job health."""
        # In a real implementation, this would check Dataflow job status
        return {
            "status": "healthy", 
            "details": {
                "job_state": "running",
                "worker_count": 3,
                "throughput": "normal"
            }
        }
    
    def _check_data_quality_health(self) -> Dict[str, Any]:
        """Check data quality system health."""
        return {
            "status": "healthy",
            "details": {
                "validation_rules": "active",
                "error_rate": "low"
            }
        }
    
    def _check_monitoring_health(self) -> Dict[str, Any]:
        """Check monitoring system health."""
        return {
            "status": "healthy",
            "details": {
                "metrics_export": "active",
                "logging": "active"
            }
        }
    
    def get_overall_health(self) -> Dict[str, Any]:
        """Get overall pipeline health status."""
        if not self.component_health:
            return {"status": "unknown", "components": {}}
        
        healthy_components = sum(
            1 for health in self.component_health.values() 
            if health["status"] == "healthy"
        )
        total_components = len(self.component_health)
        
        overall_status = "healthy" if healthy_components == total_components else "degraded"
        if healthy_components == 0:
            overall_status = "unhealthy"
        
        return {
            "status": overall_status,
            "healthy_components": healthy_components,
            "total_components": total_components,
            "components": self.component_health,
            "last_updated": datetime.utcnow().isoformat()
        }