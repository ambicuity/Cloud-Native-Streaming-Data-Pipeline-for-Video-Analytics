"""
Main Apache Beam pipeline for video analytics streaming data processing.
"""

import json
import logging
from typing import Dict, Any

import apache_beam as beam
from apache_beam.transforms import window
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions

from .transforms import ParseVideoEvent, EnrichEvent, FilterAnomalies, WindowedAggregation
from ..config.settings import PipelineConfig


class VideoAnalyticsPipeline:
    """Main video analytics streaming pipeline."""
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config: Pipeline configuration object
        """
        self.config = config
        self.pipeline_options = self._create_pipeline_options()
    
    def _create_pipeline_options(self) -> PipelineOptions:
        """Create Apache Beam pipeline options from configuration."""
        options = PipelineOptions()
        
        # Google Cloud options
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = self.config.gcp_project
        google_cloud_options.region = self.config.gcp_region
        google_cloud_options.job_name = self.config.job_name
        google_cloud_options.staging_location = self.config.staging_location
        google_cloud_options.temp_location = self.config.temp_location
        
        # Standard options
        standard_options = options.view_as(StandardOptions)
        standard_options.runner = self.config.runner
        standard_options.streaming = True
        
        return options
    
    def create_pipeline(self) -> beam.Pipeline:
        """
        Create and configure the complete video analytics pipeline.
        
        Returns:
            beam.Pipeline: Configured Apache Beam pipeline
        """
        pipeline = beam.Pipeline(options=self.pipeline_options)
        
        # Read from Pub/Sub
        raw_events = (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(
                subscription=self.config.input_subscription,
                with_attributes=False
            )
        )
        
        # Parse and validate events
        parsed_events = (
            raw_events
            | "Parse Video Events" >> beam.ParDo(ParseVideoEvent())
        )
        
        # Enrich events with additional context
        enriched_events = (
            parsed_events
            | "Enrich Events" >> beam.ParDo(EnrichEvent())
        )
        
        # Filter and flag anomalies
        filtered_events = (
            enriched_events
            | "Filter Anomalies" >> beam.ParDo(FilterAnomalies(
                confidence_threshold=self.config.anomaly_confidence_threshold
            ))
        )
        
        # Split normal and anomaly streams
        normal_events = (
            filtered_events
            | "Filter Normal Events" >> beam.Filter(lambda x: not x["is_anomaly"])
        )
        
        anomaly_events = (
            filtered_events
            | "Filter Anomaly Events" >> beam.Filter(lambda x: x["is_anomaly"])
        )
        
        # Process normal events with windowing and aggregation
        windowed_normal = (
            normal_events
            | "Add Timestamps" >> beam.Map(
                lambda x: beam.window.TimestampedValue(
                    x, 
                    beam.utils.timestamp.Timestamp.from_rfc3339(
                        x["event"]["timestamp"]
                    )
                )
            )
            | "Window Normal Events" >> beam.WindowInto(
                window.FixedWindows(self.config.window_size_seconds)
            )
            | "Key by Source" >> beam.Map(
                lambda x: (x["event"]["video_source"]["source_id"], x)
            )
            | "Group by Source" >> beam.GroupByKey()
            | "Aggregate Windows" >> beam.ParDo(WindowedAggregation())
        )
        
        # Write normal events to output topic
        _ = (
            normal_events
            | "Convert Normal to JSON" >> beam.Map(
                lambda x: json.dumps(x).encode('utf-8')
            )
            | "Write Normal Events" >> WriteToPubSub(
                topic=self.config.output_topic
            )
        )
        
        # Write anomaly events to priority topic
        _ = (
            anomaly_events
            | "Convert Anomalies to JSON" >> beam.Map(
                lambda x: json.dumps(x).encode('utf-8')
            )
            | "Write Anomaly Events" >> WriteToPubSub(
                topic=self.config.anomaly_topic
            )
        )
        
        # Write aggregated metrics to analytics topic
        _ = (
            windowed_normal
            | "Convert Aggregations to JSON" >> beam.Map(
                lambda x: json.dumps(x).encode('utf-8')
            )
            | "Write Aggregations" >> WriteToPubSub(
                topic=self.config.analytics_topic
            )
        )
        
        # Error handling: Create dead letter queue for failed events
        # This would be implemented with side outputs in a production system
        
        return pipeline
    
    def run(self) -> Any:
        """
        Run the video analytics pipeline.
        
        Returns:
            Pipeline result object
        """
        logging.info(f"Starting video analytics pipeline: {self.config.job_name}")
        logging.info(f"Input subscription: {self.config.input_subscription}")
        logging.info(f"Output topic: {self.config.output_topic}")
        
        pipeline = self.create_pipeline()
        result = pipeline.run()
        
        if self.config.runner == "DirectRunner":
            result.wait_until_finish()
        
        return result


def create_pipeline_from_args(args: Dict[str, Any]) -> VideoAnalyticsPipeline:
    """
    Create pipeline from command line arguments.
    
    Args:
        args: Dictionary of pipeline arguments
        
    Returns:
        VideoAnalyticsPipeline: Configured pipeline instance
    """
    config = PipelineConfig(
        gcp_project=args.get("project"),
        gcp_region=args.get("region", "us-central1"),
        job_name=args.get("job_name", "video-analytics-pipeline"),
        runner=args.get("runner", "DataflowRunner"),
        input_subscription=args.get("input_subscription"),
        output_topic=args.get("output_topic"),
        anomaly_topic=args.get("anomaly_topic"),
        analytics_topic=args.get("analytics_topic"),
        staging_location=args.get("staging_location"),
        temp_location=args.get("temp_location"),
        window_size_seconds=int(args.get("window_size", 60)),
        anomaly_confidence_threshold=float(args.get("anomaly_threshold", 0.95))
    )
    
    return VideoAnalyticsPipeline(config)