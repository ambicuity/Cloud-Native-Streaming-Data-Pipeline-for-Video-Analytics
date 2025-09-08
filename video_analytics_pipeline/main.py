"""
Main entry point for the video analytics pipeline.
"""

import logging
import sys
from typing import Dict, Any

import click
from dotenv import load_dotenv

from .beam.pipeline import VideoAnalyticsPipeline, create_pipeline_from_args
from .config.settings import PipelineConfig, PubSubConfig
from .pubsub.manager import PubSubManager
from .monitoring.metrics import MetricsCollector, PipelineLogger, HealthChecker
from .security.iam import IAMManager, SecurityValidator, create_security_deployment_guide


# Load environment variables
load_dotenv()


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('video_analytics_pipeline.log')
        ]
    )


@click.group()
@click.option('--log-level', default='INFO', help='Set the logging level')
@click.pass_context
def cli(ctx, log_level):
    """Video Analytics Pipeline CLI."""
    setup_logging(log_level)
    ctx.ensure_object(dict)
    ctx.obj['log_level'] = log_level


@cli.command()
@click.option('--project', required=True, help='Google Cloud project ID')
@click.option('--region', default='us-central1', help='Google Cloud region')
@click.option('--job-name', default='video-analytics-pipeline', help='Dataflow job name')
@click.option('--runner', default='DataflowRunner', help='Apache Beam runner (DataflowRunner or DirectRunner)')
@click.option('--input-subscription', required=True, help='Input Pub/Sub subscription')
@click.option('--output-topic', required=True, help='Output Pub/Sub topic')
@click.option('--anomaly-topic', required=True, help='Anomaly Pub/Sub topic')
@click.option('--analytics-topic', required=True, help='Analytics Pub/Sub topic')
@click.option('--staging-location', required=True, help='GCS staging location')
@click.option('--temp-location', required=True, help='GCS temp location')
@click.option('--window-size', default=60, help='Window size in seconds')
@click.option('--anomaly-threshold', default=0.95, help='Anomaly confidence threshold')
def run_pipeline(**kwargs):
    """Run the video analytics streaming pipeline."""
    try:
        logging.info("Starting video analytics pipeline...")
        
        # Create pipeline from arguments
        pipeline = create_pipeline_from_args(kwargs)
        
        # Run the pipeline
        result = pipeline.run()
        
        logging.info("Pipeline started successfully")
        
        if kwargs['runner'] == 'DirectRunner':
            logging.info("Pipeline completed")
        else:
            logging.info(f"Pipeline submitted to Dataflow. Job name: {kwargs['job_name']}")
            
    except Exception as e:
        logging.error(f"Failed to start pipeline: {e}")
        sys.exit(1)


@cli.command()
@click.option('--project', required=True, help='Google Cloud project ID')
@click.option('--config-file', help='Configuration file path')
def setup_infrastructure(project, config_file):
    """Setup Pub/Sub topics, subscriptions, and other infrastructure."""
    try:
        logging.info("Setting up pipeline infrastructure...")
        
        # Create Pub/Sub configuration
        pubsub_config = PubSubConfig(project_id=project)
        
        # Create Pub/Sub manager and setup topics/subscriptions
        pubsub_manager = PubSubManager(pubsub_config)
        pubsub_manager.create_topics_and_subscriptions()
        
        logging.info("Infrastructure setup completed successfully")
        
    except Exception as e:
        logging.error(f"Failed to setup infrastructure: {e}")
        sys.exit(1)


@cli.command()
@click.option('--project', required=True, help='Google Cloud project ID')
@click.option('--subscription', required=True, help='Subscription name to monitor')
def monitor_health(project, subscription):
    """Monitor pipeline health and metrics."""
    try:
        logging.info("Starting health monitoring...")
        
        # Initialize monitoring components
        metrics_collector = MetricsCollector(project_id=project)
        logger = PipelineLogger(project_id=project)
        health_checker = HealthChecker(metrics_collector, logger)
        
        # Perform health checks
        components = ["pubsub_connection", "dataflow_job", "data_quality", "monitoring"]
        
        for component in components:
            health_status = health_checker.check_component_health(component)
            click.echo(f"Component {component}: {health_status['status']}")
        
        # Get overall health
        overall_health = health_checker.get_overall_health()
        click.echo(f"\nOverall pipeline health: {overall_health['status']}")
        click.echo(f"Healthy components: {overall_health['healthy_components']}/{overall_health['total_components']}")
        
    except Exception as e:
        logging.error(f"Health monitoring failed: {e}")
        sys.exit(1)


@cli.command()
@click.option('--project', required=True, help='Google Cloud project ID')
@click.option('--output-file', help='Output file for security configuration')
def validate_security(project, output_file):
    """Validate security configuration and generate compliance report."""
    try:
        logging.info("Validating security configuration...")
        
        # Initialize security validator
        validator = SecurityValidator()
        iam_manager = IAMManager(project)
        
        # Create sample security configuration for validation
        security_config = {
            "iam": {
                "service_accounts": [
                    {
                        "name": "video-analytics-pipeline",
                        "roles": ["roles/dataflow.worker", "roles/pubsub.subscriber"]
                    }
                ]
            },
            "network": {
                "use_private_ip": True,
                "firewall_rules": ["allow-internal"]
            },
            "logging": {
                "audit_logs_enabled": True,
                "data_access_logs": True
            }
        }
        
        # Validate configuration
        validation_result = validator.validate_security_config(security_config)
        
        # Display results
        click.echo(f"Security validation status: {validation_result['overall_status']}")
        
        if validation_result['violations']:
            click.echo("\nSecurity violations found:")
            for violation in validation_result['violations']:
                click.echo(f"  - {violation['check']}: {violation['message']}")
        
        if validation_result['warnings']:
            click.echo("\nWarnings:")
            for warning in validation_result['warnings']:
                click.echo(f"  - {warning}")
        
        if validation_result['recommendations']:
            click.echo("\nRecommendations:")
            for rec in validation_result['recommendations']:
                click.echo(f"  - {rec}")
        
        # Generate deployment guide
        deployment_guide = create_security_deployment_guide()
        
        if output_file:
            import json
            with open(output_file, 'w') as f:
                json.dump({
                    "validation_result": validation_result,
                    "deployment_guide": deployment_guide
                }, f, indent=2)
            click.echo(f"\nSecurity report saved to {output_file}")
        
        logging.info("Security validation completed")
        
    except Exception as e:
        logging.error(f"Security validation failed: {e}")
        sys.exit(1)


@cli.command()
def generate_config():
    """Generate sample configuration files."""
    try:
        import json
        import os
        
        # Create sample environment file
        env_content = """# Video Analytics Pipeline Configuration
GCP_PROJECT=your-project-id
GCP_REGION=us-central1
JOB_NAME=video-analytics-pipeline
RUNNER=DataflowRunner

# Pub/Sub Configuration
INPUT_SUBSCRIPTION=projects/your-project-id/subscriptions/video-analytics-input-sub
OUTPUT_TOPIC=projects/your-project-id/topics/video-analytics-output
ANOMALY_TOPIC=projects/your-project-id/topics/video-analytics-anomalies
ANALYTICS_TOPIC=projects/your-project-id/topics/video-analytics-analytics

# Storage Configuration
STAGING_LOCATION=gs://your-staging-bucket/staging
TEMP_LOCATION=gs://your-temp-bucket/temp

# Processing Configuration
WINDOW_SIZE_SECONDS=60
ANOMALY_THRESHOLD=0.95
MAX_NUM_WORKERS=10

# Monitoring Configuration
ENABLE_MONITORING=true
METRICS_EXPORT_INTERVAL=30
"""
        
        with open('.env.example', 'w') as f:
            f.write(env_content)
        
        # Create sample pipeline configuration
        pipeline_config = {
            "pipeline": {
                "name": "video-analytics-pipeline",
                "description": "Cloud-native streaming data pipeline for video analytics",
                "version": "1.0.0"
            },
            "gcp": {
                "project_id": "your-project-id",
                "region": "us-central1",
                "zone": "us-central1-a"
            },
            "dataflow": {
                "runner": "DataflowRunner",
                "worker_machine_type": "n1-standard-2",
                "max_num_workers": 10,
                "use_public_ips": False,
                "network": "projects/your-project-id/global/networks/default",
                "subnetwork": "projects/your-project-id/regions/us-central1/subnetworks/default"
            },
            "pubsub": {
                "topics": {
                    "input": "video-analytics-input",
                    "output": "video-analytics-output", 
                    "anomalies": "video-analytics-anomalies",
                    "analytics": "video-analytics-analytics",
                    "dead_letter": "video-analytics-dead-letter"
                },
                "subscriptions": {
                    "input": "video-analytics-input-sub"
                }
            },
            "processing": {
                "window_size_seconds": 60,
                "anomaly_threshold": 0.95,
                "enable_data_quality_checks": True
            },
            "monitoring": {
                "enable_custom_metrics": True,
                "enable_alerting": True,
                "log_level": "INFO"
            },
            "security": {
                "encryption_at_rest": True,
                "encryption_in_transit": True,
                "enable_audit_logs": True,
                "service_account": "video-analytics-pipeline@your-project-id.iam.gserviceaccount.com"
            }
        }
        
        with open('pipeline_config.json', 'w') as f:
            json.dump(pipeline_config, f, indent=2)
        
        click.echo("Configuration files generated:")
        click.echo("  - .env.example (environment variables)")
        click.echo("  - pipeline_config.json (pipeline configuration)")
        
    except Exception as e:
        logging.error(f"Failed to generate configuration: {e}")
        sys.exit(1)


@cli.command()
@click.option('--project', required=True, help='Google Cloud project ID')
@click.option('--service-account', default='video-analytics-pipeline', help='Service account name')
def generate_iam_config(project, service_account):
    """Generate IAM configuration for the pipeline."""
    try:
        iam_manager = IAMManager(project)
        
        # Generate service account configuration
        sa_config = iam_manager.create_service_account_config(service_account)
        
        # Generate IAM policy template
        iam_policy = iam_manager.get_iam_policy_template()
        
        # Save configurations
        import json
        
        with open('service_account_config.json', 'w') as f:
            json.dump(sa_config, f, indent=2)
        
        with open('iam_policy_template.json', 'w') as f:
            json.dump(iam_policy, f, indent=2)
        
        click.echo("IAM configuration files generated:")
        click.echo("  - service_account_config.json")
        click.echo("  - iam_policy_template.json")
        
    except Exception as e:
        logging.error(f"Failed to generate IAM configuration: {e}")
        sys.exit(1)


def main():
    """Main entry point."""
    cli()


if __name__ == '__main__':
    main()