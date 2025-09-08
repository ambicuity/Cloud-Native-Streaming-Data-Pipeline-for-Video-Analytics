#!/bin/bash

# Deploy Video Analytics Pipeline to Google Cloud Platform
# This script sets up the infrastructure and deploys the streaming pipeline

set -e

# Configuration
PROJECT_ID=${GCP_PROJECT:-"your-project-id"}
REGION=${GCP_REGION:-"us-central1"}
ZONE=${GCP_ZONE:-"us-central1-a"}
SERVICE_ACCOUNT_NAME="video-analytics-pipeline"
STAGING_BUCKET="gs://${PROJECT_ID}-video-analytics-staging"
TEMP_BUCKET="gs://${PROJECT_ID}-video-analytics-temp"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        log_error "gcloud CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check if authenticated
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q .; then
        log_error "Not authenticated with gcloud. Please run 'gcloud auth login'"
        exit 1
    fi
    
    # Check if project is set
    if [ "$PROJECT_ID" == "your-project-id" ]; then
        log_error "Please set the GCP_PROJECT environment variable or update the script"
        exit 1
    fi
    
    log_info "Prerequisites check passed"
}

# Enable required APIs
enable_apis() {
    log_info "Enabling required Google Cloud APIs..."
    
    apis=(
        "dataflow.googleapis.com"
        "pubsub.googleapis.com"
        "storage-component.googleapis.com"
        "monitoring.googleapis.com"
        "logging.googleapis.com"
        "iam.googleapis.com"
        "cloudresourcemanager.googleapis.com"
    )
    
    for api in "${apis[@]}"; do
        log_info "Enabling $api..."
        gcloud services enable "$api" --project="$PROJECT_ID"
    done
    
    log_info "All APIs enabled"
}

# Create service account
create_service_account() {
    log_info "Creating service account..."
    
    # Create service account
    gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" \
        --display-name="Video Analytics Pipeline Service Account" \
        --description="Service account for video analytics streaming pipeline" \
        --project="$PROJECT_ID" || log_warn "Service account may already exist"
    
    # Grant required roles
    roles=(
        "roles/dataflow.worker"
        "roles/dataflow.developer"
        "roles/pubsub.subscriber"
        "roles/pubsub.publisher"
        "roles/storage.objectAdmin"
        "roles/monitoring.metricWriter"
        "roles/logging.logWriter"
    )
    
    service_account_email="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
    
    for role in "${roles[@]}"; do
        log_info "Granting role $role to service account..."
        gcloud projects add-iam-policy-binding "$PROJECT_ID" \
            --member="serviceAccount:$service_account_email" \
            --role="$role"
    done
    
    log_info "Service account created and configured"
}

# Create storage buckets
create_storage_buckets() {
    log_info "Creating storage buckets..."
    
    # Create staging bucket
    if ! gsutil ls "$STAGING_BUCKET" &> /dev/null; then
        log_info "Creating staging bucket: $STAGING_BUCKET"
        gsutil mb -p "$PROJECT_ID" -c STANDARD -l "$REGION" "$STAGING_BUCKET"
    else
        log_warn "Staging bucket already exists: $STAGING_BUCKET"
    fi
    
    # Create temp bucket
    if ! gsutil ls "$TEMP_BUCKET" &> /dev/null; then
        log_info "Creating temp bucket: $TEMP_BUCKET"
        gsutil mb -p "$PROJECT_ID" -c STANDARD -l "$REGION" "$TEMP_BUCKET"
    else
        log_warn "Temp bucket already exists: $TEMP_BUCKET"
    fi
    
    # Set bucket permissions
    service_account_email="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
    gsutil iam ch "serviceAccount:$service_account_email:objectAdmin" "$STAGING_BUCKET"
    gsutil iam ch "serviceAccount:$service_account_email:objectAdmin" "$TEMP_BUCKET"
    
    log_info "Storage buckets created and configured"
}

# Create Pub/Sub topics and subscriptions
create_pubsub_resources() {
    log_info "Creating Pub/Sub topics and subscriptions..."
    
    topics=(
        "video-analytics-input"
        "video-analytics-output"
        "video-analytics-anomalies"
        "video-analytics-analytics"
        "video-analytics-dead-letter"
    )
    
    # Create topics
    for topic in "${topics[@]}"; do
        if ! gcloud pubsub topics describe "$topic" --project="$PROJECT_ID" &> /dev/null; then
            log_info "Creating topic: $topic"
            gcloud pubsub topics create "$topic" --project="$PROJECT_ID"
        else
            log_warn "Topic already exists: $topic"
        fi
    done
    
    # Create input subscription
    subscription="video-analytics-input-sub"
    if ! gcloud pubsub subscriptions describe "$subscription" --project="$PROJECT_ID" &> /dev/null; then
        log_info "Creating subscription: $subscription"
        gcloud pubsub subscriptions create "$subscription" \
            --topic="video-analytics-input" \
            --dead-letter-topic="video-analytics-dead-letter" \
            --max-delivery-attempts=5 \
            --ack-deadline=60 \
            --message-retention-duration=7d \
            --project="$PROJECT_ID"
    else
        log_warn "Subscription already exists: $subscription"
    fi
    
    log_info "Pub/Sub resources created"
}

# Deploy the pipeline
deploy_pipeline() {
    log_info "Deploying video analytics pipeline..."
    
    # Build and install the package
    pip install -e .
    
    # Set environment variables
    export GCP_PROJECT="$PROJECT_ID"
    export GCP_REGION="$REGION"
    export STAGING_LOCATION="$STAGING_BUCKET/staging"
    export TEMP_LOCATION="$TEMP_BUCKET/temp"
    export INPUT_SUBSCRIPTION="projects/$PROJECT_ID/subscriptions/video-analytics-input-sub"
    export OUTPUT_TOPIC="projects/$PROJECT_ID/topics/video-analytics-output"
    export ANOMALY_TOPIC="projects/$PROJECT_ID/topics/video-analytics-anomalies"
    export ANALYTICS_TOPIC="projects/$PROJECT_ID/topics/video-analytics-analytics"
    
    # Run the pipeline setup
    python -m video_analytics_pipeline.main setup-infrastructure \
        --project="$PROJECT_ID"
    
    log_info "Pipeline infrastructure setup completed"
    log_info "To run the pipeline, use:"
    log_info "python -m video_analytics_pipeline.main run-pipeline \\"
    log_info "  --project='$PROJECT_ID' \\"
    log_info "  --region='$REGION' \\"
    log_info "  --input-subscription='projects/$PROJECT_ID/subscriptions/video-analytics-input-sub' \\"
    log_info "  --output-topic='projects/$PROJECT_ID/topics/video-analytics-output' \\"
    log_info "  --anomaly-topic='projects/$PROJECT_ID/topics/video-analytics-anomalies' \\"
    log_info "  --analytics-topic='projects/$PROJECT_ID/topics/video-analytics-analytics' \\"
    log_info "  --staging-location='$STAGING_BUCKET/staging' \\"
    log_info "  --temp-location='$TEMP_BUCKET/temp'"
}

# Main deployment function
main() {
    log_info "Starting Video Analytics Pipeline deployment..."
    log_info "Project: $PROJECT_ID"
    log_info "Region: $REGION"
    
    check_prerequisites
    enable_apis
    create_service_account
    create_storage_buckets
    create_pubsub_resources
    deploy_pipeline
    
    log_info "Deployment completed successfully!"
    log_info "Next steps:"
    log_info "1. Generate sample events and publish to the input topic"
    log_info "2. Run the pipeline to start processing"
    log_info "3. Monitor the pipeline using the health check command"
}

# Run main function
main "$@"