#!/bin/bash

# Phase 4: Setup Database
# Provides instructions and helper commands for database initialization

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Load configuration
CONFIG_FILE="deployment-config.env"
if [ ! -f "$CONFIG_FILE" ]; then
    print_error "Configuration file not found. Please run 01-deploy-infrastructure.sh first."
    exit 1
fi

source "$CONFIG_FILE"

echo "=== Phase 4: Database Setup ==="
echo

# Check if stack outputs exist
if [ ! -f "stack-outputs.json" ]; then
    print_error "stack-outputs.json not found. Please run 01-deploy-infrastructure.sh first."
    exit 1
fi

# Get RDS endpoint
RDS_ENDPOINT=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)

print_warning "Database setup must be done manually from within the VPC"
print_status "This script will help you set up the databases"
echo

echo "=== Database Setup Instructions ==="
echo
echo "The schema.sql file will create:"
echo "  - grafana database: For Grafana settings and dashboards"
echo "  - postgres database: For benchmark_metrics table"
echo "  - postgres user (Admin): Full access with IAM authentication"
echo "  - github_actions user: IAM-enabled for CI/CD workflows"
echo

# Ask user if they want to proceed
read -p "Do you want to set up the database now? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo
    print_status "Database setup skipped. You can run this script again later."
    echo
    echo "Manual setup commands:"
    echo "  1. kubectl run -it --rm psql-client --image=postgres:17 --restart=Never --namespace=grafana -- bash"
    echo "  2. Inside the pod, run: psql \"host=$RDS_ENDPOINT port=5432 dbname=postgres user=postgres password=YOUR_PASSWORD sslmode=require\""
    echo "  3. Copy and paste the contents of ../schema.sql"
    exit 0
fi

echo
print_status "Starting database setup..."
echo

# Create a temporary pod with PostgreSQL client
print_status "Creating PostgreSQL client pod..."
kubectl run psql-client \
    --image=postgres:17 \
    --restart=Never \
    --namespace=grafana \
    --command -- sleep 3600

# Wait for pod to be ready
print_status "Waiting for pod to be ready..."
kubectl wait --for=condition=Ready pod/psql-client -n grafana --timeout=120s

print_success "PostgreSQL client pod is ready"
echo

# Copy schema file to pod
print_status "Copying schema.sql to pod..."
kubectl cp ../schema.sql grafana/psql-client:/tmp/schema.sql

print_success "Schema file copied"
echo

# Execute schema
print_status "Executing schema.sql..."
print_warning "You will be prompted for the RDS password"
echo

kubectl exec -it -n grafana psql-client -- \
    psql "host=$RDS_ENDPOINT port=5432 dbname=postgres user=postgres sslmode=require" \
    -f /tmp/schema.sql

echo
print_success "Database schema executed"
echo

# Verify setup
print_status "Verifying database setup..."
echo

kubectl exec -it -n grafana psql-client -- \
    psql "host=$RDS_ENDPOINT port=5432 dbname=postgres user=postgres sslmode=require" \
    -c "\l" \
    -c "\c postgres" \
    -c "\dt" \
    -c "\du"

echo
print_success "Database verification complete"
echo

# Clean up pod
print_status "Cleaning up PostgreSQL client pod..."
kubectl delete pod psql-client -n grafana --ignore-not-found=true

print_success "Cleanup complete"
echo

echo "=== Database Setup Complete ==="
echo
echo "Databases created:"
echo "  - grafana: For Grafana configuration"
echo "  - postgres: For benchmark_metrics data"
echo
echo "Users created:"
echo "  - postgres: Admin user with IAM authentication"
echo "  - github_actions: IAM-enabled for CI/CD"
echo

print_success "Phase 4 complete!"
echo
echo "Next step: Run ./05-setup-cloudfront.sh to add CloudFront CDN"
