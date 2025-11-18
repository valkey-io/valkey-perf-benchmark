#!/bin/bash

# Phase 3: Deploy Grafana
# Deploys Grafana with PostgreSQL backend and ALB Ingress

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

echo "=== Phase 3: Deploy Grafana ==="
echo

# Check if stack outputs exist
if [ ! -f "stack-outputs.json" ]; then
    print_error "stack-outputs.json not found. Please run 01-deploy-infrastructure.sh first."
    exit 1
fi

# Get RDS endpoint
RDS_ENDPOINT=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)

if [ -z "$RDS_ENDPOINT" ] || [ "$RDS_ENDPOINT" == "null" ]; then
    print_error "Could not find RDS endpoint in stack outputs"
    exit 1
fi

print_status "RDS Endpoint: $RDS_ENDPOINT"
echo

# Create PostgreSQL secret for Grafana
print_status "Creating PostgreSQL secret..."
kubectl create secret generic grafana-postgres-secret \
    --namespace grafana \
    --from-literal=GF_DATABASE_TYPE=postgres \
    --from-literal=GF_DATABASE_HOST="$RDS_ENDPOINT:5432" \
    --from-literal=GF_DATABASE_NAME=grafana \
    --from-literal=GF_DATABASE_USER=postgres \
    --from-literal=GF_DATABASE_PASSWORD="$DB_PASSWORD" \
    --from-literal=GF_DATABASE_SSL_MODE=require \
    --dry-run=client -o yaml | kubectl apply -f -

print_success "PostgreSQL secret created"
echo

# Update Grafana values with RDS endpoint
print_status "Preparing Grafana configuration..."
sed "s|<RDS_ENDPOINT>|$RDS_ENDPOINT|g" ../grafana/grafana-values.yaml > grafana-values-updated.yaml

print_success "Grafana configuration prepared"
echo

# Add Grafana Helm repo
print_status "Adding Grafana Helm repository..."
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

# Install Grafana
print_status "Installing Grafana..."
helm upgrade --install grafana grafana/grafana \
    --namespace grafana \
    --values grafana-values-updated.yaml \
    --wait \
    --timeout=10m

print_success "Grafana deployed"
echo

# Apply ALB Ingress
print_status "Creating ALB Ingress..."
kubectl apply -f ../kubernetes/alb-ingress.yaml

print_success "ALB Ingress created"
echo

# Wait for ALB provisioning
print_status "Waiting for Application Load Balancer to be provisioned..."

max_attempts=60
attempt=0
start_time=$(date +%s)

while [ $attempt -lt $max_attempts ]; do
    ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    
    if [ -n "$ALB_DNS" ]; then
        elapsed=$(($(date +%s) - start_time))
        print_success "ALB provisioned: $ALB_DNS (took ${elapsed}s)"
        echo "$ALB_DNS" > alb-dns-name.txt
        echo "ALB_DNS=$ALB_DNS" >> "$CONFIG_FILE"
        break
    fi
    
    elapsed=$(($(date +%s) - start_time))
    echo -ne "\rWaiting for ALB... (${elapsed}s elapsed, attempt $((attempt + 1))/$max_attempts)"
    
    sleep 10
    ((attempt++))
done

echo # New line after progress

if [ -z "$ALB_DNS" ]; then
    print_error "Timeout waiting for ALB provisioning"
    print_error "Check AWS Load Balancer Controller logs:"
    echo "  kubectl logs -n kube-system deployment/aws-load-balancer-controller"
    exit 1
fi

# Test ALB health
print_status "Testing ALB health..."
sleep 30 # Give ALB time to register targets

health_attempts=0
while [ $health_attempts -lt 12 ]; do
    http_code=$(curl -s -o /dev/null -w "%{http_code}" "http://$ALB_DNS" 2>/dev/null || echo "000")
    if [[ "$http_code" =~ ^(200|302|404)$ ]]; then
        print_success "ALB is responding (HTTP $http_code)"
        break
    fi
    echo -n "."
    sleep 5
    ((health_attempts++))
done
echo

# Get Grafana admin password
print_status "Retrieving Grafana admin password..."
GRAFANA_PASSWORD=$(kubectl get secret --namespace grafana grafana \
    -o jsonpath="{.data.admin-password}" 2>/dev/null | base64 --decode 2>/dev/null || echo "")

echo
echo "=== Grafana Deployed ==="
echo "Grafana URL: http://$ALB_DNS"
echo "Username: admin"
echo "Password: $GRAFANA_PASSWORD"
echo
print_warning "Note: ALB is currently publicly accessible"
print_warning "CloudFront will be added in the next phase for secure access"
echo

print_success "Phase 3 complete!"
echo
echo "Next steps:"
echo "1. Run ./04-setup-database.sh to initialize PostgreSQL databases"
echo "2. Run ./05-setup-cloudfront.sh to add CloudFront CDN"
