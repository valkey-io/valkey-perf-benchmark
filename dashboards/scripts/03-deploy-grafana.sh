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

# Check if Grafana is already deployed
SKIP_DEPLOYMENT=false
if helm list -n grafana | grep -q "^grafana" && kubectl get ingress grafana-ingress -n grafana &>/dev/null; then
    print_warning "Grafana appears to be already deployed"
    ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    if [ -n "$ALB_DNS" ]; then
        print_status "Current ALB DNS: $ALB_DNS"
        read -p "Do you want to upgrade/update the deployment? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            SKIP_DEPLOYMENT=true
            print_status "Skipping deployment. Ensuring configuration files are up to date..."
        fi
    fi
fi
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

# Skip deployment steps if user chose not to update
if [ "$SKIP_DEPLOYMENT" = true ]; then
    print_status "Skipping Helm and Ingress deployment steps..."
    # Jump to the end to ensure files are created
    # Get ALB DNS
    ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
    if [ -z "$ALB_DNS" ]; then
        print_error "Could not retrieve ALB DNS from existing ingress"
        exit 1
    fi
    
    # Get Grafana password
    GRAFANA_PASSWORD=$(kubectl get secret --namespace grafana grafana \
        -o jsonpath="{.data.admin-password}" 2>/dev/null | base64 --decode 2>/dev/null || echo "")
    
    # Save files and skip to end
    echo "$ALB_DNS" > alb-dns-name.txt
    if ! grep -q "^ALB_DNS=" "$CONFIG_FILE"; then
        echo "ALB_DNS=$ALB_DNS" >> "$CONFIG_FILE"
    else
        sed -i.bak "s|^ALB_DNS=.*|ALB_DNS=$ALB_DNS|" "$CONFIG_FILE"
    fi
    
    # Create summary file
    cat > grafana-deployment-summary.json <<EOF
{
  "alb_dns": "$ALB_DNS",
  "alb_url": "http://$ALB_DNS",
  "grafana_username": "admin",
  "grafana_password": "$GRAFANA_PASSWORD",
  "rds_endpoint": "$RDS_ENDPOINT",
  "namespace": "grafana",
  "deployment_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
    
    echo
    echo "=== Current Grafana Deployment ==="
    echo "Grafana URL: http://$ALB_DNS"
    echo "Username: admin"
    echo "Password: $GRAFANA_PASSWORD"
    echo
    print_success "Configuration files updated. Phase 3 verification complete!"
    echo
    echo "Next steps:"
    echo "1. Run ./04-setup-database.sh to initialize PostgreSQL databases"
    echo "2. Run ./05-setup-cloudfront.sh to add CloudFront CDN"
    exit 0
fi

# Create PostgreSQL secret for Grafana
if kubectl get secret grafana-postgres-secret -n grafana &>/dev/null; then
    print_status "PostgreSQL secret already exists, updating..."
    kubectl create secret generic grafana-postgres-secret \
        --namespace grafana \
        --from-literal=GF_DATABASE_TYPE=postgres \
        --from-literal=GF_DATABASE_HOST="$RDS_ENDPOINT" \
        --from-literal=GF_DATABASE_NAME=grafana \
        --from-literal=GF_DATABASE_USER=postgres \
        --from-literal=GF_DATABASE_PASSWORD="$DB_PASSWORD" \
        --from-literal=GF_DATABASE_SSL_MODE=require \
        --dry-run=client -o yaml | kubectl apply -f -
    print_success "PostgreSQL secret updated"
else
    print_status "Creating PostgreSQL secret..."
    kubectl create secret generic grafana-postgres-secret \
        --namespace grafana \
        --from-literal=GF_DATABASE_TYPE=postgres \
        --from-literal=GF_DATABASE_HOST="$RDS_ENDPOINT" \
        --from-literal=GF_DATABASE_NAME=grafana \
        --from-literal=GF_DATABASE_USER=postgres \
        --from-literal=GF_DATABASE_PASSWORD="$DB_PASSWORD" \
        --from-literal=GF_DATABASE_SSL_MODE=require
    print_success "PostgreSQL secret created"
fi
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

# Install or upgrade Grafana
if helm list -n grafana | grep -q "^grafana"; then
    print_status "Grafana already installed, upgrading..."
    helm upgrade grafana grafana/grafana \
        --namespace grafana \
        --values grafana-values-updated.yaml \
        --wait \
        --timeout=10m
    print_success "Grafana upgraded"
else
    print_status "Installing Grafana..."
    helm install grafana grafana/grafana \
        --namespace grafana \
        --values grafana-values-updated.yaml \
        --wait \
        --timeout=10m
    print_success "Grafana deployed"
fi
echo

# Apply ALB Ingress
if kubectl get ingress grafana-ingress -n grafana &>/dev/null; then
    print_status "ALB Ingress already exists, updating..."
    kubectl apply -f ../kubernetes/alb-ingress.yaml
    print_success "ALB Ingress updated"
else
    print_status "Creating ALB Ingress..."
    kubectl apply -f ../kubernetes/alb-ingress.yaml
    print_success "ALB Ingress created"
fi
echo

# Wait for ALB provisioning
print_status "Checking Application Load Balancer status..."

# Check if ALB DNS already exists in config or file
if [ -f "alb-dns-name.txt" ]; then
    ALB_DNS=$(cat alb-dns-name.txt)
    print_status "Found existing ALB DNS: $ALB_DNS"
else
    ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
fi

if [ -z "$ALB_DNS" ]; then
    print_status "Waiting for Application Load Balancer to be provisioned..."
    
    max_attempts=60
    attempt=0
    start_time=$(date +%s)
    
    while [ $attempt -lt $max_attempts ]; do
        ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
        
        if [ -n "$ALB_DNS" ]; then
            elapsed=$(($(date +%s) - start_time))
            print_success "ALB provisioned: $ALB_DNS (took ${elapsed}s)"
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
fi

# Save ALB DNS to file and config
echo "$ALB_DNS" > alb-dns-name.txt
if ! grep -q "^ALB_DNS=" "$CONFIG_FILE"; then
    echo "ALB_DNS=$ALB_DNS" >> "$CONFIG_FILE"
else
    # Update existing ALB_DNS in config
    sed -i.bak "s|^ALB_DNS=.*|ALB_DNS=$ALB_DNS|" "$CONFIG_FILE"
fi

print_success "ALB DNS saved to alb-dns-name.txt and $CONFIG_FILE"
echo

# Immediately secure ALB for CloudFront-only access
print_status "Securing ALB for CloudFront-only access..."

# Get CloudFront managed prefix list
CLOUDFRONT_PREFIX_LIST=$(aws ec2 describe-managed-prefix-lists \
    --filters "Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing" \
    --region "$REGION" \
    --query 'PrefixLists[0].PrefixListId' \
    --output text 2>/dev/null || echo "")

if [ -z "$CLOUDFRONT_PREFIX_LIST" ] || [ "$CLOUDFRONT_PREFIX_LIST" == "None" ]; then
    print_warning "Could not find CloudFront managed prefix list"
    print_warning "ALB will remain publicly accessible until Phase 05"
else
    print_status "CloudFront prefix list: $CLOUDFRONT_PREFIX_LIST"
    
    # Wait a moment for security group to be created
    sleep 10
    
    # Get ALB security group
    print_status "Finding ALB security group..."
    ALB_SG=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=tag:ingress.k8s.aws/stack,Values=grafana/grafana-ingress" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$ALB_SG" ] || [ "$ALB_SG" == "None" ]; then
        # Try alternative method
        ALB_SG=$(aws ec2 describe-security-groups \
            --region "$REGION" \
            --filters "Name=group-name,Values=k8s-grafana-grafanai-*" \
            --query 'SecurityGroups[0].GroupId' \
            --output text 2>/dev/null || echo "")
    fi
    
    if [ -z "$ALB_SG" ] || [ "$ALB_SG" == "None" ]; then
        print_warning "Could not find ALB security group yet"
        print_warning "ALB will remain publicly accessible until Phase 05"
    else
        print_status "ALB security group: $ALB_SG"
        
        # Remove public access rule (0.0.0.0/0)
        print_status "Removing public access rule (0.0.0.0/0)..."
        aws ec2 revoke-security-group-ingress \
            --group-id "$ALB_SG" \
            --region "$REGION" \
            --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]' \
            2>/dev/null && print_success "Removed public access rule" || print_status "Public access rule not found or already removed"
        
        # Add CloudFront prefix list rule
        print_status "Adding CloudFront prefix list rule..."
        aws ec2 authorize-security-group-ingress \
            --group-id "$ALB_SG" \
            --region "$REGION" \
            --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 80, \"ToPort\": 80, \"PrefixListIds\": [{\"PrefixListId\": \"$CLOUDFRONT_PREFIX_LIST\", \"Description\": \"CloudFront origin access\"}]}]" \
            2>/dev/null && print_success "Added CloudFront access rule" || print_status "CloudFront rule already exists"
        
        print_success "ALB secured for CloudFront-only access"
    fi
fi
echo

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

# Create summary file for subsequent phases
print_status "Creating deployment summary file..."
cat > grafana-deployment-summary.json <<EOF
{
  "alb_dns": "$ALB_DNS",
  "alb_url": "http://$ALB_DNS",
  "grafana_username": "admin",
  "grafana_password": "$GRAFANA_PASSWORD",
  "rds_endpoint": "$RDS_ENDPOINT",
  "namespace": "grafana",
  "deployment_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF

print_success "Deployment summary saved to grafana-deployment-summary.json"
echo

print_success "Phase 3 complete!"
echo
echo "Next steps:"
echo "1. Run ./04-setup-database.sh to initialize PostgreSQL databases"
echo "2. Run ./05-setup-cloudfront.sh to add CloudFront CDN"
echo
echo "Quick access:"
echo "  Grafana URL: http://$ALB_DNS"
echo "  Username: admin"
echo "  Password: $GRAFANA_PASSWORD"
