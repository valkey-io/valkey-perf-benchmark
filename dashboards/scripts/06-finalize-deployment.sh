#!/bin/bash

# Phase 6: Finalize Deployment
# Disables public EKS access and displays final information

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

echo "=== Phase 6: Finalize Deployment ==="
echo

# Disable public access to EKS cluster
print_status "Disabling public access to EKS cluster..."
print_warning "After this, you'll need to access the cluster from within the VPC"

aws eks update-cluster-config \
    --name "$CLUSTER_NAME" \
    --region "$REGION" \
    --resources-vpc-config endpointPublicAccess=false,endpointPrivateAccess=true

print_status "Waiting for cluster endpoint configuration update..."
aws eks wait cluster-active \
    --name "$CLUSTER_NAME" \
    --region "$REGION"

print_success "EKS cluster is now private access only"
echo

# Get final information
RDS_ENDPOINT=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)
VPC_ID=$(jq -r '.[] | select(.OutputKey=="VPCId") | .OutputValue' stack-outputs.json)
GRAFANA_PASSWORD=$(kubectl get secret --namespace grafana grafana \
    -o jsonpath="{.data.admin-password}" 2>/dev/null | base64 --decode 2>/dev/null || echo "N/A")

# Display final summary
echo "╔════════════════════════════════════════════════════════════════╗"
echo "║                  DEPLOYMENT COMPLETE!                          ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo
echo "=== Infrastructure Details ==="
echo "Stack Name:      $STACK_NAME"
echo "Region:          $REGION"
echo "VPC ID:          $VPC_ID"
echo "EKS Cluster:     $CLUSTER_NAME"
echo "RDS Endpoint:    $RDS_ENDPOINT"
echo

if [ -n "$CLOUDFRONT_DOMAIN" ]; then
    echo "=== Access Information ==="
    echo "Grafana URL:     https://$CLOUDFRONT_DOMAIN"
    echo "Username:        admin"
    echo "Password:        $GRAFANA_PASSWORD"
    echo
    echo "ALB URL:         http://$ALB_DNS (CloudFront-only access)"
    echo
else
    echo "=== Access Information ==="
    echo "Grafana URL:     http://$ALB_DNS"
    echo "Username:        admin"
    echo "Password:        $GRAFANA_PASSWORD"
    echo
    print_warning "CloudFront not configured. Run 05-setup-cloudfront.sh to add CDN."
    echo
fi

echo "=== Database Information ==="
echo "Databases:"
echo "  - grafana:  For Grafana configuration and dashboards"
echo "  - postgres: For benchmark_metrics data"
echo
echo "Users:"
echo "  - postgres:       Admin user with IAM authentication"
echo "  - github_actions: IAM-enabled for CI/CD workflows"
echo

echo "=== Security Status ==="
echo "✓ EKS cluster is private access only"
echo "✓ RDS is in private subnets"
if [ -n "$CLOUDFRONT_DOMAIN" ]; then
    echo "✓ ALB restricted to CloudFront access only"
    echo "✓ Public access via CloudFront CDN with HTTPS"
else
    echo "⚠ ALB is publicly accessible (CloudFront not configured)"
fi
echo

echo "=== Next Steps ==="
echo "1. Access Grafana at: https://$CLOUDFRONT_DOMAIN"
echo "2. Import dashboards from dashboards/grafana/ directory"
echo "3. Configure data sources to use 'postgres' database"
echo "4. Enable public dashboard sharing for benchmark results"
echo "5. Test GitHub Actions IAM authentication"
echo

echo "=== Useful Commands ==="
echo
echo "# Access cluster from within VPC (requires bastion or VPN):"
echo "aws eks update-kubeconfig --name $CLUSTER_NAME --region $REGION"
echo
echo "# Connect to RDS from within VPC:"
echo "kubectl run -it --rm psql-client --image=postgres:17 --restart=Never --namespace=grafana \\"
echo "  -- psql \"host=$RDS_ENDPOINT port=5432 dbname=postgres user=postgres sslmode=require\""
echo
echo "# View Grafana logs:"
echo "kubectl logs -n grafana deployment/grafana -f"
echo
echo "# View Load Balancer Controller logs:"
echo "kubectl logs -n kube-system deployment/aws-load-balancer-controller -f"
echo

echo "=== Configuration Files ==="
echo "Stack outputs:   stack-outputs.json"
echo "Configuration:   deployment-config.env"
echo "ALB DNS:         alb-dns-name.txt"
echo

print_success "All deployment phases complete!"
echo
print_status "Your Valkey Benchmark Dashboard is ready to use!"
