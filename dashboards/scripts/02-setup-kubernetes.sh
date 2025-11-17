#!/bin/bash

# Phase 2: Setup Kubernetes
# Configures kubectl, installs AWS Load Balancer Controller

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

echo "=== Phase 2: Setup Kubernetes ==="
echo "Cluster: $CLUSTER_NAME"
echo "Region: $REGION"
echo

# Check if stack outputs exist
if [ ! -f "stack-outputs.json" ]; then
    print_error "stack-outputs.json not found. Please run 01-deploy-infrastructure.sh first."
    exit 1
fi

# Enable public access temporarily for setup
print_status "Enabling public access for EKS cluster setup..."
aws eks update-cluster-config \
    --name "$CLUSTER_NAME" \
    --region "$REGION" \
    --resources-vpc-config endpointPublicAccess=true,endpointPrivateAccess=true

print_status "Waiting for cluster endpoint configuration update..."
aws eks wait cluster-active \
    --name "$CLUSTER_NAME" \
    --region "$REGION"

print_success "EKS cluster endpoints configured"
echo

# Configure kubectl
print_status "Configuring kubectl..."
aws eks update-kubeconfig \
    --name "$CLUSTER_NAME" \
    --region "$REGION"

print_success "kubectl configured"
echo

# Wait for Fargate to be ready
print_status "Waiting for Fargate profiles to be ready..."
sleep 10

# Verify cluster access
print_status "Verifying cluster access..."
if kubectl cluster-info >/dev/null 2>&1; then
    print_success "Cluster access verified"
else
    print_error "Cannot access cluster"
    exit 1
fi

# Create Grafana namespace
print_status "Creating Grafana namespace..."
kubectl create namespace grafana --dry-run=client -o yaml | kubectl apply -f -
print_success "Grafana namespace created"
echo

# Install AWS Load Balancer Controller
print_status "Installing AWS Load Balancer Controller..."

# Get IAM role ARN
ALB_ROLE_ARN=$(jq -r '.[] | select(.OutputKey=="AWSLoadBalancerControllerRoleArn") | .OutputValue' stack-outputs.json)

if [ -z "$ALB_ROLE_ARN" ] || [ "$ALB_ROLE_ARN" == "null" ]; then
    print_error "Could not find AWS Load Balancer Controller Role ARN in stack outputs"
    exit 1
fi

print_status "Using IAM role: $ALB_ROLE_ARN"

# Create service account
kubectl create serviceaccount aws-load-balancer-controller \
    -n kube-system \
    --dry-run=client -o yaml | kubectl apply -f -

# Annotate service account with IAM role
kubectl annotate serviceaccount aws-load-balancer-controller \
    -n kube-system \
    eks.amazonaws.com/role-arn="$ALB_ROLE_ARN" \
    --overwrite

print_success "Service account configured"

# Add Helm repo
print_status "Adding AWS EKS Helm repository..."
helm repo add eks https://aws.github.io/eks-charts
helm repo update

# Install AWS Load Balancer Controller
print_status "Installing AWS Load Balancer Controller..."
helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
    -n kube-system \
    --set clusterName="$CLUSTER_NAME" \
    --set serviceAccount.create=false \
    --set serviceAccount.name=aws-load-balancer-controller \
    --wait \
    --timeout=10m

# Verify controller is running
print_status "Verifying Load Balancer Controller deployment..."
kubectl wait --for=condition=available deployment/aws-load-balancer-controller \
    -n kube-system \
    --timeout=300s

print_success "AWS Load Balancer Controller installed and ready"
echo

# Check controller logs
print_status "Controller status:"
kubectl get deployment -n kube-system aws-load-balancer-controller
echo

print_success "Phase 2 complete!"
echo
echo "Next step: Run ./03-deploy-grafana.sh"
