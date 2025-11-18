#!/bin/bash

# Phase 1: Deploy AWS Infrastructure
# Creates VPC, EKS Fargate cluster, RDS PostgreSQL, IAM roles

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

# Configuration
PROJECT_NAME="valkey-benchmark"
REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="valkey-perf-cluster"

echo "=== Phase 1: Deploy AWS Infrastructure ==="
echo "Region: $REGION"
echo "Cluster: $CLUSTER_NAME"
echo

# Check if config file exists
CONFIG_FILE="deployment-config.env"
if [ -f "$CONFIG_FILE" ]; then
    print_status "Loading configuration from $CONFIG_FILE"
    source "$CONFIG_FILE"
fi

# Get stack name
if [ -z "$STACK_NAME" ]; then
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    STACK_NAME="${PROJECT_NAME}-stack-${TIMESTAMP}"
    echo "STACK_NAME=$STACK_NAME" > "$CONFIG_FILE"
    echo "CLUSTER_NAME=$CLUSTER_NAME" >> "$CONFIG_FILE"
    echo "REGION=$REGION" >> "$CONFIG_FILE"
    print_status "Created new stack name: $STACK_NAME"
else
    print_status "Using existing stack: $STACK_NAME"
fi

# Get RDS password
if [ -z "$DB_PASSWORD" ]; then
    echo
    print_status "Database Configuration"
    while true; do
        read -s -p "Enter RDS master password (min 8 characters): " DB_PASSWORD
        echo
        if [ ${#DB_PASSWORD} -ge 8 ]; then
            break
        else
            print_error "Password must be at least 8 characters long"
        fi
    done
    echo "DB_PASSWORD=$DB_PASSWORD" >> "$CONFIG_FILE"
fi

# Check for existing GitHub OIDC Provider
if [ -z "$GITHUB_OIDC_PROVIDER_ARN" ]; then
    print_status "Checking for existing GitHub OIDC Provider..."
    GITHUB_OIDC_PROVIDER_ARN=$(aws iam list-open-id-connect-providers \
        --query 'OpenIDConnectProviderList[?contains(Arn, `token.actions.githubusercontent.com`)].Arn' \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ]; then
        print_success "Found existing GitHub OIDC Provider: $GITHUB_OIDC_PROVIDER_ARN"
        echo "GITHUB_OIDC_PROVIDER_ARN=$GITHUB_OIDC_PROVIDER_ARN" >> "$CONFIG_FILE"
    else
        print_status "No existing GitHub OIDC Provider found. Will create new one."
    fi
fi

# Prepare CloudFormation parameters
params=(
    "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
    "ParameterKey=ClusterName,ParameterValue=$CLUSTER_NAME"
    "ParameterKey=DBMasterPassword,ParameterValue=$DB_PASSWORD"
)

if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ]; then
    params+=("ParameterKey=GitHubOIDCProviderArn,ParameterValue=$GITHUB_OIDC_PROVIDER_ARN")
fi

# Check if stack exists
if aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" >/dev/null 2>&1; then
    print_status "Stack exists. Checking for updates..."
    
    # Create change set
    change_set_name="update-changeset-$(date +%s)"
    aws cloudformation create-change-set \
        --stack-name "$STACK_NAME" \
        --change-set-name "$change_set_name" \
        --template-body file://../infrastructure/cloudformation/valkey-benchmark-stack.yaml \
        --parameters "${params[@]}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$REGION" >/dev/null 2>&1
    
    sleep 5
    
    # Check if there are changes
    changes=$(aws cloudformation describe-change-set \
        --stack-name "$STACK_NAME" \
        --change-set-name "$change_set_name" \
        --region "$REGION" \
        --query 'Changes' \
        --output text 2>/dev/null || echo "")
    
    if [[ "$changes" == "None" ]] || [[ -z "$changes" ]]; then
        print_status "No changes detected. Stack is up to date."
        aws cloudformation delete-change-set \
            --stack-name "$STACK_NAME" \
            --change-set-name "$change_set_name" \
            --region "$REGION" >/dev/null 2>&1
    else
        print_status "Executing stack update..."
        aws cloudformation execute-change-set \
            --stack-name "$STACK_NAME" \
            --change-set-name "$change_set_name" \
            --region "$REGION"
        
        print_status "Waiting for stack update to complete..."
        aws cloudformation wait stack-update-complete \
            --stack-name "$STACK_NAME" \
            --region "$REGION"
        
        print_success "Stack update completed"
    fi
else
    print_status "Creating new CloudFormation stack..."
    
    aws cloudformation create-stack \
        --stack-name "$STACK_NAME" \
        --template-body file://../infrastructure/cloudformation/valkey-benchmark-stack.yaml \
        --parameters "${params[@]}" \
        --capabilities CAPABILITY_NAMED_IAM \
        --region "$REGION"
    
    print_status "Stack creation initiated..."
    echo "Resources being created:"
    echo "  - VPC with public/private subnets (2 AZs)"
    echo "  - NAT Gateways (2)"
    echo "  - EKS Fargate cluster"
    echo "  - Fargate profiles"
    echo "  - RDS PostgreSQL (Multi-AZ)"
    echo "  - Security groups"
    echo "  - IAM roles"
    echo
    
    aws cloudformation wait stack-create-complete \
        --stack-name "$STACK_NAME" \
        --region "$REGION"
    
    print_success "Stack creation completed"
fi

# Save stack outputs
print_status "Saving stack outputs..."
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs' \
    --output json > stack-outputs.json

print_success "Stack outputs saved to stack-outputs.json"
echo

# Display key outputs
RDS_ENDPOINT=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)
VPC_ID=$(jq -r '.[] | select(.OutputKey=="VPCId") | .OutputValue' stack-outputs.json)
EKS_CLUSTER=$(jq -r '.[] | select(.OutputKey=="EKSClusterName") | .OutputValue' stack-outputs.json)

echo "=== Infrastructure Deployed ==="
echo "VPC ID: $VPC_ID"
echo "EKS Cluster: $EKS_CLUSTER"
echo "RDS Endpoint: $RDS_ENDPOINT"
echo

print_success "Phase 1 complete!"
echo
echo "Next step: Run ./02-setup-kubernetes.sh"
