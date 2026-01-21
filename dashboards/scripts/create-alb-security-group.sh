#!/bin/bash

# Helper script to create/verify ALB Security Group with CloudFront access
# This is called by 03-deploy-grafana.sh

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

# Check required parameters
if [ -z "$REGION" ] || [ -z "$VPC_ID" ] || [ -z "$PROJECT_NAME" ]; then
    print_error "Required environment variables not set: REGION, VPC_ID, PROJECT_NAME"
    exit 1
fi

print_status "Setting up ALB Security Group for CloudFront-only access..."

# Check if security group already exists
ALB_SG_ID=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=grafana-alb-cloudfront-only" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "")

if [ -z "$ALB_SG_ID" ] || [ "$ALB_SG_ID" == "None" ]; then
    print_status "Creating new ALB security group..."
    
    # Get CloudFront managed prefix list
    CLOUDFRONT_PREFIX_LIST=$(aws ec2 describe-managed-prefix-lists \
        --filters "Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing" \
        --region "$REGION" \
        --query 'PrefixLists[0].PrefixListId' \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$CLOUDFRONT_PREFIX_LIST" ] || [ "$CLOUDFRONT_PREFIX_LIST" == "None" ]; then
        print_error "Could not find CloudFront managed prefix list"
        print_error "This is required for secure ALB configuration"
        exit 1
    fi
    
    print_status "CloudFront prefix list: $CLOUDFRONT_PREFIX_LIST"
    
    # Create security group
    ALB_SG_ID=$(aws ec2 create-security-group \
        --group-name "grafana-alb-cloudfront-only" \
        --description "Security group for Grafana ALB - CloudFront access only" \
        --vpc-id "$VPC_ID" \
        --region "$REGION" \
        --query 'GroupId' \
        --output text)
    
    print_success "Created security group: $ALB_SG_ID"
    
    # Tag the security group
    aws ec2 create-tags \
        --resources "$ALB_SG_ID" \
        --region "$REGION" \
        --tags \
            "Key=Name,Value=grafana-alb-cloudfront-only" \
            "Key=Purpose,Value=Grafana-ALB-CloudFront" \
            "Key=ManagedBy,Value=deployment-script" \
            "Key=Project,Value=$PROJECT_NAME"
    
    # Add ingress rule for CloudFront
    print_status "Adding CloudFront ingress rule..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$ALB_SG_ID" \
        --region "$REGION" \
        --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 80, \"ToPort\": 80, \"PrefixListIds\": [{\"PrefixListId\": \"$CLOUDFRONT_PREFIX_LIST\", \"Description\": \"CloudFront origin access\"}]}]"
    
    print_success "ALB security group configured with CloudFront-only access"
else
    print_status "Found existing ALB security group: $ALB_SG_ID"
    
    # Verify it has CloudFront access
    HAS_CLOUDFRONT=$(aws ec2 describe-security-groups \
        --group-ids "$ALB_SG_ID" \
        --region "$REGION" \
        --query 'SecurityGroups[0].IpPermissions[?PrefixListIds[0].PrefixListId!=`null`] | length(@)' \
        --output text 2>/dev/null || echo "0")
    
    if [ "$HAS_CLOUDFRONT" == "0" ]; then
        print_warning "Security group exists but may not have CloudFront access configured"
        print_warning "Please verify security group rules manually"
    else
        print_success "Security group has CloudFront access configured"
    fi
fi

# Output the security group ID for the calling script
echo "$ALB_SG_ID"
