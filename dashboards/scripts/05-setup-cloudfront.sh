#!/bin/bash

# Phase 5: Setup CloudFront
# Adds CloudFront CDN and secures ALB for CloudFront-only access

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

echo "=== Phase 5: Setup CloudFront ==="
echo

# Check if ALB DNS exists
if [ -z "$ALB_DNS" ]; then
    if [ -f "alb-dns-name.txt" ]; then
        ALB_DNS=$(cat alb-dns-name.txt)
        echo "ALB_DNS=$ALB_DNS" >> "$CONFIG_FILE"
    else
        print_error "ALB DNS not found. Please run 03-deploy-grafana.sh first."
        exit 1
    fi
fi

print_status "ALB DNS: $ALB_DNS"
echo

# Update CloudFormation stack with CloudFront
print_status "Updating CloudFormation stack to add CloudFront..."

params=(
    "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
    "ParameterKey=ClusterName,ParameterValue=$CLUSTER_NAME"
    "ParameterKey=DBMasterPassword,ParameterValue=$DB_PASSWORD"
    "ParameterKey=DBMasterUsername,UsePreviousValue=true"
    "ParameterKey=DBInstanceClass,UsePreviousValue=true"
    "ParameterKey=DBAllocatedStorage,UsePreviousValue=true"
    "ParameterKey=GrafanaAlbDnsName,ParameterValue=$ALB_DNS"
)

if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ]; then
    params+=("ParameterKey=GitHubOIDCProviderArn,ParameterValue=$GITHUB_OIDC_PROVIDER_ARN")
else
    params+=("ParameterKey=GitHubOIDCProviderArn,UsePreviousValue=true")
fi

# Create change set
change_set_name="cloudfront-changeset-$(date +%s)"
aws cloudformation create-change-set \
    --stack-name "$STACK_NAME" \
    --change-set-name "$change_set_name" \
    --template-body file://../infrastructure/cloudformation/valkey-benchmark-stack.yaml \
    --parameters "${params[@]}" \
    --capabilities CAPABILITY_NAMED_IAM \
    --region "$REGION"

print_status "Waiting for change set creation..."
sleep 10

# Check if there are changes
changes=$(aws cloudformation describe-change-set \
    --stack-name "$STACK_NAME" \
    --change-set-name "$change_set_name" \
    --region "$REGION" \
    --query 'Changes' \
    --output text 2>/dev/null || echo "")

if [[ "$changes" == "None" ]] || [[ -z "$changes" ]]; then
    print_warning "No changes detected. CloudFront may already be configured."
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
    
    print_status "Waiting for CloudFront distribution creation..."
    aws cloudformation wait stack-update-complete \
        --stack-name "$STACK_NAME" \
        --region "$REGION"
    
    print_success "Stack update completed"
fi

# Get updated stack outputs
print_status "Retrieving CloudFront information..."
aws cloudformation describe-stacks \
    --stack-name "$STACK_NAME" \
    --region "$REGION" \
    --query 'Stacks[0].Outputs' \
    --output json > stack-outputs.json

CLOUDFRONT_DOMAIN=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputValue' stack-outputs.json 2>/dev/null || echo "")

if [ -z "$CLOUDFRONT_DOMAIN" ] || [ "$CLOUDFRONT_DOMAIN" == "null" ]; then
    print_error "CloudFront domain not found in stack outputs"
    exit 1
fi

print_success "CloudFront distribution created: https://$CLOUDFRONT_DOMAIN"
echo "CLOUDFRONT_DOMAIN=$CLOUDFRONT_DOMAIN" >> "$CONFIG_FILE"
echo

# Update Grafana configuration with CloudFront URL
print_status "Updating Grafana configuration with CloudFront URL..."

helm upgrade grafana grafana/grafana \
    --namespace grafana \
    --reuse-values \
    --set env.GF_SERVER_ROOT_URL="https://$CLOUDFRONT_DOMAIN" \
    --set grafana\\.ini.server.root_url="https://$CLOUDFRONT_DOMAIN" \
    --wait

# Restart Grafana to apply changes
print_status "Restarting Grafana..."
kubectl rollout restart deployment grafana -n grafana
kubectl rollout status deployment grafana -n grafana --timeout=300s

print_success "Grafana updated with CloudFront URL"
echo

# Verify ALB security (should already be secured from Phase 03)
print_status "Verifying ALB security configuration..."

# Get CloudFront managed prefix list
CLOUDFRONT_PREFIX_LIST=$(aws ec2 describe-managed-prefix-lists \
    --filters "Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing" \
    --region "$REGION" \
    --query 'PrefixLists[0].PrefixListId' \
    --output text)

if [ -z "$CLOUDFRONT_PREFIX_LIST" ] || [ "$CLOUDFRONT_PREFIX_LIST" == "None" ]; then
    print_error "Could not find CloudFront managed prefix list"
    exit 1
fi

print_status "CloudFront prefix list: $CLOUDFRONT_PREFIX_LIST"

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
    print_error "Could not find ALB security group"
    exit 1
fi

print_status "ALB security group: $ALB_SG"

# Get EKS cluster security group
CLUSTER_SG=$(aws eks describe-cluster \
    --name "$CLUSTER_NAME" \
    --region "$REGION" \
    --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' \
    --output text)

print_status "EKS cluster security group: $CLUSTER_SG"

# Check current ingress rules
print_status "Checking current security group rules..."
CURRENT_RULES=$(aws ec2 describe-security-groups \
    --group-ids "$ALB_SG" \
    --region "$REGION" \
    --query 'SecurityGroups[0].IpPermissions[?FromPort==`80`]' \
    --output json)

# Remove public access rule if it exists
PUBLIC_RULE=$(echo "$CURRENT_RULES" | jq -r '.[] | select(.IpRanges[]?.CidrIp == "0.0.0.0/0")')
if [ -n "$PUBLIC_RULE" ]; then
    print_status "Removing public access rule (0.0.0.0/0)..."
    aws ec2 revoke-security-group-ingress \
        --group-id "$ALB_SG" \
        --region "$REGION" \
        --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]' \
        2>/dev/null && print_success "Removed public access rule" || print_warning "Failed to remove public access rule"
else
    print_suc

# Verify CloudFront prefix list rule exists
print_status "Ensuring CloudFront prefix list rule exists..."
aws ec2 authorize-security-group-ingress \
    --group-id "$ALB_SG" \
    --region "$REGION" \
    --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 80, \"ToPort\": 80, \"PrefixListIds\": [{\"PrefixListId\": \"$CLOUDFRONT_PREFIX_LIST\", \"Description\": \"CloudFront origin access\"}]}]" \
    2>/dev/null && print_warning "Added CloudFront access rule (should have been done in Phase 03)" || print_success "CloudFront access already configured"

# Ensure EKS pods can receive traffic from ALB
print_status "Ensuring EKS pods can receive traffic from ALB..."
aws ec2 authorize-security-group-ingress \
    --group-id "$CLUSTER_SG" \
    --region "$REGION" \
    --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 3000, \"ToPort\": 3000, \"UserIdGroupPairs\": [{\"GroupId\": \"$ALB_SG\", \"Description\": \"Allow ALB to reach Grafana pods\"}]}]" \
    2>/dev/null && print_status "Added ALB to EKS pod rule" || print_status "ALB to EKS pod rule already exists"

print_success "ALB security verification complete!"
echo

echo "=== CloudFront Setup Complete ==="
echo "CloudFront URL: https://$CLOUDFRONT_DOMAIN"
echo "ALB is now restricted to CloudFront access only"
echo

print_success "Phase 5 complete!"
echo
echo "Next step: Run ./06-finalize-deployment.sh to complete setup"
