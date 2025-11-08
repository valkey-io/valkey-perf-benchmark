#!/bin/bash
# Script to secure ALB by restricting access to CloudFront only
# This prevents direct internet access while keeping public access via CloudFront CDN

set -e

REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${1:-grafana-cluster}"

echo "Securing ALB for CloudFront-only access..."
echo "Region: $REGION"
echo "Cluster: $CLUSTER_NAME"
echo ""

# Get CloudFront managed prefix list
echo "Getting CloudFront managed prefix list..."
CLOUDFRONT_PREFIX_LIST=$(aws ec2 describe-managed-prefix-lists \
  --filters "Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing" \
  --region "$REGION" \
  --query 'PrefixLists[0].PrefixListId' \
  --output text)

if [ -z "$CLOUDFRONT_PREFIX_LIST" ] || [ "$CLOUDFRONT_PREFIX_LIST" == "None" ]; then
  echo "❌ Error: Could not find CloudFront managed prefix list"
  exit 1
fi

echo "✅ CloudFront prefix list: $CLOUDFRONT_PREFIX_LIST"
echo ""

# Get ALB security group (created by Load Balancer Controller)
echo "Finding ALB security group..."
ALB_SG=$(aws ec2 describe-security-groups \
  --region "$REGION" \
  --filters "Name=tag:ingress.k8s.aws/stack,Values=grafana/grafana-ingress" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || echo "")

if [ -z "$ALB_SG" ] || [ "$ALB_SG" == "None" ]; then
  echo "⚠️  ALB security group not found via tag, trying alternative method..."
  
  # Try to find by description
  ALB_SG=$(aws ec2 describe-security-groups \
    --region "$REGION" \
    --filters "Name=group-name,Values=k8s-grafana-grafanai-*" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "")
fi

if [ -z "$ALB_SG" ] || [ "$ALB_SG" == "None" ]; then
  echo "❌ Error: Could not find ALB security group"
  echo "Please ensure the ingress has been deployed and the ALB is created"
  exit 1
fi

echo "✅ ALB security group: $ALB_SG"
echo ""

# Get EKS cluster security group
echo "Finding EKS cluster security group..."
CLUSTER_SG=$(aws eks describe-cluster \
  --name "$CLUSTER_NAME" \
  --region "$REGION" \
  --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' \
  --output text)

if [ -z "$CLUSTER_SG" ] || [ "$CLUSTER_SG" == "None" ]; then
  echo "❌ Error: Could not find EKS cluster security group"
  exit 1
fi

echo "✅ EKS cluster security group: $CLUSTER_SG"
echo ""

# Check if 0.0.0.0/0 rule exists
echo "Checking for public access rule..."
PUBLIC_RULE=$(aws ec2 describe-security-groups \
  --group-ids "$ALB_SG" \
  --region "$REGION" \
  --query 'SecurityGroups[0].IpPermissions[?IpProtocol==`tcp` && FromPort==`80` && ToPort==`80` && IpRanges[?CidrIp==`0.0.0.0/0`]]' \
  --output json)

if [ "$PUBLIC_RULE" != "[]" ]; then
  echo "⚠️  Found public access rule (0.0.0.0/0), removing..."
  aws ec2 revoke-security-group-ingress \
    --group-id "$ALB_SG" \
    --region "$REGION" \
    --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]' \
    2>/dev/null || echo "  (Rule may already be removed)"
  echo "✅ Removed public access rule"
else
  echo "✅ No public access rule found"
fi

echo ""

# Add CloudFront prefix list rule
echo "Adding CloudFront prefix list rule..."
aws ec2 authorize-security-group-ingress \
  --group-id "$ALB_SG" \
  --region "$REGION" \
  --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 80, \"ToPort\": 80, \"PrefixListIds\": [{\"PrefixListId\": \"$CLOUDFRONT_PREFIX_LIST\", \"Description\": \"CloudFront origin access\"}]}]" \
  2>/dev/null && echo "✅ Added CloudFront access rule" || echo "✅ CloudFront rule already exists"

echo ""

# Ensure EKS nodes can receive traffic from ALB
echo "Ensuring EKS nodes can receive traffic from ALB..."
aws ec2 authorize-security-group-ingress \
  --group-id "$CLUSTER_SG" \
  --region "$REGION" \
  --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 3000, \"ToPort\": 3000, \"UserIdGroupPairs\": [{\"GroupId\": \"$ALB_SG\", \"Description\": \"Allow ALB to reach Grafana pods\"}]}]" \
  2>/dev/null && echo "✅ Added ALB to EKS node rule" || echo "✅ ALB to EKS node rule already exists"

echo ""
echo "Security configuration complete!"
echo ""
echo "Summary:"
echo "  - ALB Security Group: $ALB_SG"
echo "  - CloudFront Prefix List: $CLOUDFRONT_PREFIX_LIST"
echo "  - EKS Cluster Security Group: $CLUSTER_SG"
echo ""
echo "Your ALB is now restricted to CloudFront access only"
echo "Public access is available via CloudFront CDN"
echo "Direct IP access to the ALB is blocked"
