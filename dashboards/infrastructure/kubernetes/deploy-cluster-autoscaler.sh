#!/bin/bash
set -e

# Deploy Cluster Autoscaler to EKS
# This script retrieves the IAM role ARN from CloudFormation and deploys the autoscaler

STACK_NAME="${1:-valkey-benchmark-stack}"
CLUSTER_NAME="${2:-valkey-perf-cluster}"

echo "Retrieving Cluster Autoscaler Role ARN from CloudFormation stack: ${STACK_NAME}"
ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --query "Stacks[0].Outputs[?OutputKey=='ClusterAutoscalerRoleArn'].OutputValue" \
  --output text)

if [ -z "$ROLE_ARN" ]; then
  echo "Error: Could not retrieve Cluster Autoscaler Role ARN from stack ${STACK_NAME}"
  exit 1
fi

echo "Found Role ARN: ${ROLE_ARN}"
echo "Cluster Name: ${CLUSTER_NAME}"

# Create temporary file with substituted values
TEMP_FILE=$(mktemp)
sed -e "s|\${CLUSTER_AUTOSCALER_ROLE_ARN}|${ROLE_ARN}|g" \
    -e "s|\${CLUSTER_NAME}|${CLUSTER_NAME}|g" \
    cluster-autoscaler.yaml > "${TEMP_FILE}"

echo "Applying Cluster Autoscaler manifest..."
kubectl apply -f "${TEMP_FILE}"

rm "${TEMP_FILE}"

echo ""
echo "Cluster Autoscaler deployed successfully!"
echo ""
echo "To verify the deployment:"
echo "  kubectl get deployment cluster-autoscaler -n kube-system"
echo "  kubectl logs -f deployment/cluster-autoscaler -n kube-system"
