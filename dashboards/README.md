# Valkey Benchmark Dashboard Infrastructure

Complete AWS infrastructure and Grafana dashboards for visualizing Valkey performance benchmarks.

## Contents

- **infrastructure/cloudformation/valkey-benchmark-stack.yaml** - Complete AWS infrastructure (EKS, RDS, VPC, CloudFront)
- **deploy-stack.sh** - Automated deployment script
- **alb-ingress.yaml** - Kubernetes Ingress for Application Load Balancer
- **grafana-values.yaml** - Helm chart values for Grafana
- **valkey-dashboard-public.json** - Public-facing Grafana dashboard
- **valkey-dashboard-with-commits.json** - Enhanced dashboard with commit tracking

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate credentials
- kubectl installed
- helm installed (v3+)
- jq installed

### 1. Deploy Infrastructure

```bash
cd dashboards
./deploy-stack.sh
```

You'll be prompted for:
- RDS master password (min 8 characters)
- EC2 key pair (will create if it doesn't exist)

The script deploys (~25-40 minutes):
- VPC with public/private subnets and NAT gateways
- EKS cluster (v1.33) with ARM-based worker nodes
- RDS PostgreSQL 17 instance (private subnets, no public endpoint)
- IAM roles for GitHub Actions, Load Balancer Controller, EBS CSI Driver
- Security groups, networking, and IAM-backed database access

> The script prints the CloudFormation stack name it used. Reuse that name by exporting `STACK_NAME=<existing-name>` if you need to update the same stack instead of creating a fresh timestamped copy.

CloudFront needs the live ALB DNS name, which is only available after the ingress is created. After step 3 below, capture the hostname and redeploy with that value to create the CloudFront distribution.

#### 1b. Enable CloudFront once the ALB exists

1. Wait for the AWS Load Balancer Controller (step 2) and Grafana ingress (step 3) to create the ALB.
2. Fetch the hostname:
   ```bash
   ALB_DNS=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')
   ```
3. Re-run the deployment targeting the original stack:
   ```bash
   STACK_NAME=<stack-name-from-step-1> \
   GRAFANA_ALB_DNS_NAME=$ALB_DNS \
   ./deploy-stack.sh
   ```
   This updates the stack in-place and provisions the CloudFront distribution pointing at the ALB.

### 2. Install AWS Load Balancer Controller

```bash
# Create service account
kubectl create serviceaccount aws-load-balancer-controller -n kube-system

# Get the IAM role ARN from stack outputs
ALB_ROLE_ARN=$(aws cloudformation describe-stacks \
  --stack-name valkey-benchmark-stack-* \
  --query 'Stacks[0].Outputs[?OutputKey==`AWSLoadBalancerControllerRoleArn`].OutputValue' \
  --output text)

# Annotate service account
kubectl annotate serviceaccount aws-load-balancer-controller \
  -n kube-system \
  eks.amazonaws.com/role-arn=$ALB_ROLE_ARN

# Install controller
helm repo add eks https://aws.github.io/eks-charts
helm repo update

helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=valkey-perf-cluster \
  --set serviceAccount.create=false \
  --set serviceAccount.name=aws-load-balancer-controller
```

### 3. Deploy Grafana

```bash
# Create namespace
kubectl create namespace grafana

# Get RDS endpoint
RDS_ENDPOINT=$(aws cloudformation describe-stacks \
  --stack-name valkey-benchmark-stack-* \
  --query 'Stacks[0].Outputs[?OutputKey==`RDSEndpoint`].OutputValue' \
  --output text)

# Update grafana-values.yaml with RDS endpoint
# Then install Grafana
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update

helm install grafana grafana/grafana \
  --namespace grafana \
  --values grafana-values.yaml

# Apply ALB Ingress
kubectl apply -f alb-ingress.yaml

# Get ALB DNS name
kubectl get ingress -n grafana grafana-ingress
```

> RDS now resides in private subnets and only trusts the EKS node and GitHub runner security groups. Run schema migrations from within the VPC (pods, Session Manager, or self-hosted runners) rather than from the public internet.

### 4. Secure ALB for CloudFront Access Only

**Important**: By default, the ALB is publicly accessible. To restrict access to CloudFront only:

```bash
# Run the security configuration script
./secure-alb-for-cloudfront.sh grafana-cluster

### 5. Initialize Database

Connect to RDS and create the schema using `../utils/schema.sql`

### 6. Import Dashboards

1. Get Grafana admin password:
```bash
kubectl get secret --namespace grafana grafana -o jsonpath="{.data.admin-password}" | base64 --decode
```

2. Get CloudFront URL:
```bash
aws cloudformation describe-stacks \
  --stack-name valkey-benchmark-stack-* \
  --query 'Stacks[0].Outputs[?OutputKey==`CloudFrontDomainName`].OutputValue' \
  --output text
```

(Outputs for `CloudFrontDomainName`/`CloudFrontDistributionId` appear only after rerunning the stack with `GRAFANA_ALB_DNS_NAME` set.)

3. Access Grafana via CloudFront URL (https://your-distribution.cloudfront.net)
4. Navigate to Dashboards → Import
5. Upload `valkey-dashboard-public.json` or `valkey-dashboard-with-commits.json`
6. Select PostgreSQL data source
7. Click Import

## Architecture

### Infrastructure Components

- **VPC**: 10.0.0.0/16 with 2 AZs
- **EKS Cluster**: Kubernetes 1.33 with ARM-based nodes
- **RDS PostgreSQL 17**: db.t4g.micro with 20GB storage
- **CloudFront**: CDN for public dashboards
- **IAM Roles**: GitHub Actions, Load Balancer Controller, EBS CSI Driver

## 🧹 Cleanup

```bash
aws cloudformation delete-stack \
  --stack-name valkey-benchmark-stack-TIMESTAMP \
  --region us-east-1
```

## Notes

- Stack name includes timestamp for parallel deployments
- RDS deletion protection is disabled by default (enable for production)
- All resources are tagged for cost tracking

## Related Documentation

- [Main Project README](../README.md)
- [Database Schema](../utils/schema.sql)
