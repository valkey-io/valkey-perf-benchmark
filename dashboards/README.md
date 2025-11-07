# Valkey Benchmark Dashboard Infrastructure

Complete AWS infrastructure and Grafana dashboards for visualizing Valkey performance benchmarks.

## 📁 Contents

- **infrastructure/cloudformation/valkey-benchmark-stack.yaml** - Complete AWS infrastructure (EKS, RDS, VPC, CloudFront)
- **deploy-stack.sh** - Automated deployment script
- **alb-ingress.yaml** - Kubernetes Ingress for Application Load Balancer
- **grafana-values.yaml** - Helm chart values for Grafana
- **valkey-dashboard-public.json** - Public-facing Grafana dashboard
- **valkey-dashboard-with-commits.json** - Enhanced dashboard with commit tracking

## 🚀 Quick Start

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
- EC2 key pair (will create if doesn't exist)

The script deploys (~25-40 minutes):
- VPC with public/private subnets and NAT gateways
- EKS cluster (v1.32) with ARM-based worker nodes
- RDS PostgreSQL 17 instance
- IAM roles for GitHub Actions, Load Balancer Controller, EBS CSI Driver
- CloudFront distribution
- Security groups and networking

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

### 4. Initialize Database

Connect to RDS and create the schema using `../utils/schema.sql`

### 5. Import Dashboards

1. Get Grafana admin password:
```bash
kubectl get secret --namespace grafana grafana -o jsonpath="{.data.admin-password}" | base64 --decode
```

2. Access Grafana via ALB DNS name
3. Navigate to Dashboards → Import
4. Upload `valkey-dashboard-public.json` or `valkey-dashboard-with-commits.json`
5. Select PostgreSQL data source
6. Click Import

## 🏗️ Architecture

### Infrastructure Components

- **VPC**: 10.0.0.0/16 with 2 AZs
- **EKS Cluster**: Kubernetes 1.32 with ARM-based nodes
- **RDS PostgreSQL 17**: db.t4g.micro with 20GB storage
- **CloudFront**: CDN for public dashboards
- **IAM Roles**: GitHub Actions, Load Balancer Controller, EBS CSI Driver

## 🧹 Cleanup

```bash
aws cloudformation delete-stack \
  --stack-name valkey-benchmark-stack-TIMESTAMP \
  --region us-east-1
```

## 📝 Notes

- Stack name includes timestamp for parallel deployments
- RDS deletion protection is disabled by default (enable for production)
- All resources are tagged for cost tracking

## 🔗 Related Documentation

- [Main Project README](../README.md)
- [Database Schema](../utils/schema.sql)
