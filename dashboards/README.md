# Valkey Benchmark Dashboard Infrastructure

AWS infrastructure and Grafana dashboards for visualizing Valkey performance benchmarks with AWS Fargate, CloudFront CDN, EKS, and RDS PostgreSQL.

This infrastructure uses serverless containers instead of EC2 nodes, providing cost efficiency, enhanced security, and zero node management overhead.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Detailed Deployment Guide](#detailed-deployment-guide)
- [Architecture Components](#architecture-components)
- [Security](#security)
- [Monitoring & Maintenance](#monitoring--maintenance)
- [Troubleshooting](#troubleshooting)
- [Cleanup](#cleanup)

---

## Overview

This infrastructure provides a platform for visualizing Valkey performance benchmarks using:

- **AWS CloudFront** - Global CDN for low-latency dashboard access
- **Application Load Balancer** - Kubernetes ingress with automatic provisioning
- **Amazon EKS** - Managed Kubernetes (v1.33) with ARM64 nodes
- **Grafana** - Visualization platform with public dashboard sharing
- **Amazon RDS PostgreSQL 17** - Managed database for metrics and Grafana config
- **VPC with 2 AZs** - High availability networking with NAT gateways

---

## Architecture

### High-Level Diagram

```
Internet Users
     ↓ HTTPS
CloudFront CDN (TLS 1.2+, Global Edge Locations)
     ↓ HTTP
Application Load Balancer (Internet-facing, Port 80)
     ↓ HTTP:3000
EKS Fargate Cluster (Kubernetes 1.33, Serverless)
  ├─ Grafana Pods (Port 3000) - Fargate Profile
  └─ AWS Load Balancer Controller - Fargate Profile
     ↓ PostgreSQL
RDS PostgreSQL 17 (Private Subnets, Encrypted)
```

### Infrastructure Components

| Component | Specification | Purpose |
|-----------|--------------|---------|
| **VPC** | 10.0.0.0/16, 2 AZs | Network isolation |
| **Public Subnets** | 10.0.1.0/24, 10.0.2.0/24 | ALB, NAT Gateways |
| **Private Subnets** | 10.0.10.0/24, 10.0.11.0/24 | Fargate pods, RDS |
| **EKS Cluster** | v1.33, Fargate serverless | Kubernetes platform |
| **Fargate Profiles** | grafana, kube-system namespaces | Serverless containers |
| **RDS** | PostgreSQL 17, db.t4g.micro | Database |
| **CloudFront** | HTTPS, Global CDN | Content delivery |
| **ALB** | Application Load Balancer | Ingress controller |

### Key Features

**Security**
- Private subnets for EKS and RDS
- CloudFront-only ALB access
- IAM authentication for RDS
- IRSA for Kubernetes service accounts
- GitHub OIDC for CI/CD

**High Availability**
- Multi-AZ deployment
- Auto-scaling node groups
- RDS automated backups
- CloudFront global distribution

---

## Prerequisites

Before deploying, ensure you have:

- **AWS CLI** - Configured with appropriate credentials
- **kubectl** - Kubernetes command-line tool
- **helm** - Kubernetes package manager (v3+)
- **jq** - JSON processor for shell scripts
- **AWS Account** - With permissions to create VPC, EKS, RDS, CloudFront, IAM roles

### Install Prerequisites

```bash
# macOS
brew install awscli kubectl helm jq

# Linux (Ubuntu/Debian)
sudo apt-get update
sudo apt-get install -y awscli kubectl jq
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# Verify installations
aws --version
kubectl version --client
helm version
jq --version
```

### Configure AWS CLI

```bash
aws configure
# Enter your AWS Access Key ID, Secret Access Key, Region (us-east-1), and output format (json)

# Verify credentials
aws sts get-caller-identity
```

---

## Quick Start

Deploy the Fargate-based infrastructure in approximately 1-1.5 hours:

```bash
cd dashboards/

# 1. Deploy complete Fargate infrastructure (40-60 minutes)
# This single script handles everything: infrastructure, Kubernetes setup, Grafana, CloudFront, and security
./deploy-stack.sh

# 2. Initialize database and import dashboards (10 minutes)
# Follow Phase 2-3 instructions below for database schema and dashboard import
```

**The deploy script automates:**
- AWS infrastructure deployment (VPC, EKS Fargate, RDS, IAM roles)
- Kubernetes cluster configuration
- AWS Load Balancer Controller installation on Fargate
- Grafana deployment on Fargate with PostgreSQL backend
- ALB Ingress provisioning
- CloudFront CDN setup and configuration
- Grafana configuration with CloudFront URL
- ALB security hardening (restrict to CloudFront only)

**Manual steps remaining:**
- Database schema initialization
- Dashboard import and public sharing setup

---

## Detailed Deployment Guide

### Phase 1: Automated Fargate Infrastructure Deployment (40-60 minutes)

The deploy script handles all infrastructure setup automatically:

```bash
cd dashboards/
./deploy-stack.sh
```

**What you'll be prompted for:**
- RDS master password (min 8 characters) - Store this securely!

**The script creates and configures:**
- **AWS Infrastructure**: VPC, EKS Fargate cluster, RDS PostgreSQL, IAM roles, security groups
- **Fargate Profiles**: For `grafana` and `kube-system` namespaces (serverless containers)
- **Kubernetes Setup**: kubectl configuration, Grafana namespace creation
- **Load Balancer Controller**: AWS Load Balancer Controller installation on Fargate
- **Grafana Deployment**: Helm installation with PostgreSQL backend on Fargate
- **ALB Ingress**: Application Load Balancer provisioning and configuration
- **CloudFront CDN**: Global distribution setup with optimized caching
- **Grafana Configuration**: CloudFront URL integration and service restart
- **ALB Security Hardening**: Automatic restriction to CloudFront-only access

**Fargate Benefits:**
- Zero EC2 node management overhead
- Serverless container execution with automatic scaling
- Enhanced security isolation per container
- Pay-per-use pricing model (CPU/memory/time)

**Script outputs:**
- Complete deployment summary with all URLs and credentials
- `stack-outputs.json` file with all AWS resource details
- `alb-dns-name.txt` file with ALB DNS information

---

### Phase 2: Database Initialization (5 minutes)

Initialize the database schema for benchmark metrics:

```bash
# Get RDS endpoint from deploy script output or stack outputs
RDS_ENDPOINT=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)

# Connect to RDS from within VPC (use an EKS pod)
kubectl run -it --rm psql-client \
  --image=postgres:17 \
  --restart=Never \
  --namespace=grafana \
  -- psql "host=$RDS_ENDPOINT port=5432 dbname=grafana user=postgres password=$DB_PASSWORD sslmode=require"

# Once connected, run schema initialization
\i schema.sql

# Or copy and paste the SQL from schema.sql
# The schema creates:
# - benchmark_metrics table
# - Indexes for performance
# - Views for analytics (latest_commit_metrics, performance_trends, command_comparison)
# - Permissions for IAM authentication

# Verify table creation
\dt
SELECT * FROM benchmark_metrics LIMIT 5;

# Exit psql
\q
```

---

### Phase 3: Dashboard Import (5 minutes)

Import Grafana dashboards and enable public sharing:

```bash
# Get CloudFront domain from deploy script output or stack outputs
CLOUDFRONT_DOMAIN=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputValue' stack-outputs.json)

# Get Grafana admin password
GRAFANA_PASSWORD=$(kubectl get secret --namespace grafana grafana \
  -o jsonpath="{.data.admin-password}" | base64 --decode)

echo "Grafana URL: https://$CLOUDFRONT_DOMAIN"
echo "Username: admin"
echo "Password: $GRAFANA_PASSWORD"
```

**Import dashboards via UI:**
1. Navigate to `https://$CLOUDFRONT_DOMAIN`
2. Login with admin credentials
3. Go to **Dashboards → Import**
4. Upload `valkey-dashboard-public.json` or `valkey-dashboard-with-commits.json`
5. Select **PostgreSQL-Valkey** data source
6. Click **Import**

**Enable public dashboard sharing:**
1. Open the imported dashboard
2. Click **Share → Public dashboard**
3. Enable public access
4. Copy the public URL

---

## Private Access for Administration

### kubectl Port-Forward (Secure Admin Access)

Access Grafana securely through kubectl port-forwarding for all administrative tasks:

```bash
# Forward local port 3000 to Grafana pod
kubectl port-forward -n grafana svc/grafana 3000:80

# Access Grafana locally
echo "Local Access: http://localhost:3000"
echo "Username: admin"
echo "Password: $GRAFANA_PASSWORD"

# Open in browser: http://localhost:3000
# Press Ctrl+C to stop port-forwarding
```

### Access Pattern Summary

| Access Method | Use Case | Security | Availability |
|---------------|----------|----------|--------------|
| **CloudFront Public** | Public dashboards only | High (cached) | Global CDN |
| **kubectl Port-Forward** | All administrative access | Highest | Requires kubectl |

**Recommended Workflow:**
1. **Initial Setup:** Use kubectl port-forward for secure dashboard configuration
2. **Regular Admin:** Use kubectl port-forward for all administrative tasks
3. **Public Access:** Use CloudFront public dashboard URLs (cached 6 hours)

**Security Note:** kubectl port-forward provides the most secure access method as it doesn't expose any additional network ports and requires authenticated kubectl access to the cluster.

---

### Phase 4: Verification (5 minutes)

Verify all components are working:

```bash
# Get stack name and CloudFront domain from outputs
STACK_NAME=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputKey' stack-outputs.json | head -1 | sed 's/CloudFrontDomainName//' | xargs aws cloudformation describe-stacks --query 'Stacks[0].StackName' --output text)
CLOUDFRONT_DOMAIN=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputValue' stack-outputs.json)

# Check EKS cluster
kubectl get nodes
kubectl get pods -A

# Check Grafana
kubectl get pods -n grafana
kubectl get svc -n grafana
kubectl get ingress -n grafana

# Check ALB
aws elbv2 describe-load-balancers \
  --query 'LoadBalancers[?contains(LoadBalancerName, `k8s-grafana`)].{Name:LoadBalancerName,DNS:DNSName,State:State.Code}' \
  --output table

# Check CloudFront
CLOUDFRONT_ID=$(jq -r '.[] | select(.OutputKey=="CloudFrontDistributionId") | .OutputValue' stack-outputs.json)
aws cloudfront get-distribution \
  --id $CLOUDFRONT_ID \
  --query 'Distribution.Status' \
  --output text

# Check RDS
aws rds describe-db-instances \
  --db-instance-identifier valkey-benchmark-postgres \
  --query 'DBInstances[0].{Status:DBInstanceStatus,Endpoint:Endpoint.Address}' \
  --output table

# Test Grafana access
curl -I https://$CLOUDFRONT_DOMAIN/api/health
```

---

## Deployment Timeline

| Phase | Duration | Description |
|-------|----------|-------------|
| 1. **Automated Deployment** | 40-60 min | **Complete infrastructure via deploy script** |
| 2. Database Init | 5 min | Schema creation |
| 3. Dashboard Import | 5 min | UI configuration |
| 4. Verification | 5 min | Testing all components |
| **TOTAL** | **55-75 min** | **~1 hour** |

### Automated in Phase 1:
- AWS infrastructure (VPC, EKS Fargate, RDS, IAM roles, security groups)
- Kubernetes setup (kubectl config, namespaces, Load Balancer Controller)
- Grafana deployment (Helm installation with PostgreSQL backend)
- ALB Ingress provisioning and configuration
- CloudFront CDN setup with optimized caching
- Grafana configuration with CloudFront URL integration
- ALB security hardening (automatic CloudFront-only access)

---


## Architecture Components

### 1. CloudFront Distribution

**Purpose:** Global CDN for dashboard access

**Configuration:**
- HTTPS only (TLS 1.2+)
- HTTP/2 and HTTP/3 enabled
- IPv6 enabled
- Price Class 100 (North America, Europe)

**Cache Behaviors:**
- `/public-dashboards/*` - CachingOptimized (6 hour TTL)
- `/api/*` - CachingDisabled
- Default - CachingDisabled (admin access, login, setup)

**Benefits:**
- Reduces latency for global users
- Reduces load on origin
- Provides DDoS protection via AWS Shield Standard

**Access Patterns:**
- **Public Dashboards:** `https://cloudfront-domain/public-dashboards/*` (cached, 6-hour TTL)
- **Private Access:** Use kubectl port-forward for all administrative tasks

---

### 2. Application Load Balancer (ALB)

**Purpose:** Kubernetes Ingress controller

**Configuration:**
- Type: Application Load Balancer
- Scheme: Internet-facing (restricted to CloudFront)
- Target Type: IP (direct to pod IPs)
- Health Check: `/api/health` on port 3000

**Managed by:** AWS Load Balancer Controller (automatically created from Ingress resource)

---

### 3. EKS Cluster

**Purpose:** Kubernetes platform for Grafana

**Configuration:**
- Kubernetes Version: 1.33
- Endpoint Access: Private only
- Logging: All types enabled
- Node Group: t4g.small (ARM64, 2 vCPU, 2 GB RAM)
- Scaling: 1-4 nodes (default 2)

**Add-ons:**
- VPC CNI, kube-proxy, CoreDNS

---

### 4. Grafana Deployment

**Purpose:** Visualization platform

**Configuration:**
- Namespace: grafana
- Replicas: 1 (can be scaled)
- Image: grafana/grafana:latest
- Data Source: PostgreSQL (RDS)
- Storage: PostgreSQL backend (no local persistence needed)
- Public Dashboards: Enabled
- Embedding: Enabled for CloudFront

**Features:**
- Sidecar container for loading dashboards from ConfigMaps
- Health probes configured
- Resource limits: 500m CPU, 512Mi memory

---

### 5. RDS PostgreSQL

**Purpose:** Database for Grafana and benchmark metrics

**Configuration:**
- Engine: PostgreSQL 17
- Instance: db.t4g.micro (ARM, 1 GB RAM)
- Storage: 20 GB gp3 (encrypted)
- Location: Private subnets only
- Backup: 3 days retention
- Monitoring: Basic CloudWatch metrics

**Tables:**
- `benchmark_metrics` - Valkey performance data
- Grafana internal tables

**Security:**
- IAM authentication enabled
- Security group allows EKS nodes and GitHub runners only

---

### 6. Networking

**VPC:** 10.0.0.0/16 with DNS enabled

**Subnets:**
- Public (2 AZs): 10.0.1.0/24, 10.0.2.0/24 - ALB, NAT Gateways
- Private (2 AZs): 10.0.10.0/24, 10.0.11.0/24 - EKS nodes, RDS

**NAT Gateways:** 2 (one per AZ) with Elastic IPs

**Security Groups:**
- EKS Cluster SG: HTTPS (443) from VPC
- EKS Node SG: Traffic from cluster, SSH from VPC
- RDS SG: PostgreSQL (5432) from EKS nodes and GitHub runners
- ALB SG: HTTP (80) from CloudFront prefix list only

---

### 7. IAM Roles and Policies

**EKS Cluster Role:** AmazonEKSClusterPolicy

**EKS Node Role:** Worker, CNI, ECR policies

**AWS Load Balancer Controller Role:** IRSA with ALB management permissions


**GitHub Actions Role:** OIDC federation for RDS access

---

## Security

### Network Security

- **Private subnets** for EKS nodes and RDS
- **Security groups** with least privilege
- **NAT Gateways** for outbound internet access
- **CloudFront-only ALB access** via managed prefix list

### Authentication & Authorization

- **IAM authentication** for RDS
- **IRSA** (IAM Roles for Service Accounts) for Kubernetes
- **GitHub OIDC** for CI/CD authentication
- **Grafana authentication** required (no anonymous access)

### Encryption

- **RDS encryption** at rest
- **TLS 1.2+** for CloudFront
- **Secrets** stored in Kubernetes Secrets

### Best Practices

1. Store RDS password in AWS Secrets Manager
2. Rotate credentials regularly
3. Enable AWS WAF for CloudFront
4. Implement VPC Flow Logs
5. Use AWS Config for compliance monitoring

---

## Monitoring & Maintenance

### Regular Tasks

**Weekly:**
- Review CloudWatch logs
- Check RDS performance metrics
- Monitor costs in Cost Explorer

**Monthly:**
- Update Grafana version
- Review security groups
- Check for EKS updates
- Review IAM policies

**Quarterly:**
- Update CloudFormation template
- Test disaster recovery procedures
- Review and optimize costs

### Monitoring Tools

**CloudWatch:**
- EKS control plane logs (API and audit only)
- RDS basic metrics
- CloudFront access logs

**Grafana:**
- Dashboard usage metrics
- Query performance
- Data source health

**RDS:**
- Basic CloudWatch metrics
- Slow query logs
- Connection metrics

### Backup & Recovery

**RDS:**
- Automated daily backups (7-day retention)
- Manual snapshots before major changes
- Point-in-time recovery available

**Grafana:**
- Dashboard JSON files in Git
- ConfigMaps backed up with cluster
- Persistent volume snapshots

**Recovery Steps:**
1. Restore RDS from snapshot
2. Redeploy Grafana from Helm
3. Import dashboards from Git
4. Update CloudFront origin if needed

---

## Troubleshooting

### Grafana Pod Not Starting

```bash
# Check pod status
kubectl get pods -n grafana
kubectl describe pod <pod-name> -n grafana

# Check logs
kubectl logs <pod-name> -n grafana

# Common issues:
# - Database connection failed: Check RDS endpoint and credentials
# - Image pull failed: Check ECR permissions
```

### ALB Not Created

```bash
# Check ingress status
kubectl describe ingress grafana-ingress -n grafana

# Check Load Balancer Controller logs
kubectl logs -n kube-system deployment/aws-load-balancer-controller

# Common issues:
# - IAM role not annotated: Check service account annotation
# - Subnet tags missing: Check public subnet tags
# - Security group issues: Check VPC security groups
```

### CloudFront Not Working

```bash
# Check distribution status
aws cloudfront get-distribution --id <distribution-id>

# Test origin directly
curl -I http://<alb-dns-name>/api/health

# Common issues:
# - Origin not responding: Check ALB health
# - Cache not working: Check cache policies
# - SSL errors: Check CloudFront certificate
```

### Database Connection Issues

```bash
# Test from EKS pod
kubectl run -it --rm psql-test \
  --image=postgres:17 \
  --restart=Never \
  --namespace=grafana \
  -- psql "host=$RDS_ENDPOINT port=5432 dbname=grafana user=postgres password=$DB_PASSWORD sslmode=require"

# Common issues:
# - Connection timeout: Check security groups
# - Authentication failed: Check password and IAM auth
# - SSL required: Ensure sslmode=require
```


---

## Cleanup

### Delete All Resources

```bash
# Delete CloudFormation stack (deletes most resources)
aws cloudformation delete-stack \
  --stack-name valkey-benchmark-stack-TIMESTAMP \
  --region us-east-1

# Wait for deletion to complete
aws cloudformation wait stack-delete-complete \
  --stack-name valkey-benchmark-stack-TIMESTAMP \
  --region us-east-1

# Manually delete any remaining resources:
# - CloudWatch log groups
# - S3 buckets (if created)
```

### Partial Cleanup

**Delete Grafana only:**
```bash
helm uninstall grafana -n grafana
kubectl delete namespace grafana
kubectl delete ingress grafana-ingress -n grafana
```


---

## Files Reference

### Configuration Files

- `infrastructure/cloudformation/valkey-benchmark-stack.yaml` - Complete AWS infrastructure
- `deploy-stack.sh` - Automated deployment script
- `secure-alb-for-cloudfront.sh` - ALB security hardening script
- `alb-ingress.yaml` - Kubernetes Ingress for ALB
- `grafana-values.yaml` - Helm chart values for Grafana
- `schema.sql` - Database schema for benchmark metrics

### Dashboard Files

- `valkey-dashboard-public.json` - Public-facing Grafana dashboard
- `valkey-dashboard-with-commits.json` - Enhanced dashboard with commit tracking


---

## References

- [AWS EKS Documentation](https://docs.aws.amazon.com/eks/)
- [Grafana Documentation](https://grafana.com/docs/)
- [AWS Load Balancer Controller](https://kubernetes-sigs.github.io/aws-load-balancer-controller/)
- [CloudFormation Template Reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/)
- [Main Project README](../README.md)

---

## Support

For issues or questions:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review CloudFormation stack events
3. Check Kubernetes pod logs
4. Review AWS service quotas
5. Open an issue in the project repository

---
