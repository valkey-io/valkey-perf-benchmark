# Valkey Performance Dashboard - Architecture

## Overview

This document describes the complete architecture of the Grafana deployment on AWS EKS for visualizing Valkey performance benchmarks. The solution includes automated infrastructure deployment via CloudFormation, Kubernetes-based Grafana deployment, and CloudFront CDN for global distribution with public dashboard sharing capabilities.

## Architecture Diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│                              Internet Users                            │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ HTTPS
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         AWS CloudFront (CDN)                           │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ - HTTPS Termination (TLS 1.2+)                                   │  │
│  │ - Global Edge Locations                                          │  │
│  │ - Cache Policies:                                                │  │
│  │   • /public-dashboards/* - Optimized caching                     │  │
│  │   • /api/* - No cache                                            │  │
│  │   • Default - No cache                                           │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ HTTP
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    AWS Application Load Balancer (ALB)                 │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ - Type: Application Load Balancer                                │  │
│  │ - Scheme: Internet-facing                                        │  │
│  │ - Listener: Port 80 (HTTP)                                       │  │
│  │ - Target Group: IP type (Pod IPs)                                │  │
│  │ - Health Check: HTTP:3000/api/health                             │  │
│  │ - Path-based routing: /* → grafana service                       │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ HTTP:3000 (Direct to Pod)
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         AWS EKS Cluster                                │
│                                                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Worker Nodes (ARM64)                         │   │
│  │                                                                 │   │
│  │  ┌──────────────────────┐      ┌──────────────────────┐         │   │
│  │  │ Node 1 (AZ-a)        │      │ Node 2 (AZ-b)        │         │   │
│  │  │                      │      │                      │         │   │
│  │  │ ┌──────────────────┐ │      │                      │         │   │
│  │  │ │ Grafana Pod      │ │      │                      │         │   │
│  │  │ │ ┌──────────────┐ │ │      │                      │         │   │
│  │  │ │ │ grafana      │ │ │      │                      │         │   │
│  │  │ │ │ container    │ │ │      │                      │         │   │
│  │  │ │ │ Port: 3000   │ │ │      │                      │         │   │
│  │  │ │ └──────────────┘ │ │      │                      │         │   │
│  │  │ │ ┌──────────────┐ │ │      │                      │         │   │
│  │  │ │ │ sidecar      │ │ │      │                      │         │   │
│  │  │ │ │ container    │ │ │      │                      │         │   │
│  │  │ │ └──────────────┘ │ │      │                      │         │   │
│  │  │ └──────────────────┘ │      │                      │         │   │
│  │  └──────────────────────┘      └──────────────────────┘         │   │
│  │                                                                 │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │ AWS Load Balancer Controller                             │   │   │
│  │  │ - Watches Ingress resources                              │   │   │
│  │  │ - Creates/manages ALB automatically                      │   │   │
│  │  │ - Uses IRSA for AWS API access                           │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                                                                 │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │ EBS CSI Driver                                           │   │   │
│  │  │ - Manages persistent volumes                             │   │   │
│  │  │ - Uses IRSA for AWS API access                           │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                        │
│  Namespace: grafana                                                    │
│  Service: grafana (ClusterIP)                                          │
│  Ingress: grafana-ingress (ALB)                                        │
└────────────────────────────────┬───────────────────────────────────────┘
                                 │ PostgreSQL Protocol
                                 ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      AWS RDS PostgreSQL                                │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │ Engine: PostgreSQL 17                                            │  │
│  │ Instance Class: db.t4g.micro (ARM)                               │  │
│  │ Storage: 20 GB gp3 (encrypted)                                   │  │
│  │ Database: grafana                                                │  │
│  │ Tables:                                                          │  │
│  │   - benchmark_metrics (performance data)                         │  │
│  │   - Grafana internal tables                                      │  │
│  │ Backup: 7 days retention                                         │  │
│  │ Enhanced Monitoring: Enabled                                     │  │
│  │ IAM Authentication: Enabled                                      │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. CloudFront Distribution

**Purpose:** Global CDN for low-latency access to public dashboards

**Configuration:**
- Protocol: HTTPS only (redirects HTTP to HTTPS)
- TLS: TLS 1.2 minimum
- HTTP Version: HTTP/2 and HTTP/3 enabled
- IPv6: Enabled
- Price Class: 100 (North America, Europe)

**Cache Behaviors:**
- `/public-dashboards/*` - CachingOptimized policy (1 hour TTL)
- `/api/*` - CachingDisabled policy
- Default - CachingDisabled policy

**Origin:**
- Type: Custom Origin (ALB)
- Protocol: HTTP only (TLS termination at CloudFront)
- Timeout: 30 seconds

**Benefits:**
- Reduced latency for global users
- Reduced load on origin (ALB/Grafana)
- DDoS protection via AWS Shield Standard
- HTTPS termination

### 2. Application Load Balancer (ALB)

**Purpose:** Kubernetes Ingress controller for routing traffic to Grafana pods

**Configuration:**
- Type: Application Load Balancer
- Scheme: Internet-facing
- IP Address Type: IPv4
- Subnets: Public subnets in 2 AZs
- Target Type: IP (direct to pod IPs)

**Listeners:**
- Port 80 (HTTP)
- Default action: Forward to target group

**Target Group:**
- Protocol: HTTP
- Port: 3000
- Health Check:
  - Path: `/api/health`
  - Protocol: HTTP
  - Interval: 30 seconds
  - Timeout: 5 seconds
  - Healthy threshold: 2
  - Unhealthy threshold: 3

**Managed By:**
- AWS Load Balancer Controller (Kubernetes controller)
- Automatically created/updated based on Ingress resources
- Uses IRSA (IAM Roles for Service Accounts) for AWS API access

### 3. EKS Cluster

**Purpose:** Kubernetes platform for running Grafana

**Configuration:**
- Kubernetes Version: 1.33
- Control Plane: Managed by AWS
- Endpoint Access: Public and Private
- Logging: All log types enabled (API, audit, authenticator, controller manager, scheduler)

**Node Group:**
- Instance Type: t4g.small (ARM64, 2 vCPU, 2 GB RAM)
- AMI Type: AL2023_ARM_64_STANDARD
- Capacity Type: On-Demand
- Scaling:
  - Min: 1 node
  - Desired: 2 nodes
  - Max: 4 nodes
- Subnets: Private subnets in 2 AZs

**Add-ons:**
- EBS CSI Driver (v1.36.0) - For persistent volumes
- VPC CNI - For pod networking
- kube-proxy - For service networking
- CoreDNS - For DNS resolution

**IAM Roles:**
- Cluster Role: AmazonEKSClusterPolicy
- Node Role: AmazonEKSWorkerNodePolicy, AmazonEKS_CNI_Policy, AmazonEC2ContainerRegistryReadOnly
- Load Balancer Controller Role: Custom policy for ALB management
- EBS CSI Driver Role: AmazonEBSCSIDriverPolicy

### 4. Grafana Deployment

**Purpose:** Visualization and dashboard platform

**Deployment:**
- Namespace: grafana
- Replicas: 1 (can be scaled)
- Image: grafana/grafana:latest
- Port: 3000

**Configuration:**
- Admin Password: Set via Helm values
- Data Source: PostgreSQL (RDS)
- Persistence: Enabled (EBS volume via EBS CSI Driver)
- Public Dashboards: Enabled
- Anonymous Access: Disabled (authentication required)
- Embedding: Enabled (for CloudFront)

**Environment Variables:**
- `GF_SERVER_ROOT_URL`: CloudFront URL
- `GF_FEATURE_TOGGLES_ENABLE`: publicDashboards
- `GF_DASHBOARDS_PUBLIC_ENABLED`: true
- `GF_SECURITY_ALLOW_EMBEDDING`: true

**Sidecar Container:**
- Purpose: Auto-load dashboards from ConfigMaps
- Watches for ConfigMaps with label `grafana_dashboard: "1"`
- Automatically reloads dashboards on changes

**Service:**
- Type: ClusterIP
- Port: 80 → 3000
- Selector: app.kubernetes.io/name=grafana

**Ingress:**
- Class: alb
- Annotations:
  - `alb.ingress.kubernetes.io/scheme: internet-facing`
  - `alb.ingress.kubernetes.io/target-type: ip`
  - `alb.ingress.kubernetes.io/healthcheck-path: /api/health`

### 5. RDS PostgreSQL

**Purpose:** Database for Grafana configuration and benchmark metrics

**Configuration:**
- Engine: PostgreSQL 17
- Instance Class: db.t4g.micro (ARM, 2 vCPU, 1 GB RAM)
- Storage: 20 GB gp3 (encrypted at rest)
- Multi-AZ: No (can be enabled for production)
- Publicly Accessible: Yes (for GitHub Actions access)
- Backup Retention: 7 days
- Preferred Backup Window: 03:00-04:00 UTC
- Preferred Maintenance Window: Sunday 04:00-05:00 UTC

**Databases:**
- `grafana` - Main database

**Tables:**
- `benchmark_metrics` - Valkey performance data
  - Columns: timestamp, commit, command, data_size, pipeline, clients, requests, rps, latency metrics, etc.
  - Indexes: commit, timestamp, command, unique constraint
- Grafana internal tables (managed by Grafana)

**Security:**
- Encryption: Enabled (at rest)
- IAM Authentication: Enabled
- Security Group: Allows access from EKS nodes and GitHub Actions
- Enhanced Monitoring: Enabled (60-second interval)
- Performance Insights: Enabled (7-day retention)

**Parameter Group:**
- Family: postgres17
- Parameters:
  - `shared_preload_libraries`: pg_stat_statements
  - `log_statement`: all
  - `log_min_duration_statement`: 1000ms
  - `max_connections`: 200

### 6. Networking

**VPC:**
- CIDR: 10.0.0.0/16
- DNS Hostnames: Enabled
- DNS Support: Enabled

**Subnets:**
- Public Subnets (2 AZs):
  - CIDR: 10.0.1.0/24, 10.0.2.0/24
  - Internet Gateway attached
  - Used for: ALB, NAT Gateways
- Private Subnets (2 AZs):
  - CIDR: 10.0.10.0/24, 10.0.11.0/24
  - NAT Gateway attached
  - Used for: EKS nodes, RDS

**NAT Gateways:**
- 2 NAT Gateways (one per AZ)
- Elastic IPs attached
- Provides internet access for private subnets

**Security Groups:**
- EKS Cluster SG: Allows HTTPS (443) from anywhere
- EKS Node SG: Allows traffic from cluster, SSH from VPC
- RDS SG: Allows PostgreSQL (5432) from EKS nodes and GitHub Actions

**Route Tables:**
- Public Route Table: Routes 0.0.0.0/0 to Internet Gateway
- Private Route Tables: Routes 0.0.0.0/0 to NAT Gateway

### 7. IAM Roles and Policies

**EKS Cluster Role:**
- Policy: AmazonEKSClusterPolicy
- Used by: EKS control plane

**EKS Node Role:**
- Policies:
  - AmazonEKSWorkerNodePolicy
  - AmazonEKS_CNI_Policy
  - AmazonEC2ContainerRegistryReadOnly
- Used by: EKS worker nodes

**AWS Load Balancer Controller Role:**
- Custom policy for ALB management
- Permissions: EC2, ELB, IAM (limited)
- IRSA: Mapped to Kubernetes service account

**EBS CSI Driver Role:**
- Policy: AmazonEBSCSIDriverPolicy
- IRSA: Mapped to Kubernetes service account

**GitHub Actions Role:**
- Custom policy for RDS access
- Permissions: rds-db:connect, rds:DescribeDBInstances
- Federated: GitHub OIDC Provider
- Used by: GitHub Actions workflows to push metrics

**RDS Monitoring Role:**
- Policy: AmazonRDSEnhancedMonitoringRole
- Used by: RDS for enhanced monitoring

## Data Flow

### 1. User Accessing Dashboard

```
User → CloudFront → ALB → Grafana Pod → RDS PostgreSQL
```

1. User requests dashboard via CloudFront URL
2. CloudFront checks cache:
   - If cached: Returns cached response
   - If not cached: Forwards to ALB
3. ALB routes to Grafana pod based on path
4. Grafana queries PostgreSQL for data
5. Grafana renders dashboard and returns HTML/JSON
6. Response flows back through ALB → CloudFront → User
7. CloudFront caches response (if cacheable)

### 2. Benchmark Data Ingestion

```
GitHub Actions → RDS PostgreSQL → Grafana
```

1. GitHub Actions runs benchmark
2. Workflow assumes IAM role via OIDC
3. Connects to RDS using IAM authentication
4. Inserts benchmark metrics into `benchmark_metrics` table
5. Grafana queries updated data on next dashboard refresh

### 3. Dashboard Updates

```
Developer → Git → ConfigMap → Grafana Sidecar → Grafana
```

1. Developer updates dashboard JSON in Git
2. CI/CD creates/updates ConfigMap with dashboard
3. Grafana sidecar detects ConfigMap change
4. Sidecar loads new dashboard into Grafana
5. Dashboard available immediately

## Deployment Process

### Automated Deployment (Recommended)

```bash
cd dashboards/
./deploy-stack.sh
```

**Steps:**
1. Validates prerequisites (AWS CLI, kubectl, helm, jq)
2. Checks AWS credentials
3. Validates CloudFormation template
4. Checks/creates EC2 key pair
5. Prompts for RDS password
6. Finds existing GitHub OIDC Provider (or creates new)
7. Deploys CloudFormation stack (~25-40 minutes)
8. Monitors stack events in real-time
9. Configures kubectl automatically
10. Outputs all resource details

**What Gets Created:**
- VPC with subnets, NAT gateways, route tables
- EKS cluster with node group
- RDS PostgreSQL instance
- Security groups
- IAM roles and policies
- CloudFront distribution
- EBS CSI Driver addon

### Manual Deployment

**1. Deploy Infrastructure:**
```bash
aws cloudformation create-stack \
  --stack-name valkey-benchmark-stack \
  --template-body file://infrastructure/cloudformation/valkey-benchmark-stack.yaml \
  --parameters ParameterKey=DBMasterPassword,ParameterValue=YourPassword \
  --capabilities CAPABILITY_NAMED_IAM
```

**2. Configure kubectl:**
```bash
aws eks update-kubeconfig --name valkey-perf-cluster --region us-east-1
```

**3. Install AWS Load Balancer Controller:**
```bash
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=valkey-perf-cluster \
  --set serviceAccount.annotations."eks\.amazonaws\.com/role-arn"=<role-arn>
```

**4. Deploy Grafana:**
```bash
helm install grafana grafana/grafana \
  --namespace grafana \
  --values grafana-values.yaml
```

**5. Apply Ingress:**
```bash
kubectl apply -f alb-ingress.yaml
```

**6. Update CloudFront:**
- Get ALB DNS from `kubectl get ingress`
- Update CloudFront origin to ALB DNS

## Scaling Considerations

### Horizontal Scaling

**Grafana:**
- Increase replicas in Helm values
- ALB automatically distributes traffic
- Requires shared session storage (Redis/Memcached)

**EKS Nodes:**
- Increase max size in node group
- Cluster Autoscaler can auto-scale based on pod requests

**RDS:**
- Enable Multi-AZ for high availability
- Add read replicas for read-heavy workloads
- Increase instance class for more resources

### Vertical Scaling

**Grafana:**
- Increase resource requests/limits
- Use larger instance types for nodes

**RDS:**
- Increase instance class
- Increase storage size

## Security Best Practices

1. **Network Security:**
   - Private subnets for EKS nodes and RDS
   - Security groups with least privilege
   - NAT Gateways for outbound internet access

2. **Authentication:**
   - IAM authentication for RDS
   - IRSA for Kubernetes service accounts
   - GitHub OIDC for CI/CD

3. **Encryption:**
   - RDS encryption at rest
   - TLS 1.2+ for CloudFront
   - Secrets stored in Kubernetes Secrets

4. **Access Control:**
   - IAM roles with least privilege
   - Kubernetes RBAC
   - Grafana authentication required

5. **Monitoring:**
   - CloudWatch logs for EKS
   - RDS Enhanced Monitoring
   - CloudFront access logs

## Maintenance

### Regular Tasks

**Weekly:**
- Review CloudWatch logs
- Check RDS performance metrics
- Monitor costs

**Monthly:**
- Update Grafana version
- Review security groups
- Check for EKS updates

**Quarterly:**
- Review IAM policies
- Update CloudFormation template
- Test disaster recovery

### Backup and Recovery

**RDS:**
- Automated daily backups (7-day retention)
- Manual snapshots before major changes
- Point-in-time recovery available

**Grafana:**
- Dashboard JSON files in Git
- ConfigMaps backed up with cluster
- Persistent volume snapshots

**Recovery:**
1. Restore RDS from snapshot
2. Redeploy Grafana from Helm
3. Import dashboards from Git
4. Update CloudFront origin if needed

## References

- [AWS EKS Documentation](https://docs.aws.amazon.com/eks/)
- [Grafana Documentation](https://grafana.com/docs/)
- [AWS Load Balancer Controller](https://kubernetes-sigs.github.io/aws-load-balancer-controller/)
- [CloudFormation Template](infrastructure/cloudformation/valkey-benchmark-stack.yaml)
