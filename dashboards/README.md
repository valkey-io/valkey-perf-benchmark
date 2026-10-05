# Valkey Benchmark Dashboard Infrastructure

AWS infrastructure for visualizing Valkey performance benchmarks using Grafana, EKS Fargate, CloudFront CDN, and RDS PostgreSQL.

## Overview

This infrastructure provides a serverless, scalable platform for benchmark visualization:

- **AWS CloudFront** - Global CDN for low-latency dashboard access
- **Application Load Balancer** - Kubernetes ingress with automatic provisioning
- **Amazon EKS Fargate** - Serverless Kubernetes (no EC2 nodes to manage)
- **Grafana** - Visualization platform with public dashboard sharing
- **Amazon RDS PostgreSQL 17** - Dual-database setup with IAM authentication
- **VPC with 2 AZs** - High availability networking

## Architecture

```
Internet Users
     ↓ HTTPS
CloudFront CDN (Global Edge Locations)
     ↓ HTTP
Application Load Balancer (CloudFront-only access)
     ↓ HTTP:3000
EKS Fargate Cluster (Serverless Kubernetes)
  ├─ Grafana Pods
  └─ AWS Load Balancer Controller
     ↓ PostgreSQL
RDS PostgreSQL 17 (Private Subnets, Encrypted)
```

### Infrastructure Components

| Component | Specification | Purpose |
|-----------|--------------|---------|
| **VPC** | 10.0.0.0/16, 2 AZs | Network isolation |
| **Public Subnets** | 10.0.1.0/24, 10.0.2.0/24 | ALB, NAT Gateways |
| **Private Subnets** | 10.0.10.0/24, 10.0.11.0/24 | Fargate pods, RDS |
| **EKS Fargate** | Kubernetes 1.33 | Serverless container platform |
| **RDS PostgreSQL** | 17, db.t4g.micro, Multi-AZ | Database |
| **CloudFront** | HTTPS, Global CDN | Content delivery |
| **ALB** | Application Load Balancer | Ingress controller |

### Database Configuration

Two databases on a single RDS instance:

1. **`grafana`** - Grafana configuration and dashboards
2. **`postgres`** - Benchmark metrics data
   - `benchmark_metrics` table - Performance data
   - `benchmark_commits` table - Commit tracking (used by `postgres_track_commits.py`)
   - `benchmark_timeseries_tiering` table - Per-second sampler rows (used by `push_timeseries_to_postgres.py`)

Two IAM-enabled users:

1. **`postgres`** - Admin with full access
2. **`github_actions`** - CI/CD user with IAM-only authentication

### Grafana Dashboards

Dashboard definitions live in `grafana/` and are provisioned into Grafana:

| File | Purpose |
|------|---------|
| `valkey-throughput-comparison.json` | Throughput (RPS) comparison across commits |
| `valkey-performance-per-command.json` | Per-command latency/throughput breakdown |
| `valkey-module-perf.json` | Module (e.g. valkey-search) performance metrics |
| `valkey-tiering.json` | Data tiering per-second deep dive |
| `grafana-values.yaml` | Helm values for the Grafana deployment |

Changes to `grafana/*.json` on `main` are automatically synced to the running
Grafana instance by the `sync-dashboards.yml` workflow (also manually
dispatchable).

### Per-Second Tiering Timeseries

The per-second sampler appends one JSON object per second of each scenario's
measured phase to `results/<commit>/timeseries.jsonl`. Those rows go into the
`benchmark_timeseries_tiering` table, which is separate from the
`benchmark_metrics_*` tables so the module dashboard does not list it as a module.

The table stores each row as recorded: typed identity columns (commit, test,
run, `sample_time`, `elapsed_sec` and so on) and one JSONB column per sampler
source holding its raw cumulative reading (`valkey_info`, `latency_histogram`,
`process_cpu`, `disk`). Every derived number is computed in the database by the
`benchmark_timeseries_tiering_rates` view, which compares each row with the
previous row of its run, so changing a dashboard metric means editing the view,
never re-collecting or re-pushing data.

Create the table, view and helper functions from the schema. It is safe to
re-run after editing the view:

```bash
psql -h "$DB_HOST" -U postgres -d postgres -f schema.sql
```

Push the rows after a benchmark run:

```bash
python utils/push_timeseries_to_postgres.py \
  --results-dir results \
  --config-name tiering \
  --host "$DB_HOST" \
  --database postgres \
  --username github_actions \
  --password "$DB_PASSWORD"
```

`--config-name` is stored on every row and is what the dashboard's Config
variable selects. Pushing the same results twice inserts nothing, so a retried
workflow is safe. Add `--dry-run` to see the run groups and row counts without
connecting.

The `valkey-tiering.json` dashboard reads the view, and the raw `valkey_info`
column for its Any INFO field panel. Pick a config, a test and one or more
commits and runs. The per-second panels plot seconds since the run started, so
several commits line up from second 0. The Runs option picks how the selected
runs are drawn. Separate (the default) gives each run its own line. Average
gives one line per commit, the mean of the selected runs at each second. The
trend row reads the summary table `benchmark_metrics_tiering` instead and shows
the latest 60 commits whose `test_type` matches the selected config.

### Per-Commit Tiering Workflow

`.github/workflows/tiering_benchmark.yml` benchmarks the commits of
`dubey02/valkey-data-tiering` branch `data-tiering-unstable` that are not on
upstream `valkey-io/valkey` `unstable`. It runs `configs/tiering.json` daily at
06:00 UTC and on manual dispatch, on the self-hosted runner labeled
`data-tiering-benchmark-arm`. The server address comes from the
`EC2_TIERING_IP` secret.

Manual dispatch inputs:

| Input | Default | Meaning |
|-------|---------|---------|
| `max_commits` | 1 | Commits to benchmark when `commits` is empty |
| `commits` | empty | Space-separated SHAs to benchmark instead |
| `num_runs` | 3 | Runs per scenario |
| `groups` | empty | Test groups, e.g. `1,2`. Empty runs all groups |
| `engine` | `mock` | `mock` or `flashcache` |
| `timeout_minutes` | 720 | Job timeout |

The `mock` engine runs every config with `ext-storage-engine mock` and
`maxmemory-policy allkeys-lru` and without a storage file. Its rows are pushed
with config name `tiering-mock`. The `flashcache` engine runs the config as is
and pushes with config name `tiering`. Pick the config name in the dashboard's
Config variable, so mock results never mix with FlashCache results.

Commit tracking uses the `benchmark_commits_tiering` table. Summary metrics go
to `benchmark_metrics_tiering` and per-second rows to
`benchmark_timeseries_tiering`.

## Prerequisites

- **AWS CLI** - Configured with credentials
- **kubectl** - Kubernetes command-line tool
- **helm** - Kubernetes package manager (v3+)
- **jq** - JSON processor
- **AWS Account** - Permissions for VPC, EKS, RDS, CloudFront, IAM

## Quick Start

```bash
cd dashboards/scripts
chmod +x *.sh

./00-check-prerequisites.sh      # Validate tools and credentials
./01-deploy-infrastructure.sh    # Deploy AWS infrastructure
./02-setup-kubernetes.sh         # Setup Kubernetes
./03-deploy-grafana.sh           # Deploy Grafana
./04-setup-database.sh           # Initialize databases
./05-setup-cloudfront.sh         # Setup CloudFront CDN
./06-finalize-deployment.sh      # Finalize and display summary
```

**You'll be prompted for:**
- RDS master password (Phase 1)
- Database setup confirmation (Phase 4)

## Deployment Phases

| Phase | Script | Purpose |
|-------|--------|---------|
| 0 | `00-check-prerequisites.sh` | Validate tools and AWS credentials |
| 1 | `01-deploy-infrastructure.sh` | Deploy VPC, EKS, RDS via CloudFormation |
| 2 | `02-setup-kubernetes.sh` | Configure kubectl and Load Balancer Controller |
| 3 | `03-deploy-grafana.sh` | Deploy Grafana with ALB Ingress |
| 4 | `04-setup-database.sh` | Initialize PostgreSQL databases |
| 5 | `05-setup-cloudfront.sh` | Add CloudFront CDN and secure ALB |
| 6 | `06-finalize-deployment.sh` | Finalize and display summary |

### What Gets Deployed

**Phase 1 - Infrastructure:**
- VPC with public/private subnets (2 AZs)
- NAT Gateways (2)
- EKS Fargate cluster with profiles
- RDS PostgreSQL (Multi-AZ, encrypted)
- Security groups and IAM roles

**Phase 2 - Kubernetes:**
- kubectl configuration
- AWS Load Balancer Controller
- Grafana namespace

**Phase 3 - Grafana:**
- Grafana deployment (Helm)
- ALB Ingress
- Application Load Balancer (automatic)

**Phase 4 - Database:**
- Two databases (grafana, postgres)
- Two users (postgres, github_actions)
- Tables and indexes

**Phase 5 - CloudFront:**
- CloudFront distribution
- ALB security hardening (CloudFront-only access)
- Grafana configuration update

**Phase 6 - Finalization:**
- Disable public EKS access
- Display deployment summary

### Configuration Files

After deployment:
- `deployment-config.env` - All configuration values
- `stack-outputs.json` - CloudFormation outputs
- `alb-dns-name.txt` - ALB DNS name

### Public Access (Dashboards)
```
URL: https:dashboards.valkey-io/public-dashboards/*
```

## Cleanup

```bash
# Load configuration
source deployment-config.env

# Delete CloudFormation stack
aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"

# Wait for deletion
aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"

# Clean up local files
rm -f deployment-config.env stack-outputs.json alb-dns-name.txt grafana-values-updated.yaml
```

## References

- [AWS EKS Documentation](https://docs.aws.amazon.com/eks/)
- [Grafana Documentation](https://grafana.com/docs/)
- [AWS Load Balancer Controller](https://kubernetes-sigs.github.io/aws-load-balancer-controller/)
- [CloudFormation Reference](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/)

## Support

For issues please open an issue in this repository at https://github.com/valkey-io/valkey-perf-benchmark/issues
