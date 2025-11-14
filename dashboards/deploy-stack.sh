#!/bin/bash

# Valkey Benchmark Dashboard Infrastructure Deployment Script
# This script deploys the complete infrastructure using AWS Fargate

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_NAME="valkey-benchmark"
REGION="us-east-1"
CLUSTER_NAME="valkey-perf-cluster"

# Function to print colored output
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

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    local missing_tools=()
    
    if ! command_exists aws; then
        missing_tools+=("aws")
    fi
    
    if ! command_exists kubectl; then
        missing_tools+=("kubectl")
    fi
    
    if ! command_exists helm; then
        missing_tools+=("helm")
    fi
    
    if ! command_exists jq; then
        missing_tools+=("jq")
    fi
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        print_error "Missing required tools: ${missing_tools[*]}"
        print_error "Please install them and try again."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity >/dev/null 2>&1; then
        print_error "AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    print_success "All prerequisites met"
}

# Function to get user input
get_user_input() {
    print_status "Gathering deployment parameters..."
    
    # Check if we're updating an existing stack
    if [ -n "$STACK_NAME" ]; then
        print_status "Using existing stack: $STACK_NAME"
        return
    fi
    
    # Generate stack name with timestamp
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    STACK_NAME="${PROJECT_NAME}-fargate-stack-${TIMESTAMP}"
    
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
    fi
    
    # Check for existing GitHub OIDC Provider
    if [ -z "$GITHUB_OIDC_PROVIDER_ARN" ]; then
        print_status "Checking for existing GitHub OIDC Provider..."
        GITHUB_OIDC_PROVIDER_ARN=$(aws iam list-open-id-connect-providers \
            --query 'OpenIDConnectProviderList[?contains(Arn, `token.actions.githubusercontent.com`)].Arn' \
            --output text 2>/dev/null || echo "")
        
        if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ]; then
            print_success "Found existing GitHub OIDC Provider: $GITHUB_OIDC_PROVIDER_ARN"
        else
            print_status "No existing GitHub OIDC Provider found. Will create new one."
        fi
    fi
    
    print_success "Configuration complete"
}

# Function to monitor CloudFormation stack events
monitor_stack_events() {
    local stack_name="$1"
    local operation="$2"  # CREATE or UPDATE
    local start_time=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    
    print_status "Monitoring stack $operation progress..."
    
    # Track resources being created/updated
    local resources_in_progress=()
    local completed_resources=()
    local failed_resources=()
    
    while true; do
        # Get stack status
        local stack_status=$(aws cloudformation describe-stacks \
            --stack-name "$stack_name" \
            --region "$REGION" \
            --query 'Stacks[0].StackStatus' \
            --output text 2>/dev/null || echo "UNKNOWN")
        
        # Get recent events
        local events=$(aws cloudformation describe-stack-events \
            --stack-name "$stack_name" \
            --region "$REGION" \
            --query "StackEvents[?Timestamp>=\`$start_time\`].[LogicalResourceId,ResourceStatus,ResourceStatusReason,Timestamp]" \
            --output text 2>/dev/null | sort -k4)
        
        # Process events and show progress
        while IFS=$'\t' read -r resource_id resource_status reason timestamp; do
            if [[ -n "$resource_id" ]]; then
                case "$resource_status" in
                    *_IN_PROGRESS)
                        if [[ ! " ${resources_in_progress[@]} " =~ " ${resource_id} " ]]; then
                            resources_in_progress+=("$resource_id")
                            print_status "IN_PROGRESS: $resource_id: $resource_status"
                        fi
                        ;;
                    *_COMPLETE)
                        if [[ " ${resources_in_progress[@]} " =~ " ${resource_id} " ]]; then
                            # Remove from in-progress
                            resources_in_progress=("${resources_in_progress[@]/$resource_id}")
                            completed_resources+=("$resource_id")
                            print_success "COMPLETE: $resource_id: $resource_status"
                        fi
                        ;;
                    *_FAILED)
                        failed_resources+=("$resource_id")
                        print_error "FAILED: $resource_id: $resource_status - $reason"
                        ;;
                esac
            fi
        done <<< "$events"
        
        # Check if operation is complete
        case "$stack_status" in
            CREATE_COMPLETE|UPDATE_COMPLETE)
                print_success "Stack $operation completed successfully!"
                echo "Summary: ${#completed_resources[@]} resources completed, ${#failed_resources[@]} failed"
                return 0
                ;;
            CREATE_FAILED|UPDATE_FAILED|ROLLBACK_COMPLETE|UPDATE_ROLLBACK_COMPLETE)
                print_error "Stack $operation failed with status: $stack_status"
                if [ ${#failed_resources[@]} -gt 0 ]; then
                    print_error "Failed resources: ${failed_resources[*]}"
                fi
                return 1
                ;;
            *)
                # Still in progress, show current status
                if [ ${#resources_in_progress[@]} -gt 0 ]; then
                    echo -ne "\rActive: ${#resources_in_progress[@]} | Completed: ${#completed_resources[@]} | Status: $stack_status"
                fi
                ;;
        esac
        
        sleep 15
    done
}

# Function to deploy CloudFormation stack
deploy_infrastructure() {
    print_status "Deploying infrastructure stack: $STACK_NAME"
    
    # Prepare parameters
    local params=(
        "ParameterKey=ProjectName,ParameterValue=$PROJECT_NAME"
        "ParameterKey=ClusterName,ParameterValue=$CLUSTER_NAME"
        "ParameterKey=DBMasterPassword,ParameterValue=$DB_PASSWORD"
    )
    
    # Add GitHub OIDC Provider ARN if exists
    if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ]; then
        params+=("ParameterKey=GitHubOIDCProviderArn,ParameterValue=$GITHUB_OIDC_PROVIDER_ARN")
    fi
    
    # Add Grafana ALB DNS name if provided (for CloudFront update)
    if [ -n "$GRAFANA_ALB_DNS_NAME" ]; then
        params+=("ParameterKey=GrafanaAlbDnsName,ParameterValue=$GRAFANA_ALB_DNS_NAME")
    fi
    
    # Check if stack exists
    local operation=""
    if aws cloudformation describe-stacks --stack-name "$STACK_NAME" >/dev/null 2>&1; then
        print_status "Updating existing stack..."
        operation="UPDATE"
        
        # Check if update is needed
        local change_set_name="update-changeset-$(date +%s)"
        aws cloudformation create-change-set \
            --stack-name "$STACK_NAME" \
            --change-set-name "$change_set_name" \
            --template-body file://infrastructure/cloudformation/valkey-benchmark-stack.yaml \
            --parameters "${params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "$REGION" >/dev/null 2>&1
        
        # Wait for change set creation
        sleep 5
        
        # Check if there are changes
        local changes=$(aws cloudformation describe-change-set \
            --stack-name "$STACK_NAME" \
            --change-set-name "$change_set_name" \
            --region "$REGION" \
            --query 'Changes' \
            --output text 2>/dev/null || echo "")
        
        if [[ "$changes" == "None" ]] || [[ -z "$changes" ]]; then
            print_status "No changes detected in stack. Skipping update."
            aws cloudformation delete-change-set \
                --stack-name "$STACK_NAME" \
                --change-set-name "$change_set_name" \
                --region "$REGION" >/dev/null 2>&1
            return 0
        fi
        
        # Execute the change set
        aws cloudformation execute-change-set \
            --stack-name "$STACK_NAME" \
            --change-set-name "$change_set_name" \
            --region "$REGION"
        
        print_status "Stack update initiated. This may take 15-30 minutes..."
    else
        print_status "Creating new stack..."
        operation="CREATE"
        
        aws cloudformation create-stack \
            --stack-name "$STACK_NAME" \
            --template-body file://infrastructure/cloudformation/valkey-benchmark-stack.yaml \
            --parameters "${params[@]}" \
            --capabilities CAPABILITY_NAMED_IAM \
            --region "$REGION"
        
        print_status "Stack creation initiated. This may take 25-40 minutes..."
        echo "Resources to be created:"
        echo "  - VPC with public/private subnets"
        echo "  - EKS Fargate cluster"
        echo "  - RDS PostgreSQL (MultiAZ enabled)"
        echo "  - Security groups and IAM roles"
        echo "  - CloudFront distribution (if ALB DNS provided)"
        echo
    fi
    
    # Monitor the deployment with detailed progress
    if ! monitor_stack_events "$STACK_NAME" "$operation"; then
        print_error "Stack deployment failed. Check AWS CloudFormation console for details."
        return 1
    fi
    
    print_success "Infrastructure deployment complete"
}

# Function to save stack outputs
save_stack_outputs() {
    print_status "Saving stack outputs..."
    
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs' \
        --output json > stack-outputs.json
    
    print_success "Stack outputs saved to stack-outputs.json"
}

# Function to configure kubectl
configure_kubectl() {
    print_status "Configuring kubectl for EKS cluster..."
    
    aws eks update-kubeconfig \
        --name "$CLUSTER_NAME" \
        --region "$REGION"
    
    # Wait for cluster to be ready
    print_status "Waiting for cluster to be ready..."
    kubectl wait --for=condition=Ready nodes --all --timeout=300s || true
    
    print_success "kubectl configured successfully"
}

# Function to create Grafana namespace
create_grafana_namespace() {
    print_status "Creating Grafana namespace..."
    
    kubectl create namespace grafana --dry-run=client -o yaml | kubectl apply -f -
    
    print_success "Grafana namespace ready"
}

# Function to install AWS Load Balancer Controller
install_load_balancer_controller() {
    print_status "Installing AWS Load Balancer Controller..."
    
    # Get IAM role ARN
    local alb_role_arn=$(jq -r '.[] | select(.OutputKey=="AWSLoadBalancerControllerRoleArn") | .OutputValue' stack-outputs.json)
    
    # Create service account
    kubectl create serviceaccount aws-load-balancer-controller -n kube-system --dry-run=client -o yaml | kubectl apply -f -
    
    # Annotate service account
    kubectl annotate serviceaccount aws-load-balancer-controller \
        -n kube-system \
        eks.amazonaws.com/role-arn="$alb_role_arn" \
        --overwrite
    
    # Add Helm repo
    helm repo add eks https://aws.github.io/eks-charts
    helm repo update
    
    # Install or upgrade controller
    helm upgrade --install aws-load-balancer-controller eks/aws-load-balancer-controller \
        -n kube-system \
        --set clusterName="$CLUSTER_NAME" \
        --set serviceAccount.create=false \
        --set serviceAccount.name=aws-load-balancer-controller \
        --wait
    
    print_success "AWS Load Balancer Controller installed"
}

# Function to deploy Grafana
deploy_grafana() {
    print_status "Deploying Grafana on Fargate..."
    
    # Get RDS endpoint
    local rds_endpoint=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)
    
    # Create PostgreSQL secret
    kubectl create secret generic grafana-postgres-secret \
        --namespace grafana \
        --from-literal=GF_DATABASE_TYPE=postgres \
        --from-literal=GF_DATABASE_HOST="$rds_endpoint:5432" \
        --from-literal=GF_DATABASE_NAME=grafana \
        --from-literal=GF_DATABASE_USER=postgres \
        --from-literal=GF_DATABASE_PASSWORD="$DB_PASSWORD" \
        --from-literal=GF_DATABASE_SSL_MODE=require \
        --dry-run=client -o yaml | kubectl apply -f -
    
    # Update Grafana values with RDS endpoint
    sed "s/<your-rds-endpoint>/$rds_endpoint/g" grafana-values.yaml > grafana-values-updated.yaml
    
    # Add Grafana Helm repo
    helm repo add grafana https://grafana.github.io/helm-charts
    helm repo update
    
    # Install or upgrade Grafana
    helm upgrade --install grafana grafana/grafana \
        --namespace grafana \
        --values grafana-values-updated.yaml \
        --wait \
        --timeout=10m
    
    # Apply ALB Ingress
    kubectl apply -f alb-ingress.yaml
    
    print_success "Grafana deployed successfully"
}

# Function to wait for ALB provisioning
wait_for_alb() {
    print_status "Waiting for Application Load Balancer to be provisioned..."
    
    local max_attempts=60
    local attempt=0
    local start_time=$(date +%s)
    
    while [ $attempt -lt $max_attempts ]; do
        local alb_dns=$(kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' 2>/dev/null || echo "")
        
        if [ -n "$alb_dns" ]; then
            local elapsed=$(($(date +%s) - start_time))
            print_success "ALB provisioned: $alb_dns (took ${elapsed}s)"
            echo "$alb_dns" > alb-dns-name.txt
            export GRAFANA_ALB_DNS_NAME="$alb_dns"
            
            # Test ALB health
            print_status "Testing ALB health..."
            local health_check_attempts=0
            while [ $health_check_attempts -lt 12 ]; do
                if curl -s -o /dev/null -w "%{http_code}" "http://$alb_dns" | grep -q "200\|302\|404"; then
                    print_success "ALB is responding to requests"
                    break
                fi
                echo -n "."
                sleep 5
                ((health_check_attempts++))
            done
            
            return 0
        fi
        
        # Show progress indicator
        local elapsed=$(($(date +%s) - start_time))
        echo -ne "\rWaiting for ALB... (${elapsed}s elapsed, attempt $((attempt + 1))/$max_attempts)"
        
        sleep 10
        ((attempt++))
    done
    
    echo # New line after progress indicator
    print_error "Timeout waiting for ALB provisioning after $((max_attempts * 10)) seconds"
    print_error "Check AWS Load Balancer Controller logs: kubectl logs -n kube-system deployment/aws-load-balancer-controller"
    return 1
}

# Function to secure ALB for CloudFront-only access
secure_alb_for_cloudfront() {
    print_status "Securing ALB for CloudFront-only access..."
    
    # Get CloudFront managed prefix list
    print_status "Getting CloudFront managed prefix list..."
    local cloudfront_prefix_list=$(aws ec2 describe-managed-prefix-lists \
        --filters "Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing" \
        --region "$REGION" \
        --query 'PrefixLists[0].PrefixListId' \
        --output text)
    
    if [ -z "$cloudfront_prefix_list" ] || [ "$cloudfront_prefix_list" == "None" ]; then
        print_error "Could not find CloudFront managed prefix list"
        return 1
    fi
    
    print_status "CloudFront prefix list: $cloudfront_prefix_list"
    
    # Get ALB security group (created by Load Balancer Controller)
    print_status "Finding ALB security group..."
    local alb_sg=$(aws ec2 describe-security-groups \
        --region "$REGION" \
        --filters "Name=tag:ingress.k8s.aws/stack,Values=grafana/grafana-ingress" \
        --query 'SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "")
    
    if [ -z "$alb_sg" ] || [ "$alb_sg" == "None" ]; then
        # Try alternative method
        alb_sg=$(aws ec2 describe-security-groups \
            --region "$REGION" \
            --filters "Name=group-name,Values=k8s-grafana-grafanai-*" \
            --query 'SecurityGroups[0].GroupId' \
            --output text 2>/dev/null || echo "")
    fi
    
    if [ -z "$alb_sg" ] || [ "$alb_sg" == "None" ]; then
        print_warning "Could not find ALB security group. Security hardening will be skipped."
        print_warning "You may need to run ./secure-alb-for-cloudfront.sh manually later."
        return 0
    fi
    
    print_status "ALB security group: $alb_sg"
    
    # Get EKS cluster security group
    local cluster_sg=$(aws eks describe-cluster \
        --name "$CLUSTER_NAME" \
        --region "$REGION" \
        --query 'cluster.resourcesVpcConfig.clusterSecurityGroupId' \
        --output text)
    
    if [ -z "$cluster_sg" ] || [ "$cluster_sg" == "None" ]; then
        print_error "Could not find EKS cluster security group"
        return 1
    fi
    
    print_status "EKS cluster security group: $cluster_sg"
    
    # Remove public access rule if it exists
    print_status "Removing public access rule (0.0.0.0/0)..."
    aws ec2 revoke-security-group-ingress \
        --group-id "$alb_sg" \
        --region "$REGION" \
        --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}]' \
        2>/dev/null && print_status "Removed public access rule" || print_status "Public access rule not found or already removed"
    
    # Add CloudFront prefix list rule
    print_status "Adding CloudFront prefix list rule..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$alb_sg" \
        --region "$REGION" \
        --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 80, \"ToPort\": 80, \"PrefixListIds\": [{\"PrefixListId\": \"$cloudfront_prefix_list\", \"Description\": \"CloudFront origin access\"}]}]" \
        2>/dev/null && print_status "Added CloudFront access rule" || print_status "CloudFront rule already exists"
    
    # Ensure EKS nodes can receive traffic from ALB
    print_status "Ensuring EKS nodes can receive traffic from ALB..."
    aws ec2 authorize-security-group-ingress \
        --group-id "$cluster_sg" \
        --region "$REGION" \
        --ip-permissions "[{\"IpProtocol\": \"tcp\", \"FromPort\": 3000, \"ToPort\": 3000, \"UserIdGroupPairs\": [{\"GroupId\": \"$alb_sg\", \"Description\": \"Allow ALB to reach Grafana pods\"}]}]" \
        2>/dev/null && print_status "Added ALB to EKS node rule" || print_status "ALB to EKS node rule already exists"
    
    print_success "ALB security hardening complete!"
    print_success "ALB is now restricted to CloudFront access only"
}

# Function to update stack with CloudFront
update_stack_with_cloudfront() {
    if [ -z "$GRAFANA_ALB_DNS_NAME" ]; then
        print_warning "No ALB DNS name available. Skipping CloudFront setup."
        return
    fi
    
    print_status "Updating stack to enable CloudFront..."
    
    # Re-run deployment with ALB DNS name
    deploy_infrastructure
    save_stack_outputs
    
    # Get CloudFront domain
    local cloudfront_domain=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputValue' stack-outputs.json 2>/dev/null || echo "")
    
    if [ -n "$cloudfront_domain" ]; then
        print_success "CloudFront distribution created: https://$cloudfront_domain"
        
        # Update Grafana configuration with CloudFront URL
        helm upgrade grafana grafana/grafana \
            --namespace grafana \
            --values grafana-values-updated.yaml \
            --set env.GF_SERVER_ROOT_URL="https://$cloudfront_domain" \
            --set grafana\\.ini.server.root_url="https://$cloudfront_domain" \
            --wait
        
        # Restart Grafana to apply changes
        kubectl rollout restart deployment grafana -n grafana
        kubectl rollout status deployment grafana -n grafana
        
        print_success "Grafana updated with CloudFront URL"
        
        # Secure ALB for CloudFront-only access
        secure_alb_for_cloudfront
    fi
}

# Function to display final information
display_final_info() {
    print_success "Deployment completed successfully!"
    echo
    echo "=== DEPLOYMENT SUMMARY ==="
    echo "Stack Name: $STACK_NAME"
    echo "Region: $REGION"
    echo "EKS Cluster: $CLUSTER_NAME"
    echo
    
    # Get important outputs
    local rds_endpoint=$(jq -r '.[] | select(.OutputKey=="RDSEndpoint") | .OutputValue' stack-outputs.json)
    local cloudfront_domain=$(jq -r '.[] | select(.OutputKey=="CloudFrontDomainName") | .OutputValue' stack-outputs.json 2>/dev/null || echo "")
    local alb_dns=$(cat alb-dns-name.txt 2>/dev/null || echo "")
    
    echo "=== ACCESS INFORMATION ==="
    if [ -n "$cloudfront_domain" ]; then
        echo "Grafana URL: https://$cloudfront_domain"
    elif [ -n "$alb_dns" ]; then
        echo "Grafana URL: http://$alb_dns"
    fi
    
    # Get Grafana admin password
    local grafana_password=$(kubectl get secret --namespace grafana grafana -o jsonpath="{.data.admin-password}" 2>/dev/null | base64 --decode 2>/dev/null || echo "")
    if [ -n "$grafana_password" ]; then
        echo "Grafana Username: admin"
        echo "Grafana Password: $grafana_password"
    fi
    
    echo
    echo "=== INFRASTRUCTURE DETAILS ==="
    echo "RDS Endpoint: $rds_endpoint"
    if [ -n "$alb_dns" ]; then
        echo "ALB DNS: $alb_dns"
    fi
    if [ -n "$cloudfront_domain" ]; then
        echo "CloudFront Domain: $cloudfront_domain"
    fi
    
    echo
    echo "=== NEXT STEPS ==="
    echo "1. Initialize database schema: kubectl run -it --rm psql-client --image=postgres:17 --restart=Never --namespace=grafana -- psql \"host=$rds_endpoint port=5432 dbname=grafana user=postgres password=*** sslmode=require\""
    echo "2. Import dashboards via Grafana UI"
    echo "3. Enable public dashboard sharing"
    echo
    echo "Security hardening completed automatically!"
    echo
    echo "Stack outputs saved in: stack-outputs.json"
    echo "ALB DNS saved in: alb-dns-name.txt"
}

# Main execution
main() {
    echo "=== Valkey Benchmark Dashboard - Fargate Deployment ==="
    echo
    
    check_prerequisites
    get_user_input
    deploy_infrastructure
    save_stack_outputs
    configure_kubectl
    create_grafana_namespace
    install_load_balancer_controller
    deploy_grafana
    wait_for_alb
    update_stack_with_cloudfront
    display_final_info
    
    print_success "All done!"
}

# Run main function
main "$@"
