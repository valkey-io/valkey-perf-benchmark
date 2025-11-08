#!/bin/bash

# Valkey Benchmark CloudFormation Stack Deployment Script
# This script deploys the complete infrastructure for Valkey performance benchmarking

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
# Allow overriding STACK_NAME for stack updates, otherwise default to timestamped name
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
DEFAULT_STACK_NAME="${STACK_NAME_PREFIX:-valkey-benchmark-stack}-${TIMESTAMP}"
STACK_NAME="${STACK_NAME:-$DEFAULT_STACK_NAME}"
TEMPLATE_FILE="infrastructure/cloudformation/valkey-benchmark-stack.yaml"
REGION="${AWS_REGION:-us-east-1}"

# Default parameters (can be overridden via command line)
CLUSTER_NAME="${CLUSTER_NAME:-valkey-perf-cluster}"
NODE_INSTANCE_TYPE="${NODE_INSTANCE_TYPE:-t4g.small}"
NODE_DESIRED_CAPACITY="${NODE_DESIRED_CAPACITY:-2}"
NODE_MIN_SIZE="${NODE_MIN_SIZE:-1}"
NODE_MAX_SIZE="${NODE_MAX_SIZE:-4}"
DB_INSTANCE_CLASS="${DB_INSTANCE_CLASS:-db.t4g.micro}"
DB_ALLOCATED_STORAGE="${DB_ALLOCATED_STORAGE:-20}"
DB_MASTER_USERNAME="${DB_MASTER_USERNAME:-postgres}"
ENVIRONMENT="${ENVIRONMENT:-production}"
PROJECT_NAME="${PROJECT_NAME:-valkey-benchmark}"
KEY_PAIR_NAME="${KEY_PAIR_NAME:-valkey-benchmark-key}"
GRAFANA_ALB_DNS_NAME="${GRAFANA_ALB_DNS_NAME:-}"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if AWS CLI is installed
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    print_info "AWS CLI found: $(aws --version)"
}

# Function to check if jq is installed
check_jq() {
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Please install it first (https://stedolan.github.io/jq/)."
        exit 1
    fi
    print_info "jq found: $(jq --version 2>&1)"
}

# Function to find existing GitHub OIDC provider
find_github_oidc_provider() {
    print_info "Checking for existing GitHub OIDC Provider..."
    GITHUB_OIDC_PROVIDER_ARN=$(aws iam list-open-id-connect-providers \
        --query 'OpenIDConnectProviderList[?contains(Arn, `token.actions.githubusercontent.com`)].Arn' \
        --output text 2>/dev/null)
    
    if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ] && [ "$GITHUB_OIDC_PROVIDER_ARN" != "None" ]; then
        print_info "Found existing GitHub OIDC Provider: $GITHUB_OIDC_PROVIDER_ARN"
    else
        print_info "No existing GitHub OIDC Provider found - CloudFormation will create one"
        GITHUB_OIDC_PROVIDER_ARN=""
    fi
}

# Function to validate AWS credentials
check_aws_credentials() {
    if ! aws sts get-caller-identity &> /dev/null; then
        print_error "AWS credentials not configured or invalid."
        exit 1
    fi
    print_info "AWS credentials validated"
    aws sts get-caller-identity
}

# Function to check if template file exists
check_template() {
    if [ ! -f "$TEMPLATE_FILE" ]; then
        print_error "Template file not found: $TEMPLATE_FILE"
        exit 1
    fi
    print_info "Template file found: $TEMPLATE_FILE"
}

# Function to validate CloudFormation template
validate_template() {
    print_info "Validating CloudFormation template..."
    if aws cloudformation validate-template \
        --template-body file://"$TEMPLATE_FILE" \
        --region "$REGION" &> /dev/null; then
        print_info "Template validation successful"
    else
        print_error "Template validation failed"
        exit 1
    fi
}

# Function to check if EC2 key pair exists
check_key_pair() {
    print_info "Checking for EC2 key pair: $KEY_PAIR_NAME"
    if aws ec2 describe-key-pairs --key-names "$KEY_PAIR_NAME" --region "$REGION" &> /dev/null; then
        print_info "Key pair '$KEY_PAIR_NAME' exists"
    else
        print_warning "Key pair '$KEY_PAIR_NAME' not found"
        read -p "Do you want to create it? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            aws ec2 create-key-pair \
                --key-name "$KEY_PAIR_NAME" \
                --region "$REGION" \
                --query 'KeyMaterial' \
                --output text > "${KEY_PAIR_NAME}.pem"
            chmod 400 "${KEY_PAIR_NAME}.pem"
            print_info "Key pair created and saved to ${KEY_PAIR_NAME}.pem"
        else
            print_error "Key pair is required. Exiting."
            exit 1
        fi
    fi
}

# Function to prompt for DB password
get_db_password() {
    if [ -z "$DB_MASTER_PASSWORD" ]; then
        print_warning "Database master password not set"
        read -sp "Enter RDS master password (min 8 characters): " DB_MASTER_PASSWORD
        echo
        if [ ${#DB_MASTER_PASSWORD} -lt 8 ]; then
            print_error "Password must be at least 8 characters"
            exit 1
        fi
    fi
}

# Function to check if stack exists
stack_exists() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" &> /dev/null
}

# Function to get stack status
get_stack_status() {
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].StackStatus' \
        --output text 2>/dev/null || echo "NOT_FOUND"
}

# Function to delete stack
delete_stack() {
    local stack_status
    stack_status=$(get_stack_status)
    
    case "$stack_status" in
        "ROLLBACK_COMPLETE"|"DELETE_FAILED"|"CREATE_FAILED")
            print_warning "Stack is in $stack_status state. Deleting before redeploying..."
            if aws cloudformation delete-stack \
                --stack-name "$STACK_NAME" \
                --region "$REGION"; then
                print_info "Stack deletion initiated. Waiting for deletion to complete (timeout: 10 minutes)..."
                # Wait for up to 10 minutes for stack deletion
                local end_time=$(( $(date +%s) + 600 ))
                while [ $(date +%s) -lt $end_time ]; do
                    local status
                    status=$(get_stack_status)
                    case "$status" in
                        "DELETE_COMPLETE"|"NOT_FOUND")
                            print_info "Stack deleted successfully"
                            return 0
                            ;;
                        "DELETE_FAILED")
                            print_error "Stack deletion failed"
                            return 1
                            ;;
                        *)
                            # Still deleting, wait a bit more
                            sleep 10
                            ;;
                    esac
                done
                print_error "Stack deletion timed out after 10 minutes"
                return 1
            else
                print_error "Failed to initiate stack deletion"
                return 1
            fi
            ;;
        "CREATE_IN_PROGRESS"|"UPDATE_IN_PROGRESS"|"DELETE_IN_PROGRESS"|"UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
            print_warning "Stack is in $stack_status state. Waiting for operation to complete..."
            # Wait for current operation to complete before proceeding
            case "$stack_status" in
                "DELETE_IN_PROGRESS")
                    if aws cloudformation wait stack-delete-complete \
                        --stack-name "$STACK_NAME" \
                        --region "$REGION" 2>/dev/null; then
                        print_info "Stack deletion completed"
                        return 0
                    else
                        print_error "Stack operation failed or timed out"
                        return 1
                    fi
                    ;;
                "CREATE_IN_PROGRESS"|"UPDATE_IN_PROGRESS"|"UPDATE_COMPLETE_CLEANUP_IN_PROGRESS")
                    # For create/update, we'll let the deploy_stack function handle it
                    print_info "Continuing with existing stack operation"
                    return 0
                    ;;
            esac
            ;;
        *)
            # Stack is in a stable state, no action needed
            return 0
            ;;
    esac
}

# Function to deploy stack
deploy_stack() {
    print_info "Deploying CloudFormation stack: $STACK_NAME"
    
    # Check if stack needs to be deleted first
    if stack_exists; then
        if ! delete_stack; then
            print_error "Failed to clean up existing stack. Aborting deployment."
            exit 1
        fi
        
        # Check if stack still exists after cleanup
        if stack_exists; then
            print_warning "Updating existing stack..."
            OPERATION="update-stack"
        else
            print_info "Creating new stack..."
            OPERATION="create-stack"
        fi
    else
        print_info "Creating new stack..."
        OPERATION="create-stack"
    fi
    
    # Prepare parameters
    PARAMS="--parameters \
        ParameterKey=ClusterName,ParameterValue=\"$CLUSTER_NAME\" \
        ParameterKey=NodeInstanceType,ParameterValue=\"$NODE_INSTANCE_TYPE\" \
        ParameterKey=NodeGroupDesiredCapacity,ParameterValue=\"$NODE_DESIRED_CAPACITY\" \
        ParameterKey=NodeGroupMinSize,ParameterValue=\"$NODE_MIN_SIZE\" \
        ParameterKey=NodeGroupMaxSize,ParameterValue=\"$NODE_MAX_SIZE\" \
        ParameterKey=DBInstanceClass,ParameterValue=\"$DB_INSTANCE_CLASS\" \
        ParameterKey=DBAllocatedStorage,ParameterValue=\"$DB_ALLOCATED_STORAGE\" \
        ParameterKey=DBMasterUsername,ParameterValue=\"$DB_MASTER_USERNAME\" \
        ParameterKey=DBMasterPassword,ParameterValue=\"$DB_MASTER_PASSWORD\" \
        ParameterKey=Environment,ParameterValue=\"$ENVIRONMENT\" \
        ParameterKey=ProjectName,ParameterValue=\"$PROJECT_NAME\" \
        ParameterKey=KeyPairName,ParameterValue=\"$KEY_PAIR_NAME\""

    if [ -n "$GRAFANA_ALB_DNS_NAME" ]; then
        print_info "Including Grafana ALB DNS name: $GRAFANA_ALB_DNS_NAME"
        PARAMS="$PARAMS ParameterKey=GrafanaAlbDnsName,ParameterValue=\"$GRAFANA_ALB_DNS_NAME\""
    else
        print_warning "Grafana ALB DNS name not provided. CloudFront distribution will be created after you rerun with GRAFANA_ALB_DNS_NAME set."
    fi
    
    # Add GitHub OIDC Provider ARN if found
    if [ -n "$GITHUB_OIDC_PROVIDER_ARN" ] && [ "$GITHUB_OIDC_PROVIDER_ARN" != "None" ]; then
        print_info "Adding GitHub OIDC Provider ARN parameter: $GITHUB_OIDC_PROVIDER_ARN"
        PARAMS="$PARAMS ParameterKey=GitHubOIDCProviderArn,ParameterValue=\"$GITHUB_OIDC_PROVIDER_ARN\""
    else
        print_info "No existing GitHub OIDC Provider found - CloudFormation will create one"
        # Don't pass the parameter, let it use the default empty value
        # This will trigger the NeedsGitHubOIDCProvider condition
    fi
    
    print_info "Deploying with parameters: $PARAMS"
    
    aws cloudformation "$OPERATION" \
        --stack-name "$STACK_NAME" \
        --template-body file://"$TEMPLATE_FILE" \
        --region "$REGION" \
        $PARAMS \
        --capabilities CAPABILITY_NAMED_IAM \
        --tags \
            Key=Environment,Value="$ENVIRONMENT" \
            Key=Project,Value="$PROJECT_NAME" \
            Key=ManagedBy,Value=CloudFormation
    
    print_info "Stack $OPERATION initiated"
}

# Function to wait for stack completion with progress monitoring
wait_for_stack() {
    print_info "Waiting for stack operation to complete..."
    print_warning "This may take 25-40 minutes..."
    print_info "Monitoring stack events in real-time (Ctrl+C to stop monitoring, but deployment will continue)..."
    
    local last_event_id=""
    local stack_complete=false
    
    # Monitor stack progress
    while ! $stack_complete; do
        # Get recent stack events
        local events_output
        events_output=$(aws cloudformation describe-stack-events \
            --stack-name "$STACK_NAME" \
            --region "$REGION" \
            --max-items 5 2>/dev/null) || {
            sleep 10
            continue
        }
        
        # Get the latest event ID
        local latest_event_id
        latest_event_id=$(echo "$events_output" | jq -r '.StackEvents[0].EventId // empty')
        
        # Print new events
        if [ -n "$latest_event_id" ] && [ "$latest_event_id" != "$last_event_id" ]; then
            # Extract and print new events
            echo "$events_output" | jq -r '.StackEvents[] | select(.EventId != "'"$last_event_id"'" and (.ResourceStatus != null)) | 
                "\(.Timestamp) [\(.ResourceStatus)] \(.ResourceType) - \(.LogicalResourceId) \((.ResourceStatusReason // ""))"' | 
                while IFS= read -r line; do
                    if [[ $line == *"COMPLETE"* ]] && [[ $line == *"ROLLBACK"* ]]; then
                        echo -e "${RED}$line${NC}"
                    elif [[ $line == *"FAILED"* ]] || [[ $line == *"ROLLBACK"* ]]; then
                        echo -e "${RED}$line${NC}"
                    elif [[ $line == *"COMPLETE"* ]]; then
                        echo -e "${GREEN}$line${NC}"
                    else
                        echo -e "${YELLOW}$line${NC}"
                    fi
                done
            
            last_event_id="$latest_event_id"
        fi
        
        # Check if stack operation is complete
        local stack_status
        stack_status=$(aws cloudformation describe-stacks \
            --stack-name "$STACK_NAME" \
            --region "$REGION" \
            --query 'Stacks[0].StackStatus' \
            --output text 2>/dev/null) || {
            sleep 10
            continue
        }
        
        case "$stack_status" in
            CREATE_COMPLETE|UPDATE_COMPLETE)
                print_info "Stack operation completed successfully"
                stack_complete=true
                ;;
            CREATE_FAILED|ROLLBACK_COMPLETE|ROLLBACK_FAILED|UPDATE_ROLLBACK_COMPLETE|UPDATE_ROLLBACK_FAILED)
                print_error "Stack operation failed with status: $stack_status"
                print_info "Check AWS Console for details"
                exit 1
                ;;
            *)
                # Stack is still in progress, continue monitoring
                sleep 15
                ;;
        esac
    done
}

# Function to display stack outputs
display_outputs() {
    print_info "Stack Outputs:"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
}

# Function to save outputs to file
save_outputs() {
    OUTPUT_FILE="stack-outputs.json"
    print_info "Saving outputs to $OUTPUT_FILE"
    aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs' \
        --output json > "$OUTPUT_FILE"
    print_info "Outputs saved to $OUTPUT_FILE"
}

# Function to configure kubectl
configure_kubectl() {
    print_info "Configuring kubectl for EKS cluster..."
    CLUSTER_NAME_OUTPUT=$(aws cloudformation describe-stacks \
        --stack-name "$STACK_NAME" \
        --region "$REGION" \
        --query 'Stacks[0].Outputs[?OutputKey==`EKSClusterName`].OutputValue' \
        --output text)
    
    if [ -n "$CLUSTER_NAME_OUTPUT" ]; then
        aws eks update-kubeconfig \
            --name "$CLUSTER_NAME_OUTPUT" \
            --region "$REGION"
        print_info "kubectl configured for cluster: $CLUSTER_NAME_OUTPUT"
    else
        print_warning "Could not retrieve EKS cluster name from outputs"
    fi
}

# Function to display next steps
display_next_steps() {
    echo ""
    print_info "=== Deployment Complete ==="
    echo ""
    print_info "Next steps:"
    echo "  1. Configure kubectl: aws eks update-kubeconfig --name $CLUSTER_NAME --region $REGION"
    echo "  2. Install AWS Load Balancer Controller"
    echo "  3. Install EBS CSI Driver"
    echo "  4. Deploy Grafana with Helm"
    echo "  5. Capture the ALB DNS once the ingress is ready:"
    echo "       kubectl get ingress -n grafana grafana-ingress -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'"
    echo "  6. Re-run this script with STACK_NAME=$STACK_NAME and GRAFANA_ALB_DNS_NAME set to that hostname to enable CloudFront."
    echo "  7. IMPORTANT: Secure ALB for CloudFront-only access: ./secure-alb-for-cloudfront.sh"
    echo ""
    print_info "Useful commands:"
    echo "  - View stack: aws cloudformation describe-stacks --stack-name $STACK_NAME --region $REGION"
    echo "  - Delete stack: aws cloudformation delete-stack --stack-name $STACK_NAME --region $REGION"
    echo "  - View events: aws cloudformation describe-stack-events --stack-name $STACK_NAME --region $REGION"
    echo ""
    print_info "Note: During deployment, you saw real-time CloudFormation events."
    echo "      If you need to monitor a running stack, use:"
    echo "      aws cloudformation describe-stack-events --stack-name $STACK_NAME --region $REGION"
    echo ""
}

# Main execution
main() {
    echo "========================================"
    echo "Valkey Benchmark Stack Deployment"
    echo "========================================"
    echo ""
    print_info "Stack Name: $STACK_NAME"
    echo ""
    
    # Pre-flight checks
    check_aws_cli
    check_jq
    check_aws_credentials
    check_template
    validate_template
    check_key_pair
    get_db_password
    find_github_oidc_provider
    
    # Clean up any problematic stack state before deployment
    if stack_exists; then
        print_info "Checking existing stack state..."
        if ! delete_stack; then
            print_error "Failed to clean up existing stack. Aborting deployment."
            exit 1
        fi
    fi
    
    # Deploy
    deploy_stack
    wait_for_stack
    
    # Post-deployment
    display_outputs
    save_outputs
    configure_kubectl
    display_next_steps
}

# Run main function
main
