#!/bin/bash

# Phase 0: Check Prerequisites
# Validates all required tools and AWS credentials

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

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo "=== Phase 0: Prerequisites Check ==="
echo

print_status "Checking required tools..."

missing_tools=()

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
    echo
    echo "Installation instructions:"
    echo "  macOS:   brew install awscli kubectl helm jq"
    echo "  Ubuntu:  sudo apt-get install awscli kubectl jq && curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash"
    exit 1
fi

print_success "All required tools are installed"
echo

# Check AWS credentials
print_status "Checking AWS credentials..."
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    print_error "AWS credentials not configured"
    echo "Please run: aws configure"
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
AWS_USER=$(aws sts get-caller-identity --query Arn --output text)

print_success "AWS credentials configured"
echo "  Account: $AWS_ACCOUNT"
echo "  User: $AWS_USER"
echo

# Check AWS region
AWS_REGION=$(aws configure get region || echo "")
if [ -z "$AWS_REGION" ]; then
    print_error "AWS region not configured"
    echo "Please run: aws configure set region us-east-1"
    exit 1
fi

print_success "AWS region: $AWS_REGION"
echo

# Verify tool versions
print_status "Tool versions:"
echo "  AWS CLI:  $(aws --version 2>&1 | cut -d' ' -f1)"
echo "  kubectl:  $(kubectl version --client --short 2>/dev/null | grep 'Client Version' || kubectl version --client 2>&1 | head -1)"
echo "  Helm:     $(helm version --short 2>/dev/null || helm version 2>&1 | head -1)"
echo "  jq:       $(jq --version)"
echo

print_success "All prerequisites met! Ready to deploy."
echo
echo "Next step: Run ./01-deploy-infrastructure.sh"
