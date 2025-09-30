#!/bin/bash

# =============================================
# Aurora PostgreSQL Deployment Script
# =============================================

set -e

# Configuration
ENVIRONMENT=${1:-dev}
REGION=${2:-us-east-1}
STACK_NAME="six-worker-aurora-${ENVIRONMENT}"
KEY_PAIR_NAME=${3:-six-worker-key}

echo "🚀 Deploying Aurora PostgreSQL for Six Worker"
echo "Environment: ${ENVIRONMENT}"
echo "Region: ${REGION}"
echo "Stack Name: ${STACK_NAME}"

# Check if AWS CLI is configured
if ! aws sts get-caller-identity --profile lexara_super_agent >/dev/null 2>&1; then
    echo "❌ AWS CLI not configured with lexara_super_agent profile"
    exit 1
fi

# Check if key pair exists
if ! aws ec2 describe-key-pairs --key-names "${KEY_PAIR_NAME}" --profile lexara_super_agent --region "${REGION}" >/dev/null 2>&1; then
    echo "⚠️  Creating EC2 key pair: ${KEY_PAIR_NAME}"
    aws ec2 create-key-pair \
        --key-name "${KEY_PAIR_NAME}" \
        --profile lexara_super_agent \
        --region "${REGION}" \
        --query 'KeyMaterial' \
        --output text > "${KEY_PAIR_NAME}.pem"
    chmod 400 "${KEY_PAIR_NAME}.pem"
    echo "✅ Key pair created and saved as ${KEY_PAIR_NAME}.pem"
fi

# Validate CloudFormation template
echo "🔍 Validating CloudFormation template..."
aws cloudformation validate-template \
    --template-body file://aurora-postgresql.yaml \
    --profile lexara_super_agent \
    --region "${REGION}"

if [ $? -eq 0 ]; then
    echo "✅ Template validation successful"
else
    echo "❌ Template validation failed"
    exit 1
fi

# Deploy stack
echo "🚀 Deploying CloudFormation stack..."
aws cloudformation deploy \
    --template-file aurora-postgresql.yaml \
    --stack-name "${STACK_NAME}" \
    --parameter-overrides \
        Environment="${ENVIRONMENT}" \
        KeyPairName="${KEY_PAIR_NAME}" \
    --capabilities CAPABILITY_IAM \
    --profile lexara_super_agent \
    --region "${REGION}" \
    --no-fail-on-empty-changeset

if [ $? -eq 0 ]; then
    echo "✅ Stack deployment successful!"
    
    # Get stack outputs
    echo "📋 Stack Outputs:"
    aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --profile lexara_super_agent \
        --region "${REGION}" \
        --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
        --output table
        
    # Get connection details
    echo ""
    echo "🔗 Connection Information:"
    CLUSTER_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --profile lexara_super_agent \
        --region "${REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`ClusterEndpoint`].OutputValue' \
        --output text)
    
    BASTION_IP=$(aws cloudformation describe-stacks \
        --stack-name "${STACK_NAME}" \
        --profile lexara_super_agent \
        --region "${REGION}" \
        --query 'Stacks[0].Outputs[?OutputKey==`BastionHostIP`].OutputValue' \
        --output text)
    
    echo "Database Endpoint: ${CLUSTER_ENDPOINT}"
    echo "Bastion Host IP: ${BASTION_IP}"
    echo ""
    echo "🔐 To connect to the database:"
    echo "1. SSH to bastion: ssh -i ${KEY_PAIR_NAME}.pem ec2-user@${BASTION_IP}"
    echo "2. Run: ./connect-db.sh"
    echo ""
    echo "💾 To run migrations:"
    echo "scp -i ${KEY_PAIR_NAME}.pem ../db/migrations/*.sql ec2-user@${BASTION_IP}:~/"
    echo "scp -i ${KEY_PAIR_NAME}.pem ../db/test-data/*.sql ec2-user@${BASTION_IP}:~/"
    
else
    echo "❌ Stack deployment failed"
    exit 1
fi