#! /bin/bash
# get short git commit hash
TAG=$(git rev-parse --short HEAD)
echo "Tag: $TAG"

set -eou pipefail


read -p "Have you logged in to aws and ecr? ([y]/n): " confirm
if [ "$confirm" == "n" ]; then    
    echo "Logging in to aws and ecr"
    aws sso login
    aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
fi

echo "Building and deploying $TAG"
make build
# echo "Running tests"
# make test > /dev/null 2>&1
# if [ $? -ne 0 ]; then
#     echo "Tests failed"    
#     exit 1
# fi
make image TAG=public.ecr.aws/k6t4m3l7/ib-kubernetes:$TAG
# ask to confirm
read -p "Are you sure you want to push and deploy $TAG? (y/[n]): " confirm
if [ "$confirm" != "y" ]; then
    echo "Deployment cancelled"
    exit 1
fi
make docker-push TAG=public.ecr.aws/k6t4m3l7/ib-kubernetes:$TAG

echo "To Deploy, install/upgrade via helm!"