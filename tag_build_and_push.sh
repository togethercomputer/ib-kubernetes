#! /bin/bash
# if env var exists, use it - or else get short git commit hash
TAG=${TAG:-$(git rev-parse --short HEAD)}
echo "Going to build and push tag: $TAG (Use 'TAG=TAG_NAME ./$0' to override)"

set -eou pipefail

# (tc-prod private repo)
IMAGE_REPO=651706779278.dkr.ecr.us-west-2.amazonaws.com

read -p "Have you logged in to aws and ecr? ([y]/n): " confirm
if [ "$confirm" == "n" ]; then    
    echo "Run:"
    echo "aws sso login (login to tc-prod)"
    echo "aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $IMAGE_REPO"
    exit 1
fi

echo "Building and deploying $TAG"
make build
# echo "Running tests"
# make test > /dev/null 2>&1
# if [ $? -ne 0 ]; then
#     echo "Tests failed"    
#     exit 1
# fi
make image TAG=$IMAGE_REPO/ib-kubernetes:$TAG
# ask to confirm
read -p "Are you sure you want to push and deploy $TAG? (y/[n]): " confirm
if [ "$confirm" != "y" ]; then
    echo "Deployment cancelled"
    exit 1
fi
make docker-push TAG=$IMAGE_REPO/ib-kubernetes:$TAG

echo "To Deploy, install/upgrade via helm!"