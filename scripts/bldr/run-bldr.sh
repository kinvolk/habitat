#!/bin/bash
set -euo pipefail

source bldr-env.sh

docker_run() {
    docker run \
           --rm \
           -p 80:80 \
           -p 443:443 \
           -p 9631:9631 \
           -p 9638:9638 \
           --privileged \
           --name builder \
           $BLDR_IMAGE
}

docker_build() {
    docker build \
           --no-cache -f BLDR-Dockerfile \
           --build-arg GITHUB_CLIENT_ID=$GITHUB_CLIENT_ID \
           --build-arg GITHUB_CLIENT_SECRET=$GITHUB_CLIENT_SECRET \
           --build-arg GITHUB_ADDR=$GITHUB_ADDR \
           --build-arg GITHUB_API_URL=$GITHUB_API_URL \
           --build-arg GITHUB_WEB_URL=$GITHUB_WEB_URL \
           --build-arg WORKER_AUTH_TOKEN=$WORKER_AUTH_TOKEN \
           --build-arg GITHUB_ADMIN_TEAM=$GITHUB_ADMIN_TEAM \
           --build-arg GITHUB_WORKER_TEAM=$GITHUB_WORKER_TEAM \
           -t $BLDR_IMAGE .
}

if [[ "$SKIP_BUILD" = true ]]; then
    docker_run;
else
    docker_build
    docker_run;
fi;
