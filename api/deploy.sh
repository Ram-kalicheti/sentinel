#!/usr/bin/env bash
# reproducible deploy for the sentinel scorer
# reuses the meridian container apps environment and acr since the student sub allows one env per region
# REDIS_URL is read from the environment so no secret lands in git - source it from api/.env
set -euo pipefail

RG="meridian-rg"
ENV="meridian-cae"
ACR="meridianacrrk1"
APP="sentinel-scorer"
IMAGE="${ACR}.azurecr.io/${APP}:latest"

az acr build --registry "$ACR" --image "${APP}:latest" --file api/Dockerfile .

ACR_USER=$(az acr credential show -n "$ACR" --query username -o tsv)
ACR_PASS=$(az acr credential show -n "$ACR" --query 'passwords[0].value' -o tsv)

az containerapp create \
  --name "$APP" \
  --resource-group "$RG" \
  --environment "$ENV" \
  --image "$IMAGE" \
  --registry-server "${ACR}.azurecr.io" \
  --registry-username "$ACR_USER" \
  --registry-password "$ACR_PASS" \
  --target-port 8000 \
  --ingress external \
  --min-replicas 0 \
  --max-replicas 2 \
  --secrets redis-url="$REDIS_URL" \
  --env-vars REDIS_URL=secretref:redis-url MODEL_PATH=/app/api/model DECISION_THRESHOLD=0.5