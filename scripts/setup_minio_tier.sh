#!/usr/bin/env bash
# MinIO-native hot->cold transition (server-side tiering) using the `mc` client.
# Requires the compose stack:  docker compose -f deploy/docker-compose.yml up -d minio minio-cold
# Note: MinIO stores transitioned objects under its own naming in the remote tier, so they are NOT directly
# readable by key there. That's why StorVault ALSO implements portable, orchestrator-driven tiering
# (service.tier_snapshot) which keeps cold objects restorable by the agent. See docs/design.md.
set -euo pipefail
mc alias set hot  http://localhost:9000 minioadmin minioadmin
mc alias set cold http://localhost:9100 coldadmin coldadmin123
mc mb --ignore-existing cold/storvault-cold-remote
mc mb --ignore-existing hot/storvault-hot
mc admin tier add minio hot COLDTIER --endpoint http://minio-cold:9000 \
   --access-key coldadmin --secret-key coldadmin123 --bucket storvault-cold-remote --prefix tiered/ || true
mc ilm rule add hot/storvault-hot --transition-days 30 --transition-tier COLDTIER
mc ilm rule add hot/storvault-hot --noncurrent-expire-days 7 || true
mc ilm rule ls hot/storvault-hot
# Same bucket-level rules via the S3 API (also works on Ceph RGW):
#   python -m storvault.lifecycle 30 COLDTIER
