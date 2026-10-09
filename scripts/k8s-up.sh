#!/usr/bin/env bash
# Bring the whole stack up on a local kind cluster.
set -euo pipefail
cd "$(dirname "$0")/.."
CLUSTER=${CLUSTER:-storvault}
kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER"
docker build -f deploy/Dockerfile --target orchestrator -t storvault:latest .
kind load docker-image storvault:latest --name "$CLUSTER"
kubectl apply -f deploy/k8s/00-namespace-config.yaml
# monitoring configs live in deploy/monitoring/ (single source of truth); k8s service names differ from compose
sed 's/minio:9000/minio-hot:9000/' deploy/monitoring/prometheus.yml > /tmp/prometheus.k8s.yml
kubectl -n storvault create configmap prometheus-config --from-file=prometheus.yml=/tmp/prometheus.k8s.yml \
  --from-file=alerts.yml=deploy/monitoring/alerts.yml --dry-run=client -o yaml | kubectl apply -f -
kubectl -n storvault create configmap grafana-dashboards \
  --from-file=storvault.json=deploy/monitoring/grafana/dashboards/storvault.json --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/k8s/10-storage-tiers.yaml -f deploy/k8s/20-storvault.yaml -f deploy/k8s/30-monitoring.yaml
kubectl -n storvault rollout status deploy/storvault --timeout=180s
cat <<MSG
Up. Try:
  kubectl -n storvault port-forward svc/storvault 8080:8080     # API  http://localhost:8080/docs
  kubectl -n storvault port-forward svc/grafana 3000:3000        # Grafana (admin/admin)
  kubectl -n storvault port-forward svc/prometheus 9090:9090
Kill the pod to watch it recover:  kubectl -n storvault delete pod -l app=storvault
MSG
