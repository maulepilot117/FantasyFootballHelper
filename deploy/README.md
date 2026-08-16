# deploy/

Kustomize base + homelab overlay + ArgoCD Application. **Do not `kubectl apply` by hand** —
ArgoCD owns rollouts (docs/DEPLOYMENT.md). Phase 0 ships only the namespace; workloads,
PVCs, ExternalSecrets, HTTPRoute, and CronJobs land in Phase 3. The ArgoCD Application is
NOT applied to the cluster until Phase 3.
