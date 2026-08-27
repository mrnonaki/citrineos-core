# CitrineOS v2 (wallet fork) — Kubernetes manifests

Reproducible k8s deploy of the `v2-wallet` branch. Captured from the working kind
cluster (`kind-citrineos-v2`) and hardened for **multi-replica** operation.

## Why this exists — the #219 enum race

The stock CitrineOS image runs schema migrations **at every container boot**:

- `apps/ocpp-server/entrypoint.sh` → `pnpm run db:migrate` (sequelize-cli), and
- a second auto-DDL path, sequelize `sync()` in
  `packages/core/src/dal/layers/sequelize/util.ts` (`syncDb()`), guarded by
  `BOOTSTRAP_CITRINEOS_DATABASE_SYNC`.

With **one** replica that's fine. With **>1** replica every pod migrates at once
and they collide on DDL such as `CREATE TYPE ... AS ENUM` (#219) — pods crash-loop
and the schema is left half-applied.

## The fix

Migrations run **exactly once**, in a `Job`, before any app pod:

| File | What |
|------|------|
| `00-namespace.yaml` | `citrineos` namespace |
| `10-rbac-migrate.yaml` | SA + Role (get/list/watch **jobs** only) for the wait initContainer |
| `20-migrate-job.yaml` | run-once `sequelize-cli db:migrate` Job |
| `30-router.yaml` | `citrine-router`, patched: no boot-migrate, `SYNC=false`, waits for the Job |
| `31-modules.yaml` | `citrine-modules` (wallet fork), same patch |
| `40-infra.yaml` | postgres / redis / rabbitmq / hasura Deployments |
| `45-services.yaml` | all Services |

Each app pod's `wait-migrate` initContainer blocks on
`kubectl wait --for=condition=complete job/citrineos-migrate` before the app
starts, so replicas never touch schema DDL. The app command is overridden to start
`node` directly (skipping `entrypoint.sh`'s migrate) and `SYNC=false` disables the
`sync()` path. **Result: `citrine-router` / `citrine-modules` are safe at replicas > 1.**

## Apply (fresh install)

```sh
kubectl apply -f 00-namespace.yaml
kubectl apply -f 40-infra.yaml -f 45-services.yaml   # db/redis/amqp/hasura
kubectl apply -f 10-rbac-migrate.yaml
kubectl apply -f 20-migrate-job.yaml
kubectl -n citrineos wait --for=condition=complete --timeout=300s job/citrineos-migrate
kubectl apply -f 30-router.yaml -f 31-modules.yaml
```

(The app initContainers also self-gate on the Job, so applying everything at once
works too — the app pods just wait in Init until the Job finishes.)

## Upgrade that ships NEW migrations

Jobs are immutable and this one is intentionally kept around (its status is what
the app initContainers read), so re-run it explicitly **before** rolling the app:

```sh
kubectl -n citrineos delete job citrineos-migrate
kubectl apply -f 20-migrate-job.yaml
kubectl -n citrineos wait --for=condition=complete --timeout=300s job/citrineos-migrate
kubectl -n citrineos rollout restart deploy/citrine-router deploy/citrine-modules
```

`SequelizeMeta` makes the Job idempotent — re-running against an up-to-date schema
is a no-op.

## Images

`localhost/citrineos-v2:beta4` (router) and `:beta4-wallet` (modules) are loaded
into kind locally (`imagePullPolicy: Never`). For a real registry, push both and
flip the pull policy. The `wait-migrate` initContainer uses upstream
`docker.io/alpine/k8s:1.31.0` (has a shell + kubectl) — on an air-gapped/offline
kind, `kind load` it too. If pull size matters, `docker.io/rancher/kubectl`
(~55 MB, no shell) works as a slimmer swap using bare `kubectl wait` args
(`command: ["kubectl"]`, `args: ["wait","--for=condition=complete", ...]`) — it
relies on initContainer retry to handle the "Job not created yet" case.

> Infra manifests (`40-infra.yaml`) carry the dev Postgres/Hasura credentials that
> were live on kind. For anything beyond local dev, move POSTGRES_PASSWORD /
> HASURA_GRAPHQL_ADMIN_SECRET / DB URL into a Secret and reference them via
> `secretKeyRef` instead of inline `value:`.
