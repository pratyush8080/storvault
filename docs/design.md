# StorVault design notes

## Data model
A **snapshot** = prefix `snap-<ts>-<id>/` in the hot bucket + `manifest.tsv` (path, key, size, sha256, mtime).
The manifest is written **last**, so a snapshot without a manifest is by definition incomplete and never restorable.
Incremental snapshots reference objects from earlier snapshots (same size+mtime ⇒ reuse key), so objects are
**reference-counted** through the catalog table `snapshot_objects`; deleting a snapshot only deletes keys no other
snapshot (in that tier) still references.

## Integrity — three layers
1. **At upload:** SHA-256 computed (small files: over the exact bytes sent; large: streaming) and stored as
   `x-amz-meta-sha256` + in the manifest. File mtime/size re-checked after multipart → "file changed during backup".
2. **Verify (shallow):** HEAD each object — size and stored checksum metadata. Cheap, catches missing/truncated objects.
   It *cannot* detect same-size bit-rot because it trusts metadata.
3. **Verify (deep) / restore:** download, re-hash, compare to the manifest (independent of S3 metadata).
   Restore writes `file.sv-part` then atomically renames, so corrupt data never appears at the final path.
**ETag is not a checksum:** for multipart uploads it is MD5-of-part-MD5s + `-N`, and under SSE-KMS it isn't an MD5 at all.
Tiering is gated: the cold copy is deep-verified *before* hot copies are evicted.

## Retry semantics
- Classification: transport errors, 408, 429, 5xx → retryable; other 4xx (403, 404) → fatal, fail fast.
- Backoff: **full jitter**, `delay = U(0, min(cap, base·2^attempt))`, so clients that failed together don't retry together.
- Two levels: agent retries each S3 call; orchestrator retries the whole job (only if the agent saw retryable errors).
  Retried backups are idempotent: same prefix, PUT overwrites, manifest written last.
- Stalled-connection detection via libcurl low-speed limits (a hung TCP stream becomes a retryable error).
- Multipart: failed part retried individually; if a part ultimately fails the upload is aborted (no orphan parts billed).
  Bucket lifecycle rule aborts incomplete multipart uploads after 1 day for agent-crash cases.

## Concurrency (agent)
- File-level parallelism: fixed `ThreadPool` (mutex + condvar queue). Shared state: atomics for counters, a mutex for the
  histogram and the error list; per-file results written to distinct vector slots (no sharing).
- Part-level parallelism inside a large file: workers pull part numbers from an `atomic<uint64_t>`, each writes only its own
  `etags[i]`. Memory bound ≈ `threads × part_threads × part_size`.
- Each request uses its own libcurl easy handle (no shared handles ⇒ no locking). ThreadSanitizer-clean.

## Tiering: two mechanisms
- **Orchestrator-driven (portable):** after `tier_after_days`, copy snapshot objects hot→cold (idempotent, metadata
  preserved), deep-verify cold, optionally evict hot. Objects remain restorable by key from the cold endpoint.
- **Storage-native:** `lifecycle.py` / `scripts/setup_minio_tier.sh` apply S3 lifecycle rules (expiration, noncurrent cleanup,
  abort-multipart, transition to a named tier). Server-side transitioned objects aren't readable by key on the remote side,
  hence the orchestrator path for restorable cold storage.

## Crash safety
On start, the orchestrator marks `running` jobs `interrupted` and `running` snapshots `failed`. Nothing half-written is
ever referenced by a complete snapshot. Catalog is on a PVC in k8s (SQLite ⇒ single replica, `Recreate` strategy).

## What I'd do at scale
Postgres catalog + multiple orchestrator replicas with a job queue (leader election/`SELECT … FOR UPDATE SKIP LOCKED`);
content-defined chunking + dedup across files; server-side encryption/client-side encryption; Object Lock for ransomware
protection; gRPC between orchestrator and long-running agents (instead of subprocess); Pushgateway/OTel from agents.

## Known limitations (be upfront in interviews)
Whole-file change detection (no block-level delta); symlinks/permissions/xattrs not preserved; restore doesn't restore mode/owner;
no encryption; multipart uploads are not resumed across *agent* restarts (parts are retried within a run).
