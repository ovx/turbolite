# SQLCEs Roadmap

## Current Status

Chunk-based tiered storage (v2): S3/Tigris is source of truth, local disk is a chunk-level LRU cache. 128 pages per chunk at 64KB page size = 8MB per S3 object. Full OLTP with indexes. Supports WAL and DELETE journal modes. 27 tests pass against Tigris.

Speculative chunk prefetching: on cache miss, fetches requested chunk + `prefetch_ahead` adjacent chunks in parallel via tokio::spawn. Default: 8 chunks ahead.

Benchmark (tiered-bench): realistic event-log workload (1M-2M rows, JSON metadata, multiple indexes). Tests hot/warm/cold/constrained tiers with truly cold measurements (fresh VFS per iteration).

---

## Phase 8: Interior Page Detection + Weighted Eviction

B-tree internal pages (root, branch nodes) are accessed on every query but LRU treats them the same as leaf pages. Under cache pressure, evicting an internal page causes an S3 re-fetch for the most critical data in the tree. Fix: detect interior pages at read time, evict them last.

### Detection (in `read_exact_at`)

SQLite page type is byte 0 of each page (byte 100 for page 0 due to 100-byte DB header):
- `0x05` = table interior, `0x02` = index interior → mark chunk as interior
- `0x0a` = table leaf, `0x0d` = index leaf → normal eviction
- No manifest changes — detection happens organically at read time
- Interior pages are read on every query, so detection converges on first query

### Storage

- Add `interior_chunks: Mutex<HashSet<u64>>` to DiskCache
- Add `mark_interior(chunk_id: u64)` method
- No serialization needed — rebuilt from reads on each open

### Eviction

In `evict_if_needed`, sort by `(is_interior ASC, last_access ASC)`:
- All leaf-only chunks evicted first (oldest to newest)
- Interior chunks evicted only when no leaf chunks remain
- Dirty chunk protection unchanged

### Pin chunk 0

Chunk 0 contains page 1 (sqlite_master root + schema). Always accessed, never evict.
- On VFS open, fetch chunk 0 if not cached
- Chunk 0 exempt from eviction (treated like dirty chunk)
- Saves 1 S3 roundtrip on every cold query

### Changes

~20 lines in `src/tiered.rs`:
- DiskCache: `interior_chunks` field + `mark_interior()` method
- `read_exact_at`: check page type byte after decompression, call `mark_interior`
- `evict_if_needed`: partition sort — leaf chunks first, interior chunks last
- VFS open: eagerly fetch + pin chunk 0

---

## Phase 9: Hop-Based Adaptive Prefetching

Current prefetch is fixed-size (`prefetch_ahead` chunks on every miss). Wastes bandwidth on point lookups (fetches 9 chunks for 1 page) and isn't aggressive enough for full scans. Fix: exponentially escalate prefetch window on consecutive cache misses, reset on hit. Inspired by RocksDB's adaptive readahead.

### Hop escalation

Quartic escalation: `hop_size = hop^4`. Aggressive — SQL semantics require full result sets, not ranked results, so we need to cache fast.

```
Hop 1:    1 chunk
Hop 2:   16 chunks
Hop 3:   81 chunks
Hop 4:  256 chunks   (cumulative: 354 chunks = 2.8GB)
Hop 5:  625 chunks   (cumulative: 979 chunks = 7.8GB)
```

S3 handles massive parallelism, so no artificial cap. Fetch all remaining uncached chunks once hop size exceeds what's left.

### Reset on cache hit

Counter resets to 0 on any cache hit in `read_exact_at`. This naturally adapts:
- **Point lookup**: root hit → internal hit → 1 leaf miss → fetches 1 chunk. Minimal waste.
- **Range scan**: misses escalate → by hop 3, fetching 81 chunks per roundtrip.
- **Full table scan**: 200-chunk DB cached in 3-4 hops. 1000-chunk DB in 5 hops. Wall-clock ≈ 300-500ms.
- **Mixed workload**: each cache hit resets, so isolated misses between cached regions stay small.

### Cache pressure backoff

Assume cache can hold the entire database (typical deployment). When `cache_max_bytes` is set and cache exceeds 80%, stop prefetching (fetch only requested chunk). When `cache_max_bytes = 0` (unlimited, default), no backoff — prefetch keeps doubling until every chunk is cached.

### State

One field on TieredHandle: `consecutive_misses: u8`. No persistence, no manifest changes, no background tasks.

### Replaces

Removes the fixed `prefetch_ahead: u32` config field. The hop-based approach subsumes it — prefetch size is now dynamic, starting at 0 and growing unbounded based on access pattern + cache pressure.

### Changes

~25 lines in `src/tiered.rs`:
- TieredHandle: add `consecutive_misses: u8`, remove `prefetch_ahead: u32`
- TieredConfig: remove `prefetch_ahead` field
- `read_exact_at` cache hit path: `self.consecutive_misses = 0`
- `read_exact_at` cache miss path: increment `consecutive_misses`, compute prefetch count as `miss^4`. Fetch all remaining when hop size exceeds uncached chunks.
- Add cache pressure check: if `cache.total_bytes() > cache.max_bytes * 4/5`, prefetch count = 0

---

## Phase 10: Rename to sqlite-turbo-vfs

Rename the project from `sqlite-compress-encrypt-vfs` / `sqlces` to `sqlite-turbo-vfs` to match `redb-turbo` branding.

### Files to update
- Directory: `sqlite-compress-encrypt-vfs/` → `sqlite-turbo-vfs/`
- `Cargo.toml`: package name `sqlite-compress-encrypt-vfs` → `sqlite-turbo-vfs`
- All `use sqlite_compress_encrypt_vfs::` → `use sqlite_turbo_vfs::` in bin/, tests/, examples/
- `.gitmodules`: submodule path and URL
- `redlite-cloud/ROADMAP.md`: path reference
- `russellromney/website/src/routes/Tools.js`: project listing
- `sqlite-tantivy/benchmarks/README.md`: cross-project reference
- Binary names: decide whether `sqlces` CLI becomes `turbo-sqlite` or stays
- Fly app name: `cinch-tiered-bench` (may keep as-is, it's infra not branding)
- Soup project: `sqlces` → update or keep
- Cargo.lock files in redlite/ will regenerate

---

## Phase 11: Future Optimizations

### Bidirectional prefetch
- Track `last_chunk_accessed` on TieredHandle
- Forward access → prefetch N+1, N+2... (current behavior)
- Backward access (DESC queries) → prefetch N-1, N-2...

### Application-level parallel fetch API
- `vfs.fetch_chunks(chunk_ids)` — parallel S3 GETs, populate cache
- `vfs.fetch_all()` — background hydration (a la Litestream)
- `vfs.fetch_range(start, end)` — contiguous range fetch
- Useful when application knows the query pattern (e.g., full export)

### Access pattern tracking (Markov model)
- `transitions: HashMap<u64, Vec<(u64, u32)>>` — sparse transition counts
- On miss: `transitions[last_chunk][current_chunk] += 1`
- Prefetch top-K likely successors from `transitions[N]`
- Captures B-tree traversal patterns (root → internal → leaf)

### Tiered chunk sizes
- Small chunks for internal pages (1-2MB), large for data (16-32MB)
- Requires page-type tracking in manifest (extend Phase 8 detection to persist)

### Chunk-level key ranges
- Store min/max keys per chunk in manifest
- `vfs.chunks_for_key_range(start, end)` — binary search for relevant chunks
- Complex: requires parsing SQLite B-tree cell format

---

## Phase 1: Remaining

- [ ] Add `--dict <path>` flag to sqlces-bench for benchmarking

---

## Phase 3: Page-level Checksums (Optional)

- CRC32 per page for corruption detection (~0.1% storage overhead, ~0.8µs per page)
- Optional verification on read
- Note: SQLite itself doesn't have checksums by default (cksumvfs is an extension)
- Note: AES-GCM already provides authentication for encrypted databases
- Decision: Implement only if users request it

---

## Phase 5: Tiered Storage — Remaining

### Encryption in tiered mode
- [ ] Compose `compress.rs` encrypt/decrypt functions into TieredHandle read/write path

### Multi-writer coordination
- [ ] Distributed locks for concurrent writers (if needed)

### WAL replication
- [ ] Replicate WAL to S3 for zero-durability-gap (if needed)
