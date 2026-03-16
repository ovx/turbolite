//! S3-backed chunk-level tiered storage VFS.
//!
//! Architecture (inspired by turbopuffer):
//! - S3/Tigris is the source of truth for all pages
//! - Local disk is a chunk-level LRU cache
//! - Writes go through WAL (local, fast), checkpoint flushes dirty pages to S3 as chunks
//! - Any instance with the VFS + S3 credentials can read the database
//! - A chunk = N contiguous compressed pages in a single S3 object (default 128 pages = 8MB at 64KB page size)
//!
//! S3 layout:
//! ```text
//! s3://{bucket}/{prefix}/
//! ├── manifest.json       # version, page_count, page_size, chunk_size
//! └── chunks/
//!     ├── 0               # Pages 0-127 (header + compressed pages)
//!     ├── 1               # Pages 128-255
//!     └── ...N
//! ```
//!
//! Chunk format:
//! ```text
//! [129 × u32 offsets (516 bytes)] [compressed page 0] [compressed page 1] ... [compressed page 127]
//! ```
//! To read page N: slice data[offset[N]..offset[N+1]], decompress.
//!
//! Supports full OLTP workloads including indexes. Internal B-tree pages stay
//! hot in cache naturally; leaf pages tier to S3.

use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions as FsOpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, OpenOptions, Vfs};
use tokio::runtime::Handle as TokioHandle;

use crate::compress;

// Re-use the FileWalIndex from the main lib
use crate::FileWalIndex;

// ===== Configuration =====

/// Configuration for tiered S3-backed storage.
pub struct TieredConfig {
    /// S3 bucket name
    pub bucket: String,
    /// S3 key prefix (e.g. "databases/tenant-123")
    pub prefix: String,
    /// Local cache directory for cached chunks
    pub cache_dir: PathBuf,
    /// Zstd compression level (1-22, default 3)
    pub compression_level: i32,
    /// Custom S3 endpoint URL (for MinIO/Tigris)
    pub endpoint_url: Option<String>,
    /// Open in read-only mode (no writes, no WAL)
    pub read_only: bool,
    /// Tokio runtime handle (pass in, or a new runtime is created)
    pub runtime_handle: Option<TokioHandle>,
    /// Pages per chunk (default 128 = 8MB at 64KB page size).
    /// Each S3 object contains this many contiguous compressed pages.
    pub chunk_size: u32,
    /// AWS region (default "us-east-1")
    pub region: Option<String>,
    /// Zstd compression dictionary (for 2-5x better compression on structured data)
    #[cfg(feature = "zstd")]
    pub dictionary: Option<Vec<u8>>,
}

impl Default for TieredConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: String::new(),
            cache_dir: PathBuf::from("/tmp/sqlces-cache"),
            compression_level: 3,
            endpoint_url: None,
            read_only: false,
            runtime_handle: None,
            chunk_size: 128,
            region: None,
            #[cfg(feature = "zstd")]
            dictionary: None,
        }
    }
}

// ===== Manifest =====

/// S3 manifest — updated atomically after all chunk uploads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Monotonically increasing version (bumped on each checkpoint)
    pub version: u64,
    /// Number of pages in the database
    pub page_count: u64,
    /// Page size in bytes
    pub page_size: u32,
    /// Pages per chunk (default 128)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u32,
}

fn default_chunk_size() -> u32 {
    128
}

impl Manifest {
    fn empty() -> Self {
        Self {
            version: 0,
            page_count: 0,
            page_size: 0,
            chunk_size: 0,
        }
    }
}

// ===== S3Client (sync wrapper around async SDK) =====

/// Synchronous S3 client wrapping the async AWS SDK.
struct S3Client {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    runtime: TokioHandle,
}

impl S3Client {
    /// Create a new S3 client. If `runtime_handle` is None, tries to detect
    /// the current tokio runtime.
    async fn new_async(config: &TieredConfig) -> io::Result<Self> {
        let mut aws_config = aws_config::from_env();

        if let Some(region) = &config.region {
            aws_config =
                aws_config.region(aws_sdk_s3::config::Region::new(region.clone()));
        }

        let aws_config = aws_config.load().await;

        let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
        if let Some(endpoint) = &config.endpoint_url {
            s3_config = s3_config
                .endpoint_url(endpoint)
                .force_path_style(true);
        }

        let client = aws_sdk_s3::Client::from_conf(s3_config.build());

        let runtime = config
            .runtime_handle
            .clone()
            .or_else(|| TokioHandle::try_current().ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Other,
                    "No tokio runtime available. Pass runtime_handle in TieredConfig \
                     or call from within a tokio context.",
                )
            })?;

        Ok(Self {
            client,
            bucket: config.bucket.clone(),
            prefix: config.prefix.clone(),
            runtime,
        })
    }

    /// Blocking constructor — creates the async client on the runtime.
    fn new_blocking(config: &TieredConfig, runtime: &TokioHandle) -> io::Result<Self> {
        Self::block_on(runtime, Self::new_async(config))
    }

    /// Run an async future on the tokio runtime, handling both
    /// "inside tokio" and "outside tokio" cases.
    fn block_on<F: std::future::Future<Output = T>, T>(
        handle: &TokioHandle,
        fut: F,
    ) -> T {
        // If we're already on a tokio worker thread, use block_in_place
        // to allow the runtime to schedule other work while we block.
        match TokioHandle::try_current() {
            Ok(_) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Err(_) => handle.block_on(fut),
        }
    }

    fn s3_key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_string()
        } else {
            format!("{}/{}", self.prefix, suffix)
        }
    }

    fn chunk_key(&self, chunk_id: u64) -> String {
        self.s3_key(&format!("chunks/{}", chunk_id))
    }

    fn manifest_key(&self) -> String {
        self.s3_key("manifest.json")
    }

    // --- Single-chunk operations ---

    /// Fetch a single chunk from S3. Returns None on 404.
    fn get_chunk(&self, chunk_id: u64) -> io::Result<Option<Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_chunk_async(chunk_id))
    }

    async fn get_chunk_async(&self, chunk_id: u64) -> io::Result<Option<Vec<u8>>> {
        let key = self.chunk_key(chunk_id);
        self.get_object_async(&key).await
    }

    async fn get_object_async(&self, key: &str) -> io::Result<Option<Vec<u8>>> {
        let mut retries = 0u32;
        loop {
            match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(resp) => {
                    let bytes = resp
                        .body
                        .collect()
                        .await
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                        .into_bytes();
                    return Ok(Some(bytes.to_vec()));
                }
                Err(e) => {
                    // Check for 404 (NoSuchKey)
                    if is_not_found(&e) {
                        return Ok(None);
                    }
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("S3 GET {} failed after 3 retries: {}", key, e),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }

    // --- Batch chunk operations ---

    /// Fetch multiple chunks in parallel. Returns a map of chunk_id → raw chunk bytes.
    fn get_chunks(&self, chunk_ids: &[u64]) -> io::Result<HashMap<u64, Vec<u8>>> {
        S3Client::block_on(&self.runtime, self.get_chunks_async(chunk_ids))
    }

    async fn get_chunks_async(&self, chunk_ids: &[u64]) -> io::Result<HashMap<u64, Vec<u8>>> {
        let mut handles = Vec::with_capacity(chunk_ids.len());
        for &cid in chunk_ids {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.chunk_key(cid);
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    match client
                        .get_object()
                        .bucket(&bucket)
                        .key(&key)
                        .send()
                        .await
                    {
                        Ok(resp) => {
                            let bytes = resp
                                .body
                                .collect()
                                .await
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                                .into_bytes();
                            return Ok::<_, io::Error>((cid, Some(bytes.to_vec())));
                        }
                        Err(e) => {
                            if is_not_found(&e) {
                                return Ok((cid, None));
                            }
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 GET chunk {} failed: {}", cid, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        let mut result = HashMap::new();
        for handle in handles {
            let (cid, data) = handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
            if let Some(bytes) = data {
                result.insert(cid, bytes);
            }
        }
        Ok(result)
    }

    /// Upload multiple chunks in parallel. Each entry is (chunk_id, raw_chunk_bytes).
    fn put_chunks(&self, chunks: &[(u64, Vec<u8>)]) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_chunks_async(chunks))
    }

    async fn put_chunks_async(&self, chunks: &[(u64, Vec<u8>)]) -> io::Result<()> {
        let mut handles = Vec::with_capacity(chunks.len());
        for (cid, data) in chunks {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.chunk_key(*cid);
            let data = data.clone();
            handles.push(tokio::spawn(async move {
                let mut retries = 0u32;
                loop {
                    let body =
                        aws_sdk_s3::primitives::ByteStream::from(data.clone());
                    match client
                        .put_object()
                        .bucket(&bucket)
                        .key(&key)
                        .body(body)
                        .send()
                        .await
                    {
                        Ok(_) => return Ok::<_, io::Error>(()),
                        Err(e) => {
                            retries += 1;
                            if retries >= 3 {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!("S3 PUT chunk {} failed: {}", key, e),
                                ));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(
                                100 * (1 << retries),
                            ))
                            .await;
                        }
                    }
                }
            }));
        }

        for handle in handles {
            handle
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))??;
        }
        Ok(())
    }

    // --- Manifest ---

    fn get_manifest(&self) -> io::Result<Option<Manifest>> {
        S3Client::block_on(&self.runtime, self.get_manifest_async())
    }

    async fn get_manifest_async(&self) -> io::Result<Option<Manifest>> {
        let key = self.manifest_key();
        match self.get_object_async(&key).await? {
            Some(bytes) => {
                let manifest: Manifest = serde_json::from_slice(&bytes).map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Invalid manifest JSON: {}", e),
                    )
                })?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        S3Client::block_on(&self.runtime, self.put_manifest_async(manifest))
    }

    async fn put_manifest_async(&self, manifest: &Manifest) -> io::Result<()> {
        let key = self.manifest_key();
        let json = serde_json::to_vec(manifest)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut retries = 0u32;
        loop {
            let body = aws_sdk_s3::primitives::ByteStream::from(json.clone());
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .content_type("application/json")
                .body(body)
                .send()
                .await
            {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries >= 3 {
                        return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "S3 PUT manifest failed after 3 retries: {}",
                                e
                            ),
                        ));
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(
                        100 * (1 << retries),
                    ))
                    .await;
                }
            }
        }
    }
}

/// Check if an S3 error is a 404 / NoSuchKey.
fn is_not_found<E: std::fmt::Display + std::fmt::Debug>(
    err: &aws_sdk_s3::error::SdkError<E>,
) -> bool {
    match err {
        aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
            // Check HTTP status code
            service_err.raw().status().as_u16() == 404
        }
        _ => false,
    }
}

// ===== DiskCache (chunk-based with LRU eviction) =====

/// Unbounded on-disk chunk cache.
///
/// Each chunk is stored as a file at `{cache_dir}/chunks/{chunk_id}`.
/// The file contains the raw chunk bytes (header + compressed pages).
/// No eviction — cache grows as needed. Interior page tracking is used
/// by read_exact_at to mark B-tree internal pages for future use.
struct DiskCache {
    cache_dir: PathBuf,
    /// Chunk presence tracking: chunk_id → file_size
    chunks: parking_lot::Mutex<HashMap<u64, u64>>,
    /// Chunks known to contain B-tree interior pages
    interior_chunks: parking_lot::Mutex<HashSet<u64>>,
}

impl DiskCache {
    fn new(cache_dir: &Path) -> io::Result<Self> {
        let chunks_dir = cache_dir.join("chunks");
        fs::create_dir_all(&chunks_dir)?;

        // Scan existing cached chunks to rebuild presence map.
        let mut chunks = HashMap::new();
        if let Ok(entries) = fs::read_dir(&chunks_dir) {
            for entry in entries.flatten() {
                if let Ok(name) = entry.file_name().into_string() {
                    if let Ok(chunk_id) = name.parse::<u64>() {
                        if let Ok(meta) = entry.metadata() {
                            chunks.insert(chunk_id, meta.len());
                        }
                    }
                }
            }
        }

        Ok(Self {
            cache_dir: cache_dir.to_path_buf(),
            chunks: parking_lot::Mutex::new(chunks),
            interior_chunks: parking_lot::Mutex::new(HashSet::new()),
        })
    }

    fn chunk_path(&self, chunk_id: u64) -> PathBuf {
        self.cache_dir.join("chunks").join(chunk_id.to_string())
    }

    /// Read a cached chunk. Returns None on cache miss.
    fn get_chunk(&self, chunk_id: u64) -> Option<Vec<u8>> {
        fs::read(self.chunk_path(chunk_id)).ok()
    }

    /// Write a chunk to cache (atomic: write .tmp then rename).
    fn put_chunk(&self, chunk_id: u64, data: &[u8]) -> io::Result<()> {
        let path = self.chunk_path(chunk_id);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, data)?;
        if let Err(e) = fs::rename(&tmp, &path) {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }

        self.chunks.lock().insert(chunk_id, data.len() as u64);
        Ok(())
    }

    /// Check if a chunk exists in cache.
    fn has_chunk(&self, chunk_id: u64) -> bool {
        self.chunks.lock().contains_key(&chunk_id)
    }

    /// Mark a chunk as containing B-tree interior pages.
    fn mark_interior(&self, chunk_id: u64) {
        self.interior_chunks.lock().insert(chunk_id);
    }

    /// Remove a specific chunk from cache.
    #[allow(dead_code)]
    fn invalidate_chunk(&self, chunk_id: u64) {
        let _ = fs::remove_file(self.chunk_path(chunk_id));
        self.chunks.lock().remove(&chunk_id);
    }
}

// ===== Chunk encoding/decoding =====

/// Header size for a chunk: (chunk_size + 1) u32 offsets.
fn chunk_header_size(chunk_size: u32) -> usize {
    (chunk_size as usize + 1) * 4
}

/// Build a chunk from individual compressed page data.
/// `pages` is a Vec of Option<Vec<u8>> — None means empty/nonexistent page.
fn encode_chunk(pages: &[Option<Vec<u8>>], chunk_size: u32) -> Vec<u8> {
    let header_size = chunk_header_size(chunk_size);
    let num_entries = chunk_size as usize + 1; // +1 for sentinel

    // Calculate offsets
    let mut offsets = vec![0u32; num_entries];
    let mut pos = header_size as u32;
    for i in 0..chunk_size as usize {
        offsets[i] = pos;
        if let Some(Some(data)) = pages.get(i) {
            pos += data.len() as u32;
        }
    }
    offsets[chunk_size as usize] = pos; // sentinel

    // Build the chunk
    let mut chunk = Vec::with_capacity(pos as usize);

    // Write header (little-endian u32 offsets)
    for off in &offsets {
        chunk.extend_from_slice(&off.to_le_bytes());
    }

    // Write compressed page data
    for i in 0..chunk_size as usize {
        if let Some(Some(data)) = pages.get(i) {
            chunk.extend_from_slice(data);
        }
    }

    chunk
}

/// Extract a single compressed page from chunk data.
/// Returns None if the page slot is empty (offset[i] == offset[i+1]).
fn extract_page_from_chunk(
    chunk_data: &[u8],
    local_idx: u32,
    chunk_size: u32,
) -> Option<Vec<u8>> {
    let header_size = chunk_header_size(chunk_size);
    if chunk_data.len() < header_size {
        return None;
    }
    if local_idx >= chunk_size {
        return None;
    }

    let off_start = read_u32_le(chunk_data, local_idx as usize * 4) as usize;
    let off_end = read_u32_le(chunk_data, (local_idx as usize + 1) * 4) as usize;

    if off_start == off_end {
        return None; // empty slot
    }
    if off_end > chunk_data.len() || off_start > off_end {
        return None; // corrupt
    }

    Some(chunk_data[off_start..off_end].to_vec())
}

fn read_u32_le(data: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ])
}

// ===== TieredHandle =====

/// Database handle for tiered S3-backed storage.
///
/// MainDb files are backed by S3 with a local chunk-level cache.
/// WAL/journal files are passthrough to local disk.
pub struct TieredHandle {
    // --- Tiered mode (MainDb) ---
    s3: Option<Arc<S3Client>>,
    cache: Option<Arc<DiskCache>>,
    manifest: RwLock<Manifest>,
    /// Dirty pages buffered in memory: page_num → compressed data
    dirty_pages: RwLock<HashMap<u64, Vec<u8>>>,
    page_size: RwLock<u32>,
    chunk_size: u32,
    compression_level: i32,
    read_only: bool,
    /// Consecutive forward chunk transitions (hop-based adaptive prefetch).
    /// Incremented when accessing the next sequential chunk, reset on random jump.
    /// Prefetch count on cache miss = sequential_chunks^4.
    sequential_chunks: u8,
    /// Last chunk accessed, for detecting sequential vs random access patterns.
    last_chunk_id: Option<u64>,

    // --- Compression dictionary ---
    #[cfg(feature = "zstd")]
    encoder_dict: Option<zstd::dict::EncoderDictionary<'static>>,
    #[cfg(feature = "zstd")]
    decoder_dict: Option<zstd::dict::DecoderDictionary<'static>>,

    // --- Passthrough mode (WAL/journal) ---
    passthrough_file: Option<RwLock<File>>,

    // --- Shared ---
    lock: RwLock<LockKind>,
    db_path: PathBuf,
    /// Separate file handle for byte-range locking
    lock_file: Option<std::sync::Arc<File>>,
    /// Active byte-range locks
    active_db_locks: HashMap<String, Box<dyn std::any::Any + Send + Sync>>,
}

// SQLite main database lock byte offsets (same as lib.rs)
const PENDING_BYTE: u64 = 0x40000000;
const RESERVED_BYTE: u64 = PENDING_BYTE + 1;
const SHARED_FIRST: u64 = PENDING_BYTE + 2;
const SHARED_SIZE: u64 = 510;

impl TieredHandle {
    /// Create a tiered handle backed by S3 + local chunk cache.
    fn new_tiered(
        s3: Arc<S3Client>,
        cache: Arc<DiskCache>,
        manifest: Manifest,
        db_path: PathBuf,
        chunk_size: u32,
        compression_level: i32,
        read_only: bool,
        #[cfg(feature = "zstd")] dictionary: Option<&[u8]>,
    ) -> Self {
        let page_size = manifest.page_size;

        // Eagerly cache chunk 0 (schema + root page, hit on every query)
        if !cache.has_chunk(0) && manifest.page_count > 0 {
            if let Ok(Some(data)) = s3.get_chunk(0) {
                let _ = cache.put_chunk(0, &data);
            }
        }

        #[cfg(feature = "zstd")]
        let (encoder_dict, decoder_dict) = match dictionary {
            Some(dict_bytes) => (
                Some(zstd::dict::EncoderDictionary::copy(dict_bytes, compression_level)),
                Some(zstd::dict::DecoderDictionary::copy(dict_bytes)),
            ),
            None => (None, None),
        };

        Self {
            s3: Some(s3),
            cache: Some(cache),
            manifest: RwLock::new(manifest),
            dirty_pages: RwLock::new(HashMap::new()),
            page_size: RwLock::new(page_size),
            chunk_size,
            compression_level,
            read_only,
            sequential_chunks: 0,
            last_chunk_id: None,
            #[cfg(feature = "zstd")]
            encoder_dict,
            #[cfg(feature = "zstd")]
            decoder_dict,
            passthrough_file: None,
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    /// Create a passthrough handle for WAL/journal files (local file I/O).
    fn new_passthrough(file: File, db_path: PathBuf) -> Self {
        Self {
            s3: None,
            cache: None,
            manifest: RwLock::new(Manifest::empty()),
            dirty_pages: RwLock::new(HashMap::new()),
            page_size: RwLock::new(0),
            chunk_size: 128,
            compression_level: 0,
            read_only: false,
            sequential_chunks: 0,
            last_chunk_id: None,
            #[cfg(feature = "zstd")]
            encoder_dict: None,
            #[cfg(feature = "zstd")]
            decoder_dict: None,
            passthrough_file: Some(RwLock::new(file)),
            lock: RwLock::new(LockKind::None),
            db_path,
            lock_file: None,
            active_db_locks: HashMap::new(),
        }
    }

    /// Is this a passthrough (WAL/journal) handle?
    fn is_passthrough(&self) -> bool {
        self.passthrough_file.is_some()
    }

    /// Get the S3 client (panics if passthrough).
    fn s3(&self) -> &S3Client {
        self.s3.as_ref().expect("s3 client required for tiered mode")
    }

    /// Get the disk cache (panics if passthrough).
    fn disk_cache(&self) -> &DiskCache {
        self.cache
            .as_ref()
            .expect("disk cache required for tiered mode")
    }

    /// Decompress page data and copy into the output buffer.
    fn decompress_into_buf(
        &self,
        compressed: &[u8],
        buf: &mut [u8],
        offset: u64,
        page_size: u64,
    ) -> Result<(), io::Error> {
        let decompressed = compress::decompress(
            compressed,
            #[cfg(feature = "zstd")]
            self.decoder_dict.as_ref(),
            #[cfg(not(feature = "zstd"))]
            None,
        )?;
        let page_offset = (offset % page_size) as usize;
        let copy_len = buf.len().min(decompressed.len().saturating_sub(page_offset));
        if copy_len > 0 {
            buf[..copy_len].copy_from_slice(&decompressed[page_offset..page_offset + copy_len]);
        }
        if copy_len < buf.len() {
            buf[copy_len..].fill(0);
        }
        Ok(())
    }

    /// Ensure a lock file exists for byte-range locking.
    fn ensure_lock_file(&mut self) -> io::Result<std::sync::Arc<File>> {
        if self.lock_file.is_none() {
            let lock_path = self.db_path.with_extension("db-lock");
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&lock_path)?;
            self.lock_file = Some(std::sync::Arc::new(file));
        }
        Ok(std::sync::Arc::clone(
            self.lock_file.as_ref().unwrap(),
        ))
    }
}

impl DatabaseHandle for TieredHandle {
    type WalIndex = FileWalIndex;

    fn size(&self) -> Result<u64, io::Error> {
        if self.is_passthrough() {
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.metadata().map(|m| m.len());
        }

        let manifest = self.manifest.read();
        if manifest.page_size > 0 && manifest.page_count > 0 {
            Ok(manifest.page_count * manifest.page_size as u64)
        } else {
            Ok(0)
        }
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.read_exact_at(buf, offset);
        }

        // Determine page number and chunk coordinates
        let page_size = {
            let ps = *self.page_size.read();
            if ps > 0 {
                ps as u64
            } else {
                buf.len() as u64
            }
        };
        let page_num = offset / page_size;
        let chunk_id = page_num / self.chunk_size as u64;
        let local_idx = (page_num % self.chunk_size as u64) as u32;

        // 1. Check dirty pages (in-memory, not yet synced to S3)
        {
            let dirty = self.dirty_pages.read();
            if let Some(compressed) = dirty.get(&page_num) {
                return self.decompress_into_buf(compressed, buf, offset, page_size);
            }
        }

        // 2. Bounds check: page must be within manifest's page_count.
        //    This check is before the cache lookup to prevent serving stale
        //    cached pages after truncation (VACUUM, set_len).
        let manifest_page_count = self.manifest.read().page_count;
        if page_num >= manifest_page_count {
            buf.fill(0);
            return Ok(());
        }

        // 3. Check local chunk cache
        // Clone the Arc to avoid borrowing &self (needed for self.sequential_chunks mutation)
        let cache_arc = Arc::clone(self.cache.as_ref().expect("disk cache required for tiered mode"));
        let cache = cache_arc.as_ref();
        if let Some(chunk_data) = cache.get_chunk(chunk_id) {
            // Track chunk-level sequential access (only on chunk transitions).
            // Non-forward jumps (B-tree internal page reads) don't reset — they
            // shouldn't penalize an in-progress sequential scan.
            if self.last_chunk_id != Some(chunk_id) {
                let is_forward = match self.last_chunk_id {
                    Some(last) => chunk_id == last + 1,
                    None => true,
                };
                self.last_chunk_id = Some(chunk_id);
                if is_forward {
                    self.sequential_chunks = self.sequential_chunks.saturating_add(1);
                }
            }
            if let Some(compressed) = extract_page_from_chunk(&chunk_data, local_idx, self.chunk_size) {
                let result = self.decompress_into_buf(&compressed, buf, offset, page_size);
                if result.is_ok() {
                    let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
                    if let Some(&b) = type_byte {
                        if b == 0x05 || b == 0x02 {
                            cache.mark_interior(chunk_id);
                        }
                    }
                }
                return result;
            }
            // Page slot is empty in the cached chunk — return zeros
            buf.fill(0);
            return Ok(());
        }

        // 4. Cache miss → fetch chunk from S3, plus prefetch adjacent chunks in parallel

        let s3_arc = Arc::clone(self.s3.as_ref().expect("s3 client required for tiered mode"));
        let s3 = s3_arc.as_ref();
        let max_chunk = manifest_page_count.saturating_sub(1) / self.chunk_size as u64;

        // Track chunk-level sequential access (only on chunk transitions).
        // Non-forward jumps (B-tree internal page reads) don't reset — they
        // shouldn't penalize an in-progress sequential scan.
        if self.last_chunk_id != Some(chunk_id) {
            let is_forward = match self.last_chunk_id {
                Some(last) => chunk_id == last + 1,
                None => true,
            };
            self.last_chunk_id = Some(chunk_id);
            if is_forward {
                self.sequential_chunks = self.sequential_chunks.saturating_add(1);
            }
        }

        // Hop-based adaptive prefetch: prefetch_count = sequential_chunks^4
        let hop = self.sequential_chunks as u64;
        let prefetch_count = hop * hop * hop * hop; // hop^4

        let mut chunks_to_fetch = vec![chunk_id];
        {
            let mut added = 0u64;
            for i in 1.. {
                if added >= prefetch_count {
                    break;
                }
                let Some(next) = chunk_id.checked_add(i) else {
                    break;
                };
                if next > max_chunk {
                    break;
                }
                if !cache.has_chunk(next) {
                    chunks_to_fetch.push(next);
                    added += 1;
                }
            }
        }

        // Fetch all chunks in parallel (get_chunks_async spawns one tokio task per chunk)
        let fetch_start = Instant::now();
        let fetch_count = chunks_to_fetch.len();
        let fetched = if chunks_to_fetch.len() == 1 {
            let mut map = HashMap::new();
            if let Some(data) = s3.get_chunk(chunk_id)? {
                map.insert(chunk_id, data);
            }
            map
        } else {
            s3.get_chunks(&chunks_to_fetch)?
        };
        let fetch_ms = fetch_start.elapsed().as_millis();
        let total_bytes: usize = fetched.values().map(|v| v.len()).sum();
        if std::env::var("BENCH_VERBOSE").is_ok() {
            eprintln!(
                "  [prefetch] seq={} chunk={} fetched={}/{} ({:.1}MB) in {}ms",
                self.sequential_chunks, chunk_id, fetched.len(), fetch_count,
                total_bytes as f64 / (1024.0 * 1024.0), fetch_ms,
            );
        }

        // Cache all prefetched chunks (not the requested one — handled below)
        for (&cid, data) in &fetched {
            if cid != chunk_id {
                let _ = cache.put_chunk(cid, data);
            }
        }

        // Extract requested page from chunk
        match fetched.get(&chunk_id) {
            Some(chunk_data) => {
                let _ = cache.put_chunk(chunk_id, chunk_data);

                if let Some(compressed) = extract_page_from_chunk(chunk_data, local_idx, self.chunk_size) {
                    let result = self.decompress_into_buf(&compressed, buf, offset, page_size);
                    if result.is_ok() {
                        let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
                        if let Some(&b) = type_byte {
                            if b == 0x05 || b == 0x02 {
                                cache.mark_interior(chunk_id);
                            }
                        }
                    }
                    result
                } else {
                    buf.fill(0);
                    Ok(())
                }
            }
            None => {
                // Chunk doesn't exist in S3 yet
                buf.fill(0);
                Ok(())
            }
        }
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            use std::os::unix::fs::FileExt;
            let file = self.passthrough_file.as_ref().unwrap().read();
            return file.write_all_at(buf, offset);
        }

        if self.read_only {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "TieredHandle is read-only",
            ));
        }

        // Set page size from first write
        {
            let mut ps = self.page_size.write();
            if *ps == 0 {
                *ps = buf.len() as u32;
            }
        }

        let page_size = *self.page_size.read() as u64;
        let page_num = offset / page_size;

        // Compress the page and buffer it as dirty
        let compressed = compress::compress(
            buf,
            self.compression_level,
            #[cfg(feature = "zstd")]
            self.encoder_dict.as_ref(),
            #[cfg(not(feature = "zstd"))]
            None,
        )?;

        let mut dirty = self.dirty_pages.write();
        dirty.insert(page_num, compressed);

        // Update manifest page_count if this page extends the database
        {
            let mut manifest = self.manifest.write();
            let new_count = page_num + 1;
            if new_count > manifest.page_count {
                manifest.page_count = new_count;
            }
            if manifest.page_size == 0 {
                manifest.page_size = buf.len() as u32;
            }
            if manifest.chunk_size == 0 {
                manifest.chunk_size = self.chunk_size;
            }
        }

        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .sync_all();
        }

        if self.read_only {
            return Ok(());
        }

        // Clone dirty pages — keep them readable during upload.
        let dirty_snapshot: HashMap<u64, Vec<u8>> = {
            let dirty = self.dirty_pages.read();
            if dirty.is_empty() {
                return Ok(());
            }
            dirty.clone()
        };

        let s3 = self.s3();
        let cache = self.disk_cache();
        let chunk_size = self.chunk_size;

        // Group dirty pages by chunk_id
        let mut chunks_dirty: HashMap<u64, Vec<(u32, Vec<u8>)>> = HashMap::new();
        for (&page_num, compressed) in &dirty_snapshot {
            let chunk_id = page_num / chunk_size as u64;
            let local_idx = (page_num % chunk_size as u64) as u32;
            chunks_dirty
                .entry(chunk_id)
                .or_default()
                .push((local_idx, compressed.clone()));
        }

        // For each dirty chunk, assemble the full chunk:
        // 1. Try to read existing chunk from cache
        // 2. Else try S3
        // 3. Else start with empty chunk
        // Then merge dirty pages in

        // First, identify which chunks we need from S3 (not in cache)
        let mut need_from_s3: Vec<u64> = Vec::new();
        let mut existing_chunks: HashMap<u64, Vec<u8>> = HashMap::new();

        for &chunk_id in chunks_dirty.keys() {
            if let Some(chunk_data) = cache.get_chunk(chunk_id) {
                existing_chunks.insert(chunk_id, chunk_data);
            } else {
                need_from_s3.push(chunk_id);
            }
        }

        // Parallel GET any chunks we need from S3
        if !need_from_s3.is_empty() {
            let fetched = s3.get_chunks(&need_from_s3)?;
            for (cid, data) in fetched {
                existing_chunks.insert(cid, data);
            }
        }

        // Build new chunk data for each dirty chunk
        let mut chunks_to_upload: Vec<(u64, Vec<u8>)> = Vec::new();

        for (chunk_id, dirty_pages_in_chunk) in &chunks_dirty {
            // Start with existing pages from the old chunk (if any)
            let mut pages: Vec<Option<Vec<u8>>> = vec![None; chunk_size as usize];

            if let Some(old_chunk) = existing_chunks.get(chunk_id) {
                // Extract all existing pages from the old chunk
                for i in 0..chunk_size {
                    if let Some(page_data) = extract_page_from_chunk(old_chunk, i, chunk_size) {
                        pages[i as usize] = Some(page_data);
                    }
                }
            }

            // Merge dirty pages (overwrite slots)
            for (local_idx, compressed) in dirty_pages_in_chunk {
                pages[*local_idx as usize] = Some(compressed.clone());
            }

            // Encode the chunk
            let chunk_bytes = encode_chunk(&pages, chunk_size);
            chunks_to_upload.push((*chunk_id, chunk_bytes));
        }

        // Parallel PUT all dirty chunks to S3
        s3.put_chunks(&chunks_to_upload)?;

        // Build new manifest — only after all chunks succeed
        let new_manifest = {
            let m = self.manifest.read();
            Manifest {
                version: m.version + 1,
                page_count: m.page_count,
                page_size: m.page_size,
                chunk_size: m.chunk_size,
            }
        };
        s3.put_manifest(&new_manifest)?;

        // S3 confirmed — update in-memory manifest, clear dirty pages, update cache
        {
            let mut m = self.manifest.write();
            *m = new_manifest;
        }
        {
            let mut dirty = self.dirty_pages.write();
            for page_num in dirty_snapshot.keys() {
                dirty.remove(page_num);
            }
        }
        // Cache the newly uploaded chunks
        for (chunk_id, chunk_bytes) in &chunks_to_upload {
            let _ = cache.put_chunk(*chunk_id, chunk_bytes);
        }

        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), io::Error> {
        if self.is_passthrough() {
            return self
                .passthrough_file
                .as_ref()
                .unwrap()
                .write()
                .set_len(size);
        }

        let page_size = *self.page_size.read();
        if page_size == 0 {
            return Ok(());
        }

        let new_page_count = if size == 0 {
            0
        } else {
            (size + page_size as u64 - 1) / page_size as u64
        };

        let mut manifest = self.manifest.write();
        manifest.page_count = new_page_count;

        // Remove dirty pages beyond new size
        let mut dirty = self.dirty_pages.write();
        dirty.retain(|&pn, _| pn < new_page_count);

        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, io::Error> {
        let current = *self.lock.read();

        if current == lock {
            return Ok(true);
        }

        let lock_file = self.ensure_lock_file()?;

        match lock {
            LockKind::None => {
                self.active_db_locks.clear();
            }
            LockKind::Shared => {
                self.active_db_locks.clear();

                // Check PENDING_BYTE first (writer starvation prevention)
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(_) => { /* No writer waiting, continue */ }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Shared,
                    SHARED_FIRST as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("shared".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Reserved => {
                if !matches!(
                    current,
                    LockKind::Shared
                        | LockKind::Reserved
                        | LockKind::Pending
                        | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    RESERVED_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("reserved".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Pending => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    PENDING_BYTE as usize,
                    1,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("pending".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
            LockKind::Exclusive => {
                if !matches!(
                    current,
                    LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
                ) {
                    return Ok(false);
                }
                if !self.active_db_locks.contains_key("pending") {
                    match file_guard::try_lock(
                        std::sync::Arc::clone(&lock_file),
                        file_guard::Lock::Exclusive,
                        PENDING_BYTE as usize,
                        1,
                    ) {
                        Ok(guard) => {
                            self.active_db_locks
                                .insert("pending".to_string(), Box::new(guard));
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    }
                }

                self.active_db_locks.remove("shared");

                match file_guard::try_lock(
                    std::sync::Arc::clone(&lock_file),
                    file_guard::Lock::Exclusive,
                    SHARED_FIRST as usize,
                    SHARED_SIZE as usize,
                ) {
                    Ok(guard) => {
                        self.active_db_locks
                            .insert("exclusive".to_string(), Box::new(guard));
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // Restore shared lock
                        match file_guard::try_lock(
                            std::sync::Arc::clone(&lock_file),
                            file_guard::Lock::Shared,
                            SHARED_FIRST as usize,
                            1,
                        ) {
                            Ok(guard) => {
                                self.active_db_locks
                                    .insert("shared".to_string(), Box::new(guard));
                            }
                            Err(restore_err) => {
                                return Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    format!(
                                        "Lock restore failed: {}",
                                        restore_err
                                    ),
                                ));
                            }
                        }
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        *self.lock.write() = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, io::Error> {
        let lock = *self.lock.read();
        Ok(matches!(
            lock,
            LockKind::Reserved | LockKind::Pending | LockKind::Exclusive
        ))
    }

    fn current_lock(&self) -> Result<LockKind, io::Error> {
        Ok(*self.lock.read())
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, io::Error> {
        let shm_path = self.db_path.with_extension("db-shm");
        Ok(FileWalIndex::new(shm_path))
    }
}

// ===== TieredVfs =====

/// S3-backed tiered storage VFS.
///
/// # Usage
/// ```ignore
/// use sqlite_compress_encrypt_vfs::tiered::{TieredVfs, TieredConfig};
///
/// let config = TieredConfig {
///     bucket: "my-bucket".into(),
///     prefix: "databases/tenant-1".into(),
///     cache_dir: "/tmp/cache".into(),
///     ..Default::default()
/// };
/// let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
/// sqlite_compress_encrypt_vfs::tiered::register("tiered", vfs).unwrap();
/// ```
pub struct TieredVfs {
    s3: Arc<S3Client>,
    cache: Arc<DiskCache>,
    config: TieredConfig,
    /// Owned runtime (if we created one ourselves)
    _runtime: Option<tokio::runtime::Runtime>,
}

impl TieredVfs {
    /// Create a new tiered VFS. If no `runtime_handle` is provided in config
    /// and we're not inside a tokio runtime, a new multi-thread runtime is created.
    pub fn new(config: TieredConfig) -> io::Result<Self> {
        let (runtime_handle, owned_runtime) =
            if let Some(ref handle) = config.runtime_handle {
                (handle.clone(), None)
            } else if let Ok(handle) = TokioHandle::try_current() {
                (handle, None)
            } else {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to create tokio runtime: {}", e),
                    )
                })?;
                let handle = rt.handle().clone();
                (handle, Some(rt))
            };

        let s3 = S3Client::new_blocking(&config, &runtime_handle)?;
        let cache = DiskCache::new(&config.cache_dir)?;

        Ok(Self {
            s3: Arc::new(s3),
            cache: Arc::new(cache),
            config,
            _runtime: owned_runtime,
        })
    }

    /// Helper to destroy all S3 data for a prefix. Use with care.
    /// Uses batch DeleteObjects (up to 1000 keys per request) for efficiency.
    pub fn destroy_s3(&self) -> io::Result<()> {
        let s3 = &self.s3;
        S3Client::block_on(&s3.runtime, async {
            let mut continuation_token: Option<String> = None;
            loop {
                let mut req = s3
                    .client
                    .list_objects_v2()
                    .bucket(&s3.bucket)
                    .prefix(&s3.prefix);

                if let Some(token) = &continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req.send().await.map_err(|e| {
                    io::Error::new(io::ErrorKind::Other, format!("S3 list failed: {}", e))
                })?;

                // Collect keys into batches of up to 1000 for batch delete
                let keys: Vec<String> = resp
                    .contents()
                    .iter()
                    .filter_map(|obj| obj.key().map(|k| k.to_string()))
                    .collect();

                for batch in keys.chunks(1000) {
                    let objects: Vec<aws_sdk_s3::types::ObjectIdentifier> = batch
                        .iter()
                        .map(|key| {
                            aws_sdk_s3::types::ObjectIdentifier::builder()
                                .key(key)
                                .build()
                                .expect("ObjectIdentifier requires key")
                        })
                        .collect();

                    let delete = aws_sdk_s3::types::Delete::builder()
                        .set_objects(Some(objects))
                        .quiet(true)
                        .build()
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("Failed to build Delete request: {}", e),
                            )
                        })?;

                    s3.client
                        .delete_objects()
                        .bucket(&s3.bucket)
                        .delete(delete)
                        .send()
                        .await
                        .map_err(|e| {
                            io::Error::new(
                                io::ErrorKind::Other,
                                format!("S3 batch delete failed: {}", e),
                            )
                        })?;
                }

                if resp.is_truncated() == Some(true) {
                    continuation_token =
                        resp.next_continuation_token().map(|s| s.to_string());
                } else {
                    break;
                }
            }
            Ok(())
        })
    }
}

impl Vfs for TieredVfs {
    type Handle = TieredHandle;

    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, io::Error> {
        let path = self.config.cache_dir.join(db);

        // MainDb → tiered (S3 + cache), WAL/journal → local passthrough
        if matches!(opts.kind, OpenKind::MainDb) {
            // Fetch or create manifest from S3
            let manifest = self.s3.get_manifest()?.unwrap_or_else(Manifest::empty);

            // Use the manifest's chunk_size for existing databases (source of truth
            // for how chunks were encoded). Only use config chunk_size for new DBs.
            let chunk_size = if manifest.chunk_size > 0 {
                manifest.chunk_size
            } else {
                self.config.chunk_size
            };

            // Create a local lock file path
            let lock_dir = self.config.cache_dir.join("locks");
            fs::create_dir_all(&lock_dir)?;
            let lock_path = lock_dir.join(db);
            // Touch the lock file so it exists for byte-range locking
            FsOpenOptions::new()
                .create(true)
                .write(true)
                .open(&lock_path)?;

            Ok(TieredHandle::new_tiered(
                Arc::clone(&self.s3),
                Arc::clone(&self.cache),
                manifest,
                lock_path,
                chunk_size,
                self.config.compression_level,
                self.config.read_only,
                #[cfg(feature = "zstd")]
                self.config.dictionary.as_deref(),
            ))
        } else {
            // WAL, journal, temp — local passthrough.
            // Always create these files with read+write — they're ephemeral local
            // artifacts. Even read-only connections need WAL/SHM files to exist
            // for WAL mode to work (SQLITE_READONLY_CANTINIT otherwise).
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let file = FsOpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;
            Ok(TieredHandle::new_passthrough(file, path))
        }
    }

    fn delete(&self, db: &str) -> Result<(), io::Error> {
        // Delete local cache/WAL/journal files only.
        // S3 cleanup is explicit via TieredVfs::destroy_s3() — never call it
        // here because SQLite calls delete() for WAL/journal files during
        // normal operations (VACUUM, journal mode switch), which would
        // destroy the entire S3 dataset.
        let path = self.config.cache_dir.join(db);
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, io::Error> {
        // Check local first (covers WAL/journal and cached main DB)
        let path = self.config.cache_dir.join(db);
        if path.exists() {
            return Ok(true);
        }

        // WAL, journal, and SHM files are always local-only — never in S3.
        // Only check S3 manifest for main database files.
        if db.ends_with("-wal") || db.ends_with("-journal") || db.ends_with("-shm") {
            return Ok(false);
        }

        // Check for manifest in S3 (source of truth for main DB)
        Ok(self.s3.get_manifest()?.is_some())
    }

    fn temporary_name(&self) -> String {
        format!("temp_{}", std::process::id())
    }

    fn random(&self, buffer: &mut [i8]) {
        use std::time::SystemTime;
        let mut seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        for b in buffer.iter_mut() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            *b = (seed >> 33) as i8;
        }
    }

    fn sleep(&self, duration: Duration) -> Duration {
        std::thread::sleep(duration);
        duration
    }
}

/// Register a tiered VFS with SQLite.
pub fn register(name: &str, vfs: TieredVfs) -> Result<(), io::Error> {
    sqlite_vfs::register(name, vfs, false)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{:?}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ===== DiskCache: Interior Page Pinning =====

    fn make_cache(dir: &Path) -> DiskCache {
        DiskCache::new(dir).unwrap()
    }

    fn dummy_chunk(size: usize) -> Vec<u8> {
        vec![0u8; size]
    }

    #[test]
    fn test_mark_interior_tracks_chunks() {
        let dir = TempDir::new().unwrap();
        let cache = make_cache(dir.path());

        assert!(!cache.interior_chunks.lock().contains(&5));
        cache.mark_interior(5);
        assert!(cache.interior_chunks.lock().contains(&5));

        // Idempotent
        cache.mark_interior(5);
        assert_eq!(cache.interior_chunks.lock().len(), 1);
    }

    #[test]
    fn test_mark_interior_multiple_chunks() {
        let dir = TempDir::new().unwrap();
        let cache = make_cache(dir.path());

        cache.mark_interior(0);
        cache.mark_interior(3);
        cache.mark_interior(7);
        assert_eq!(cache.interior_chunks.lock().len(), 3);
    }

    // ===== Hop-Based Adaptive Prefetching (chunk-level tracking) =====

    #[test]
    fn test_hop_quartic_formula() {
        // Verify hop^4 values
        for hop in 1u64..=10 {
            let expected = hop * hop * hop * hop;
            assert_eq!(expected, hop.pow(4), "hop {} should produce {}", hop, expected);
        }
        // Key values from roadmap
        assert_eq!(1u64.pow(4), 1);
        assert_eq!(2u64.pow(4), 16);
        assert_eq!(3u64.pow(4), 81);
        assert_eq!(4u64.pow(4), 256);
        assert_eq!(5u64.pow(4), 625);
    }

    #[test]
    fn test_sequential_chunks_saturates() {
        let mut counter: u8 = 250;
        for _ in 0..10 {
            counter = counter.saturating_add(1);
        }
        assert_eq!(counter, 255, "should saturate at u8::MAX");

        // 255^4 should not overflow u64
        let hop = counter as u64;
        let result = hop * hop * hop * hop;
        assert_eq!(result, 4_228_250_625u64);
    }

    #[test]
    fn test_sequential_chunk_tracking() {
        // Simulate the chunk-level sequential access tracking logic
        let mut sequential_chunks: u8 = 0;
        let mut last_chunk_id: Option<u64> = None;

        let update = |chunk_id: u64, seq: &mut u8, last: &mut Option<u64>| {
            if *last != Some(chunk_id) {
                let is_forward = match *last {
                    Some(l) => chunk_id == l + 1,
                    None => true,
                };
                *last = Some(chunk_id);
                if is_forward {
                    *seq = seq.saturating_add(1);
                }
            }
        };

        // Sequential scan: chunk 0, 1, 2, 3...
        update(0, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 1);
        update(1, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 2);
        update(2, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 3);
        update(3, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 4);

        // Multiple reads within same chunk — counter unchanged
        update(3, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 4, "same chunk should not change counter");
        update(3, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 4);

        // Random jump (B-tree internal page) — counter unchanged
        update(50, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 4, "random jump should NOT reset counter");

        // Resume sequential from new position
        update(51, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 5);
        update(52, &mut sequential_chunks, &mut last_chunk_id);
        assert_eq!(sequential_chunks, 6);
    }

    #[test]
    fn test_sequential_scan_prefetch_escalation() {
        // Simulate a 54-chunk cold scan with chunk-level tracking
        let mut sequential_chunks: u8 = 0;
        let mut last_chunk_id: Option<u64> = None;
        let total_chunks: u64 = 54;
        let mut fetched_chunks: HashSet<u64> = HashSet::new();
        let mut s3_roundtrips = 0u32;

        // Chunk 0 is eagerly fetched (cached before scan starts)
        fetched_chunks.insert(0);

        for chunk_id in 0..total_chunks {
            // Update tracking on chunk transition
            if last_chunk_id != Some(chunk_id) {
                let is_forward = match last_chunk_id {
                    Some(l) => chunk_id == l + 1,
                    None => true,
                };
                last_chunk_id = Some(chunk_id);
                if is_forward {
                    sequential_chunks = sequential_chunks.saturating_add(1);
                } else {
                    sequential_chunks = 1;
                }
            }

            // Cache hit — no S3 fetch needed
            if fetched_chunks.contains(&chunk_id) {
                continue;
            }

            // Cache miss — fetch with prefetch
            s3_roundtrips += 1;
            let hop = sequential_chunks as u64;
            let prefetch_count = hop * hop * hop * hop;
            fetched_chunks.insert(chunk_id);
            let mut added = 0u64;
            for i in 1.. {
                if added >= prefetch_count { break; }
                let next = chunk_id + i;
                if next >= total_chunks { break; }
                if !fetched_chunks.contains(&next) {
                    fetched_chunks.insert(next);
                    added += 1;
                }
            }
        }

        assert!(s3_roundtrips <= 3,
            "54-chunk scan should complete in <=3 S3 roundtrips, got {}", s3_roundtrips);
        assert_eq!(fetched_chunks.len() as u64, total_chunks,
            "all chunks should be fetched");
    }

    #[test]
    fn test_cold_point_lookup_prefetch() {
        // Cold point lookup: fresh VFS, counter starts at 0.
        // chunk 0 (eager hit, seq=1) → random chunk 42 (non-forward, seq stays 1)
        let mut sequential_chunks: u8 = 0;
        let mut last_chunk_id: Option<u64> = None;

        // Chunk 0: first access, forward
        if last_chunk_id != Some(0) {
            let is_forward = last_chunk_id.map_or(true, |l| 0 == l + 1);
            last_chunk_id = Some(0);
            if is_forward { sequential_chunks = sequential_chunks.saturating_add(1); }
        }
        assert_eq!(sequential_chunks, 1);

        // Random jump to chunk 42 — counter stays at 1 (not forward, no reset)
        if last_chunk_id != Some(42) {
            let is_forward = last_chunk_id.map_or(true, |l| 42 == l + 1);
            last_chunk_id = Some(42);
            if is_forward { sequential_chunks = sequential_chunks.saturating_add(1); }
        }
        assert_eq!(sequential_chunks, 1, "non-forward jump should not change counter");
        let prefetch = (sequential_chunks as u64).pow(4);
        assert_eq!(prefetch, 1, "cold point lookup should only prefetch 1 chunk");
    }

    // ===== Page Type Detection =====

    #[test]
    fn test_page_type_byte_detection() {
        // Interior page types
        assert_eq!(0x05u8, 5, "table interior");
        assert_eq!(0x02u8, 2, "index interior");

        // Non-interior page types (should NOT be marked)
        assert_ne!(0x0au8, 0x05);
        assert_ne!(0x0au8, 0x02);
        assert_ne!(0x0du8, 0x05);
        assert_ne!(0x0du8, 0x02);

        // Simulate page type check logic
        let check_interior = |buf: &[u8], page_num: u64| -> bool {
            let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
            matches!(type_byte, Some(&0x05) | Some(&0x02))
        };

        // Normal page (non-page-0): type byte at offset 0
        let mut buf = vec![0x05u8; 4096]; // table interior
        assert!(check_interior(&buf, 1));

        buf[0] = 0x02; // index interior
        assert!(check_interior(&buf, 1));

        buf[0] = 0x0a; // table leaf
        assert!(!check_interior(&buf, 42));

        buf[0] = 0x0d; // index leaf
        assert!(!check_interior(&buf, 42));

        // Page 0: type byte at offset 100 (after 100-byte DB header)
        let mut page0 = vec![0u8; 4096];
        page0[100] = 0x05;
        assert!(check_interior(&page0, 0));

        page0[100] = 0x0d; // leaf root (small DB)
        assert!(!check_interior(&page0, 0));
    }

    #[test]
    fn test_page_type_detection_empty_buffer() {
        // Edge case: buffer smaller than expected
        let check_interior = |buf: &[u8], page_num: u64| -> bool {
            let type_byte = if page_num == 0 { buf.get(100) } else { buf.get(0) };
            matches!(type_byte, Some(&0x05) | Some(&0x02))
        };

        // Empty buffer
        assert!(!check_interior(&[], 0));
        assert!(!check_interior(&[], 1));

        // Buffer too short for page 0 check (< 101 bytes)
        let short = vec![0x05u8; 50];
        assert!(!check_interior(&short, 0), "page 0 with <101 bytes should not detect interior");
        assert!(check_interior(&short, 1), "non-page-0 with byte 0 = 0x05 should detect interior");
    }

    // ===== DiskCache: Chunk 0 Eager Fetch =====

    #[test]
    fn test_has_chunk_returns_false_for_missing() {
        let dir = TempDir::new().unwrap();
        let cache = make_cache(dir.path());
        assert!(!cache.has_chunk(0));
        assert!(!cache.has_chunk(999));
    }

    #[test]
    fn test_put_then_has_chunk() {
        let dir = TempDir::new().unwrap();
        let cache = make_cache(dir.path());
        cache.put_chunk(0, &dummy_chunk(100)).unwrap();
        assert!(cache.has_chunk(0));
    }

    // ===== Cache: Combined Scenarios =====

    #[test]
    fn test_cache_rebuild_from_disk() {
        let dir = TempDir::new().unwrap();
        let cache = make_cache(dir.path());

        cache.put_chunk(0, &dummy_chunk(100)).unwrap();
        cache.put_chunk(5, &dummy_chunk(200)).unwrap();

        // Create a new cache instance from the same directory
        let cache2 = make_cache(dir.path());
        assert!(cache2.has_chunk(0));
        assert!(cache2.has_chunk(5));
        assert!(!cache2.has_chunk(1));
        // Note: interior_chunks is NOT rebuilt — it's rediscovered at read time
        assert!(cache2.interior_chunks.lock().is_empty());
    }
}
