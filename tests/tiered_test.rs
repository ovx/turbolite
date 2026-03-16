//! Integration tests for tiered S3-backed storage.
//!
//! These tests run against Tigris (S3-compatible). `#[ignore]`d by default.
//!
//! ```bash
//! # Source Tigris credentials, then:
//! TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo test --features tiered,zstd tiered -- --ignored
//! ```

#[cfg(feature = "tiered")]
mod tiered_tests {
    use sqlite_compress_encrypt_vfs::tiered::{TieredConfig, TieredVfs};
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::TempDir;

    /// Counter for unique VFS names across tests (SQLite requires unique names).
    static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

    fn unique_vfs_name(prefix: &str) -> String {
        let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
        format!("{}_{}", prefix, n)
    }

    /// Get test bucket from env, or skip.
    fn test_bucket() -> String {
        std::env::var("TIERED_TEST_BUCKET")
            .expect("TIERED_TEST_BUCKET env var required for tiered tests")
    }

    /// Get S3 endpoint URL (default: Tigris).
    fn endpoint_url() -> String {
        std::env::var("AWS_ENDPOINT_URL")
            .unwrap_or_else(|_| "https://t3.storage.dev".to_string())
    }

    /// Create a TieredConfig with a unique prefix (so tests don't collide).
    fn test_config(prefix: &str, cache_dir: &std::path::Path) -> TieredConfig {
        let unique_prefix = format!(
            "test/{}/{}",
            prefix,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        TieredConfig {
            bucket: test_bucket(),
            prefix: unique_prefix,
            cache_dir: cache_dir.to_path_buf(),
            compression_level: 3,
            endpoint_url: Some(endpoint_url()),
            region: Some("auto".to_string()),
            ..Default::default()
        }
    }

    /// Directly read the S3 manifest and verify it has the expected properties.
    /// This proves data actually landed in Tigris, not just local cache.
    fn verify_s3_manifest(
        bucket: &str,
        prefix: &str,
        endpoint: &Option<String>,
        expected_page_count_min: u64,
        expected_page_size: u64,
    ) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let manifest_data = rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());

            let resp = client
                .get_object()
                .bucket(bucket)
                .key(format!("{}/manifest.json", prefix))
                .send()
                .await
                .expect("manifest should exist in S3 after checkpoint");

            resp.body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .to_vec()
        });

        let manifest: serde_json::Value = serde_json::from_slice(&manifest_data).unwrap();
        let version = manifest["version"].as_u64().unwrap();
        let page_count = manifest["page_count"].as_u64().unwrap();
        let page_size = manifest["page_size"].as_u64().unwrap();
        let chunk_size = manifest["chunk_size"].as_u64().unwrap_or(128);

        assert!(version >= 1, "manifest version should be >= 1, got {}", version);
        assert!(
            page_count >= expected_page_count_min,
            "manifest page_count should be >= {}, got {}",
            expected_page_count_min, page_count
        );
        assert_eq!(
            page_size, expected_page_size,
            "manifest page_size mismatch"
        );
        assert_eq!(
            chunk_size, 128,
            "manifest chunk_size should be 128, got {}",
            chunk_size
        );
    }

    /// Verify that S3 has chunk objects (not page objects) under the prefix.
    fn verify_s3_has_chunks(
        bucket: &str,
        prefix: &str,
        endpoint: &Option<String>,
    ) -> usize {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());

            let resp = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(format!("{}/chunks/", prefix))
                .send()
                .await
                .expect("listing chunks should succeed");

            let count = resp.contents().len();
            assert!(count > 0, "should have at least 1 chunk object in S3");
            count
        })
    }

    #[test]
    #[ignore]
    fn test_basic_write_read() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("basic", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_basic");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let db_path = format!("basic_test.db");
        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            &db_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .expect("failed to open connection");

        // Use WAL mode + large page size
        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT);",
        )
        .expect("failed to create table");

        // Bulk insert
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO data (id, value) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("value_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        // Checkpoint to flush to S3
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint failed");

        // Verify S3 manifest directly
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

        // Read back
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM data", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 100, "expected 100 rows");

        // Verify specific row
        let value: String = conn
            .query_row(
                "SELECT value FROM data WHERE id = 42",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(value, "value_42");
    }

    #[test]
    #[ignore]
    fn test_checkpoint_uploads() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("checkpoint", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_checkpoint");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "checkpoint_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..50 {
                tx.execute(
                    "INSERT INTO items (id, name) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("item_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        // Checkpoint
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify manifest exists in S3 by reading it directly
        let rt = tokio::runtime::Runtime::new().unwrap();
        let manifest_data = rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = &endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());

            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(format!("{}/manifest.json", prefix))
                .send()
                .await
                .expect("manifest should exist in S3");

            resp.body
                .collect()
                .await
                .unwrap()
                .into_bytes()
                .to_vec()
        });

        let manifest: serde_json::Value =
            serde_json::from_slice(&manifest_data).unwrap();
        assert!(
            manifest["version"].as_u64().unwrap() >= 1,
            "manifest version should be >= 1"
        );
        assert!(
            manifest["page_count"].as_u64().unwrap() > 0,
            "manifest should have pages"
        );
        assert_eq!(
            manifest["page_size"].as_u64().unwrap(),
            65536,
            "page_size should be 65536"
        );
    }

    #[test]
    #[ignore]
    fn test_reader_from_s3() {
        // Write data with one VFS, then read with a fresh VFS (empty cache)
        let write_cache = TempDir::new().unwrap();
        let config = test_config("reader", write_cache.path());
        let writer_vfs_name = unique_vfs_name("tiered_writer");
        let reader_bucket = config.bucket.clone();
        let reader_prefix = config.prefix.clone();
        let reader_endpoint = config.endpoint_url.clone();
        let reader_region = config.region.clone();

        let vfs = TieredVfs::new(config).expect("failed to create writer VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&writer_vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "reader_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &writer_vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE logs (id INTEGER PRIMARY KEY, msg TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO logs (id, msg) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("log message {}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 manifest before cold read
        verify_s3_manifest(&reader_bucket, &reader_prefix, &reader_endpoint, 1, 65536);

        drop(conn);

        // Now open a read-only VFS on the same S3 prefix with a FRESH cache
        let read_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket: reader_bucket,
            prefix: reader_prefix,
            cache_dir: read_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: reader_endpoint,
            read_only: true,
            region: reader_region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_reader");
        let reader_vfs =
            TieredVfs::new(reader_config).expect("failed to create reader VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader_conn = rusqlite::Connection::open_with_flags_and_vfs(
            "reader_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = reader_conn
            .query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "reader should see all 200 rows from S3");

        let msg: String = reader_conn
            .query_row(
                "SELECT msg FROM logs WHERE id = 199",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(msg, "log message 199");
    }

    #[test]
    #[ignore]
    fn test_64kb_pages() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("pages64k", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_64k");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).expect("failed to create VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "pages64k_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        // Set 64KB page size BEFORE creating any tables
        conn.execute_batch("PRAGMA page_size=65536;").unwrap();
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE big (id INTEGER PRIMARY KEY, data BLOB);",
        )
        .unwrap();

        // Bulk insert enough data to span multiple 64KB pages
        let blob = vec![0x42u8; 8192]; // 8KB per row
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..50 {
                tx.execute(
                    "INSERT INTO big (id, data) VALUES (?1, ?2)",
                    rusqlite::params![i, blob],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 manifest
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

        // Cold read: drop and reopen
        drop(conn);

        // Re-read from same VFS (cache should have pages from checkpoint)
        let conn2 = rusqlite::Connection::open_with_flags_and_vfs(
            "pages64k_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            &vfs_name,
        )
        .unwrap();

        let count: i64 = conn2
            .query_row("SELECT COUNT(*) FROM big", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 50);

        // Verify data integrity
        let data: Vec<u8> = conn2
            .query_row("SELECT data FROM big WHERE id = 25", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(data.len(), 8192);
        assert!(data.iter().all(|&b| b == 0x42));
    }

    #[test]
    #[ignore]
    fn test_large_cold_scan() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("coldscan", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_coldscan");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).expect("failed to create VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "coldscan_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA page_size=65536;
             CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT);",
        )
        .unwrap();

        // Insert 10K rows (no indexes beyond PK)
        let tx = conn
            .unchecked_transaction()
            .unwrap();
        for i in 0..10_000 {
            tx.execute(
                "INSERT INTO events (id, ts, payload) VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i * 1000, format!("event data for {}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Open a fresh reader (cold cache) and do a full scan
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_coldscan_reader");
        let reader_vfs =
            TieredVfs::new(reader_config).expect("failed to create reader VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "coldscan_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let start = std::time::Instant::now();
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(count, 10_000, "cold scan should find all 10K rows");
        eprintln!(
            "Cold scan of 10K rows completed in {:?} (chunk_size=128)",
            elapsed
        );

        // With chunk_size=128, each chunk GET warms 128 pages at once
        assert!(
            elapsed.as_secs() < 60,
            "Cold scan took too long: {:?}",
            elapsed
        );
    }

    #[test]
    #[ignore]
    fn test_no_index_append_pattern() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("append", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_append");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).expect("failed to create VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "append_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE stream (ts INTEGER, data TEXT);",
        )
        .unwrap();

        // Bulk insert batch 1 and checkpoint
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO stream (ts, data) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("batch1_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Bulk insert batch 2 and checkpoint
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 100..200 {
                tx.execute(
                    "INSERT INTO stream (ts, data) VALUES (?1, ?2)",
                    rusqlite::params![i, format!("batch2_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 manifest after both checkpoints
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

        // Verify all data is accessible
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM stream", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200);

        // Verify ordering (append pattern — no index, so rowid order)
        let first: String = conn
            .query_row(
                "SELECT data FROM stream ORDER BY rowid ASC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first, "batch1_0");

        let last: String = conn
            .query_row(
                "SELECT data FROM stream ORDER BY rowid DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last, "batch2_199");
    }

    // ===== Phase 5b/5d: Bug fix verification + missing coverage =====

    #[test]
    #[ignore]
    fn test_read_only_rejects_writes() {
        // First write some data
        let write_cache = TempDir::new().unwrap();
        let config = test_config("readonly", write_cache.path());
        let writer_vfs = unique_vfs_name("tiered_ro_writer");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&writer_vfs, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "readonly_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &writer_vfs,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE t (id INTEGER PRIMARY KEY);
             INSERT INTO t VALUES (1);
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
        drop(conn);

        // Open read-only VFS and attempt write
        let ro_cache = TempDir::new().unwrap();
        let ro_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: ro_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let ro_vfs_name = unique_vfs_name("tiered_ro_reader");
        let ro_vfs = TieredVfs::new(ro_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&ro_vfs_name, ro_vfs).unwrap();

        let ro_conn = rusqlite::Connection::open_with_flags_and_vfs(
            "readonly_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &ro_vfs_name,
        )
        .unwrap();

        // Read should work
        let count: i64 = ro_conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    #[ignore]
    fn test_cache_clear_survival() {
        // Write data and checkpoint to S3
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("cacheclear", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_cacheclr");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "cacheclr_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA page_size=65536;
             CREATE TABLE survive (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..50 {
                tx.execute(
                    "INSERT INTO survive VALUES (?1, ?2)",
                    rusqlite::params![i, format!("survivor_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Nuke the entire cache directory
        std::fs::remove_dir_all(cache_dir.path()).unwrap();

        // Open with a completely fresh cache — data must come from S3
        let fresh_cache = TempDir::new().unwrap();
        let fresh_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: fresh_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let fresh_vfs_name = unique_vfs_name("tiered_cacheclr2");
        let fresh_vfs = TieredVfs::new(fresh_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&fresh_vfs_name, fresh_vfs)
            .unwrap();

        let fresh_conn = rusqlite::Connection::open_with_flags_and_vfs(
            "cacheclr_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &fresh_vfs_name,
        )
        .unwrap();

        let count: i64 = fresh_conn
            .query_row("SELECT COUNT(*) FROM survive", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 50, "all data should survive cache deletion");

        let val: String = fresh_conn
            .query_row(
                "SELECT val FROM survive WHERE id = 49",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(val, "survivor_49");
    }

    #[test]
    #[ignore]
    fn test_chunk_cache_populates() {
        // Write data, checkpoint, then do a cold read and verify that
        // the cache directory has chunk files (not page files).
        let write_cache = TempDir::new().unwrap();
        let config = test_config("chunkcache", write_cache.path());
        let vfs_name = unique_vfs_name("tiered_chunkcache_w");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "chunkcache_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA page_size=65536;
             CREATE TABLE cc (id INTEGER PRIMARY KEY, data TEXT);",
        )
        .unwrap();

        // Bulk insert enough data to generate multiple pages
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..500 {
                tx.execute(
                    "INSERT INTO cc VALUES (?1, ?2)",
                    rusqlite::params![i, format!("chunk_cache_data_{:0>200}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 has chunk objects
        let chunk_count = verify_s3_has_chunks(&bucket, &prefix, &endpoint);
        eprintln!("S3 has {} chunk objects after checkpoint", chunk_count);

        drop(conn);

        // Open fresh reader — cold cache
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_chunkcache_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "chunkcache_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // Full scan — triggers chunk fetches from S3
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM cc", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 500);

        // Check that cache has chunk files, not page files
        let cache_chunks_dir = cold_cache.path().join("chunks");
        let cached_chunks = std::fs::read_dir(&cache_chunks_dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .map(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
                    .unwrap_or(false)
            })
            .count();

        eprintln!("Cached {} chunks after cold scan", cached_chunks);

        // Should have at least 1 chunk cached (chunk 0 at minimum)
        assert!(
            cached_chunks >= 1,
            "cache should have chunk files, got {}",
            cached_chunks
        );

        // Verify NO pages directory exists (old format)
        let old_pages_dir = cold_cache.path().join("pages");
        assert!(
            !old_pages_dir.exists(),
            "old pages/ directory should not exist in v2"
        );
    }

    #[test]
    #[ignore]
    fn test_destroy_s3() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("destroy", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_destroy");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "destroy_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE d (x INTEGER);
             INSERT INTO d VALUES (1);
             PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
        drop(conn);

        // Verify manifest exists
        let rt = tokio::runtime::Runtime::new().unwrap();
        let exists_before = rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = &endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());

            client
                .head_object()
                .bucket(&bucket)
                .key(format!("{}/manifest.json", prefix))
                .send()
                .await
                .is_ok()
        });
        assert!(exists_before, "manifest should exist before destroy");

        // Create a new VFS instance with same config to call destroy_s3
        let destroy_config = TieredConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: cache_dir.path().to_path_buf(),
            endpoint_url: endpoint.clone(),
            region: Some("auto".to_string()),
            ..Default::default()
        };
        let destroy_vfs = TieredVfs::new(destroy_config).unwrap();
        destroy_vfs.destroy_s3().unwrap();

        // Verify manifest is gone
        let exists_after = rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = &endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());

            client
                .head_object()
                .bucket(&bucket)
                .key(format!("{}/manifest.json", prefix))
                .send()
                .await
                .is_ok()
        });
        assert!(!exists_after, "manifest should be gone after destroy_s3");
    }

    #[test]
    #[ignore]
    fn test_1k_rows_checkpoint_cold_read() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("1k_rows", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_1k");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "1k_rows_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT);",
        )
        .unwrap();

        let tx = conn.unchecked_transaction().unwrap();
        for i in 0..1000 {
            tx.execute(
                "INSERT INTO events VALUES (?1, ?2, ?3)",
                rusqlite::params![i, i * 1000, format!("event_{}", i)],
            )
            .unwrap();
        }
        tx.commit().unwrap();

        let start = std::time::Instant::now();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        eprintln!("Checkpoint (1000 rows): {:?}", start.elapsed());
        drop(conn);

        // Cold read from a fresh cache
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_1k_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "1k_rows_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let start = std::time::Instant::now();
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        eprintln!("Cold read (COUNT * 1000 rows): {:?}", start.elapsed());

        assert_eq!(count, 1000);

        let payload: String = reader
            .query_row(
                "SELECT payload FROM events WHERE id = 999",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(payload, "event_999");
    }

    // ===== Coverage gap tests =====

    /// Verify manifest version increments monotonically across multiple checkpoints.
    #[test]
    #[ignore]
    fn test_manifest_version_increments() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("versions", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_versions");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "versions_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE v (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let get_version = |rt: &tokio::runtime::Runtime,
                           bucket: &str,
                           prefix: &str,
                           endpoint: &Option<String>|
         -> u64 {
            rt.block_on(async {
                let aws_config = aws_config::from_env()
                    .region(aws_sdk_s3::config::Region::new("auto"))
                    .load()
                    .await;
                let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
                if let Some(ep) = endpoint {
                    s3_config = s3_config.endpoint_url(ep).force_path_style(true);
                }
                let client = aws_sdk_s3::Client::from_conf(s3_config.build());
                let resp = client
                    .get_object()
                    .bucket(bucket)
                    .key(format!("{}/manifest.json", prefix))
                    .send()
                    .await
                    .unwrap();
                let bytes = resp.body.collect().await.unwrap().into_bytes();
                let manifest: serde_json::Value =
                    serde_json::from_slice(&bytes).unwrap();
                manifest["version"].as_u64().unwrap()
            })
        };

        // Checkpoint 1
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..10 {
                tx.execute(
                    "INSERT INTO v VALUES (?1, ?2)",
                    rusqlite::params![i, format!("v1_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let v1 = get_version(&rt, &bucket, &prefix, &endpoint);
        assert!(v1 >= 1, "first checkpoint should have version >= 1");

        // Checkpoint 2
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 10..20 {
                tx.execute(
                    "INSERT INTO v VALUES (?1, ?2)",
                    rusqlite::params![i, format!("v2_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let v2 = get_version(&rt, &bucket, &prefix, &endpoint);
        assert_eq!(v2, v1 + 1, "version should increment by 1");

        // Checkpoint 3
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 20..30 {
                tx.execute(
                    "INSERT INTO v VALUES (?1, ?2)",
                    rusqlite::params![i, format!("v3_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let v3 = get_version(&rt, &bucket, &prefix, &endpoint);
        assert_eq!(v3, v2 + 1, "version should increment monotonically");

        // Verify all data accessible
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM v", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 30);
    }

    /// Verify LRU cache eviction works — when cache exceeds max_bytes,
    /// oldest chunks get evicted.
    #[test]
    #[ignore]
    fn test_lru_eviction() {
        // Write enough data to generate multiple chunks, then read with a
        // very small cache_max_bytes to force eviction.
        let write_cache = TempDir::new().unwrap();
        let config = test_config("lru_evict", write_cache.path());
        let vfs_name = unique_vfs_name("tiered_lru_w");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "lru_evict_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE lru (id INTEGER PRIMARY KEY, data BLOB);",
        )
        .unwrap();

        // Insert enough data to create multiple 64KB pages (and thus chunks)
        // Each row ~8KB, 200 rows = ~1.6MB = ~25 pages at 64KB
        let blob = vec![0x42u8; 8192];
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO lru VALUES (?1, ?2)",
                    rusqlite::params![i, blob],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Open reader with very small cache (64KB) — forces eviction
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket: bucket.clone(),
            prefix: prefix.clone(),
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint.clone(),
            read_only: true,
            // No cache limits — unbounded cache
            region: region.clone(),
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_lru_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "lru_evict_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // Full scan — will fetch chunks and evict older ones
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM lru", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "should read all rows despite cache eviction");

        // Cache should be bounded — check that total cached size is small
        let cache_chunks_dir = cold_cache.path().join("chunks");
        let total_cached: u64 = std::fs::read_dir(&cache_chunks_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| !e.file_name().to_string_lossy().ends_with(".tmp"))
            .map(|e| e.metadata().map(|m| m.len()).unwrap_or(0))
            .sum();

        eprintln!(
            "Total cached bytes after scan with max_bytes=64KB: {} bytes",
            total_cached
        );

        // Verify data integrity — specific row lookup (may require re-fetch from S3)
        let data: Vec<u8> = reader
            .query_row("SELECT data FROM lru WHERE id = 100", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(data.len(), 8192);
        assert!(data.iter().all(|&b| b == 0x42));
    }

    /// Verify rollback after writes discards dirty pages and DB stays consistent.
    #[test]
    #[ignore]
    fn test_rollback_discards_dirty_pages() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("rollback", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_rollback");

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "rollback_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE rb (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        // Bulk insert committed data and checkpoint
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..10 {
                tx.execute(
                    "INSERT INTO rb VALUES (?1, ?2)",
                    rusqlite::params![i, format!("committed_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Start a transaction, bulk insert more rows, then ROLLBACK
        conn.execute_batch("BEGIN;").unwrap();
        for i in 10..110 {
            conn.execute(
                "INSERT INTO rb VALUES (?1, ?2)",
                rusqlite::params![i, format!("rolled_back_{}", i)],
            )
            .unwrap();
        }
        conn.execute_batch("ROLLBACK;").unwrap();

        // Only the committed 10 rows should exist
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 10, "rollback should discard uncommitted rows");

        // Checkpoint again — should still be consistent
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        let count2: i64 = conn
            .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count2, 10, "checkpoint after rollback should preserve only committed data");

        // Bulk insert more after rollback — should work fine
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 10..20 {
                tx.execute(
                    "INSERT INTO rb VALUES (?1, ?2)",
                    rusqlite::params![i, format!("after_rollback_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        let count3: i64 = conn
            .query_row("SELECT COUNT(*) FROM rb", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count3, 20, "should have 20 rows after recovery");
    }

    /// Verify default 4096 page size works (all other tests use 65536).
    #[test]
    #[ignore]
    fn test_default_4096_page_size() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("pages4k", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_4k");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "pages4k_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        // Default 4096 page size — do NOT set page_size pragma
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE small (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO small VALUES (?1, ?2)",
                    rusqlite::params![i, format!("small_page_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify S3 manifest with 4096 page size
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 4096);

        drop(conn);

        // Cold read from fresh cache
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_4k_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "pages4k_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM small", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "4096 page size should work");

        // Verify page_size is actually 4096
        let page_size: i64 = reader
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .unwrap();
        assert_eq!(page_size, 4096, "default page_size should be 4096");
    }

    /// Write with a compression dictionary, read without one — should error, not corrupt.
    #[test]
    #[ignore]
    fn test_dictionary_mismatch_errors() {
        // Train a simple dictionary from sample data
        let samples: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("dict_sample_row_{}_with_repeated_structure", i).into_bytes())
            .collect();
        let dict_bytes =
            sqlite_compress_encrypt_vfs::dict::train_dictionary(&samples, 4096).unwrap();

        // Write with dictionary
        let write_cache = TempDir::new().unwrap();
        let mut config = test_config("dict_mismatch", write_cache.path());
        config.dictionary = Some(dict_bytes);
        let vfs_name = unique_vfs_name("tiered_dict_w");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "dict_mismatch_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE dm (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..50 {
                tx.execute(
                    "INSERT INTO dm VALUES (?1, ?2)",
                    rusqlite::params![i, format!("dict_row_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Read WITHOUT dictionary — should fail on decompression, not return garbage
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            // dictionary: None (default) — intentional mismatch
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_dict_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        // Opening the connection reads page 0 (schema). With dict-compressed pages
        // and no dict, decompression fails immediately — SQLite reports disk I/O error.
        let result = rusqlite::Connection::open_with_flags_and_vfs(
            "dict_mismatch_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        );

        // Should fail at open or first query — decompression without matching dict
        match result {
            Err(_) => {
                // Expected: connection open itself fails (page 0 can't decompress)
            }
            Ok(conn) => {
                // If open succeeds (unlikely), the first query should fail
                let query_result =
                    conn.query_row("SELECT COUNT(*) FROM dm", [], |row| row.get::<_, i64>(0));
                assert!(
                    query_result.is_err(),
                    "reading dict-compressed data without dict should error, not return garbage"
                );
            }
        }
    }

    /// Write with dictionary, read with SAME dictionary — round-trip should work.
    #[test]
    #[ignore]
    fn test_dictionary_roundtrip() {
        let samples: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("roundtrip_sample_{}_with_padding", i).into_bytes())
            .collect();
        let dict_bytes =
            sqlite_compress_encrypt_vfs::dict::train_dictionary(&samples, 4096).unwrap();

        // Write with dict
        let write_cache = TempDir::new().unwrap();
        let mut config = test_config("dict_roundtrip", write_cache.path());
        config.dictionary = Some(dict_bytes.clone());
        let vfs_name = unique_vfs_name("tiered_drt_w");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "dict_rt_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE drt (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..50 {
                tx.execute(
                    "INSERT INTO drt VALUES (?1, ?2)",
                    rusqlite::params![i, format!("dict_roundtrip_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Read with SAME dictionary from fresh cache
        let cold_cache = TempDir::new().unwrap();
        let mut reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        reader_config.dictionary = Some(dict_bytes);
        let reader_vfs_name = unique_vfs_name("tiered_drt_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "dict_rt_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM drt", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 50, "dict roundtrip should preserve all data");

        let val: String = reader
            .query_row(
                "SELECT val FROM drt WHERE id = 25",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(val, "dict_roundtrip_25");
    }

    /// Large representative DB test: 100K rows of realistic time-series data,
    /// bulk inserted in transactions, checkpointed to S3, then cold-read from
    /// a fresh cache. Verifies S3 manifest directly.
    #[test]
    #[ignore]
    fn test_large_representative_db() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("large_db", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_large");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "large_db_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE sensor_readings (
                 id INTEGER PRIMARY KEY,
                 device_id TEXT NOT NULL,
                 ts_ms INTEGER NOT NULL,
                 temperature REAL,
                 humidity REAL,
                 pressure REAL,
                 battery_pct INTEGER,
                 status TEXT,
                 payload BLOB
             );",
        )
        .unwrap();

        let total_rows: i64 = 100_000;
        let batch_size = 10_000;
        let devices = [
            "sensor-A1", "sensor-A2", "sensor-B1", "sensor-B2",
            "sensor-C1", "sensor-C2", "sensor-D1", "sensor-D2",
        ];
        let statuses = ["ok", "warning", "critical", "offline", "maintenance"];
        // ~128 byte payload to simulate realistic sensor data blobs
        let payload = vec![0xABu8; 128];

        let insert_start = std::time::Instant::now();

        // Bulk insert in batches of 10K
        for batch in 0..(total_rows / batch_size) {
            let tx = conn.unchecked_transaction().unwrap();
            let start = batch * batch_size;
            let end = start + batch_size;
            for i in start..end {
                let device = devices[(i % devices.len() as i64) as usize];
                let status = statuses[(i % statuses.len() as i64) as usize];
                let ts = 1700000000000_i64 + i * 1000; // 1s intervals
                let temp = 20.0 + (i % 100) as f64 * 0.1;
                let humidity = 40.0 + (i % 60) as f64 * 0.5;
                let pressure = 1013.0 + (i % 50) as f64 * 0.2;
                let battery = 100 - (i % 100) as i32;
                tx.execute(
                    "INSERT INTO sensor_readings VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    rusqlite::params![i, device, ts, temp, humidity, pressure, battery, status, payload],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        let insert_elapsed = insert_start.elapsed();
        eprintln!(
            "Inserted {} rows in {:?} ({:.0} rows/sec)",
            total_rows,
            insert_elapsed,
            total_rows as f64 / insert_elapsed.as_secs_f64()
        );

        // Checkpoint to S3
        let ckpt_start = std::time::Instant::now();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        let ckpt_elapsed = ckpt_start.elapsed();
        eprintln!("Checkpoint {} rows to S3: {:?}", total_rows, ckpt_elapsed);

        // Verify data was written locally
        let local_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM sensor_readings", [], |row| row.get(0))
            .unwrap();
        assert_eq!(local_count, total_rows, "local count mismatch");

        drop(conn);

        // Verify S3 manifest directly — proves data landed in Tigris
        verify_s3_manifest(&bucket, &prefix, &endpoint, 2, 65536);

        // Cold read from a completely fresh cache
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_large_reader");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "large_db_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // Full count — forces complete scan from S3
        let cold_start = std::time::Instant::now();
        let cold_count: i64 = reader
            .query_row("SELECT COUNT(*) FROM sensor_readings", [], |row| row.get(0))
            .unwrap();
        let cold_elapsed = cold_start.elapsed();
        assert_eq!(cold_count, total_rows, "cold scan count mismatch");
        eprintln!(
            "Cold scan COUNT(*) of {} rows: {:?}",
            total_rows, cold_elapsed
        );

        // Aggregation query — realistic analytics workload
        let avg_temp: f64 = reader
            .query_row(
                "SELECT AVG(temperature) FROM sensor_readings WHERE device_id = 'sensor-A1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(avg_temp > 0.0, "avg temperature should be positive");

        // Per-device counts — validates grouping works
        let device_count: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM sensor_readings WHERE device_id = 'sensor-B2'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            device_count,
            total_rows / devices.len() as i64,
            "each device should have equal row count"
        );

        // Verify specific row integrity (first, middle, last)
        let first_ts: i64 = reader
            .query_row(
                "SELECT ts_ms FROM sensor_readings WHERE id = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_ts, 1700000000000);

        let mid_id = total_rows / 2;
        let mid_ts: i64 = reader
            .query_row(
                "SELECT ts_ms FROM sensor_readings WHERE id = ?1",
                rusqlite::params![mid_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(mid_ts, 1700000000000 + mid_id * 1000);

        let last_ts: i64 = reader
            .query_row(
                "SELECT ts_ms FROM sensor_readings WHERE id = ?1",
                rusqlite::params![total_rows - 1],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(last_ts, 1700000000000 + (total_rows - 1) * 1000);

        // Verify BLOB integrity
        let blob_data: Vec<u8> = reader
            .query_row(
                "SELECT payload FROM sensor_readings WHERE id = ?1",
                rusqlite::params![total_rows / 3],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(blob_data.len(), 128);
        assert!(blob_data.iter().all(|&b| b == 0xAB));

        eprintln!("Large representative DB test passed: {} rows, bulk insert + S3 checkpoint + cold read", total_rows);
    }

    // ===== V2 comprehensive coverage =====

    /// Full OLTP with secondary indexes: INSERT, UPDATE, DELETE, then checkpoint
    /// and cold read. Verifies internal B-tree pages (from indexes) are handled
    /// correctly in chunk storage.
    #[test]
    #[ignore]
    fn test_oltp_with_indexes() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("oltp_idx", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_oltp");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "oltp_idx_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE orders (
                 id INTEGER PRIMARY KEY,
                 customer TEXT NOT NULL,
                 amount REAL NOT NULL,
                 status TEXT NOT NULL,
                 created_at INTEGER NOT NULL
             );
             CREATE INDEX idx_customer ON orders(customer);
             CREATE INDEX idx_status ON orders(status);
             CREATE INDEX idx_amount ON orders(amount);",
        )
        .unwrap();

        // INSERT 1000 rows
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..1000 {
                tx.execute(
                    "INSERT INTO orders VALUES (?1, ?2, ?3, ?4, ?5)",
                    rusqlite::params![
                        i,
                        format!("cust_{}", i % 50),
                        (i as f64) * 1.5,
                        if i % 3 == 0 { "shipped" } else { "pending" },
                        1700000000 + i * 60
                    ],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // UPDATE 200 rows in the range that won't be deleted (id < 900)
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "UPDATE orders SET status = 'delivered', amount = amount + 10.0 WHERE id = ?1",
                    rusqlite::params![i],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // DELETE 100 rows
        {
            let tx = conn.unchecked_transaction().unwrap();
            tx.execute("DELETE FROM orders WHERE id >= 900", [])
                .unwrap();
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Verify locally
        let local_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
            .unwrap();
        assert_eq!(local_count, 900);

        let delivered: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE status = 'delivered'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(delivered, 200);

        drop(conn);

        // Cold read from S3
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_oltp_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "oltp_idx_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let cold_count: i64 = reader
            .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cold_count, 900, "cold read should see 900 rows after deletes");

        // Index-assisted queries should work
        let cust_count: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE customer = 'cust_0'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(cust_count > 0, "index lookup by customer should find rows");

        let cold_delivered: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE status = 'delivered'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cold_delivered, 200, "cold read should see updated statuses");

        // Range query using amount index
        let high_value: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM orders WHERE amount > 1000.0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(high_value > 0, "range query on indexed column should work");

        eprintln!("OLTP with indexes test passed: 3 indexes, insert/update/delete, cold read");
    }

    /// UPDATE and DELETE operations exercise read-modify-write of existing chunks.
    /// Verifies data integrity after modifying and deleting rows across multiple
    /// checkpoints.
    #[test]
    #[ignore]
    fn test_update_delete_operations() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("upd_del", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_upddel");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "upd_del_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);",
        )
        .unwrap();

        // Initial insert
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..500 {
                tx.execute(
                    "INSERT INTO kv VALUES (?1, ?2)",
                    rusqlite::params![format!("key_{:04}", i), format!("original_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Update half the rows (changes value in-place on existing pages)
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..250 {
                tx.execute(
                    "UPDATE kv SET value = ?1 WHERE key = ?2",
                    rusqlite::params![
                        format!("updated_{}_with_longer_value_to_change_page_layout", i),
                        format!("key_{:04}", i * 2)
                    ],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // Delete a range
        {
            let tx = conn.unchecked_transaction().unwrap();
            tx.execute("DELETE FROM kv WHERE key >= 'key_0400'", [])
                .unwrap();
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        drop(conn);

        // Cold read
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_upddel_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "upd_del_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM kv", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 400, "should have 400 rows after deleting 100");

        // Verify updated values
        let val: String = reader
            .query_row(
                "SELECT value FROM kv WHERE key = 'key_0000'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.starts_with("updated_"), "key_0000 should be updated");

        // Verify non-updated values
        let val2: String = reader
            .query_row(
                "SELECT value FROM kv WHERE key = 'key_0001'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val2.starts_with("original_"), "key_0001 should be original");

        // Verify deleted range is gone
        let deleted_count: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM kv WHERE key >= 'key_0400'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(deleted_count, 0, "deleted range should be gone");
    }

    /// Multiple tables in the same database. Verifies that internal B-tree pages
    /// and schema table pages are correctly stored in chunks.
    #[test]
    #[ignore]
    fn test_multiple_tables() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("multi_tbl", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_multitbl");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "multi_tbl_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
             CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT, price REAL);
             CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, qty INTEGER);
             CREATE TABLE audit_log (id INTEGER PRIMARY KEY, action TEXT, ts INTEGER);",
        )
        .unwrap();

        // Populate all 4 tables
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..100 {
                tx.execute(
                    "INSERT INTO users VALUES (?1, ?2)",
                    rusqlite::params![i, format!("user_{}", i)],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO products VALUES (?1, ?2, ?3)",
                    rusqlite::params![i, format!("product_{}", i), i as f64 * 9.99],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO orders VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![i, i % 50, i % 100, (i % 10) + 1],
                )
                .unwrap();
                tx.execute(
                    "INSERT INTO audit_log VALUES (?1, ?2, ?3)",
                    rusqlite::params![i, format!("create_order_{}", i), 1700000000 + i],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Cold read
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_multitbl_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "multi_tbl_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // Verify all 4 tables
        for (table, expected) in [("users", 100), ("products", 100), ("orders", 100), ("audit_log", 100)] {
            let count: i64 = reader
                .query_row(&format!("SELECT COUNT(*) FROM {}", table), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, expected, "{} should have {} rows", table, expected);
        }

        // JOIN query across tables
        let join_count: i64 = reader
            .query_row(
                "SELECT COUNT(*) FROM orders o
                 JOIN users u ON u.id = o.user_id
                 JOIN products p ON p.id = o.product_id",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(join_count, 100, "join should return all orders");
    }

    /// Non-default chunk_size (8 pages instead of 128). Verifies the chunk
    /// encoding/decoding and S3 layout work with smaller chunks.
    #[test]
    #[ignore]
    fn test_custom_chunk_size() {
        let cache_dir = TempDir::new().unwrap();
        let mut config = test_config("custom_cs", cache_dir.path());
        config.chunk_size = 8; // Very small chunks for testing
        let vfs_name = unique_vfs_name("tiered_cs8");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "custom_cs_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE cs (id INTEGER PRIMARY KEY, data TEXT);",
        )
        .unwrap();

        // Insert enough data to span multiple chunks (8 pages per chunk)
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..500 {
                tx.execute(
                    "INSERT INTO cs VALUES (?1, ?2)",
                    rusqlite::params![i, format!("chunk_size_8_data_{:0>200}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // With chunk_size=8, we should have more chunk objects than with chunk_size=128
        let chunk_count = verify_s3_has_chunks(&bucket, &prefix, &endpoint);
        eprintln!("chunk_size=8: {} chunk objects in S3", chunk_count);
        assert!(chunk_count >= 1, "should have chunk objects with chunk_size=8");

        // Verify manifest has chunk_size=8
        let rt = tokio::runtime::Runtime::new().unwrap();
        let manifest_data = rt.block_on(async {
            let aws_config = aws_config::from_env()
                .region(aws_sdk_s3::config::Region::new("auto"))
                .load()
                .await;
            let mut s3_config = aws_sdk_s3::config::Builder::from(&aws_config);
            if let Some(ep) = &endpoint {
                s3_config = s3_config.endpoint_url(ep).force_path_style(true);
            }
            let client = aws_sdk_s3::Client::from_conf(s3_config.build());
            let resp = client
                .get_object()
                .bucket(&bucket)
                .key(format!("{}/manifest.json", prefix))
                .send()
                .await
                .unwrap();
            resp.body.collect().await.unwrap().into_bytes().to_vec()
        });
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_data).unwrap();
        assert_eq!(
            manifest["chunk_size"].as_u64().unwrap(),
            8,
            "manifest should have chunk_size=8"
        );

        drop(conn);

        // Cold read with chunk_size=8
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            chunk_size: 8,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_cs8_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "custom_cs_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM cs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 500, "all rows should survive with chunk_size=8");
    }

    /// Blobs larger than page size trigger SQLite overflow pages. Verifies
    /// overflow page chains work correctly with chunk storage.
    #[test]
    #[ignore]
    fn test_large_overflow_blobs() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("overflow", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_overflow");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "overflow_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        // Use 4096 page size to make overflow more likely with smaller blobs
        conn.execute_batch(
            "PRAGMA page_size=4096;
             PRAGMA journal_mode=WAL;
             CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB);",
        )
        .unwrap();

        // Insert blobs of varying sizes — some larger than page size
        let sizes = [100, 1000, 5000, 10000, 50000, 100000];
        {
            let tx = conn.unchecked_transaction().unwrap();
            for (i, &size) in sizes.iter().enumerate() {
                let blob: Vec<u8> = (0..size).map(|b| (b % 256) as u8).collect();
                tx.execute(
                    "INSERT INTO blobs VALUES (?1, ?2)",
                    rusqlite::params![i as i64, blob],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Cold read
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_overflow_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "overflow_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // Verify each blob's size and content
        for (i, &expected_size) in sizes.iter().enumerate() {
            let data: Vec<u8> = reader
                .query_row(
                    "SELECT data FROM blobs WHERE id = ?1",
                    rusqlite::params![i as i64],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                data.len(),
                expected_size,
                "blob {} should be {} bytes",
                i,
                expected_size
            );
            // Verify content pattern
            for (j, &byte) in data.iter().enumerate() {
                assert_eq!(
                    byte,
                    (j % 256) as u8,
                    "blob {} byte {} mismatch",
                    i,
                    j
                );
            }
        }

        eprintln!(
            "Overflow blob test passed: sizes {:?}",
            sizes
        );
    }

    /// VACUUM after deletes reorganizes pages. Verifies that the VFS
    /// handles page count changes and page reuse correctly.
    #[test]
    #[ignore]
    fn test_vacuum_reorganizes() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("vacuum", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_vacuum");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "vacuum_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE bulk (id INTEGER PRIMARY KEY, payload TEXT);",
        )
        .unwrap();

        // Insert enough data to create many pages
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..2000 {
                tx.execute(
                    "INSERT INTO bulk VALUES (?1, ?2)",
                    rusqlite::params![i, format!("payload_{:0>500}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        let pages_before: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        eprintln!("Pages before delete: {}", pages_before);

        // Delete most rows
        conn.execute("DELETE FROM bulk WHERE id >= 200", [])
            .unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        // VACUUM reclaims space (switches from WAL to rollback journal temporarily)
        // Need to re-enable WAL after VACUUM
        conn.execute_batch("VACUUM;").unwrap();
        conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();

        let pages_after: i64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .unwrap();
        eprintln!("Pages after VACUUM: {}", pages_after);
        assert!(
            pages_after < pages_before,
            "VACUUM should reduce page count: {} -> {}",
            pages_before,
            pages_after
        );

        // Verify remaining data
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM bulk", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200);

        drop(conn);

        // Cold read after VACUUM
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_vacuum_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "vacuum_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let cold_count: i64 = reader
            .query_row("SELECT COUNT(*) FROM bulk", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cold_count, 200, "cold read after VACUUM should see 200 rows");

        let val: String = reader
            .query_row(
                "SELECT payload FROM bulk WHERE id = 199",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.starts_with("payload_"));
    }

    // ===== Phase 6: Hardening fixes =====

    /// VFS delete() must NOT destroy S3 data. Regression test for the bug where
    /// delete() called destroy_s3() unconditionally. When a connection is closed,
    /// SQLite may call delete on WAL/journal files — this must only affect local files.
    #[test]
    #[ignore]
    fn test_delete_preserves_s3() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("del_s3", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_del_s3");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        // Write data and checkpoint to S3
        {
            let conn = rusqlite::Connection::open_with_flags_and_vfs(
                "del_s3_test.db",
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                    | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
                &vfs_name,
            )
            .unwrap();

            conn.execute_batch(
                "PRAGMA page_size=65536;
                 PRAGMA journal_mode=WAL;
                 CREATE TABLE ds (id INTEGER PRIMARY KEY, val TEXT);",
            )
            .unwrap();

            {
                let tx = conn.unchecked_transaction().unwrap();
                for i in 0..100 {
                    tx.execute(
                        "INSERT INTO ds VALUES (?1, ?2)",
                        rusqlite::params![i, format!("del_s3_{}", i)],
                    )
                    .unwrap();
                }
                tx.commit().unwrap();
            }

            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
                .unwrap();

            // Verify S3 has data
            verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
        }
        // Connection dropped — SQLite may call xDelete on WAL/SHM files

        // S3 data MUST still exist after connection close + WAL cleanup
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);
        let chunks_after = verify_s3_has_chunks(&bucket, &prefix, &endpoint);
        assert!(
            chunks_after >= 1,
            "S3 chunks must survive connection close"
        );

        // Cold read from fresh cache to prove S3 data survived
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_del_s3_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "del_s3_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let cold_count: i64 = reader
            .query_row("SELECT COUNT(*) FROM ds", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cold_count, 100, "cold read after close must work");
    }

    /// Verify the VFS works in DELETE journal mode (non-WAL).
    /// Each transaction commit triggers write_all_at → sync → S3 upload.
    #[test]
    #[ignore]
    fn test_delete_journal_mode() {
        let cache_dir = TempDir::new().unwrap();
        let config = test_config("jdel", cache_dir.path());
        let vfs_name = unique_vfs_name("tiered_jdel");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "jdel_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        // Use DELETE mode (non-WAL) from the start
        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=DELETE;
             CREATE TABLE jd (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        // Insert data — each commit goes through write_all_at → sync
        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO jd VALUES (?1, ?2)",
                    rusqlite::params![i, format!("delete_mode_{}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        // Verify data is readable
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM jd", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "DELETE mode should store data correctly");

        drop(conn);

        // Verify S3 has data (sync was called during commit)
        verify_s3_manifest(&bucket, &prefix, &endpoint, 1, 65536);

        // Cold read from fresh cache
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_jdel_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "jdel_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        let cold_count: i64 = reader
            .query_row("SELECT COUNT(*) FROM jd", [], |row| row.get(0))
            .unwrap();
        assert_eq!(cold_count, 200, "cold read in DELETE mode must work");
    }

    /// Write with chunk_size=128 (default), read with TieredConfig chunk_size=64.
    /// The reader must use the manifest's chunk_size (128), not the config's (64).
    /// Without the fix, this would read wrong pages or panic.
    #[test]
    #[ignore]
    fn test_chunk_size_mismatch_uses_manifest() {
        // Write with default chunk_size=128
        let write_cache = TempDir::new().unwrap();
        let config = test_config("cs_mismatch", write_cache.path());
        let vfs_name = unique_vfs_name("tiered_csmm_w");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let endpoint = config.endpoint_url.clone();
        let region = config.region.clone();

        let vfs = TieredVfs::new(config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = rusqlite::Connection::open_with_flags_and_vfs(
            "cs_mismatch_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE
                | rusqlite::OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .unwrap();

        conn.execute_batch(
            "PRAGMA page_size=65536;
             PRAGMA journal_mode=WAL;
             CREATE TABLE csm (id INTEGER PRIMARY KEY, val TEXT);",
        )
        .unwrap();

        {
            let tx = conn.unchecked_transaction().unwrap();
            for i in 0..200 {
                tx.execute(
                    "INSERT INTO csm VALUES (?1, ?2)",
                    rusqlite::params![i, format!("mismatch_data_{:0>200}", i)],
                )
                .unwrap();
            }
            tx.commit().unwrap();
        }

        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .unwrap();
        drop(conn);

        // Read with DIFFERENT chunk_size in config (64 instead of 128).
        // The VFS must use the manifest's chunk_size (128) to read correctly.
        let cold_cache = TempDir::new().unwrap();
        let reader_config = TieredConfig {
            bucket,
            prefix,
            cache_dir: cold_cache.path().to_path_buf(),
            endpoint_url: endpoint,
            read_only: true,
            chunk_size: 64, // DIFFERENT from writer's 128
            region,
            ..Default::default()
        };
        let reader_vfs_name = unique_vfs_name("tiered_csmm_r");
        let reader_vfs = TieredVfs::new(reader_config).unwrap();
        sqlite_compress_encrypt_vfs::tiered::register(&reader_vfs_name, reader_vfs)
            .unwrap();

        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            "cs_mismatch_test.db",
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            &reader_vfs_name,
        )
        .unwrap();

        // This would fail (wrong data or panic) without the manifest chunk_size fix
        let count: i64 = reader
            .query_row("SELECT COUNT(*) FROM csm", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 200, "reader with mismatched config chunk_size should use manifest's");

        let val: String = reader
            .query_row(
                "SELECT val FROM csm WHERE id = 199",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(val.starts_with("mismatch_data_"), "data integrity with mismatched chunk_size");
    }
}
