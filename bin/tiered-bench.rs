//! tiered-bench - Cold start benchmark for tiered S3-backed VFS
//!
//! Early-Facebook-style dataset:
//!   - users, posts, friendships, likes (4 tables, indexed)
//!   - Point lookups and small JOINs only (no scans, no graph traversals)
//!
//! Every iteration: fresh VFS + empty cache → every read hits S3.
//! Measures true cold start latency.
//!
//! ```bash
//! TIERED_TEST_BUCKET=sqlces-test \
//!   AWS_ENDPOINT_URL=https://t3.storage.dev \
//!   cargo run --release --features tiered,zstd --bin tiered-bench
//! ```

use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use sqlite_compress_encrypt_vfs::tiered::{TieredConfig, TieredVfs};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tempfile::TempDir;

static VFS_COUNTER: AtomicU32 = AtomicU32::new(0);

// =========================================================================
// Data constants
// =========================================================================

const FIRST_NAMES: &[&str] = &[
    "Mark", "Eduardo", "Dustin", "Chris", "Sean", "Priscilla", "Sheryl",
    "Andrew", "Adam", "Mike", "Sarah", "Jessica", "Emily", "David", "Alex",
    "Randi", "Naomi", "Kevin", "Amy", "Dan", "Lisa", "Tom", "Rachel",
    "Brian", "Caitlin", "Nicole", "Matt", "Laura", "Jake", "Megan",
];

const LAST_NAMES: &[&str] = &[
    "Zuckerberg", "Saverin", "Moskovitz", "Hughes", "Parker", "Chan",
    "Sandberg", "McCollum", "D'Angelo", "Schroepfer", "Smith", "Johnson",
    "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez",
    "Martinez", "Anderson", "Taylor", "Thomas", "Hernandez", "Moore",
    "Martin", "Jackson", "Thompson", "White", "Lopez",
];

const SCHOOLS: &[&str] = &[
    "Harvard", "Stanford", "MIT", "Yale", "Princeton", "Columbia",
    "Penn", "Brown", "Cornell", "Dartmouth", "Duke", "Georgetown",
    "UCLA", "Berkeley", "Michigan", "NYU", "Boston University",
    "Northeastern", "USC", "Emory",
];

const CITIES: &[&str] = &[
    "Palo Alto, CA", "San Francisco, CA", "New York, NY", "Boston, MA",
    "Cambridge, MA", "Seattle, WA", "Austin, TX", "Chicago, IL",
    "Los Angeles, CA", "Miami, FL", "Denver, CO", "Portland, OR",
    "Philadelphia, PA", "Washington, DC", "Atlanta, GA",
];

const POST_TEMPLATES: &[&str] = &[
    "Just moved into my new dorm room! {} is going to be amazing this year.",
    "Can't believe we won the game last night. Go {}!",
    "Anyone else studying for the {} midterm? This is brutal.",
    "Looking for people to join our {} intramural team. DM me!",
    "Had the best {} at that new place downtown. Highly recommend!",
    "Working on a new project with {}. Can't say much yet but stay tuned...",
    "Missing home but {} makes it worth it. Great people here.",
    "Just finished reading {}. Changed my perspective on everything.",
    "Road trip to {} this weekend! Who's in?",
    "Three exams in one week. {} life is no joke.",
    "Happy birthday to my roommate {}! Best {} ever.",
    "Throwback to that {} concert last summer. Need to see them again.",
    "Anyone want to grab {} at the dining hall? Meeting at 6pm.",
    "Finally submitted my {} paper. Time to celebrate!",
    "The weather in {} is unreal today. Perfect for frisbee on the quad.",
];

const FILL_WORDS: &[&str] = &[
    "college", "freshman year", "organic chemistry", "basketball",
    "pizza", "the team", "campus", "Malcolm Gladwell", "New York",
    "Harvard", "Alex", "friend", "Radiohead", "dinner", "thesis",
    "San Francisco",
];

#[derive(Parser)]
#[command(name = "tiered-bench")]
#[command(about = "Cold start benchmark for tiered S3-backed VFS")]
struct Cli {
    /// Row counts (total posts) to benchmark (comma-separated)
    #[arg(long, default_value = "500000")]
    sizes: String,

    /// Number of cold start iterations per query
    #[arg(long, default_value = "10")]
    iterations: usize,

    /// Skip S3 cleanup after benchmarks (keeps data for --reuse)
    #[arg(long)]
    no_cleanup: bool,

    /// Reuse existing S3 data at this prefix (skip data gen + upload)
    #[arg(long)]
    reuse: Option<String>,
}

// =========================================================================
// Helpers
// =========================================================================

fn unique_vfs_name(prefix: &str) -> String {
    let n = VFS_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("bench_{}_{}", prefix, n)
}

fn test_bucket() -> String {
    std::env::var("TIERED_TEST_BUCKET")
        .or_else(|_| std::env::var("BUCKET_NAME"))
        .expect("TIERED_TEST_BUCKET or BUCKET_NAME env var required")
}

fn endpoint_url() -> String {
    std::env::var("AWS_ENDPOINT_URL")
        .or_else(|_| std::env::var("AWS_ENDPOINT_URL_S3"))
        .unwrap_or_else(|_| "https://fly.storage.tigris.dev".to_string())
}

/// Deterministic pseudo-random hash
fn phash(seed: u64) -> u64 {
    let mut x = seed;
    x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x
}

fn percentile(latencies: &[f64], p: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    let mut sorted = latencies.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((p * sorted.len() as f64) as usize).min(sorted.len() - 1);
    sorted[idx]
}

struct BenchResult {
    label: String,
    ops: usize,
    latencies_us: Vec<f64>,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        let total_secs: f64 = self.latencies_us.iter().sum::<f64>() / 1_000_000.0;
        if total_secs == 0.0 {
            return 0.0;
        }
        self.ops as f64 / total_secs
    }

    fn p50(&self) -> f64 {
        percentile(&self.latencies_us, 0.5)
    }

    fn p90(&self) -> f64 {
        percentile(&self.latencies_us, 0.9)
    }

    fn p99(&self) -> f64 {
        percentile(&self.latencies_us, 0.99)
    }
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

fn format_ms(us: f64) -> String {
    if us >= 1_000_000.0 {
        format!("{:.1}s", us / 1_000_000.0)
    } else if us >= 1000.0 {
        format!("{:.0}ms", us / 1000.0)
    } else {
        format!("{:.0}us", us)
    }
}


fn make_config(
    prefix: &str,
    cache_dir: &std::path::Path,
) -> TieredConfig {
    let unique_prefix = format!(
        "bench/{}/{}",
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

fn make_reader_config(
    prefix: &str,
    cache_dir: &std::path::Path,
) -> TieredConfig {
    TieredConfig {
        bucket: test_bucket(),
        prefix: prefix.to_string(),
        cache_dir: cache_dir.to_path_buf(),
        compression_level: 3,
        endpoint_url: Some(endpoint_url()),
        read_only: true,
        region: Some("auto".to_string()),
        ..Default::default()
    }
}

// =========================================================================
// Schema
// =========================================================================

const SCHEMA: &str = "
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    school TEXT NOT NULL,
    city TEXT NOT NULL,
    bio TEXT NOT NULL,
    joined_at INTEGER NOT NULL
);

CREATE TABLE posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    content TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    like_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE friendships (
    user_a INTEGER NOT NULL,
    user_b INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_a, user_b)
);

CREATE TABLE likes (
    user_id INTEGER NOT NULL,
    post_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (user_id, post_id)
);

CREATE INDEX idx_posts_user ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at);
CREATE INDEX idx_friendships_b ON friendships(user_b, user_a);
CREATE INDEX idx_likes_post ON likes(post_id);
CREATE INDEX idx_likes_user ON likes(user_id, created_at);
CREATE INDEX idx_users_school ON users(school);
";

// =========================================================================
// Data generation
// =========================================================================

fn generate_post_content(id: i64) -> String {
    let h = phash(id as u64 + 9_000_000);
    let template = POST_TEMPLATES[(h as usize) % POST_TEMPLATES.len()];
    let fill1 = FILL_WORDS[((h >> 16) as usize) % FILL_WORDS.len()];
    let fill2 = FILL_WORDS[((h >> 24) as usize) % FILL_WORDS.len()];

    // Replace first {} with fill1, second with fill2
    let mut content = template.replacen("{}", fill1, 1);
    content = content.replacen("{}", fill2, 1);

    // Pad some posts longer (200-2000 chars) to vary compression
    let target_len = 200 + ((h >> 32) % 1800) as usize;
    if content.len() < target_len {
        let padding_phrases = [
            " Can't wait to see what happens next.",
            " This semester is flying by.",
            " Anyone else feel the same way?",
            " Comment below if you're interested!",
            " Life is good right now.",
            " Really grateful for this experience.",
            " Shoutout to everyone who made this happen.",
            " More updates coming soon!",
        ];
        while content.len() < target_len {
            let pidx = phash(content.len() as u64 + id as u64) as usize % padding_phrases.len();
            content.push_str(padding_phrases[pidx]);
        }
    }
    content
}

fn generate_bio(id: i64) -> String {
    let h = phash(id as u64 + 7_000_000);
    let interests = ["music", "startups", "hiking", "photography", "cooking",
                     "travel", "reading", "sports", "gaming", "art"];
    let i1 = interests[((h >> 8) as usize) % interests.len()];
    let i2 = interests[((h >> 16) as usize) % interests.len()];
    let i3 = interests[((h >> 24) as usize) % interests.len()];
    let year = 2004 + (h % 4);
    format!("Class of {}. Into {}, {}, and {}. Looking to connect!", year, i1, i2, i3)
}

fn generate_data(conn: &Connection, n_posts: usize) {
    // Scale: ~1 user per 10 posts, ~50 friends per user, ~3 likes per post
    let n_users = (n_posts / 10).max(100);
    let friends_per_user = 50usize;
    let n_friendships = n_users * friends_per_user / 2; // bidirectional
    let n_likes = n_posts * 3;

    eprintln!(
        "  Generating: {} users, {} posts, {} friendships, {} likes",
        format_number(n_users),
        format_number(n_posts),
        format_number(n_friendships),
        format_number(n_likes),
    );

    let tx = conn.unchecked_transaction().unwrap();

    // Users
    {
        let mut stmt = tx
            .prepare("INSERT INTO users VALUES (?1,?2,?3,?4,?5,?6,?7,?8)")
            .unwrap();
        for i in 0..n_users as i64 {
            let h = phash(i as u64);
            let first = FIRST_NAMES[(h as usize) % FIRST_NAMES.len()];
            let last = LAST_NAMES[((h >> 16) as usize) % LAST_NAMES.len()];
            let school = SCHOOLS[((h >> 24) as usize) % SCHOOLS.len()];
            let city = CITIES[((h >> 32) as usize) % CITIES.len()];
            let joined_ts = 1075000000 + (h % 100_000_000) as i64; // 2004-ish epoch
            stmt.execute(rusqlite::params![
                i,
                first,
                last,
                format!("{}.{}{}@{}.edu", first.to_lowercase(), last.to_lowercase(), i, school.to_lowercase().replace(' ', "")),
                school,
                city,
                generate_bio(i),
                joined_ts,
            ])
            .unwrap();
        }
    }

    // Posts
    {
        let mut stmt = tx
            .prepare("INSERT INTO posts (id, user_id, content, created_at, like_count) VALUES (?1,?2,?3,?4,?5)")
            .unwrap();
        for i in 0..n_posts as i64 {
            let h = phash(i as u64 + 1_000_000);
            let user_id = (h % n_users as u64) as i64;
            // Spread posts across 2004-2006 timeline
            let created_ts = 1075000000 + (h >> 16) as i64 % 94_000_000;
            let like_count = (phash(i as u64 + 2_000_000) % 200) as i64;
            stmt.execute(rusqlite::params![
                i,
                user_id,
                generate_post_content(i),
                created_ts,
                like_count,
            ])
            .unwrap();
        }
    }

    // Friendships (power-law: some users have way more friends)
    {
        let mut stmt = tx
            .prepare("INSERT OR IGNORE INTO friendships VALUES (?1,?2,?3)")
            .unwrap();
        let mut count = 0usize;
        for i in 0..n_users as u64 {
            // Each user gets ~50 friends, but scattered randomly
            let n_friends = friends_per_user.min(n_users - 1);
            for j in 0..n_friends {
                let h = phash(i * 100 + j as u64 + 3_000_000);
                let friend = (h % n_users as u64) as i64;
                if friend != i as i64 {
                    let (a, b) = if (i as i64) < friend {
                        (i as i64, friend)
                    } else {
                        (friend, i as i64)
                    };
                    let ts = 1075000000 + (h >> 16) as i64 % 94_000_000;
                    let _ = stmt.execute(rusqlite::params![a, b, ts]);
                    count += 1;
                }
                if count >= n_friendships {
                    break;
                }
            }
            if count >= n_friendships {
                break;
            }
        }
        eprintln!("    friendships inserted: {}", format_number(count));
    }

    // Likes (scattered: random users liking random posts)
    {
        let mut stmt = tx
            .prepare("INSERT OR IGNORE INTO likes VALUES (?1,?2,?3)")
            .unwrap();
        let mut count = 0usize;
        for i in 0..n_likes as u64 {
            let h = phash(i + 4_000_000);
            let user_id = (h % n_users as u64) as i64;
            let post_id = ((h >> 16) % n_posts as u64) as i64;
            let ts = 1075000000 + (h >> 32) as i64 % 94_000_000;
            let _ = stmt.execute(rusqlite::params![user_id, post_id, ts]);
            count += 1;
        }
        eprintln!("    likes inserted: {}", format_number(count));
    }

    tx.commit().unwrap();
}

// =========================================================================
// Benchmark queries
// =========================================================================

/// Who liked this post: likes + users join
const Q_WHO_LIKED: &str = "\
SELECT u.first_name, u.last_name, u.school, l.created_at
FROM likes l
JOIN users u ON u.id = l.user_id
WHERE l.post_id = ?1
ORDER BY l.created_at DESC
LIMIT 50";

/// Mutual friends between two users (self-join on friendships)
const Q_MUTUAL: &str = "\
SELECT u.id, u.first_name, u.last_name, u.school
FROM friendships f1
JOIN friendships f2 ON f1.user_b = f2.user_b
JOIN users u ON u.id = f1.user_b
WHERE f1.user_a = ?1 AND f2.user_a = ?2
LIMIT 20";

/// Point lookup: single post with author info (2-way join)
const Q_POST_DETAIL: &str = "\
SELECT p.id, p.content, p.created_at, p.like_count,
       u.first_name, u.last_name, u.school, u.city
FROM posts p
JOIN users u ON u.id = p.user_id
WHERE p.id = ?1";

/// User profile: user + their recent posts
const Q_PROFILE: &str = "\
SELECT u.first_name, u.last_name, u.school, u.city, u.bio,
       p.id, p.content, p.created_at, p.like_count
FROM users u
JOIN posts p ON p.user_id = u.id
WHERE u.id = ?1
ORDER BY p.created_at DESC
LIMIT 10";

// =========================================================================
// Benchmark functions (hot/warm — reuse connection)
// =========================================================================

// =========================================================================
// Cold benchmarks (fresh VFS per iteration)
// =========================================================================

fn bench_cold_parameterized(
    s3_prefix: &str,
    db_name: &str,
    label: &str,
    sql: &str,
    param_fn: &dyn Fn(usize) -> Vec<rusqlite::types::Value>,
    iterations: usize,
) -> BenchResult {
    let mut latencies = Vec::with_capacity(iterations);
    let mut errors = 0usize;
    for i in 0..iterations {
        let cache = TempDir::new().expect("temp dir");
        let vfs_name = unique_vfs_name("cold");
        let config = make_reader_config(s3_prefix, cache.path());
        let vfs = TieredVfs::new(config).expect("cold VFS");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let params = param_fn(i);
        let start = Instant::now();
        let conn = Connection::open_with_flags_and_vfs(
            db_name, OpenFlags::SQLITE_OPEN_READ_ONLY, &vfs_name,
        ).expect("cold connection");
        let result: Result<Vec<Vec<rusqlite::types::Value>>, _> = (|| {
            let mut stmt = conn.prepare(sql)?;
            let rows: Result<Vec<_>, _> = stmt
                .query_map(rusqlite::params_from_iter(&params), |row: &rusqlite::Row| {
                    let n = row.as_ref().column_count();
                    let mut vals = Vec::with_capacity(n);
                    for i in 0..n {
                        vals.push(row.get::<_, rusqlite::types::Value>(i)?);
                    }
                    Ok(vals)
                })?
                .collect();
            rows
        })();
        match result {
            Ok(_) => latencies.push(start.elapsed().as_micros() as f64),
            Err(e) => {
                errors += 1;
                eprintln!("    {} iter {} error: {}", label, i, e);
            }
        }
        drop(conn);
    }
    if errors > 0 {
        eprintln!("    {} had {}/{} errors", label, errors, iterations);
    }
    BenchResult {
        label: label.to_string(),
        ops: latencies.len(),
        latencies_us: latencies,
    }
}

// =========================================================================
// Main benchmark runner
// =========================================================================

fn print_header() {
    println!(
        "  {:<18} {:>10} {:>10} {:>10} {:>10}",
        "", "ops/sec", "p50", "p90", "p99"
    );
    println!(
        "  {:-<18} {:->10} {:->10} {:->10} {:->10}",
        "", "", "", "", ""
    );
}

fn print_result(r: &BenchResult) {
    println!(
        "  {:<18} {:>10} {:>10} {:>10} {:>10}",
        r.label,
        format_number(r.ops_per_sec() as usize),
        format_ms(r.p50()),
        format_ms(r.p90()),
        format_ms(r.p99()),
    );
}

fn run_benchmark(n_posts: usize, cli: &Cli) {
    let n_users = (n_posts / 10).max(100);
    let est_bytes = n_posts as f64 * 700.0
        + n_users as f64 * 200.0
        + (n_users as f64 * 25.0) * 24.0
        + (n_posts as f64 * 3.0) * 24.0;
    let db_size_mb = est_bytes / (1024.0 * 1024.0);
    let chunk_size_bytes: u64 = 128 * 65536;
    let est_chunks = (est_bytes as u64) / chunk_size_bytes + 1;
    let db_name = format!("social_{}.db", n_posts);

    println!();
    println!(
        "--- {} posts, {} users (~{:.0} MB, ~{} chunks) ---",
        format_number(n_posts), format_number(n_users), db_size_mb, est_chunks,
    );

    let cache_dir = TempDir::new().expect("failed to create temp dir");

    // Either reuse existing S3 data or generate + upload
    let s3_prefix = if let Some(ref prefix) = cli.reuse {
        println!("Reusing S3 prefix: {}", prefix);
        prefix.clone()
    } else {
        let config = make_config(&format!("social_{}", n_posts), cache_dir.path());
        let s3_prefix = config.prefix.clone();
        let vfs_name = unique_vfs_name("write");
        let vfs = TieredVfs::new(config).expect("failed to create TieredVfs");
        sqlite_compress_encrypt_vfs::tiered::register(&vfs_name, vfs).unwrap();

        let conn = Connection::open_with_flags_and_vfs(
            &db_name,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            &vfs_name,
        )
        .expect("failed to open connection");
        conn.execute_batch(
            "PRAGMA page_size=65536; PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;",
        )
        .expect("pragma setup failed");
        conn.execute_batch(SCHEMA).expect("create tables failed");

        let gen_start = Instant::now();
        generate_data(&conn, n_posts);
        println!("Data gen:    {:.2}s", gen_start.elapsed().as_secs_f64());

        let cp_start = Instant::now();
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint failed");
        println!("Checkpoint:  {:.2}s", cp_start.elapsed().as_secs_f64());
        drop(conn);

        println!("REUSE PREFIX: {}", s3_prefix);
        s3_prefix
    };

    // =====================================================================
    // COLD: fresh VFS per iteration (every read -> S3)
    // =====================================================================
    println!();
    println!("=== COLD START (fresh VFS, every read -> S3) ===");
    print_header();

    print_result(&bench_cold_parameterized(
        &s3_prefix, &db_name, "post+user", Q_POST_DETAIL,
        &|i| {
            let pid = phash(i as u64 + 500) % n_posts as u64;
            vec![rusqlite::types::Value::Integer(pid as i64)]
        },
        cli.iterations,
    ));

    print_result(&bench_cold_parameterized(
        &s3_prefix, &db_name, "profile", Q_PROFILE,
        &|i| {
            let uid = phash(i as u64 + 100) % n_users as u64;
            vec![rusqlite::types::Value::Integer(uid as i64)]
        },
        cli.iterations,
    ));

    print_result(&bench_cold_parameterized(
        &s3_prefix, &db_name, "who-liked", Q_WHO_LIKED,
        &|i| {
            let pid = phash(i as u64 + 200) % n_posts as u64;
            vec![rusqlite::types::Value::Integer(pid as i64)]
        },
        cli.iterations,
    ));

    print_result(&bench_cold_parameterized(
        &s3_prefix, &db_name, "mutual", Q_MUTUAL,
        &|i| {
            let a = phash(i as u64 + 300) % n_users as u64;
            let b = phash(i as u64 + 400) % n_users as u64;
            vec![
                rusqlite::types::Value::Integer(a as i64),
                rusqlite::types::Value::Integer(b as i64),
            ]
        },
        cli.iterations,
    ));

    println!();

    // --- Cleanup S3 ---
    if !cli.no_cleanup {
        eprint!("  Cleaning up S3... ");
        let cleanup_cache = TempDir::new().expect("cleanup temp dir");
        let cleanup_config = TieredConfig {
            bucket: test_bucket(),
            prefix: s3_prefix,
            cache_dir: cleanup_cache.path().to_path_buf(),
            compression_level: 3,
            endpoint_url: Some(endpoint_url()),
            region: Some("auto".to_string()),
            ..Default::default()
        };
        let cleanup_vfs = TieredVfs::new(cleanup_config).expect("cleanup VFS");
        cleanup_vfs.destroy_s3().expect("S3 cleanup failed");
        eprintln!("done");
    }
}

fn main() {
    let cli = Cli::parse();
    let sizes: Vec<usize> = cli
        .sizes
        .split(',')
        .map(|s| s.trim().parse().expect("invalid size"))
        .collect();

    println!("=== Cold Start Benchmark (Tiered VFS) ===");
    println!("Bucket: {}", test_bucket());
    println!("Endpoint: {}", endpoint_url());
    println!("Page size: 65536, Chunk size: 128 pages (8 MB/chunk)");
    println!("Schema: users, posts, friendships, likes (early Facebook)");
    println!("Queries: post detail (2-join), profile (join+sort), who-liked (2-join), mutual friends (self-join)");
    println!("Iterations: {} per query (fresh VFS + empty cache each)", cli.iterations);
    println!("Each iteration: new VFS → every read hits S3");

    for size in sizes {
        run_benchmark(size, &cli);
    }

    println!();
    println!("Neon cold start reference: ~500ms (us-east-2, compute pool)");
    println!("Done.");
}
