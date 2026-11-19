//! LRU response cache for LLM API responses.
//!
//! Caches upstream LLM API responses keyed by a hash of the request
//! body. Uses an LRU eviction policy with a configurable max size.
//! Only caches successful (2xx) responses.

use std::num::NonZeroUsize;
use std::time::Duration;

use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::sync::Mutex;

/// Configuration for the response cache.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of cached responses.
    pub max_entries: usize,
    /// How long a cached entry is valid before it expires.
    pub ttl: Duration,
    /// Upper bound on the summed size of cached bodies and headers.
    pub max_bytes: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(3600), // 1 hour
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A cached response entry.
#[derive(Debug, Clone)]
struct CacheEntry {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    cached_at: std::time::Instant,
}

impl CacheEntry {
    fn size(&self, key: &str) -> usize {
        key.len()
            + self.body.len()
            + self
                .headers
                .iter()
                .map(|(name, value)| name.len() + value.len())
                .sum::<usize>()
    }
}

/// The LRU and the summed size of its entries.
struct Entries {
    lru: LruCache<String, CacheEntry>,
    bytes: usize,
}

/// Statistics about the response cache.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub stores: u64,
    pub evictions: u64,
    pub expirations: u64,
    pub size: usize,
}

impl CacheStats {
    /// Returns the cache hit rate (0.0 to 1.0).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// A cached response: (status, headers, body).
pub type CachedResponse = (u16, Vec<(String, String)>, Vec<u8>);

/// An LRU cache for LLM API responses.
pub struct ResponseCache {
    cache: Mutex<Entries>,
    config: CacheConfig,
    stats: Mutex<CacheStats>,
}

impl std::fmt::Debug for ResponseCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponseCache")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ResponseCache {
    /// Creates a new response cache with the given configuration.
    pub fn new(config: CacheConfig) -> Self {
        let capacity = NonZeroUsize::new(config.max_entries.max(1)).unwrap();
        Self {
            cache: Mutex::new(Entries {
                lru: LruCache::new(capacity),
                bytes: 0,
            }),
            config,
            stats: Mutex::new(CacheStats::default()),
        }
    }

    /// Looks up a cached response by key.
    ///
    /// Returns `None` if the entry is not cached or has expired.
    /// Expired entries are removed and counted as expirations.
    pub fn get(&self, key: &str) -> Option<CachedResponse> {
        let mut cache = lock(&self.cache);
        match cache.lru.get(key) {
            Some(entry) => {
                if entry.cached_at.elapsed() >= self.config.ttl {
                    if let Some(entry) = cache.lru.pop(key) {
                        cache.bytes = cache.bytes.saturating_sub(entry.size(key));
                    }
                    let mut stats = lock(&self.stats);
                    stats.expirations += 1;
                    stats.size = cache.lru.len();
                    drop(stats);
                    drop(cache);
                    lock(&self.stats).misses += 1;
                    return None;
                }
                lock(&self.stats).hits += 1;
                Some((entry.status, entry.headers.clone(), entry.body.clone()))
            }
            None => {
                lock(&self.stats).misses += 1;
                None
            }
        }
    }

    /// Stores a response in the cache.
    ///
    /// Only stores successful (2xx) responses. Evicted entries are
    /// counted in stats.
    pub fn put(&self, key: &str, status: u16, headers: Vec<(String, String)>, body: Vec<u8>) {
        // Only cache successful responses.
        if !(200..300).contains(&status) {
            return;
        }

        let entry = CacheEntry {
            status,
            headers,
            body,
            cached_at: std::time::Instant::now(),
        };

        let size = entry.size(key);
        if size > self.config.max_bytes {
            return;
        }
        let mut cache = lock(&self.cache);
        if let Some(old) = cache.lru.pop(key) {
            cache.bytes = cache.bytes.saturating_sub(old.size(key));
        }
        let mut evictions = 0;
        while cache.bytes + size > self.config.max_bytes
            || cache.lru.len() >= self.config.max_entries.max(1)
        {
            let Some((old_key, old)) = cache.lru.pop_lru() else {
                break;
            };
            cache.bytes = cache.bytes.saturating_sub(old.size(&old_key));
            evictions += 1;
        }
        cache.lru.put(key.to_string(), entry);
        cache.bytes += size;
        let mut stats = lock(&self.stats);
        stats.evictions += evictions;
        stats.stores += 1;
        stats.size = cache.lru.len();
    }

    /// Computes a cache key scoped to an authenticated workload.
    ///
    /// The workload identity is mixed into the key so identical prompts from
    /// different workloads cannot share responses.
    pub fn compute_key_for_workload(
        workload: &str,
        method: &str,
        path: &str,
        body: &[u8],
    ) -> String {
        Self::compute_key_for_workload_route(workload, "", "", method, path, body)
    }

    /// Computes a workload cache key isolated by end customer, route and
    /// complete path/query.
    ///
    /// Every field goes length-prefixed into one SHA-256 digest: the body is
    /// chosen by the caller, and a cheap collision would serve one customer
    /// another customer's cached response.
    pub fn compute_key_for_workload_route(
        workload: &str,
        customer: &str,
        route: &str,
        method: &str,
        path_and_query: &str,
        body: &[u8],
    ) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        for field in [
            workload.as_bytes(),
            customer.as_bytes(),
            route.as_bytes(),
            method.as_bytes(),
            path_and_query.as_bytes(),
            body,
        ] {
            hasher.update((field.len() as u64).to_le_bytes());
            hasher.update(field);
        }
        hasher
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    /// Returns current cache statistics.
    pub fn stats(&self) -> CacheStats {
        let mut stats = lock(&self.stats);
        stats.size = lock(&self.cache).lru.len();
        stats.clone()
    }

    /// Clears all cached entries.
    pub fn clear(&self) {
        {
            let mut cache = lock(&self.cache);
            cache.lru.clear();
            cache.bytes = 0;
        }
        let mut stats = lock(&self.stats);
        stats.size = 0;
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        lock(&self.cache).lru.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> CacheConfig {
        CacheConfig {
            max_entries: 10,
            ttl: Duration::from_secs(60),
            max_bytes: 1 << 20,
        }
    }

    #[test]
    fn cache_is_bounded_by_bytes() {
        let cache = ResponseCache::new(CacheConfig {
            max_entries: 100,
            ttl: Duration::from_secs(60),
            max_bytes: 100,
        });
        for key in ["a", "b", "c"] {
            cache.put(key, 200, vec![], vec![0; 40]);
        }
        assert!(cache.get("a").is_none(), "oldest entry evicted for space");
        assert!(cache.get("c").is_some());
        cache.put("huge", 200, vec![], vec![0; 101]);
        assert!(
            cache.get("huge").is_none(),
            "an entry over the bound is skipped"
        );
    }

    #[test]
    fn put_and_get() {
        let cache = ResponseCache::new(test_config());
        let key = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"hello");
        cache.put(
            &key,
            200,
            vec![("content-type".into(), "json".into())],
            b"response".to_vec(),
        );
        let result = cache.get(&key);
        assert!(result.is_some());
        let (status, headers, body) = result.unwrap();
        assert_eq!(status, 200);
        assert_eq!(body, b"response");
        assert_eq!(headers.len(), 1);
    }

    #[test]
    fn customers_never_share_entries() {
        let cache = ResponseCache::new(test_config());
        let key = |customer, route| {
            ResponseCache::compute_key_for_workload_route(
                "task",
                customer,
                route,
                "POST",
                "/v1/chat",
                b"same prompt",
            )
        };
        cache.put(&key("alice", "primary"), 200, vec![], b"for alice".to_vec());
        assert!(cache.get(&key("bob", "primary")).is_none());
        assert!(cache.get(&key("", "primary")).is_none());
        assert!(cache.get(&key("alice", "primary")).is_some());
        // Length prefixes keep fields apart: no shifting bytes between them.
        assert_ne!(key("ab", "c"), key("a", "bc"));
        assert_eq!(key("alice", "primary").len(), 64);
    }

    #[test]
    fn route_and_query_isolate_cache_keys() {
        let key = |route, path| {
            ResponseCache::compute_key_for_workload_route("task", "", route, "POST", path, b"body")
        };
        assert_ne!(
            key("primary", "/v1/chat?x=1"),
            key("fallback", "/v1/chat?x=1")
        );
        assert_ne!(
            key("primary", "/v1/chat?x=1"),
            key("primary", "/v1/chat?x=2")
        );
    }

    #[test]
    fn get_missing() {
        let cache = ResponseCache::new(test_config());
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn only_caches_2xx() {
        let cache = ResponseCache::new(test_config());
        cache.put("key-500", 500, vec![], b"error".to_vec());
        assert!(cache.get("key-500").is_none());
        assert_eq!(cache.len(), 0);

        cache.put("key-404", 404, vec![], b"not found".to_vec());
        assert!(cache.get("key-404").is_none());

        cache.put("key-200", 200, vec![], b"ok".to_vec());
        assert!(cache.get("key-200").is_some());
    }

    #[test]
    fn lru_eviction() {
        let config = CacheConfig {
            max_entries: 2,
            ttl: Duration::from_secs(60),
            max_bytes: 1 << 20,
        };
        let cache = ResponseCache::new(config);
        cache.put("a", 200, vec![], b"1".to_vec());
        cache.put("b", 200, vec![], b"2".to_vec());
        cache.put("c", 200, vec![], b"3".to_vec()); // evicts "a" (LRU)

        assert!(cache.get("a").is_none());
        assert!(cache.get("b").is_some());
        assert!(cache.get("c").is_some());
    }

    #[test]
    fn ttl_expiration() {
        let config = CacheConfig {
            max_entries: 10,
            ttl: Duration::from_millis(10),
            max_bytes: 1 << 20,
        };
        let cache = ResponseCache::new(config);
        cache.put("key", 200, vec![], b"data".to_vec());
        assert!(cache.get("key").is_some());
        std::thread::sleep(Duration::from_millis(15));
        assert!(cache.get("key").is_none());
    }

    #[test]
    fn stats_tracking() {
        let cache = ResponseCache::new(test_config());
        cache.put("a", 200, vec![], b"1".to_vec());
        cache.get("a"); // hit
        cache.get("a"); // hit
        cache.get("b"); // miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.stores, 1);
        assert_eq!(stats.size, 1);
        assert!((stats.hit_rate() - 0.666).abs() < 0.01);
    }

    #[test]
    fn workload_cache_keys_are_isolated() {
        let cache = ResponseCache::new(test_config());
        let first = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"same");
        let second = ResponseCache::compute_key_for_workload("task-b", "POST", "/v1/chat", b"same");
        assert_ne!(first, second);
        cache.put(&first, 200, vec![], b"task-a response".to_vec());
        assert!(cache.get(&second).is_none());
    }

    #[test]
    fn compute_key_for_workload_deterministic() {
        let k1 = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"hello");
        let k2 = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"hello");
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_for_workload_varies_with_request() {
        let k1 = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"hello");
        let k2 = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/chat", b"world");
        let k3 = ResponseCache::compute_key_for_workload("task-a", "GET", "/v1/chat", b"hello");
        let k4 = ResponseCache::compute_key_for_workload("task-a", "POST", "/v1/other", b"hello");
        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
        assert_ne!(k1, k4);
    }

    #[test]
    fn clear() {
        let cache = ResponseCache::new(test_config());
        cache.put("a", 200, vec![], b"1".to_vec());
        cache.put("b", 200, vec![], b"2".to_vec());
        assert_eq!(cache.len(), 2);
        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn overwrite_existing_key() {
        let cache = ResponseCache::new(test_config());
        cache.put("key", 200, vec![], b"old".to_vec());
        cache.put("key", 200, vec![], b"new".to_vec());
        let (_, _, body) = cache.get("key").unwrap();
        assert_eq!(body, b"new");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn eviction_stats() {
        let config = CacheConfig {
            max_entries: 2,
            ttl: Duration::from_secs(60),
            max_bytes: 1 << 20,
        };
        let cache = ResponseCache::new(config);
        cache.put("a", 200, vec![], b"1".to_vec());
        cache.put("b", 200, vec![], b"2".to_vec());
        cache.put("c", 200, vec![], b"3".to_vec()); // evicts "a"

        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.stores, 3);
    }

    #[test]
    fn expiration_stats() {
        let config = CacheConfig {
            max_entries: 10,
            ttl: Duration::from_millis(10),
            max_bytes: 1 << 20,
        };
        let cache = ResponseCache::new(config);
        cache.put("key", 200, vec![], b"data".to_vec());
        std::thread::sleep(Duration::from_millis(15));
        cache.get("key"); // expired → miss + expiration

        let stats = cache.stats();
        assert_eq!(stats.expirations, 1);
        assert_eq!(stats.misses, 1);
    }
}
