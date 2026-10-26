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
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            ttl: Duration::from_secs(3600), // 1 hour
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
    cache: Mutex<LruCache<String, CacheEntry>>,
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
            cache: Mutex::new(LruCache::new(capacity)),
            config,
            stats: Mutex::new(CacheStats::default()),
        }
    }

    /// Looks up a cached response by key.
    ///
    /// Returns `None` if the entry is not cached or has expired.
    /// Expired entries are removed and counted as expirations.
    pub fn get(&self, key: &str) -> Option<CachedResponse> {
        let mut cache = self.cache.lock().unwrap();
        match cache.get(key) {
            Some(entry) => {
                if entry.cached_at.elapsed() >= self.config.ttl {
                    cache.pop(key);
                    let mut stats = self.stats.lock().unwrap();
                    stats.expirations += 1;
                    stats.size = cache.len();
                    drop(stats);
                    drop(cache);
                    self.stats.lock().unwrap().misses += 1;
                    return None;
                }
                self.stats.lock().unwrap().hits += 1;
                Some((entry.status, entry.headers.clone(), entry.body.clone()))
            }
            None => {
                self.stats.lock().unwrap().misses += 1;
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

        let mut cache = self.cache.lock().unwrap();
        // Check if we're at capacity before inserting (for eviction counting).
        let was_full = cache.len() >= self.config.max_entries;
        let had_key = cache.contains(key);
        cache.put(key.to_string(), entry);
        if was_full && !had_key {
            self.stats.lock().unwrap().evictions += 1;
        }
        let mut stats = self.stats.lock().unwrap();
        stats.stores += 1;
        stats.size = cache.len();
    }

    /// Computes a cache key from a request method, path, and body.
    ///
    /// Uses a simple hash (FNV-1a) of the body to generate a compact key.
    pub fn compute_key(method: &str, path: &str, body: &[u8]) -> String {
        let mut hash = fnv1a(body);
        // Mix in method and path for uniqueness.
        for b in method.as_bytes() {
            hash = (hash ^ (*b as u64)).wrapping_mul(0x100000001b3);
        }
        for b in path.as_bytes() {
            hash = (hash ^ (*b as u64)).wrapping_mul(0x100000001b3);
        }
        format!("{method}:{path}:{hash:016x}")
    }

    /// Returns current cache statistics.
    pub fn stats(&self) -> CacheStats {
        let mut stats = self.stats.lock().unwrap();
        stats.size = self.cache.lock().unwrap().len();
        stats.clone()
    }

    /// Clears all cached entries.
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
        let mut stats = self.stats.lock().unwrap();
        stats.size = 0;
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// FNV-1a hash of a byte slice.
fn fnv1a(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &b in data {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
        }
    }

    #[test]
    fn put_and_get() {
        let cache = ResponseCache::new(test_config());
        let key = ResponseCache::compute_key("POST", "/v1/chat", b"hello");
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
    fn compute_key_deterministic() {
        let k1 = ResponseCache::compute_key("POST", "/v1/chat", b"hello");
        let k2 = ResponseCache::compute_key("POST", "/v1/chat", b"hello");
        assert_eq!(k1, k2);
    }

    #[test]
    fn compute_key_different_inputs() {
        let k1 = ResponseCache::compute_key("POST", "/v1/chat", b"hello");
        let k2 = ResponseCache::compute_key("POST", "/v1/chat", b"world");
        let k3 = ResponseCache::compute_key("GET", "/v1/chat", b"hello");
        let k4 = ResponseCache::compute_key("POST", "/v1/other", b"hello");
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
