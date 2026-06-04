package com.theyawns.throttle;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.io.Serial;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;

/**
 * A throttling cache in front of {@link IMap#isEmpty()}, backed by a distributed
 * Hazelcast {@link IMap} so that every client in the cluster shares the result.
 *
 * <p>{@code IMap.isEmpty()} is a cluster-wide operation: each call fans out to every
 * partition on every member. Applications that poll it at high frequency can drive
 * significant load and latency. {@code EmptyMapCache} caches the boolean answer per map
 * name for a configurable <em>max staleness</em> window (default one second). Because the
 * cache itself is a distributed {@code IMap}, a refresh performed by any client is
 * immediately visible to all of them, so the underlying fan-out happens at most about once
 * per map per window across the whole cluster.
 *
 * <p>Typical use:
 * <pre>{@code
 * EmptyMapCache.configure(hazelcastInstance);   // required: the cache lives in the cluster
 * if (EmptyMapCache.isEmpty("MyMap")) { ... }
 * // or, when you already hold the map:
 * if (EmptyMapCache.isEmpty(myMap)) { ... }
 * }</pre>
 *
 * <p>When an entry is stale, refresh is <em>single-flight cluster-wide</em>: one caller
 * acquires the cache key's lock and refreshes while every other caller (on any client)
 * continues to receive the last cached value, keeping the underlying operation count at
 * about one per map per window regardless of how many clients poll.
 *
 * <p>Freshness is judged from a timestamp stamped at refresh time. Across multiple hosts
 * this assumes member clocks are reasonably synchronized (skew should be well under the
 * staleness window); a synchronized clock source such as NTP is sufficient.
 */
public final class EmptyMapCache {

    /** Default maximum staleness applied until {@link #setMaxStaleness} is called. */
    public static final long DEFAULT_MAX_STALENESS_MILLIS = 1_000L;

    /** Default name of the distributed map that holds cached results. */
    public static final String DEFAULT_CACHE_MAP_NAME = "__EmptyMapCache";

    private static volatile HazelcastInstance hazelcastInstance;
    private static volatile String cacheMapName = DEFAULT_CACHE_MAP_NAME;
    private static volatile long maxStalenessMillis = DEFAULT_MAX_STALENESS_MILLIS;
    private static volatile Clock clock = Clock.systemUTC();

    private EmptyMapCache() {
    }

    // ---------------------------------------------------------------------
    // Configuration
    // ---------------------------------------------------------------------

    /**
     * Sets the Hazelcast instance that hosts the shared cache and resolves target maps by
     * name. Required before any {@code isEmpty(...)} call.
     */
    public static void configure(HazelcastInstance instance) {
        configure(instance, DEFAULT_CACHE_MAP_NAME);
    }

    /**
     * Sets the Hazelcast instance and the name of the distributed map used to hold cached
     * results. Use a distinct cache map name to isolate independent caches on one cluster.
     */
    public static void configure(HazelcastInstance instance, String cacheMapName) {
        hazelcastInstance = instance;
        EmptyMapCache.cacheMapName = cacheMapName;
    }

    /** Sets the maximum staleness window. Must be positive. */
    public static void setMaxStaleness(Duration maxStaleness) {
        setMaxStalenessMillis(maxStaleness.toMillis());
    }

    /** Sets the maximum staleness window in milliseconds. Must be positive. */
    public static void setMaxStalenessMillis(long millis) {
        if (millis <= 0) {
            throw new IllegalArgumentException("maxStaleness must be positive: " + millis);
        }
        maxStalenessMillis = millis;
    }

    /** Returns the current maximum staleness window in milliseconds. */
    public static long getMaxStalenessMillis() {
        return maxStalenessMillis;
    }

    /** Drops all cached entries across the cluster. Subsequent calls refresh from the underlying maps. */
    public static void clear() {
        HazelcastInstance instance = hazelcastInstance;
        if (instance != null) {
            instance.getMap(cacheMapName).clear();
        }
    }

    // ---------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------

    /**
     * Returns whether the named map is empty, served from the shared cache when fresh.
     *
     * @throws IllegalStateException if no Hazelcast instance has been configured
     */
    public static boolean isEmpty(String mapName) {
        return lookup(mapName, null);
    }

    /**
     * Returns whether the given map is empty, served from the shared cache when fresh.
     * The supplied map is used directly for any refresh, avoiding a name lookup.
     *
     * @throws IllegalStateException if no Hazelcast instance has been configured
     */
    public static boolean isEmpty(IMap<?, ?> map) {
        return lookup(map.getName(), map);
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private static boolean lookup(String mapName, IMap<?, ?> target) {
        IMap<String, CachedStatus> cache = cacheMap();
        CachedStatus current = cache.get(mapName);
        if (isFresh(current)) {
            return current.empty();
        }
        return refresh(cache, mapName, target, current);
    }

    private static boolean isFresh(CachedStatus status) {
        return status != null && clock.millis() - status.timestampMillis() < maxStalenessMillis;
    }

    private static boolean refresh(IMap<String, CachedStatus> cache, String mapName,
                                   IMap<?, ?> target, CachedStatus stale) {
        // Single-flight cluster-wide: only the holder of the key lock calls through.
        if (cache.tryLock(mapName)) {
            try {
                CachedStatus current = cache.get(mapName);
                if (isFresh(current)) {
                    return current.empty();
                }
                return computeAndStore(cache, mapName, target);
            } finally {
                cache.unlock(mapName);
            }
        }

        // Another client is refreshing. If we have a prior value, serve it (at most one
        // staleness window old). Otherwise this is the first load anywhere, so wait for it.
        if (stale != null) {
            return stale.empty();
        }
        cache.lock(mapName);
        try {
            CachedStatus current = cache.get(mapName);
            if (current != null) {
                return current.empty();
            }
            return computeAndStore(cache, mapName, target);
        } finally {
            cache.unlock(mapName);
        }
    }

    private static boolean computeAndStore(IMap<String, CachedStatus> cache, String mapName, IMap<?, ?> target) {
        boolean empty = resolveTarget(mapName, target).isEmpty();
        cache.set(mapName, new CachedStatus(empty, clock.millis()));
        return empty;
    }

    private static IMap<?, ?> resolveTarget(String mapName, IMap<?, ?> target) {
        if (target != null) {
            return target;
        }
        return requireInstance().getMap(mapName);
    }

    private static IMap<String, CachedStatus> cacheMap() {
        // Every isEmpty() served from cache costs one remote single-key get here. Configuring
        // a near-cache on this map would make repeat reads local (memory speed), recovering
        // most of the throughput a purely local cache had. The trade-off is additional
        // staleness: a near-cache serves its own copy until invalidated, so an entry can be
        // read slightly past our maxStaleness window (bounded by the near-cache's own
        // time-to-live / invalidation settings). Keep the near-cache TTL at or below
        // maxStaleness if that extra slack is unacceptable.
        return requireInstance().getMap(cacheMapName);
    }

    private static HazelcastInstance requireInstance() {
        HazelcastInstance instance = hazelcastInstance;
        if (instance == null) {
            throw new IllegalStateException("No HazelcastInstance configured; call EmptyMapCache.configure(...)");
        }
        return instance;
    }

    /** Package-private hook for deterministic testing of the staleness window. */
    static void setClock(Clock newClock) {
        clock = newClock;
    }

    /** Cached empty-status for one map, stored in the distributed cache map. */
    private record CachedStatus(boolean empty, long timestampMillis) implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
    }
}
