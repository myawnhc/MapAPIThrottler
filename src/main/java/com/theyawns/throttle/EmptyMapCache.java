package com.theyawns.throttle;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A throttling cache in front of {@link IMap#isEmpty()}.
 *
 * <p>{@code IMap.isEmpty()} is a cluster-wide operation: each call fans out to every
 * partition on every member. Applications that poll it at high frequency can drive
 * significant load and latency. {@code EmptyMapCache} caches the boolean answer per map
 * name for a configurable <em>max staleness</em> window (default one second), so a burst
 * of thousands of calls per second collapses to roughly one underlying call per window.
 *
 * <p>Typical use:
 * <pre>{@code
 * EmptyMapCache.configure(hazelcastInstance);   // needed only for the String overload
 * if (EmptyMapCache.isEmpty("MyMap")) { ... }
 * // or, when you already hold the map:
 * if (EmptyMapCache.isEmpty(myMap)) { ... }
 * }</pre>
 *
 * <p>The cache is shared process-wide via static state and is safe for concurrent use.
 * When an entry is stale, a single caller refreshes it ("single-flight") while other
 * callers continue to receive the last cached value, so the underlying operation count
 * stays at about one per map per staleness window regardless of call concurrency.
 */
public final class EmptyMapCache {

    /** Default maximum staleness applied until {@link #setMaxStaleness} is called. */
    public static final long DEFAULT_MAX_STALENESS_MILLIS = 1_000L;

    private static final ConcurrentHashMap<String, CachedValue> CACHE = new ConcurrentHashMap<>();

    private static volatile HazelcastInstance hazelcastInstance;
    private static volatile long maxStalenessMillis = DEFAULT_MAX_STALENESS_MILLIS;
    private static volatile Clock clock = Clock.systemUTC();

    private EmptyMapCache() {
    }

    // ---------------------------------------------------------------------
    // Configuration
    // ---------------------------------------------------------------------

    /**
     * Sets the Hazelcast instance used to resolve a map by name in {@link #isEmpty(String)}.
     * Not required if every call passes an {@link IMap}, since the map reference is then
     * cached and reused for refreshes.
     */
    public static void configure(HazelcastInstance instance) {
        hazelcastInstance = instance;
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

    /** Drops all cached entries. Subsequent calls refresh from the underlying maps. */
    public static void clear() {
        CACHE.clear();
    }

    // ---------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------

    /**
     * Returns whether the named map is empty, served from cache when fresh.
     *
     * @throws IllegalStateException if no Hazelcast instance has been configured and the
     *                               map has not previously been seen via {@link #isEmpty(IMap)}
     */
    public static boolean isEmpty(String mapName) {
        return lookup(mapName, null);
    }

    /**
     * Returns whether the given map is empty, served from cache when fresh.
     * The map reference is remembered so later refreshes (and {@link #isEmpty(String)}
     * calls for the same name) do not require a configured instance.
     */
    public static boolean isEmpty(IMap<?, ?> map) {
        return lookup(map.getName(), map);
    }

    // ---------------------------------------------------------------------
    // Internals
    // ---------------------------------------------------------------------

    private static boolean lookup(String mapName, IMap<?, ?> map) {
        CachedValue entry = CACHE.computeIfAbsent(mapName, name -> new CachedValue());
        if (map != null) {
            entry.map = map;
        }

        if (entry.initialized && !isStale(entry)) {
            return entry.empty;
        }
        return refresh(mapName, entry);
    }

    private static boolean isStale(CachedValue entry) {
        return clock.millis() - entry.timestampMillis >= maxStalenessMillis;
    }

    private static boolean refresh(String mapName, CachedValue entry) {
        if (entry.refreshing.compareAndSet(false, true)) {
            try {
                boolean empty = resolveMap(mapName, entry).isEmpty();
                entry.empty = empty;
                entry.timestampMillis = clock.millis();
                entry.initialized = true;
                return empty;
            } finally {
                entry.refreshing.set(false);
            }
        }

        // Another caller is refreshing. If we already have a value, return it (it is at
        // most one staleness window old by construction). On the very first load there is
        // no value yet, so wait for the winner to publish one.
        while (!entry.initialized) {
            Thread.onSpinWait();
        }
        return entry.empty;
    }

    private static IMap<?, ?> resolveMap(String mapName, CachedValue entry) {
        IMap<?, ?> map = entry.map;
        if (map != null) {
            return map;
        }
        HazelcastInstance instance = hazelcastInstance;
        if (instance == null) {
            throw new IllegalStateException(
                    "No HazelcastInstance configured; call EmptyMapCache.configure(...) "
                            + "or invoke isEmpty(IMap) at least once for map '" + mapName + "'");
        }
        map = instance.getMap(mapName);
        entry.map = map;
        return map;
    }

    /** Package-private hook for deterministic testing of the staleness window. */
    static void setClock(Clock newClock) {
        clock = newClock;
    }

    private static final class CachedValue {
        private final AtomicBoolean refreshing = new AtomicBoolean(false);
        private volatile boolean initialized;
        private volatile boolean empty;
        private volatile long timestampMillis;
        private volatile IMap<?, ?> map;
    }
}
