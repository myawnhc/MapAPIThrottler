package com.theyawns.throttle;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises {@link EmptyMapCache} against a 3-member cluster configured with 4999
 * partitions, mirroring the customer's topology.
 */
class EmptyMapCacheTest {

    private static final int MEMBER_COUNT = 3;
    private static final int PARTITION_COUNT = 4999;
    private static final String CLUSTER_NAME = "empty-map-cache-test";

    private static final int LOAD_THREADS = 4;
    private static final long LOAD_DURATION_MILLIS = 2_000L;
    private static final long STALENESS_MILLIS = 1_000L;

    private static final List<HazelcastInstance> CLUSTER = new ArrayList<>();

    @BeforeAll
    static void startCluster() {
        for (int i = 0; i < MEMBER_COUNT; i++) {
            CLUSTER.add(Hazelcast.newHazelcastInstance(memberConfig()));
        }
        awaitClusterFormed();
    }

    @AfterAll
    static void stopCluster() {
        Hazelcast.shutdownAll();
        CLUSTER.clear();
    }

    @AfterEach
    void resetCacheState() {
        EmptyMapCache.setClock(Clock.systemUTC());
        EmptyMapCache.setMaxStalenessMillis(EmptyMapCache.DEFAULT_MAX_STALENESS_MILLIS);
        EmptyMapCache.clear();
    }

    /**
     * Drives {@code isEmpty()} at high frequency, first directly against the map and then
     * through {@link EmptyMapCache}, and compares the operation count the cluster recorded
     * for the underlying map in each phase.
     */
    @Test
    void throttleReducesUnderlyingOps() {
        HazelcastInstance hz = CLUSTER.get(0);
        IMap<String, String> map = hz.getMap("throttle-load");
        map.clear(); // ensure empty: isEmpty() must then fan out to every partition

        EmptyMapCache.configure(hz);
        EmptyMapCache.setMaxStalenessMillis(STALENESS_MILLIS);
        EmptyMapCache.clear();

        // Baseline: every user call reaches the map.
        long baselineBefore = totalOtherOps(map.getName());
        long baselineCalls = hammer(map::isEmpty);
        long baselineOps = totalOtherOps(map.getName()) - baselineBefore;

        // Throttled: identical load, but served from the staleness-bounded cache.
        long throttledBefore = totalOtherOps(map.getName());
        long throttledCalls = hammer(() -> EmptyMapCache.isEmpty(map));
        long throttledOps = totalOtherOps(map.getName()) - throttledBefore;

        // The per-call operation factor is identical in both phases, so the ratio of
        // recorded ops equals the ratio of calls that actually reached the map.
        double reductionFactor = (double) baselineOps / Math.max(1, throttledOps);
        double impliedUnderlyingCalls = throttledOps / Math.max(1.0, (double) baselineOps / baselineCalls);

        System.out.printf(
                "baseline: %,d calls -> %,d ops%n"
                        + "throttled: %,d calls -> %,d ops (~%.1f underlying calls)%n"
                        + "reduction: %.0fx fewer underlying operations%n",
                baselineCalls, baselineOps, throttledCalls, throttledOps,
                impliedUnderlyingCalls, reductionFactor);

        assertTrue(baselineOps > 0,
                "expected the direct isEmpty() load to register operations on the map");
        assertTrue(throttledCalls >= baselineCalls / 2,
                "throttled API should service a comparable volume of user calls");
        assertTrue(reductionFactor >= 50.0,
                "expected a large reduction in underlying operations, got " + reductionFactor + "x");
    }

    /**
     * Deterministic check of the staleness contract using an injected clock: a value is
     * served from cache within the window and refreshed once the window elapses.
     */
    @Test
    void servesFromCacheWithinWindowAndRefreshesAfter() {
        HazelcastInstance hz = CLUSTER.get(0);
        IMap<String, String> map = hz.getMap("throttle-window");
        map.clear();

        MutableClock clock = new MutableClock(1_000_000L);
        EmptyMapCache.configure(hz);
        EmptyMapCache.setClock(clock);
        EmptyMapCache.setMaxStalenessMillis(STALENESS_MILLIS);
        EmptyMapCache.clear();

        assertTrue(EmptyMapCache.isEmpty(map), "empty map should report empty");

        // Map becomes non-empty, but within the staleness window the cache still says empty.
        map.put("k", "v");
        clock.advanceMillis(STALENESS_MILLIS - 1);
        assertTrue(EmptyMapCache.isEmpty(map), "value within window must come from cache");

        // Once the window elapses, the next call refreshes and observes the new state.
        clock.advanceMillis(1);
        assertFalse(EmptyMapCache.isEmpty(map), "after the window the cache must refresh");
    }

    /** The String and IMap overloads must agree, and String must work once configured. */
    @Test
    void stringAndMapOverloadsAgree() {
        HazelcastInstance hz = CLUSTER.get(0);
        IMap<String, String> map = hz.getMap("throttle-overloads");
        map.clear();
        EmptyMapCache.configure(hz);
        EmptyMapCache.clear();

        assertEquals(map.isEmpty(), EmptyMapCache.isEmpty(map.getName()));
        assertEquals(EmptyMapCache.isEmpty(map.getName()), EmptyMapCache.isEmpty(map));
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /** Sums the "other operations" counter (size/isEmpty/etc.) across every member. */
    private static long totalOtherOps(String mapName) {
        long total = 0;
        for (HazelcastInstance member : CLUSTER) {
            // Same data Hazelcast publishes over JMX (type=IMap, attribute otherOperationCount).
            total += member.<String, String>getMap(mapName).getLocalMapStats().getOtherOperationCount();
        }
        return total;
    }

    /** Runs {@code call} from {@link #LOAD_THREADS} threads for the load duration; returns total calls. */
    private static long hammer(Runnable call) {
        LongAdder calls = new LongAdder();
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < LOAD_THREADS; i++) {
            Thread t = new Thread(() -> {
                while (!stop.get()) {
                    call.run();
                    calls.increment();
                }
            });
            threads.add(t);
            t.start();
        }
        try {
            Thread.sleep(LOAD_DURATION_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        stop.set(true);
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return calls.sum();
    }

    private static Config memberConfig() {
        Config config = new Config();
        config.setClusterName(CLUSTER_NAME);
        config.setProperty("hazelcast.partition.count", String.valueOf(PARTITION_COUNT));
        config.setProperty("hazelcast.logging.type", "none");

        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true).setMembers(List.of("127.0.0.1"));
        return config;
    }

    private static void awaitClusterFormed() {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MINUTES.toNanos(2);
        while (System.nanoTime() < deadline) {
            boolean ready = CLUSTER.stream()
                    .allMatch(m -> m.getCluster().getMembers().size() == MEMBER_COUNT)
                    && CLUSTER.get(0).getPartitionService().isClusterSafe();
            if (ready) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for cluster", e);
            }
        }
        throw new IllegalStateException("cluster of " + MEMBER_COUNT + " members did not form in time");
    }

    /** A clock whose epoch-millis value is advanced explicitly by the test. */
    private static final class MutableClock extends Clock {
        private volatile long millis;

        MutableClock(long startMillis) {
            this.millis = startMillis;
        }

        void advanceMillis(long delta) {
            millis += delta;
        }

        @Override
        public long millis() {
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }
}
