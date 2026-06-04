# MapAPIThrottler

A small utility, `EmptyMapCache`, that throttles calls to Hazelcast
[`IMap.isEmpty()`](https://docs.hazelcast.com/).

## Why

`IMap.isEmpty()` is a cluster-wide operation: each call fans a partition operation out to
every partition on every member. Polling it thousands of times per second therefore drives
heavy load and latency — especially on large clusters (e.g. thousands of partitions across
several nodes).

`EmptyMapCache` caches the empty / not-empty answer per map name for a configurable
**max staleness** window (default one second). A burst of thousands of calls per second
collapses to roughly **one** underlying `isEmpty()` per map per window. The cache is a
**distributed Hazelcast `IMap`**, so a refresh performed by any client is shared by every
client in the cluster — the fan-out is amortized cluster-wide, not just per JVM.

## Usage

```java
// Once, at startup — the cache lives in the cluster, so an instance is required:
EmptyMapCache.configure(hazelcastInstance);
EmptyMapCache.setMaxStaleness(Duration.ofSeconds(1)); // optional; this is the default

// Then, anywhere, at any frequency:
if (EmptyMapCache.isEmpty("MyMap")) {
    // ...
}

// An IMap may be passed directly; it is used as-is for any refresh:
if (EmptyMapCache.isEmpty(myMap)) {
    // ...
}
```

### Behavior

- **Single-flight, cluster-wide.** When an entry is stale, one caller takes the cache key's
  lock (`IMap.tryLock`) and refreshes while every other caller — on any client — returns the
  last cached value. The underlying operation count stays near one per map per window no
  matter how many clients poll.
- **Staleness.** An entry is served from cache while `now - timestamp < maxStaleness`;
  otherwise it is refreshed. Freshness is a timestamp comparison, so member clocks should be
  reasonably synchronized (NTP; skew well under the staleness window).
- **Configuration.** `configure(instance)` or `configure(instance, cacheMapName)` to isolate
  independent caches; `setMaxStaleness(Duration)` / `setMaxStalenessMillis(long)`; `clear()`.

### Tuning: near-cache

Each cached call costs one remote single-key `get` on the cache map. Enabling a Hazelcast
**near-cache** on the cache map (`__EmptyMapCache` by default) makes repeat reads local
(memory speed). The trade-off is additional staleness — a near-cache serves its own copy
until invalidated — so keep its time-to-live at or below `maxStaleness` if that extra slack
matters.

## Build and test

Requires a JDK 21 (Hazelcast 5.6 supports LTS JDKs). Build with Maven:

```bash
mvn test       # run the tests
mvn package    # build the jar
```

The test suite stands up a 3-member embedded cluster with 4999 partitions and drives
`isEmpty()` at high frequency directly and through `EmptyMapCache`, then compares the
operation count recorded for the underlying map (via `LocalMapStats`) — demonstrating a
large reduction (hundreds-fold) in underlying operations.

## Requirements

- Java 21+
- Hazelcast 5.6
