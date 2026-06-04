# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A small utility, `EmptyMapCache`, that throttles calls to Hazelcast `IMap.isEmpty()`.
`isEmpty()` is a cluster-wide operation that fans a partition operation out to every
partition on every member, so polling it thousands of times per second drives heavy load
and latency. `EmptyMapCache` caches the boolean answer per map name for a configurable
*max staleness* window (default 1 s), collapsing high-frequency calls down to roughly one
underlying operation per map per window.

## Project facts

- **Group / artifact:** `com.theyawns` / `MapAPIThrottler`, `1.0-SNAPSHOT`
- **Java:** compiles to release **21** (Hazelcast 5.6 supports LTS JDKs; Java 24 is not an
  officially supported Hazelcast runtime). A JDK 21 is at
  `/Library/Java/JavaVirtualMachines/zulu-21.jdk` — point `JAVA_HOME` there to build/test.
- **Hazelcast:** `5.6.0`. **JUnit 5** for tests.

## Commands

```bash
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-21.jdk/Contents/Home

mvn test                                                   # all tests
mvn -Dtest=EmptyMapCacheTest#throttleReducesUnderlyingOps test   # the load/stats test only
mvn package                                                # build the jar
```

The default `java`/`mvn` on this machine resolve to a newer JDK; **always set `JAVA_HOME` to
JDK 21** before invoking Maven or the embedded Hazelcast cluster may hit reflection issues.

## Architecture notes

- **`EmptyMapCache`** (`src/main/java/.../throttle/`) is a static, process-wide utility.
  `configure(HazelcastInstance)` is only needed for the `isEmpty(String)` overload;
  `isEmpty(IMap)` caches the map reference and needs no configured instance. On a stale or
  missing entry, refresh is **single-flight**: one caller calls through to `IMap.isEmpty()`
  while concurrent callers return the last cached value, keeping underlying ops near one per
  window even under heavy concurrency. A package-private `setClock(Clock)` hook makes the
  staleness window deterministic in tests.
- **Staleness semantics:** an entry is served from cache while
  `now - timestamp < maxStaleness`; otherwise it refreshes. (The original brief stated this
  condition inverted — the implemented behavior is the corrected version.)
- **`EmptyMapCacheTest`** stands up a **3-member embedded cluster with 4999 partitions**
  (the customer's topology) and measures `LocalMapStats.getOtherOperationCount()` summed
  across members — the same counter Hazelcast publishes over JMX — before/after a high-rate
  load, asserting a large *relative* reduction. Because the direct `isEmpty()` path is a slow
  network fan-out, this test is heavier/slower than a unit test (several seconds). The
  Surefire `argLine` in `pom.xml` carries the JDK reflection flags Hazelcast needs.

## Conventions

- This is customer-facing code for an AI-averse customer: **no references to AI/Claude** in
  code, comments, or commit messages, and no `Co-Authored-By` trailers.
