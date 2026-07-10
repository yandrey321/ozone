/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.checksum;

import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientReply;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Benchmark for ECFileChecksumHelper that measures the actual checksum
 * collection code path. Not part of the regular test suite — it takes ~2min
 * per run (warmup + measurement across two EC configs × three latency buckets).
 *
 * Validates fixes to BaseFileChecksumHelper and ECFileChecksumHelper:
 *   Fix 1: BaseFileChecksumHelper 7-arg constructor no longer chains to 6-arg
 *           (redundant OM lookupKey RPC eliminated: 2 calls/file -> 1).
 *           Measured by runBenchmark.
 *   Fix 2+3 combined: ECFileChecksumHelper caches the STANDALONE checksum
 *           pipeline per EC placement group (Fix 3 intra-file reuse) and
 *           derives its ID deterministically from the sorted node UUIDs
 *           (Fix 2 cross-file reuse). Measured by runBlockCacheBenchmark.
 *
 * Covers RS-3-2 (5 nodes, 3 data + 2 parity) and RS-6-3 (9 nodes, 6 data + 3 parity).
 * For RS-3-2 the standalone pipeline has 3 selected nodes (index=1 + parity {4,5}),
 * stripe checksum = 12 bytes. For RS-6-3: 4 selected nodes (index=1 + parity {7,8,9}),
 * stripe checksum = 16 bytes.
 *
 * Each latency bucket runs a warmup followed by a 20-second measurement window.
 * Throughput and per-file RPC counts are reported from the measurement window only.
 *
 * Run with:
 *   mvn test -pl hadoop-ozone/client \
 *     -Dtest=FileChecksumBenchmark#runBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dsurefire.fork.timeout=3600
 *
 *   mvn test -pl hadoop-ozone/client \
 *     -Dtest=FileChecksumBenchmark#runBlockCacheBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false \
 *     -Dsurefire.fork.timeout=3600
 *
 * To profile with async-profiler, pass the agent via surefire argLine, e.g.:
 *   mvn test -pl hadoop-ozone/client \
 *     -Dtest=FileChecksumBenchmark#runBenchmark \
 *     -Dsurefire.failIfNoSpecifiedTests=false \
 *     -DargLine="-agentpath:/opt/homebrew/lib/libasyncProfiler.dylib=start,event=wall,\
 *       interval=10ms,file=/tmp/profile.html"
 */
@Tag("benchmark")
public class FileChecksumBenchmark {

  private static final int NUM_THREADS = 5;
  private static final int WARMUP_SECS = 10;
  private static final int MEASURE_SECS = 20;
  private static final int[] LATENCIES_MS = {0, 5, 10};
  private static final int[] BLOCKS_PER_FILE_VARIANTS = {1, 3, 5, 10};
  private static final long CONTAINER_ID = 1001L;
  private static final long FILE_SIZE = 5000L;
  // 136 = 4 x 34: every 4th file (idx % 4 == 0) gets a freshly built OmKeyInfo
  // with random node UUIDs on each call, making the deterministic pipeline ID
  // also effectively random -> guaranteed cache miss for those files.
  // Max theoretical cache hit rate = 75% (102 stable / 136 total).
  private static final int KEY_POOL = 136;
  private static final int UNSTABLE_STEP = 4;

  private static final ECReplicationConfig EC32 = new ECReplicationConfig(3, 2);
  private static final ECReplicationConfig EC63 = new ECReplicationConfig(6, 3);

  // RS-3-2: selected nodes = index 1 + parity {4, 5} = 3 nodes → 3 × 4 = 12 stripe bytes
  private static final List<DatanodeDetails> EC_NODES = buildEcNodes(5);
  private static final Pipeline EC_PIPELINE = buildEcPipeline(EC_NODES, EC32);
  private static final OmKeyInfo[] KEY_INFOS = buildKeyInfos(EC_PIPELINE, EC32);
  private static final ContainerProtos.ContainerCommandResponseProto GET_BLOCK_RESPONSE =
      buildGetBlockResponse(12);

  // RS-6-3: selected nodes = index 1 + parity {7, 8, 9} = 4 nodes → 4 × 4 = 16 stripe bytes
  private static final List<DatanodeDetails> EC63_NODES = buildEcNodes(9);
  private static final Pipeline EC63_PIPELINE = buildEcPipeline(EC63_NODES, EC63);
  private static final OmKeyInfo[] EC63_KEY_INFOS = buildKeyInfos(EC63_PIPELINE, EC63);
  private static final ContainerProtos.ContainerCommandResponseProto EC63_GET_BLOCK_RESPONSE =
      buildGetBlockResponse(16);

  // 15-node cluster pool for runBlockCacheBenchmark. Both scenarios draw from this pool.
  // RS-3-2 draws 5 from 15 → C(15,5)=3,003 distinct placement groups.
  // RS-6-3 draws 9 from 15 → C(15,9)=5,005 distinct placement groups.
  // Fix 2 ensures the same node subset maps to the same standalone pipeline ID,
  // so the XceiverClient pool is fully warmed after warmup (≈ 0 new connections/file).
  private static final List<DatanodeDetails> EC15_NODES = buildEcNodes(15);

  // ---------------------------------------------------------------------------
  // Instrumented XceiverClientFactory
  // ---------------------------------------------------------------------------

  static class CountingXceiverClientFactory implements XceiverClientFactory {
    // Both scenarios use the 15-node pool, so the pool converges to at most C(15,9)=5,005
    // entries (all placement groups covered during warmup). 2M is a safe upper bound.
    private static final int MAX_POOL_SIZE = 2_000_000;
    private final ContainerProtos.ContainerCommandResponseProto blockResponse;
    private final int dnConnectionLatencyMs;
    private final int maxPoolSize;
    private final AtomicLong newConnectionCount = new AtomicLong();
    private final AtomicLong reuseCount = new AtomicLong();
    // clientPool is intentionally NOT cleared between warmup and measurement:
    // simulates a warmed-up JVM where connections established during warmup
    // remain available -- the same state as a long-running service.
    private final ConcurrentHashMap<String, XceiverClientSpi> clientPool =
        new ConcurrentHashMap<>();

    CountingXceiverClientFactory(
        ContainerProtos.ContainerCommandResponseProto blockResponse) {
      this(blockResponse, 0, MAX_POOL_SIZE);
    }

    CountingXceiverClientFactory(ContainerProtos.ContainerCommandResponseProto blockResponse,
        int dnConnectionLatencyMs, int maxPoolSize) {
      this.blockResponse = blockResponse;
      this.dnConnectionLatencyMs = dnConnectionLatencyMs;
      this.maxPoolSize = maxPoolSize;
    }

    void resetCounters() {
      newConnectionCount.set(0);
      reuseCount.set(0);
    }

    long getNewConnectionCount() {
      return newConnectionCount.get();
    }

    long getReuseCount() {
      return reuseCount.get();
    }

    @Override
    public XceiverClientSpi acquireClientForReadData(Pipeline pipeline)
        throws IOException {
      String key = pipeline.getId().toString();
      XceiverClientSpi existing = clientPool.get(key);
      if (existing != null) {
        reuseCount.incrementAndGet();
        return existing;
      }
      if (dnConnectionLatencyMs > 0) {
        try {
          Thread.sleep(dnConnectionLatencyMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException(ie);
        }
      }
      XceiverClientSpi newClient = createMockDnClient(pipeline, blockResponse);
      if (clientPool.size() < maxPoolSize) {
        XceiverClientSpi winner = clientPool.putIfAbsent(key, newClient);
        if (winner != null) {
          reuseCount.incrementAndGet();
          return winner;
        }
      }
      newConnectionCount.incrementAndGet();
      return newClient;
    }

    @Override
    public void releaseClientForReadData(XceiverClientSpi client,
        boolean invalidate) { }

    @Override
    public XceiverClientSpi acquireClient(Pipeline pipeline) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidate) { }

    @Override
    public XceiverClientSpi acquireClient(Pipeline pipeline,
        boolean topologyAware) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseClient(XceiverClientSpi client, boolean invalidate,
        boolean topologyAware) { }

    @Override
    public void close() { }
  }

  // ---------------------------------------------------------------------------
  // Benchmark result
  // ---------------------------------------------------------------------------

  static class BenchmarkResult {
    private final long measuredFiles;
    private final long wallMs;
    private final long omCalls;
    private final long xceiverNewCount;
    private final long xceiverReuseCount;
    private final int latencyMs;

    BenchmarkResult(long measuredFiles, long wallMs, long omCalls,
        long xceiverNewCount, long xceiverReuseCount, int latencyMs) {
      this.measuredFiles = measuredFiles;
      this.wallMs = wallMs;
      this.omCalls = omCalls;
      this.xceiverNewCount = xceiverNewCount;
      this.xceiverReuseCount = xceiverReuseCount;
      this.latencyMs = latencyMs;
    }

    long getMeasuredFiles() {
      return measuredFiles;
    }

    long getWallMs() {
      return wallMs;
    }

    long getOmCalls() {
      return omCalls;
    }

    long getXceiverNewCount() {
      return xceiverNewCount;
    }

    long getXceiverReuseCount() {
      return xceiverReuseCount;
    }

    int getLatencyMs() {
      return latencyMs;
    }

    double filesPerSec() {
      return wallMs == 0
          ? Double.POSITIVE_INFINITY : measuredFiles * 1000.0 / wallMs;
    }

    double omCallsPerFile() {
      return measuredFiles == 0 ? 0 : (double) omCalls / measuredFiles;
    }

    long cacheHitPercent() {
      long total = xceiverNewCount + xceiverReuseCount;
      return total == 0 ? 0 : xceiverReuseCount * 100 / total;
    }
  }

  // ---------------------------------------------------------------------------
  // Core runner
  // ---------------------------------------------------------------------------

  private static BenchmarkResult measure(int latencyMs, ECReplicationConfig repConfig,
      OmKeyInfo[] keyPool, ContainerProtos.ContainerCommandResponseProto blockResponse)
      throws Exception {
    AtomicLong omCallCount = new AtomicLong();
    CountingXceiverClientFactory xceiverFactory =
        new CountingXceiverClientFactory(blockResponse);
    AtomicLong fileIdx = new AtomicLong();

    OzoneManagerProtocol mockOm = mock(OzoneManagerProtocol.class, withSettings().stubOnly());
    when(mockOm.lookupKey(any(OmKeyArgs.class))).thenAnswer(invocation -> {
      omCallCount.incrementAndGet();
      if (latencyMs > 0) {
        try {
          Thread.sleep(latencyMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException(ie);
        }
      }
      OmKeyArgs args = invocation.getArgument(0);
      int idx = Integer.parseInt(args.getKeyName().split("-")[1]);
      // Every UNSTABLE_STEP-th file returns a freshly built OmKeyInfo with new
      // random node UUIDs. Fix 2 computes its deterministic pipeline ID from
      // those node UUIDs, so the result also changes each call -> cache miss.
      if (idx % UNSTABLE_STEP == 0) {
        return buildRandomKeyInfo(args.getKeyName(), repConfig);
      }
      return keyPool[idx];
    });

    RpcClient mockRpcClient = mock(RpcClient.class, withSettings().stubOnly());
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(mockOm);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(xceiverFactory);

    OzoneVolume mockVolume = mock(OzoneVolume.class, withSettings().stubOnly());
    when(mockVolume.getName()).thenReturn("vol");
    OzoneBucket mockBucket = mock(OzoneBucket.class, withSettings().stubOnly());
    when(mockBucket.getName()).thenReturn("bucket");

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;

    Runnable task = () -> {
      try {
        int idx = (int) (fileIdx.getAndIncrement() % KEY_POOL);
        String keyName = "file-" + idx;
        OmKeyInfo keyInfo = mockRpcClient.getOzoneManagerClient().lookupKey(
            new OmKeyArgs.Builder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName(keyName)
                .setSortDatanodesInPipeline(true)
                .setLatestVersionLocation(true)
                .build());
        new ECFileChecksumHelper(
            mockVolume, mockBucket, keyName, FILE_SIZE, combineMode,
            mockRpcClient, keyInfo)
            .compute();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    // Warmup: establish connections, fill JIT caches, discard counts.
    runFor(WARMUP_SECS * 1000L, task);
    omCallCount.set(0);
    xceiverFactory.resetCounters();
    fileIdx.set(0);

    // Measurement window.
    long start = System.currentTimeMillis();
    long measuredFiles = runFor(MEASURE_SECS * 1000L, task);
    long wallMs = System.currentTimeMillis() - start;

    return new BenchmarkResult(measuredFiles, wallMs, omCallCount.get(),
        xceiverFactory.getNewConnectionCount(), xceiverFactory.getReuseCount(),
        latencyMs);
  }

  /**
   * @param keyInfoSupplier  called once per lookupKey invocation to produce a fresh OmKeyInfo;
   *                         must be thread-safe (called concurrently by {@link #NUM_THREADS}).
   */
  private static BenchmarkResult measureBlockCache(int blocksPerFile, int dnLatencyMs,
      Supplier<OmKeyInfo> keyInfoSupplier,
      ContainerProtos.ContainerCommandResponseProto blockResponse)
      throws Exception {
    AtomicLong omCallCount = new AtomicLong();
    CountingXceiverClientFactory xceiverFactory = new CountingXceiverClientFactory(
        blockResponse, dnLatencyMs, CountingXceiverClientFactory.MAX_POOL_SIZE);

    long fileLength = (long) blocksPerFile * FILE_SIZE;

    OzoneManagerProtocol mockOm = mock(OzoneManagerProtocol.class, withSettings().stubOnly());
    when(mockOm.lookupKey(any(OmKeyArgs.class))).thenAnswer(invocation -> {
      omCallCount.incrementAndGet();
      return keyInfoSupplier.get();
    });

    RpcClient mockRpcClient = mock(RpcClient.class, withSettings().stubOnly());
    when(mockRpcClient.getOzoneManagerClient()).thenReturn(mockOm);
    when(mockRpcClient.getXceiverClientManager()).thenReturn(xceiverFactory);

    OzoneVolume mockVolume = mock(OzoneVolume.class, withSettings().stubOnly());
    when(mockVolume.getName()).thenReturn("vol");
    OzoneBucket mockBucket = mock(OzoneBucket.class, withSettings().stubOnly());
    when(mockBucket.getName()).thenReturn("bucket");

    OzoneClientConfig.ChecksumCombineMode combineMode =
        OzoneClientConfig.ChecksumCombineMode.COMPOSITE_CRC;

    Runnable task = () -> {
      try {
        OmKeyInfo keyInfo = mockRpcClient.getOzoneManagerClient().lookupKey(
            new OmKeyArgs.Builder()
                .setVolumeName("vol")
                .setBucketName("bucket")
                .setKeyName("file")
                .setSortDatanodesInPipeline(true)
                .setLatestVersionLocation(true)
                .build());
        new ECFileChecksumHelper(
            mockVolume, mockBucket, keyInfo.getKeyName(), fileLength, combineMode,
            mockRpcClient, keyInfo)
            .compute();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    };

    runFor(WARMUP_SECS * 1000L, task);
    omCallCount.set(0);
    xceiverFactory.resetCounters();

    long start = System.currentTimeMillis();
    long measuredFiles = runFor(MEASURE_SECS * 1000L, task);
    long wallMs = System.currentTimeMillis() - start;

    return new BenchmarkResult(measuredFiles, wallMs, omCallCount.get(),
        xceiverFactory.getNewConnectionCount(), xceiverFactory.getReuseCount(), dnLatencyMs);
  }

  private static long runFor(long durationMs, Runnable task) throws Exception {
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicLong count = new AtomicLong();
    ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
    List<Future<?>> futures = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS; i++) {
      futures.add(executor.submit((Callable<Void>) () -> {
        while (running.get()) {
          task.run();
          count.incrementAndGet();
        }
        return null;
      }));
    }
    Thread.sleep(durationMs);
    running.set(false);
    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    return count.get();
  }

  // ---------------------------------------------------------------------------
  // JUnit entry point
  // ---------------------------------------------------------------------------

  @Test
  public void runBenchmark() throws Exception {
    System.out.println();
    System.out.println("=== ECFileChecksum Collection Benchmark ===");
    System.out.printf("Workload: %d threads, %ds warmup + %ds measurement per config%n%n",
        NUM_THREADS, WARMUP_SECS, MEASURE_SECS);

    String header = String.format(
        "%-10s  %-10s  %-10s  %-10s  %-12s  %-10s  %-14s  %-14s  %-9s",
        "Latency", "Wall(ms)", "Files", "Files/s", "OM calls", "OM/file",
        "XcNew(conn)", "XcReuse", "CacheHit%");
    String rule = new String(new char[105]).replace('\0', '-');

    System.out.println("--- RS-3-2 (3 data + 2 parity, 5 nodes) ---");
    System.out.println(header);
    System.out.println(rule);
    for (int latencyMs : LATENCIES_MS) {
      printRow(measure(latencyMs, EC32, KEY_INFOS, GET_BLOCK_RESPONSE));
    }

    System.out.println();
    System.out.println("--- RS-6-3 (6 data + 3 parity, 9 nodes) ---");
    System.out.println(header);
    System.out.println(rule);
    for (int latencyMs : LATENCIES_MS) {
      printRow(measure(latencyMs, EC63, EC63_KEY_INFOS, EC63_GET_BLOCK_RESPONSE));
    }

  }

  /**
   * Compares Fix 1+2 (no intra-file cache) against Fix 1+2+3 (with intra-file cache)
   * for multi-block EC file checksum collection. Both scenarios use the same 15-node
   * cluster pool and the same random-placement file generation, so the only variable
   * is whether Fix 3 (checksumPipelineCache per ECFileChecksumHelper instance) is active.
   *
   * Scenario A — Fix 1+2 only (no intra-file pipeline cache):
   *   Each file uses the same randomly-selected node subset, but every block in the
   *   file carries a distinct EC pipeline ID (PipelineID.randomId() per block). The
   *   checksumPipelineCache in ECFileChecksumHelper sees N distinct IDs → N
   *   buildChecksumPipeline calls per file. Fix 2 is still active: each call derives
   *   the same deterministic standalone pipeline ID from the sorted node UUIDs, so
   *   the XceiverClient pool is reused across files after warmup.
   *   Expected: NewConn/file ≈ 0.00, CacheHit% = 0% for all block counts.
   *
   * Scenario B — Fix 1+2+3 combined (intra-file + cross-file reuse):
   *   All N blocks in each file share one EC pipeline ID. The checksumPipelineCache
   *   fires for blocks 2..N → 1 buildChecksumPipeline call per file instead of N.
   *   Fix 2 continues to give cross-file pool reuse.
   *   Expected: NewConn/file ≈ 0.00, CacheHit% = (N-1)/N for N-block files.
   *
   * Throughput difference reflects the overhead of N vs 1 buildChecksumPipeline
   * calls per file. At 0ms DN latency this overhead dominates; at higher latencies
   * the DN sleep cost is amortized across connections and the Fix 3 gain is
   * proportionally smaller.
   */
  @Test
  @Timeout(value = 2400, unit = TimeUnit.SECONDS)
  public void runBlockCacheBenchmark() throws Exception {
    System.out.println();
    System.out.println("=== ECFileChecksum Per-Block Pipeline Cache Benchmark ===");
    System.out.printf("Workload: %d threads, %ds warmup + %ds measurement%n%n",
        NUM_THREADS, WARMUP_SECS, MEASURE_SECS);

    String[] ecLabels = {"RS-3-2", "RS-6-3"};
    ECReplicationConfig[] ecConfigs = {EC32, EC63};
    ContainerProtos.ContainerCommandResponseProto[] blockResponses =
        {GET_BLOCK_RESPONSE, EC63_GET_BLOCK_RESPONSE};

    String rowFmt = "%-6d  %-10d  %-10.1f  %-13.2f  %-11d  %-9d  %-9s";
    String hdr = String.format("%-6s  %-10s  %-10s  %-13s  %-11s  %-9s  %-9s",
        "Blk/f", "Wall(ms)", "Files/s", "NewConn/file", "XcNew", "XcReuse", "CacheHit%");
    String rule = new String(new char[hdr.length()]).replace('\0', '-');

    for (int ecIdx = 0; ecIdx < ecLabels.length; ecIdx++) {
      System.out.printf("=== %s ===%n", ecLabels[ecIdx]);
      ECReplicationConfig ecConfig = ecConfigs[ecIdx];
      ContainerProtos.ContainerCommandResponseProto blockResponse = blockResponses[ecIdx];

      for (int dnLatencyMs : LATENCIES_MS) {
        System.out.printf("DN latency = %dms%n", dnLatencyMs);

        System.out.println("  [A] Fix 1+2 only (no intra-file cache): N buildChecksumPipeline calls/file");
        System.out.println("  " + hdr);
        System.out.println("  " + rule);
        for (int blocksPerFile : BLOCKS_PER_FILE_VARIANTS) {
          int bpf = blocksPerFile;
          BenchmarkResult r = measureBlockCache(blocksPerFile, dnLatencyMs,
              () -> buildSharedClusterKeyInfoMultiBlock("file", EC15_NODES, ecConfig, bpf, false),
              blockResponse);
          printBlockCacheRow(rowFmt, blocksPerFile, r);
        }

        System.out.println();
        System.out.println("  [B] Fix 1+2+3 (intra-file cache): 1 buildChecksumPipeline call/file");
        System.out.println("  " + hdr);
        System.out.println("  " + rule);
        for (int blocksPerFile : BLOCKS_PER_FILE_VARIANTS) {
          int bpf = blocksPerFile;
          BenchmarkResult r = measureBlockCache(blocksPerFile, dnLatencyMs,
              () -> buildSharedClusterKeyInfoMultiBlock("file", EC15_NODES, ecConfig, bpf, true),
              blockResponse);
          printBlockCacheRow(rowFmt, blocksPerFile, r);
        }
        System.out.println();
      }
    }
  }

  private static void printBlockCacheRow(String fmt, int blocksPerFile, BenchmarkResult r) {
    double newConnPerFile = r.getMeasuredFiles() == 0
        ? 0 : (double) r.getXceiverNewCount() / r.getMeasuredFiles();
    System.out.printf("  " + fmt + "%n",
        blocksPerFile, r.getWallMs(), r.filesPerSec(), newConnPerFile,
        r.getXceiverNewCount(), r.getXceiverReuseCount(),
        r.cacheHitPercent() + "%");
  }

  private static void printRow(BenchmarkResult r) {
    System.out.printf(
        "%-10s  %-10d  %-10d  %-10.1f  %-12d  %-10.2f  %-14d  %-14d  %d%%%n",
        r.getLatencyMs() + "ms",
        r.getWallMs(),
        r.getMeasuredFiles(),
        r.filesPerSec(),
        r.getOmCalls(),
        r.omCallsPerFile(),
        r.getXceiverNewCount(),
        r.getXceiverReuseCount(),
        r.cacheHitPercent());
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static List<DatanodeDetails> buildEcNodes(int count) {
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      nodes.add(DatanodeDetails.newBuilder()
          .setUuid(UUID.fromString(String.format("00000000-0000-0000-0000-%012d", i)))
          .setHostName("dn" + i)
          .setIpAddress("10.0.0." + i)
          .build());
    }
    return nodes;
  }

  private static Pipeline buildEcPipeline(List<DatanodeDetails> nodes,
      ECReplicationConfig repConfig) {
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < nodes.size(); i++) {
      replicaIndexes.put(nodes.get(i), i + 1);
    }
    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(nodes)
        .setReplicaIndexes(replicaIndexes)
        .build();
  }

  private static OmKeyInfo[] buildKeyInfos(Pipeline pipeline,
      ECReplicationConfig repConfig) {
    OmKeyInfo[] infos = new OmKeyInfo[KEY_POOL];
    for (int i = 0; i < KEY_POOL; i++) {
      OmKeyLocationInfo loc = new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(CONTAINER_ID, i))
          .setPipeline(pipeline)
          .setLength(FILE_SIZE)
          .build();
      infos[i] = new OmKeyInfo.Builder()
          .setVolumeName("vol")
          .setBucketName("bucket")
          .setKeyName("file-" + i)
          .setOmKeyLocationInfos(Collections.singletonList(
              new OmKeyLocationInfoGroup(0,
                  Collections.singletonList(loc))))
          .setCreationTime(0L)
          .setModificationTime(0L)
          .setDataSize(FILE_SIZE)
          .setReplicationConfig(repConfig)
          .setFileChecksum(null)
          .setAcls(Collections.emptyList())
          .build();
    }
    return infos;
  }

  private static ContainerProtos.ContainerCommandResponseProto buildGetBlockResponse(
      int stripeChecksumBytes) {
    ByteString fourBytes = ByteString.copyFrom(new byte[4]);
    ByteString stripeChecksum = ByteString.copyFrom(new byte[stripeChecksumBytes]);

    ContainerProtos.ChecksumData checksumData =
        ContainerProtos.ChecksumData.newBuilder()
            .setType(ContainerProtos.ChecksumType.CRC32)
            .setBytesPerChecksum(512 * 1024)
            .addChecksums(fourBytes)
            .build();

    ContainerProtos.ChunkInfo chunk = ContainerProtos.ChunkInfo.newBuilder()
        .setChunkName("chunk0")
        .setOffset(0)
        .setLen(FILE_SIZE)
        .setChecksumData(checksumData)
        .setStripeChecksum(stripeChecksum)
        .build();

    ContainerProtos.DatanodeBlockID dnBlockId =
        ContainerProtos.DatanodeBlockID.newBuilder()
            .setContainerID(CONTAINER_ID)
            .setLocalID(1)
            .setBlockCommitSequenceId(1)
            .build();

    ContainerProtos.BlockData blockData =
        ContainerProtos.BlockData.newBuilder()
            .setBlockID(dnBlockId)
            .addChunks(chunk)
            .build();

    ContainerProtos.GetBlockResponseProto getBlockResponse =
        ContainerProtos.GetBlockResponseProto.newBuilder()
            .setBlockData(blockData)
            .build();

    return ContainerProtos.ContainerCommandResponseProto.newBuilder()
        .setCmdType(ContainerProtos.Type.GetBlock)
        .setResult(ContainerProtos.Result.SUCCESS)
        .setGetBlock(getBlockResponse)
        .build();
  }

  /**
   * Builds an OmKeyInfo whose EC pipeline has freshly generated random node UUIDs.
   * Used by unstable files in {@link #measure} and by Scenario A in
   * {@link #runBlockCacheBenchmark} so the deterministic pipeline ID computed
   * by Fix 2 is also effectively random per call, guaranteeing a cache miss.
   */
  private static OmKeyInfo buildRandomKeyInfo(String keyName,
      ECReplicationConfig repConfig) {
    return buildRandomKeyInfoMultiBlock(keyName, repConfig, 1);
  }

  /**
   * Like {@link #buildRandomKeyInfo} but produces a file with {@code nBlocks} blocks,
   * all backed by the same freshly-randomized EC pipeline. All N blocks share the same
   * (unique-per-call) EC pipeline ID, so Fix 3's cache fires for blocks 2..N while
   * Fix 2 still produces a unique standalone pipeline ID per file (random nodes).
   */
  private static OmKeyInfo buildRandomKeyInfoMultiBlock(String keyName,
      ECReplicationConfig repConfig, int nBlocks) {
    int nodeCount = repConfig.getData() + repConfig.getParity();
    List<DatanodeDetails> nodes = new ArrayList<>();
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      DatanodeDetails dn = DatanodeDetails.newBuilder()
          .setUuid(UUID.randomUUID())
          .setHostName("rdn" + i)
          .setIpAddress("10.1.0." + i)
          .build();
      nodes.add(dn);
      replicaIndexes.put(dn, i + 1);
    }
    Pipeline pipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(nodes)
        .setReplicaIndexes(replicaIndexes)
        .build();
    List<OmKeyLocationInfo> locs = new ArrayList<>();
    for (int b = 0; b < nBlocks; b++) {
      locs.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(CONTAINER_ID + b, 0))
          .setPipeline(pipeline)
          .setLength(FILE_SIZE)
          .build());
    }
    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locs)))
        .setCreationTime(0L)
        .setModificationTime(0L)
        .setDataSize((long) nBlocks * FILE_SIZE)
        .setReplicationConfig(repConfig)
        .setFileChecksum(null)
        .setAcls(Collections.emptyList())
        .build();
  }

  /**
   * Builds an OmKeyInfo whose EC pipeline has nodes randomly selected from {@code clusterNodes}.
   *
   * @param sharedPipelineId  when {@code true} (Scenario B / Fix 1+2+3): all N blocks share one
   *                          EC pipeline ID, so ECFileChecksumHelper's checksumPipelineCache fires
   *                          for blocks 2..N → 1 buildChecksumPipeline call per file.
   *                          When {@code false} (Scenario A / Fix 1+2 only): each block carries a
   *                          distinct PipelineID.randomId() with the same node set → N cache misses
   *                          → N buildChecksumPipeline calls per file. Fix 2 still produces the
   *                          same deterministic standalone pipeline ID from sorted node UUIDs in
   *                          both cases, so XceiverClient pool reuse is unaffected.
   */
  private static OmKeyInfo buildSharedClusterKeyInfoMultiBlock(String keyName,
      List<DatanodeDetails> clusterNodes, ECReplicationConfig ecConfig, int nBlocks,
      boolean sharedPipelineId) {
    int nodeCount = ecConfig.getData() + ecConfig.getParity();
    int clusterSize = clusterNodes.size();
    // O(nodeCount) selection without replacement using a small stack-allocated array
    // to track already-picked indices. For nodeCount <= 9 the inner scan is O(1).
    int[] picked = new int[nodeCount];
    List<DatanodeDetails> selected = new ArrayList<>(nodeCount);
    while (selected.size() < nodeCount) {
      int idx = ThreadLocalRandom.current().nextInt(clusterSize);
      boolean duplicate = false;
      for (int p = 0; p < selected.size(); p++) {
        if (picked[p] == idx) {
          duplicate = true;
          break;
        }
      }
      if (!duplicate) {
        picked[selected.size()] = idx;
        selected.add(clusterNodes.get(idx));
      }
    }
    Map<DatanodeDetails, Integer> replicaIndexes = new HashMap<>();
    for (int i = 0; i < selected.size(); i++) {
      replicaIndexes.put(selected.get(i), i + 1);
    }
    // Build the first (or shared) pipeline once.
    Pipeline firstPipeline = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(ecConfig)
        .setState(Pipeline.PipelineState.CLOSED)
        .setNodes(selected)
        .setReplicaIndexes(replicaIndexes)
        .build();
    List<OmKeyLocationInfo> locs = new ArrayList<>();
    for (int b = 0; b < nBlocks; b++) {
      // sharedPipelineId=true: reuse firstPipeline for all blocks (Fix 3 fires for blocks 2..N).
      // sharedPipelineId=false: unique pipeline ID per block, same nodes (Fix 3 always misses).
      Pipeline blockPipeline = (b == 0 || sharedPipelineId) ? firstPipeline
          : Pipeline.newBuilder()
              .setId(PipelineID.randomId())
              .setReplicationConfig(ecConfig)
              .setState(Pipeline.PipelineState.CLOSED)
              .setNodes(selected)
              .setReplicaIndexes(replicaIndexes)
              .build();
      locs.add(new OmKeyLocationInfo.Builder()
          .setBlockID(new BlockID(CONTAINER_ID + b, 0))
          .setPipeline(blockPipeline)
          .setLength(FILE_SIZE)
          .build());
    }
    return new OmKeyInfo.Builder()
        .setVolumeName("vol")
        .setBucketName("bucket")
        .setKeyName(keyName)
        .setOmKeyLocationInfos(Collections.singletonList(
            new OmKeyLocationInfoGroup(0, locs)))
        .setCreationTime(0L)
        .setModificationTime(0L)
        .setDataSize((long) nBlocks * FILE_SIZE)
        .setReplicationConfig(ecConfig)
        .setFileChecksum(null)
        .setAcls(Collections.emptyList())
        .build();
  }

  private static XceiverClientSpi createMockDnClient(Pipeline standalonePipeline,
      ContainerProtos.ContainerCommandResponseProto response) throws IOException {
    XceiverClientSpi mockDn = mock(XceiverClientSpi.class,
        withSettings().stubOnly().defaultAnswer(CALLS_REAL_METHODS));
    XceiverClientReply reply = new XceiverClientReply(
        CompletableFuture.completedFuture(response));
    try {
      doReturn(reply).when(mockDn).sendCommandAsync(any());
    } catch (ExecutionException | InterruptedException e) {
      throw new IOException(e);
    }
    when(mockDn.getPipeline()).thenReturn(standalonePipeline);
    return mockDn;
  }
}
