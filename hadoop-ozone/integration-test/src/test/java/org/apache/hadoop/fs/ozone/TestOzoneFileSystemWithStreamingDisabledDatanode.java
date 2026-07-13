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

package org.apache.hadoop.fs.ozone;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies that when DataNode-side datastream is disabled (so pipelines carry
 * no RATIS_DATASTREAM port), a streaming-enabled client does not fail. Instead
 * of using the gRPC port for streaming, it gracefully falls back to the
 * non-streaming write path and the data is written correctly (HDDS-12991).
 */
public class TestOzoneFileSystemWithStreamingDisabledDatanode {

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;
  private static OzoneConfiguration conf;

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();

    final BucketLayout layout = BucketLayout.FILE_SYSTEM_OPTIMIZED;

    // DataNode side: disable datastream, so pipeline should not explicitly
    // carry RATIS_DATASTREAM ports.
    conf.setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);

    // Client side: enable datastream so the write path will attempt streaming.
    conf.setBoolean(OZONE_FS_DATASTREAM_ENABLED, true);

    conf.set(OZONE_FS_DATASTREAM_AUTO_THRESHOLD, "1B");

    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setChunkSize(16 << 10)
        .setStreamBufferFlushSize(32 << 10)
        .setStreamBufferMaxSize(64 << 10)
        .setBlockSize(128 << 10)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();

    client = cluster.newClient();
    bucket = TestDataUtil.createVolumeAndBucket(client, layout);
  }

  @AfterAll
  public static void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testDatastreamWriteFallsBackWhenDatanodeStreamingDisabled()
      throws Exception {
    final byte[] bytes = new byte[3 << 20];
    ThreadLocalRandom.current().nextBytes(bytes);

    // o3fs (BasicOzoneFileSystem): bucket-scoped scheme.
    final String o3fsRoot = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    assertFallbackRoundTrip(o3fsRoot,
        new Path("/streaming-disabled-dn.dat"), bytes);

    // ofs (BasicRootedOzoneFileSystem): root-scoped scheme.
    final String ofsRoot = String.format("%s://%s/",
        OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    final Path ofsFile = new Path(String.format("/%s/%s/streaming-disabled-ofs.dat",
        bucket.getVolumeName(), bucket.getName()));
    assertFallbackRoundTrip(ofsRoot, ofsFile, bytes);
  }

  /**
   * Write via the given filesystem root with client datastream enabled; the
   * streaming path must detect the missing RATIS_DATASTREAM port, silently fall
   * back to the non-streaming path, and the data must round-trip correctly.
   */
  private void assertFallbackRoundTrip(String rootPath, Path file, byte[] bytes)
      throws IOException {
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    try (FileSystem fs = FileSystem.get(conf)) {
      try (FSDataOutputStream out = fs.create(file, true)) {
        out.write(bytes);
      }
      final byte[] readBack = new byte[bytes.length];
      try (FSDataInputStream in = fs.open(file)) {
        in.readFully(readBack);
      }
      assertArrayEquals(bytes, readBack);
    }
  }
}
