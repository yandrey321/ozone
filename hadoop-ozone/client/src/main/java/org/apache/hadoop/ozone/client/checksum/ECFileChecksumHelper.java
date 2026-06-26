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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.token.OzoneBlockTokenIdentifier;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.security.token.Token;

/**
 * The helper class to compute file checksum for EC files.
 */
public class ECFileChecksumHelper extends BaseFileChecksumHelper {

  // Blocks in the same EC placement group share identical node sets, so the
  // STANDALONE checksum pipeline is the same for every block. Cache it to
  // avoid rebuilding it (node filtering, toBuilder, object allocation) per block.
  private final Map<PipelineID, Pipeline> checksumPipelineCache = new HashMap<>();

  public ECFileChecksumHelper(OzoneVolume volume, OzoneBucket bucket,
      String keyName, long length, OzoneClientConfig.ChecksumCombineMode
      checksumCombineMode, ClientProtocol rpcClient, OmKeyInfo keyInfo)
      throws IOException {
    super(volume, bucket, keyName, length, checksumCombineMode, rpcClient,
        keyInfo);
  }

  @Override
  protected AbstractBlockChecksumComputer getBlockChecksumComputer(List<ContainerProtos.ChunkInfo> chunkInfos,
      long blockLength) {
    return new ECBlockChecksumComputer(chunkInfos, getKeyInfo(), blockLength);
  }

  @Override
  protected List<ContainerProtos.ChunkInfo> getChunkInfos(OmKeyLocationInfo keyLocationInfo) throws IOException {
    Token<OzoneBlockTokenIdentifier> token = keyLocationInfo.getToken();
    BlockID blockID = keyLocationInfo.getBlockID();
    Pipeline ecPipeline = keyLocationInfo.getPipeline();
    Pipeline checksumPipeline = checksumPipelineCache.computeIfAbsent(
        ecPipeline.getId(), id -> buildChecksumPipeline(ecPipeline));

    List<ContainerProtos.ChunkInfo> chunks;
    XceiverClientSpi xceiverClientSpi = null;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Initializing BlockInputStream for get key to access {}",
            blockID.getContainerID());
      }
      xceiverClientSpi = getXceiverClientFactory().acquireClientForReadData(checksumPipeline);

      ContainerProtos.GetBlockResponseProto response = ContainerProtocolCalls
          .getBlock(xceiverClientSpi, blockID, token, checksumPipeline.getReplicaIndexes());

      chunks = response.getBlockData().getChunksList();
    } finally {
      if (xceiverClientSpi != null) {
        getXceiverClientFactory().releaseClientForReadData(xceiverClientSpi, false);
      }
    }
    return chunks;
  }

  // To read an EC block we need a STANDALONE pipeline containing only replica
  // index 1 (which holds stripe checksums) and the parity nodes. Blocks in the
  // same placement group share the same node set, so this result is cached by
  // the caller and built at most once per placement group per file.
  private Pipeline buildChecksumPipeline(Pipeline ecPipeline) {
    ECReplicationConfig repConfig = (ECReplicationConfig) ecPipeline.getReplicationConfig();
    List<DatanodeDetails> nodes = new ArrayList<>();
    for (DatanodeDetails dn : ecPipeline.getNodes()) {
      int replicaIndex = ecPipeline.getReplicaIndex(dn);
      if (replicaIndex == 1 || replicaIndex > repConfig.getData()) {
        nodes.add(dn);
      }
    }
    return ecPipeline.toBuilder()
        .setReplicationConfig(StandaloneReplicationConfig
            .getInstance(HddsProtos.ReplicationFactor.THREE))
        .setNodes(nodes)
        .build();
  }
}
