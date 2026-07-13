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

package org.apache.hadoop.hdds.scm.storage;

import java.io.IOException;

/**
 * Thrown when a Ratis DataStream write cannot proceed because the target
 * pipeline does not support streaming (e.g. its datanodes were created before
 * DataStream was enabled and therefore lack the RATIS_DATASTREAM port).
 *
 * <p>Callers may catch this to fall back to the non-streaming write path
 * instead of failing the write (HDDS-12991).
 */
public class StreamNotSupportedException extends IOException {

  public StreamNotSupportedException(String message) {
    super(message);
  }
}
