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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import org.apache.hadoop.hdds.scm.ByteStringConversion;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bounded pool implementation that provides {@link ChunkBuffer}s. This pool allows allocating and releasing
 * {@link ChunkBuffer}.
 * This pool is designed for concurrent access to allocation and release. It imposes a maximum number of buffers to be
 * allocated at the same time and once the limit has been approached, the thread requesting a new allocation needs to
 * wait until a allocated buffer is released.
 */
public class BufferPool {
  private static final Logger LOG = LoggerFactory.getLogger(BufferPool.class);

  private static final BufferPool EMPTY = new BufferPool(0, 0);
  private final int bufferSize;
  private final int capacity;
  private final Function<ByteBuffer, ByteString> byteStringConversion;

  // Semaphore replaces ReentrantLock.lockInterruptibly() as the capacity gate.
  // The uncontended fast path is a single CAS (no per-call interrupt-flag spin).
  private final Semaphore permits;
  // LIFO free list: recycled buffers are returned stack-fashion so assertSame tests pass.
  private final ConcurrentLinkedDeque<ChunkBuffer> free = new ConcurrentLinkedDeque<>();
  // Identity-tracked in-use list. ChunkBuffer.equals() is content-based, so we cannot use
  // a hash-based collection (hash changes as the buffer is written). Use == comparisons instead.
  private final LinkedList<ChunkBuffer> inUse = new LinkedList<>();
  private final Object inUseLock = new Object();
  private volatile ChunkBuffer currentBuffer;

  public static BufferPool empty() {
    return EMPTY;
  }

  public BufferPool(int bufferSize, int capacity) {
    this(bufferSize, capacity,
        ByteStringConversion.createByteBufferConversion(false));
  }

  public BufferPool(int bufferSize, int capacity,
      Function<ByteBuffer, ByteString> byteStringConversion) {
    this.capacity = capacity;
    this.bufferSize = bufferSize;
    this.byteStringConversion = byteStringConversion;
    this.permits = new Semaphore(capacity);
  }

  public Function<ByteBuffer, ByteString> byteStringConversion() {
    return byteStringConversion;
  }

  ChunkBuffer getCurrentBuffer() {
    return currentBuffer;
  }

  /**
   * Allocate a new {@link ChunkBuffer}, waiting for a buffer to be released when this pool already allocates at
   * capacity.
   */
  public ChunkBuffer allocateBuffer(int increment) throws InterruptedException {
    if (permits.availablePermits() == 0) {
      LOG.debug("Allocation needs to wait the pool is at capacity (allocated = capacity = {}).", capacity);
    }
    permits.acquire();
    ChunkBuffer buffer = free.pollFirst();
    if (buffer == null) {
      buffer = ChunkBuffer.allocate(bufferSize, increment);
    }
    synchronized (inUseLock) {
      inUse.add(buffer);
    }
    currentBuffer = buffer;
    LOG.debug("Allocated new buffer {}, number of used buffers {}, capacity {}.",
        buffer, getNumberOfUsedBuffers(), capacity);
    return buffer;
  }

  void releaseBuffer(ChunkBuffer buffer) {
    LOG.debug("Releasing buffer {}", buffer);
    synchronized (inUseLock) {
      Preconditions.assertTrue(removeByIdentity(inUse, buffer), "Releasing unknown buffer");
    }
    buffer.clear();
    if (buffer == currentBuffer) {
      currentBuffer = null;
    }
    free.addFirst(buffer);
    permits.release();
  }

  /**
   * Remove an item from a list by identity.
   * @return true if the item is found and removed from the list, otherwise false.
   */
  private static boolean removeByIdentity(LinkedList<ChunkBuffer> list, ChunkBuffer toRemove) {
    java.util.ListIterator<ChunkBuffer> it = list.listIterator();
    while (it.hasNext()) {
      if (it.next() == toRemove) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * Wait until one buffer is available.
   * @throws InterruptedException
   */
  @VisibleForTesting
  public void waitUntilAvailable() throws InterruptedException {
    permits.acquire();
    permits.release();
  }

  public void clearBufferPool() {
    synchronized (inUseLock) {
      inUse.forEach(ChunkBuffer::close);
      inUse.clear();
    }
    ChunkBuffer buf;
    while ((buf = free.pollFirst()) != null) {
      buf.close();
    }
    currentBuffer = null;
  }

  public long computeBufferData() {
    synchronized (inUseLock) {
      long totalBufferSize = 0;
      for (ChunkBuffer buf : inUse) {
        totalBufferSize += buf.position();
      }
      return totalBufferSize;
    }
  }

  public int getSize() {
    synchronized (inUseLock) {
      return inUse.size() + free.size();
    }
  }

  public List<ChunkBuffer> getAllocatedBuffers() {
    synchronized (inUseLock) {
      return new ArrayList<>(inUse);
    }
  }

  public int getNumberOfUsedBuffers() {
    synchronized (inUseLock) {
      return inUse.size();
    }
  }

  public boolean isAtCapacity() {
    return permits.availablePermits() == 0;
  }

  public int getCapacity() {
    return capacity;
  }

  public int getBufferSize() {
    return bufferSize;
  }

}
