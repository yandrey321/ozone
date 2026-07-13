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

package org.apache.hadoop.ozone.client.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.scm.storage.StreamNotSupportedException;
import org.apache.ratis.util.MemoizedSupplier;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link SelectorOutputStream}.
 */
class TestSelectorOutputStream {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestSelectorOutputStream.class);

  enum Op {
    FLUSH(SelectorOutputStream::flush),
    HFLUSH(SelectorOutputStream::hflush),
    HSYNC(SelectorOutputStream::hsync),
    CLOSE(SelectorOutputStream::close);

    private final CheckedConsumer<SelectorOutputStream<?>, IOException> method;

    Op(CheckedConsumer<SelectorOutputStream<?>, IOException> method) {
      this.method = method;
    }

    void accept(SelectorOutputStream<OutputStream> out) throws IOException {
      method.accept(out);
    }
  }

  static class SyncableOutputStreamForTesting
      extends ByteArrayOutputStream implements Syncable {
    @Override
    public void hflush() {
      LOG.info("hflush");
    }

    @Override
    public void hsync() {
      LOG.info("hsync");
    }
  }

  static Supplier<OutputStream> getOutputStreamSupplier(boolean isSyncable) {
    return isSyncable ? SyncableOutputStreamForTesting::new
        : ByteArrayOutputStream::new;
  }

  static void runTestSelector(int threshold, int byteToWrite,
      Op op) throws Exception {
    runTestSelector(threshold, byteToWrite, op, false);
  }

  static void runTestSelector(int threshold, int byteToWrite,
      Op op, boolean isSyncable) throws Exception {
    LOG.info("run: threshold={}, byteToWrite={}, op={}, isSyncable? {}",
        threshold, byteToWrite, op, isSyncable);
    final MemoizedSupplier<OutputStream> belowThreshold
        = MemoizedSupplier.valueOf(getOutputStreamSupplier(isSyncable));
    final MemoizedSupplier<OutputStream> aboveThreshold
        = MemoizedSupplier.valueOf(getOutputStreamSupplier(isSyncable));
    final CheckedFunction<Integer, OutputStream, IOException> selector
        = byteWritten -> byteWritten <= threshold ?
        belowThreshold.get() : aboveThreshold.get();

    final SelectorOutputStream<OutputStream> out = new SelectorOutputStream<>(
        threshold, selector);
    for (int i = 0; i < byteToWrite; i++) {
      out.write(i);
    }

    // checkout auto selection
    final boolean isAbove = byteToWrite > threshold;
    assertFalse(belowThreshold.isInitialized());
    assertEquals(isAbove, aboveThreshold.isInitialized());

    final boolean isBelow = !isAbove;
    if (op != null) {
      op.accept(out);
      assertEquals(isBelow, belowThreshold.isInitialized());
      assertEquals(isAbove, aboveThreshold.isInitialized());
    }
  }

  @Test
  void testFlush() throws Exception {
    runTestSelector(10, 2, Op.FLUSH);
    runTestSelector(10, 10, Op.FLUSH);
    runTestSelector(10, 20, Op.FLUSH);
  }

  @Test
  void testClose() throws Exception {
    runTestSelector(10, 2, Op.CLOSE);
    runTestSelector(10, 10, Op.CLOSE);
    runTestSelector(10, 20, Op.CLOSE);
  }

  @Test
  void testHflushSyncable() throws Exception {
    runTestSelector(10, 2, Op.HFLUSH, true);
    runTestSelector(10, 10, Op.HFLUSH, true);
    runTestSelector(10, 20, Op.HFLUSH, true);
  }

  @Test
  void testHflushNonSyncable() {
    final IllegalStateException thrown = assertThrows(
        IllegalStateException.class,
        () -> runTestSelector(10, 2, Op.HFLUSH, false));
    LOG.info("thrown", thrown);
    assertThat(thrown).hasMessageContaining("not Syncable");
  }

  @Test
  void testHSyncSyncable() throws Exception {
    runTestSelector(10, 2, Op.HSYNC, true);
    runTestSelector(10, 10, Op.HSYNC, true);
    runTestSelector(10, 20, Op.HSYNC, true);
  }

  @Test
  void testHSyncNonSyncable() {
    final IllegalStateException thrown = assertThrows(
        IllegalStateException.class,
        () -> runTestSelector(10, 2, Op.HSYNC, false));
    LOG.info("thrown", thrown);
    assertThat(thrown).hasMessageContaining("not Syncable");
  }

  /**
   * When the selected (streaming) output throws
   * {@link StreamNotSupportedException} on its first write, the stream must
   * fall back to the non-streaming selector and preserve all buffered data
   * (HDDS-12991).
   */
  @Test
  void testFallbackOnStreamNotSupported() throws Exception {
    final int threshold = 10;
    final AtomicBoolean streamingClosed = new AtomicBoolean(false);
    // Streaming output that cannot stream: always throws on write.
    final OutputStream streamingOut = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
      }

      @Override
      public void write(byte[] b, int off, int len)
          throws IOException {
        throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
      }

      @Override
      public void close() {
        streamingClosed.set(true);
      }
    };
    final ByteArrayOutputStream fallbackOut = new ByteArrayOutputStream();

    final CheckedFunction<Integer, OutputStream, IOException> selector =
        byteWritten -> byteWritten <= threshold
            ? new ByteArrayOutputStream() : streamingOut;
    final CheckedFunction<Integer, OutputStream, IOException> fallbackSelector =
        byteWritten -> fallbackOut;

    final SelectorOutputStream<OutputStream> out = new SelectorOutputStream<>(
        threshold, selector, fallbackSelector);

    final byte[] data = new byte[threshold + 5];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) i;
      out.write(data[i]);
    }
    out.close();

    // The streaming attempt was made and closed, and every byte (buffered and
    // subsequent) landed in the non-streaming fallback.
    assertTrue(streamingClosed.get());
    assertArrayEquals(data, fallbackOut.toByteArray());
  }

  /**
   * Without a fallback selector, StreamNotSupportedException propagates.
   */
  @Test
  void testNoFallbackPropagates() {
    final int threshold = 10;
    final CheckedFunction<Integer, OutputStream, IOException> selector =
        byteWritten -> new OutputStream() {
          @Override
          public void write(int b) throws IOException {
            throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
          }

          @Override
          public void write(byte[] b, int off, int len)
              throws IOException {
            throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
          }
        };
    final SelectorOutputStream<OutputStream> out =
        new SelectorOutputStream<>(threshold, selector);
    assertThrows(StreamNotSupportedException.class, () -> {
      for (int i = 0; i < threshold + 5; i++) {
        out.write(i);
      }
    });
  }

  /**
   * A {@code write(byte[], off, len)} that stays below the threshold is
   * buffered and only flushed to the selected stream on close.
   */
  @Test
  void testArrayWriteBuffered() throws Exception {
    final int threshold = 100;
    final MemoizedSupplier<OutputStream> below
        = MemoizedSupplier.valueOf(ByteArrayOutputStream::new);
    final MemoizedSupplier<OutputStream> above
        = MemoizedSupplier.valueOf(ByteArrayOutputStream::new);
    final CheckedFunction<Integer, OutputStream, IOException> selector
        = byteWritten -> byteWritten <= threshold ? below.get() : above.get();

    final byte[] data = newData(20);
    try (SelectorOutputStream<OutputStream> out =
        new SelectorOutputStream<>(threshold, selector)) {
      out.write(data, 0, 10);
      out.write(data, 10, 10);
      // Still buffered: no stream selected yet.
      assertFalse(below.isInitialized());
      assertFalse(above.isInitialized());
    }
    assertTrue(below.isInitialized());
    assertFalse(above.isInitialized());
    assertArrayEquals(data,
        ((ByteArrayOutputStream) below.get()).toByteArray());
  }

  /**
   * A {@code write(byte[], off, len)} above the threshold selects the stream
   * and flushes the buffered prefix; a subsequent array write goes directly to
   * the selected stream.
   */
  @Test
  void testArrayWriteSelectsThenDirect() throws Exception {
    final int threshold = 10;
    final ByteArrayOutputStream selected = new ByteArrayOutputStream();
    final CheckedFunction<Integer, OutputStream, IOException> selector =
        byteWritten -> selected;

    final byte[] data = newData(30);
    final SelectorOutputStream<OutputStream> out =
        new SelectorOutputStream<>(threshold, selector);
    out.write(data, 0, 5);        // buffered (below threshold)
    out.write(data, 5, 20);       // exceeds threshold: select + firstFlush
    assertSame(selected, out.getUnderlying());
    out.write(data, 25, 5);       // started: direct write
    out.close();

    assertArrayEquals(data, selected.toByteArray());
  }

  /**
   * Fallback triggered by a {@code write(byte[], off, len)} whose streaming
   * stream both throws {@link StreamNotSupportedException} and fails to close;
   * the close failure must not mask the fallback, which still receives the data.
   */
  @Test
  void testArrayWriteFallbackWithFailingClose() throws Exception {
    final int threshold = 10;
    final OutputStream streamingOut = new OutputStream() {
      @Override
      public void write(int b) throws IOException {
        throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        throw new StreamNotSupportedException("no RATIS_DATASTREAM port");
      }

      @Override
      public void close() throws IOException {
        throw new IOException("close failed");
      }
    };
    final ByteArrayOutputStream fallbackOut = new ByteArrayOutputStream();
    final CheckedFunction<Integer, OutputStream, IOException> selector =
        byteWritten -> byteWritten <= threshold
            ? new ByteArrayOutputStream() : streamingOut;
    final CheckedFunction<Integer, OutputStream, IOException> fallbackSelector =
        byteWritten -> fallbackOut;

    final byte[] data = newData(threshold + 5);
    try (SelectorOutputStream<OutputStream> out = new SelectorOutputStream<>(
        threshold, selector, fallbackSelector)) {
      out.write(data, 0, data.length);
      assertSame(fallbackOut, out.getUnderlying());
    }
    assertArrayEquals(data, fallbackOut.toByteArray());
  }

  /**
   * {@link SelectorOutputStream#hasCapability(String)} delegates to the selected
   * stream when it is {@link StreamCapabilities}, returns false otherwise, and
   * returns false when selection fails.
   */
  @Test
  void testHasCapability() throws Exception {
    final int threshold = 10;

    // Underlying supports capabilities: delegate to it.
    final CheckedFunction<Integer, OutputStream, IOException> capable =
        byteWritten -> new CapableOutputStream(true);
    try (SelectorOutputStream<OutputStream> out =
        new SelectorOutputStream<>(threshold, capable)) {
      assertTrue(out.hasCapability("hsync"));
    }

    // Underlying is not StreamCapabilities: false.
    try (SelectorOutputStream<OutputStream> out = new SelectorOutputStream<>(
        threshold, byteWritten -> new ByteArrayOutputStream())) {
      assertFalse(out.hasCapability("hsync"));
    }

    // Selection throws: swallowed, false.
    final SelectorOutputStream<OutputStream> failing =
        new SelectorOutputStream<>(threshold, byteWritten -> {
          throw new IOException("cannot select");
        });
    assertFalse(failing.hasCapability("hsync"));
  }

  private static byte[] newData(int length) {
    final byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) i;
    }
    return data;
  }

  private static final class CapableOutputStream extends ByteArrayOutputStream
      implements StreamCapabilities {
    private final boolean capable;

    private CapableOutputStream(boolean capable) {
      this.capable = capable;
    }

    @Override
    public boolean hasCapability(String capability) {
      return capable;
    }
  }
}
