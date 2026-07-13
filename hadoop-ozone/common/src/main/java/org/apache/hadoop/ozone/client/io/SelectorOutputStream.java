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

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hdds.scm.storage.StreamNotSupportedException;
import org.apache.ratis.util.function.CheckedConsumer;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputStream} first write data to a buffer up to the capacity.
 * Then, select {@code Underlying} by the number of bytes written.
 * When {@link #flush()}, {@link #hflush()}, {@link #hsync()}
 * or {@link #close()} is invoked,
 * it will force flushing the buffer and {@link OutputStream} selection.
 * <p>
 * This class, like many {@link OutputStream} subclasses, is NOT threadsafe.
 *
 * @param <OUT> The underlying {@link OutputStream} type.
 */
public class SelectorOutputStream<OUT extends OutputStream>
    extends OutputStream implements Syncable, StreamCapabilities {

  private static final Logger LOG =
      LoggerFactory.getLogger(SelectorOutputStream.class);

  private final ByteArrayBuffer buffer;
  private final Underlying underlying;

  /** A buffer backed by a byte[]. */
  static final class ByteArrayBuffer {
    private byte[] array;
    /** Write offset of {@link #array}. */
    private int offset = 0;

    private ByteArrayBuffer(int capacity) {
      this.array = new byte[capacity];
    }

    private void assertRemaining(int outstandingBytes) {
      Objects.requireNonNull(array, "array == null");

      final int remaining = array.length - offset;
      if (remaining < 0) {
        throw new IllegalStateException("remaining = " + remaining + " <= 0");
      }
      if (remaining < outstandingBytes) {
        throw new IllegalArgumentException("Buffer overflow: remaining = "
            + remaining + " < outstandingBytes = " + outstandingBytes);
      }
    }

    void write(byte b) {
      assertRemaining(1);
      array[offset] = b;
      offset++;
    }

    void write(byte[] src, int srcOffset, int length) {
      Objects.requireNonNull(src, "src == null");
      assertRemaining(length);
      System.arraycopy(src, srcOffset, array, offset, length);
      offset += length;
    }

    /** Whether an {@link OutputStream} must be selected to hold the bytes. */
    boolean requiresSelection(int outstandingBytes, boolean force) {
      assertRemaining(0);
      return force || offset + outstandingBytes > array.length;
    }

    /** The total number of bytes the selected stream must accept. */
    int required(int outstandingBytes) {
      return offset + outstandingBytes;
    }

    /** Write the buffered bytes to {@code out}; the buffer is not released. */
    void writePrefixTo(OutputStream out) throws IOException {
      if (offset > 0) {
        out.write(array, 0, offset);
      }
    }

    void release() {
      array = null;
    }
  }

  /** To select the underlying {@link OutputStream}. */
  final class Underlying {
    /** Select an {@link OutputStream} by the number of bytes. */
    private final CheckedFunction<Integer, OUT, IOException> selector;
    /** Selector used when the primary selection cannot stream; may be null. */
    private final CheckedFunction<Integer, OUT, IOException> fallbackSelector;
    private OUT out;
    /** No byte has been accepted by {@link #out} yet, so the buffered prefix is
     * still intact and a {@link StreamNotSupportedException} can be recovered
     * by switching to the fallback (non-streaming) output. */
    private boolean started = false;

    private Underlying(CheckedFunction<Integer, OUT, IOException> selector,
        CheckedFunction<Integer, OUT, IOException> fallbackSelector) {
      this.selector = selector;
      this.fallbackSelector = fallbackSelector;
    }

    /** Select an {@link OutputStream} if the buffer must be flushed. */
    private OUT select(int outstandingBytes, boolean force) throws IOException {
      if (out == null && buffer.requiresSelection(outstandingBytes, force)) {
        out = selector.apply(buffer.required(outstandingBytes));
      }
      return out;
    }

    /**
     * Flush the buffered prefix and the given payload to the selected stream.
     * On the first flush, a {@link StreamNotSupportedException} (e.g. the chosen
     * streaming output cannot be used because the pipeline lacks the
     * RATIS_DATASTREAM port) is recovered by switching to the non-streaming
     * fallback and replaying the buffered prefix and the payload. Once the
     * first flush succeeds, the buffer is released and later writes go directly
     * to the selected stream.
     */
    private void firstFlush(CheckedConsumer<OUT, IOException> payload)
        throws IOException {
      try {
        buffer.writePrefixTo(out);
        payload.accept(out);
      } catch (StreamNotSupportedException e) {
        if (fallbackSelector == null) {
          throw e;
        }
        // The selected (streaming) output cannot be used for this pipeline
        // (e.g. datanodes without the RATIS_DATASTREAM port). Fall back to the
        // non-streaming output; the buffered prefix is still intact.
        LOG.warn("Streaming output is not supported for this write; "
            + "falling back to the non-streaming output stream.", e);
        try {
          out.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
        out = fallbackSelector.apply(buffer.required(0));
        buffer.writePrefixTo(out);
        payload.accept(out);
      }
      started = true;
      buffer.release();
    }

    /** Ensure the buffered prefix has been flushed to the selected stream.
     * The caller selects with {@code force}, so {@link #out} is already set. */
    private void ensureStarted() throws IOException {
      if (!started) {
        firstFlush(ignored -> { });
      }
    }
  }

  /**
   * Construct a {@link SelectorOutputStream} which first writes to a buffer.
   * Once the buffer has become full, select an {@link OutputStream}.
   *
   * @param selectionThreshold The buffer capacity.
   * @param selector Use bytes-written to select an {@link OutputStream}.
   */
  public SelectorOutputStream(int selectionThreshold,
      CheckedFunction<Integer, OUT, IOException> selector) {
    this(selectionThreshold, selector, null);
  }

  /**
   * Same as {@link #SelectorOutputStream(int, CheckedFunction)} but with a
   * fallback selector used when the primary selection throws
   * {@link StreamNotSupportedException} (e.g. the chosen streaming output cannot
   * be used because the pipeline lacks the RATIS_DATASTREAM port). The buffered
   * data is re-written to the fallback stream.
   *
   * @param fallbackSelector produces the non-streaming output; {@code null}
   *                         disables the fallback (original behavior).
   */
  public SelectorOutputStream(int selectionThreshold,
      CheckedFunction<Integer, OUT, IOException> selector,
      CheckedFunction<Integer, OUT, IOException> fallbackSelector) {
    this.buffer = new ByteArrayBuffer(selectionThreshold);
    this.underlying = new Underlying(selector, fallbackSelector);
  }

  public OUT getUnderlying() {
    return underlying.out;
  }

  @Override
  public void write(int b) throws IOException {
    final OUT out = underlying.select(1, false);
    if (out == null) {
      buffer.write((byte) b);
    } else if (!underlying.started) {
      underlying.firstFlush(o -> o.write(b));
    } else {
      out.write(b);
    }
  }

  @Override
  public void write(@Nonnull byte[] array, int off, int len)
      throws IOException {
    final OUT out = underlying.select(len, false);
    if (out == null) {
      buffer.write(array, off, len);
    } else if (!underlying.started) {
      underlying.firstFlush(o -> o.write(array, off, len));
    } else {
      out.write(array, off, len);
    }
  }

  private OUT select() throws IOException {
    final OUT out = underlying.select(0, true);
    underlying.ensureStarted();
    return out;
  }

  @Override
  public void flush() throws IOException {
    select().flush();
  }

  @Override
  public void hflush() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hflush();
    } else {
      throw new IllegalStateException(
          "Failed to hflush: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public void hsync() throws IOException {
    final OUT out = select();
    if (out instanceof Syncable) {
      ((Syncable)out).hsync();
    } else {
      throw new IllegalStateException(
          "Failed to hsync: The underlying OutputStream ("
              + out.getClass() + ") is not Syncable.");
    }
  }

  @Override
  public boolean hasCapability(String capability) {
    try {
      final OUT out = select();
      if (out instanceof StreamCapabilities) {
        return ((StreamCapabilities) out).hasCapability(capability);
      } else {
        return false;
      }
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    select().close();
  }
}
