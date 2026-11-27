package com.helesto.journal;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Append-only mirror of one directional FIX capture. Access is serialized by the capture monitor.
 */
final class RawHistory {
  private static final int CHUNK_SIZE = 64 * 1024;
  private final List<byte[]> chunks = new ArrayList<>();
  private long length;
  private long pendingStart = -1;
  private boolean inChecksum;
  private int frameCount;
  private static final int FRAMES_PER_BLOCK = 4096;
  private final List<long[]> frameOffsets = new ArrayList<>();

  record Frame(int id, String text) {}

  private byte byteAt(long pos) {
    return chunks.get(Math.toIntExact(pos / CHUNK_SIZE))[(int) (pos % CHUNK_SIZE)];
  }

  // Recognize the same header and trailer delimiters as the existing public text log,
  // including across capture callbacks and chunk boundaries. Ignore an unfinished tail.
  private void index(byte[] chunk, int offset, int count, long start) {
    for (int i = 0; i < count; i++) {
      byte b = chunk[offset + i];
      long pos = start + i;
      if (inChecksum) {
        if (b == 1) {
          int slot = frameCount % FRAMES_PER_BLOCK;
          if (slot == 0) frameOffsets.add(new long[2 * FRAMES_PER_BLOCK]);
          long[] offsets = frameOffsets.get(frameCount / FRAMES_PER_BLOCK);
          offsets[2 * slot] = pendingStart;
          offsets[2 * slot + 1] = pos + 1;
          frameCount = Math.incrementExact(frameCount);
          pendingStart = -1;
          inChecksum = false;
        }
      } else if (b == '.'
          && pos >= 5
          && byteAt(pos - 1) == 'X'
          && byteAt(pos - 2) == 'I'
          && byteAt(pos - 3) == 'F'
          && byteAt(pos - 4) == '='
          && byteAt(pos - 5) == '8') {
        pendingStart = pos - 5;
      } else if (b == '='
          && pendingStart >= 0
          && pos >= pendingStart + 9
          && byteAt(pos - 1) == '0'
          && byteAt(pos - 2) == '1'
          && byteAt(pos - 3) == 1) {
        inChecksum = true;
      }
    }
  }

  synchronized List<Frame> page(Integer before, int limit) {
    int end = before == null ? frameCount : Math.min(frameCount, before - 1);
    List<Frame> page = new ArrayList<>(Math.min(end, limit));
    for (int id = end; id > Math.max(0, end - limit); id--) {
      int slot = (id - 1) % FRAMES_PER_BLOCK;
      long[] offsets = frameOffsets.get((id - 1) / FRAMES_PER_BLOCK);
      long pos = offsets[2 * slot];
      byte[] frame = new byte[Math.toIntExact(offsets[2 * slot + 1] - pos)];
      int written = 0;
      while (written < frame.length) {
        int offset = (int) (pos % CHUNK_SIZE);
        int count = Math.min(CHUNK_SIZE - offset, frame.length - written);
        System.arraycopy(
            chunks.get(Math.toIntExact(pos / CHUNK_SIZE)), offset, frame, written, count);
        pos += count;
        written += count;
      }
      page.add(new Frame(id, new String(frame, StandardCharsets.ISO_8859_1)));
    }
    return page;
  }

  void load(InputStream input) throws IOException {
    byte[] buffer = new byte[8192];
    ByteBuffer view = ByteBuffer.wrap(buffer);
    int n;
    while ((n = input.read(buffer)) != -1) {
      view.clear().limit(n);
      append(view);
    }
  }

  void append(ByteBuffer data) {
    while (data.hasRemaining()) {
      int offset = (int) (length % CHUNK_SIZE);
      if (offset == 0) chunks.add(new byte[CHUNK_SIZE]);
      int count = Math.min(data.remaining(), CHUNK_SIZE - offset);
      byte[] chunk = chunks.get(chunks.size() - 1);
      data.get(chunk, offset, count);
      index(chunk, offset, count, length);
      length += count;
    }
  }

  synchronized String asLatin1() {
    int size = Math.toIntExact(length);
    var snapshot = new byte[size];
    int written = 0;
    for (byte[] chunk : chunks) {
      int count = Math.min(CHUNK_SIZE, size - written);
      System.arraycopy(chunk, 0, snapshot, written, count);
      written += count;
    }
    return new String(snapshot, StandardCharsets.ISO_8859_1);
  }
}
