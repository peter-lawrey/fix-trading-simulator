package com.helesto.journal;

import static java.nio.file.StandardOpenOption.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/** Single-writer, multi-process append log. Matching lengths frame each UTF-8 event. */
public final class FileSpool implements AutoCloseable {
  static final int MAX = 16 << 20;
  final Path file;
  final FileChannel writer;
  private ByteBuffer buffer = ByteBuffer.allocate(4096);

  public FileSpool(Path dir) throws IOException {
    Files.createDirectories(dir);
    file = dir.resolve("events.bin");
    writer = FileChannel.open(file, CREATE, WRITE);
    writer.position(writer.size());
  }

  public void writeText(CharSequence text) throws IOException {
    byte[] data = text.toString().getBytes(StandardCharsets.UTF_8);
    if (data.length == 0 || data.length > MAX) throw new IOException("invalid event size");
    if (buffer.capacity() < data.length + 8) buffer = ByteBuffer.allocate(data.length + 8);
    buffer.clear().putInt(data.length).put(data).putInt(data.length).flip();
    while (buffer.hasRemaining()) writer.write(buffer);
  }

  public void sync() throws IOException {
    writer.force(false);
  }

  public void close() throws IOException {
    writer.close();
  }

  public static final class Reader implements AutoCloseable {
    final FileChannel channel;
    long pos, last;

    public long lastOffset() {
      return last;
    }

    ByteBuffer block = ByteBuffer.allocate(64 * 1024);

    public Reader(Path dir) throws IOException {
      channel = FileChannel.open(dir.resolve("events.bin"), READ);
      block.limit(0);
    }

    public void seek(long offset) throws IOException {
      if (offset < 0 || offset > channel.size())
        throw new IOException("invalid checkpoint " + offset);
      pos = offset;
      block.limit(0);
    }

    // Retain any partial concurrent append across polls. A partial final record is never delivered.
    boolean available(int count) throws IOException {
      int remaining = block.remaining();
      if (remaining >= count) return true;
      if (count > block.capacity()) {
        ByteBuffer larger = ByteBuffer.allocate(count);
        larger.put(block);
        block = larger;
      } else block.compact();
      while (block.position() < count) {
        if (channel.read(block, pos + block.position()) <= 0) break;
      }
      block.flip();
      return block.remaining() >= count;
    }

    public String readText() {
      try {
        if (!available(4)) return null;
        int length = block.getInt(block.position());
        if (length <= 0 || length > MAX) throw new IOException("invalid length at " + pos);
        if (!available(length + 8)) return null;
        block.getInt();
        String text = new String(block.array(), block.position(), length, StandardCharsets.UTF_8);
        block.position(block.position() + length);
        if (block.getInt() != length) throw new IOException("invalid trailer at " + pos);
        last = pos;
        pos += length + 8L;
        return text;
      } catch (IOException ex) {
        throw new UncheckedIOException(ex);
      }
    }

    public String lastText() throws IOException {
      long size = channel.size();
      if (size < 8) throw new EOFException("empty or incomplete journal");
      seek(size - 4);
      if (!available(4)) throw new EOFException();
      int length = block.getInt();
      seek(size - length - 8L);
      String result = readText();
      if (result == null || pos != size) throw new EOFException("incomplete final event");
      return result;
    }

    public void close() {
      try {
        channel.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public Reader reader() {
    try {
      return new Reader(file.getParent());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public void finishRecovery(Reader tail) {
    try {
      if (tail.pos < writer.size()) {
        // Writer lock is held; a published complete record has already been validated by the
        // reader.
        writer.truncate(tail.pos);
        writer.position(tail.pos);
        sync();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
