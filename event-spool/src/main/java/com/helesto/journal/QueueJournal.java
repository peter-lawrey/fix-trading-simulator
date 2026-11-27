package com.helesto.journal;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.function.Consumer;
import net.openhft.chronicle.bytes.SyncMode;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;

/** Stores the same JSON envelopes in a mapped queue, checkpointed by absolute queue index. */
final class QueueJournal implements BusinessJournal {
  private static final long BLOCK_SIZE = 64L << 20;
  private final ChronicleQueue queue;
  private final ExcerptAppender appender;

  QueueJournal(Path directory) {
    queue =
        ChronicleQueue.singleBuilder(directory)
            .blockSize(BLOCK_SIZE)
            .syncMode(SyncMode.SYNC)
            .build();
    try {
      appender = queue.createAppender();
      // The ledger serializes all writes. FIX callbacks and the sender can take turns owning it.
      appender.singleThreadedCheckDisabled(true);
    } catch (RuntimeException | Error failure) {
      queue.close();
      throw failure;
    }
  }

  public void writeText(CharSequence text) throws IOException {
    int length = text.length();
    if (length == 0
        || length > FileSpool.MAX
        || (length > FileSpool.MAX / 3
            && text.toString().getBytes(StandardCharsets.UTF_8).length > FileSpool.MAX)) {
      throw new IOException("Invalid business record size");
    }
    appender.writeText(text);
  }

  public void sync() {
    appender.sync();
  }

  public void recover(Consumer<String> replay) {
    try (ExcerptTailer tail = queue.createTailer()) {
      String text;
      while ((text = tail.readText()) != null) replay.accept(text);
    }
  }

  public void close() {
    try {
      appender.close();
    } finally {
      queue.close();
    }
  }

  static Reader openReader(Path directory) {
    ChronicleQueue queue =
        ChronicleQueue.singleBuilder(directory).blockSize(BLOCK_SIZE).readOnly(true).build();
    try {
      return new QueueReader(queue);
    } catch (RuntimeException | Error e) {
      queue.close();
      throw e;
    }
  }

  private static final class QueueReader implements Reader {
    private final ChronicleQueue queue;
    private final ExcerptTailer tail;

    QueueReader(ChronicleQueue queue) {
      this.queue = queue;
      tail = queue.createTailer();
    }

    public String readText() {
      return tail.readText();
    }

    public long lastOffset() {
      return tail.lastReadIndex();
    }

    public void seek(long checkpoint) throws IOException {
      if (checkpoint == 0) tail.toStart();
      else if (!tail.moveToIndex(checkpoint))
        throw new IOException("Missing queue index " + checkpoint);
    }

    public String lastText() throws IOException {
      try (ExcerptTailer end = queue.createTailer()) {
        String value = end.direction(TailerDirection.BACKWARD).toEnd().readText();
        if (value == null) throw new EOFException("Empty queue");
        return value;
      }
    }

    public void close() {
      try {
        tail.close();
      } finally {
        queue.close();
      }
    }
  }
}
