package com.helesto.journal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

/** One producer owns a stream. Readers may run in another process. */
public interface BusinessJournal extends AutoCloseable {
  void writeText(CharSequence text) throws IOException;

  void recover(Consumer<String> replay) throws IOException;

  void sync() throws IOException;

  @Override
  void close() throws IOException;

  interface Reader extends AutoCloseable {
    String readText();

    String lastText() throws IOException;

    /** Stable checkpoint of the last record read: a byte offset or an absolute Queue index. */
    long lastOffset();

    void seek(long checkpoint) throws IOException;

    @Override
    void close();
  }

  enum Format {
    FILE,
    QUEUE;

    static Format detect(Path directory) throws IOException {
      boolean file = Files.exists(directory.resolve("events.bin"));
      boolean queue = Files.exists(directory.resolve("metadata.cq4t"));
      if (file && queue) throw new IOException("Mixed journal formats in " + directory);
      return file ? FILE : queue ? QUEUE : null;
    }
  }

  static BusinessJournal open(Path dir, String requested) throws IOException {
    Format existing = Format.detect(dir);
    Format format =
        switch (requested) {
          case "auto" -> existing == null ? Format.FILE : existing;
          case "file" -> Format.FILE;
          case "queue" -> Format.QUEUE;
          default ->
              throw new IllegalArgumentException(
                  "SIM_BUSINESS_JOURNAL must be auto, file or queue");
        };
    if (existing != null && format != existing)
      throw new IOException(
          "Existing " + existing + " journal in " + dir + ", requested " + format);
    if (existing == null && Files.isDirectory(dir)) {
      try (var files = Files.list(dir)) {
        if (files.findAny().isPresent()) throw new IOException("Unrecognized journal in " + dir);
      }
    }
    return format == Format.FILE ? new FileSpool(dir) : new QueueJournal(dir);
  }

  static boolean exists(Path dir) throws IOException {
    return Format.detect(dir) != null;
  }

  static Reader openReader(Path dir) throws IOException {
    Format format = Format.detect(dir);
    if (format == null) throw new IOException("Missing journal in " + dir);
    return format == Format.FILE ? new FileSpool.Reader(dir) : QueueJournal.openReader(dir);
  }
}
