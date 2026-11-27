package com.helesto.journal;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

class FileSpoolTest {
  private static void check(boolean condition) {
    if (!condition) throw new AssertionError();
  }

  @org.junit.jupiter.api.Test
  void readsLiveAppendsAndRecoversIncompleteTail(@org.junit.jupiter.api.io.TempDir Path dir)
      throws Exception {
    String large = "order-".repeat(12000);
    try (FileSpool log = new FileSpool(dir);
        FileSpool.Reader reader = log.reader()) {
      log.writeText("one");
      check("one".equals(reader.readText()));
      check(reader.readText() == null);
      log.writeText(large);
      check(large.equals(reader.readText()));
      long checkpoint = reader.last;
      reader.seek(checkpoint);
      check(large.equals(reader.readText()));
      log.writeText("three");
      check("three".equals(reader.readText()));
      check("three".equals(reader.lastText()));
      log.sync();
    }
    try (var append = FileChannel.open(dir.resolve("events.bin"), StandardOpenOption.APPEND)) {
      append.write(ByteBuffer.wrap(new byte[] {0, 0, 0, 5, 'x'}));
    }
    try (FileSpool log = new FileSpool(dir);
        FileSpool.Reader reader = log.reader()) {
      check("one".equals(reader.readText()));
      check(large.equals(reader.readText()));
      check("three".equals(reader.readText()));
      check(reader.readText() == null);
      log.finishRecovery(reader);
      log.writeText("four");
      reader.seek(reader.pos);
      check("four".equals(reader.readText()));
      check(reader.readText() == null);
      check("four".equals(reader.lastText()));
    }
    System.out.println(
        "passed live EOF, large records, checkpoint seek, torn final record and append after"
            + " recovery");
  }
}
