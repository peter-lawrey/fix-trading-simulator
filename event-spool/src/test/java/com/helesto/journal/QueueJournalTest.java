package com.helesto.journal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueueJournalTest {
  @Test
  void liveReaderCanResumeFromItsCommittedIndex(@TempDir Path dir) throws Exception {
    long checkpoint;
    String payload = "order ₹✓".repeat(10000);
    try (BusinessJournal writer = BusinessJournal.open(dir, "queue")) {
      assertThrows(IOException.class, () -> writer.writeText(""));
      writer.writeText("first");
      try (BusinessJournal.Reader tail = BusinessJournal.openReader(dir)) {
        assertEquals("first", tail.readText());
        checkpoint = tail.lastOffset();
        assertNull(tail.readText());
        writer.writeText(payload);
        writer.sync();
        assertEquals(payload, tail.readText());
        assertEquals(payload, tail.lastText());
        tail.seek(checkpoint);
        assertEquals("first", tail.readText());
        assertThrows(IOException.class, () -> tail.seek(Long.MAX_VALUE));
      }
    }
    try (BusinessJournal writer = BusinessJournal.open(dir, "auto")) {
      var replayed = new ArrayList<String>();
      writer.recover(replayed::add);
      assertEquals(List.of("first", payload), replayed);
    }
    assertThrows(IOException.class, () -> BusinessJournal.open(dir, "file"));
  }

  @Test
  void autoReopensExistingFileJournal(@TempDir Path dir) throws Exception {
    try (BusinessJournal writer = BusinessJournal.open(dir, "auto")) {
      writer.writeText("first");
    }
    assertThrows(IOException.class, () -> BusinessJournal.open(dir, "queue"));
    try (BusinessJournal.Reader reader = BusinessJournal.openReader(dir)) {
      assertEquals("first", reader.readText());
    }
  }
}
