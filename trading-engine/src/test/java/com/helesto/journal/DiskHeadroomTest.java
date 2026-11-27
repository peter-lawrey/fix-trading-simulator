package com.helesto.journal;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class DiskHeadroomTest {
  @Test
  void admissionUsesOnlyRecentSuccessfulReadings() {
    AtomicLong usable = new AtomicLong(DiskHeadroom.MIN_BYTES);
    AtomicLong clock = new AtomicLong();
    try (var disk =
        new DiskHeadroom(
            () -> {
              long free = usable.get();
              if (free < 0) throw new IOException("simulated probe failure");
              return free;
            },
            clock::get)) {
      assertFalse(disk.canAdmit());
      disk.sample();
      assertTrue(disk.canAdmit());
      clock.set(TimeUnit.SECONDS.toNanos(1));
      assertFalse(disk.canAdmit());
      usable.decrementAndGet();
      disk.sample();
      assertFalse(disk.canAdmit());
      usable.set(-1);
      disk.sample();
      assertFalse(disk.canAdmit());
      usable.set(DiskHeadroom.MIN_BYTES);
      disk.sample();
      assertTrue(disk.canAdmit());
    }
  }

  @Test
  void aLateCompletionDoesNotRefreshItsAge() {
    AtomicLong clock = new AtomicLong();
    try (var disk =
        new DiskHeadroom(
            () -> {
              clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
              return Long.MAX_VALUE;
            },
            clock::get)) {
      disk.sample();
      assertFalse(disk.canAdmit());
    }
  }
}
