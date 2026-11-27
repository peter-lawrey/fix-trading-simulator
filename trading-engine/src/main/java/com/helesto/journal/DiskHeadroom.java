package com.helesto.journal;

import java.io.IOException;
import java.nio.file.FileStore;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/** Tracks space usable by this process. The order thread reads a timestamp only. */
final class DiskHeadroom implements AutoCloseable {
  interface Probe {
    long usableBytes() throws IOException;
  }

  static final long MIN_BYTES = 256L << 20;
  private static final long MAX_AGE = Duration.ofSeconds(1).toNanos();
  private final Probe probe;
  private final LongSupplier clock;
  private final ScheduledExecutorService executor;

  private record Sample(long start, boolean sufficient) {}

  private volatile Sample last;

  DiskHeadroom(Probe probe, LongSupplier clock) {
    this.probe = probe;
    this.clock = clock;
    this.executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "journal-disk-headroom");
              thread.setDaemon(true);
              return thread;
            });
  }

  static DiskHeadroom start(FileStore store) {
    DiskHeadroom health = new DiskHeadroom(store::getUsableSpace, System::nanoTime);
    health.sample();
    health.executor.scheduleWithFixedDelay(health::sample, 500, 500, TimeUnit.MILLISECONDS);
    return health;
  }

  void sample() {
    long start = clock.getAsLong();
    try {
      last = new Sample(start, probe.usableBytes() >= MIN_BYTES);
    } catch (IOException failure) {
      last = new Sample(start, false);
    }
  }

  boolean canAdmit() {
    Sample sample = last;
    return sample != null && sample.sufficient() && clock.getAsLong() - sample.start() < MAX_AGE;
  }

  public void close() {
    executor.shutdownNow();
  }
}
