# Two-session order-entry benchmark

Measure the time from a scheduled order to its exchange-confirmed NEW update on the broker WebSocket. Choose HTTP or WebSocket order entry. Every scheduled order must get one matching admission response and one business acknowledgement on its assigned session.

Build the application with JDK 17 and Maven. Then build the separate JLBH driver.

```sh
mvn clean package
cd benchmarks/order-entry
mvn package dependency:build-classpath -Dmdep.outputFile=target/benchmark.cp
```

The local runner starts two brokers, two exchanges, a reporting gateway and a disposable PostgreSQL server. Supply the PostgreSQL binary directory and a writable directory for its temporary data and WAL. Run as an ordinary user with Python 3.9 or newer. Each invocation needs a new output directory. Use an ordinary disk filesystem for the output directory to keep both FIX and business journals on disk.

```sh
python3 local.py --pg-bin /usr/lib/postgresql/15/bin --pg-root /tmp \
  --journal file --transport ws --output /var/tmp/simulator-file-ws-1
python3 local.py --pg-bin /usr/lib/postgresql/15/bin --pg-root /tmp \
  --journal queue --transport ws --output /var/tmp/simulator-queue-ws-1
```

The defaults use 512 MiB JVM heaps, 12,000 paced warm-up orders and five 60-second windows at 2,000 orders/s, alternating sessions. Both order transports wait for the same business update. Each run retains the two FIX directions in each role, business journals, individual timestamps, JLBH output, percentile CSV, FIX hashes and stopped SQL audit. The runner checks FIX framing and checksums, identities, order fields and acknowledgements, then compares every retained business event and final order with PostgreSQL. The acknowledgement can precede SQL visibility. The temporary database uses fsync and synchronous commit. The filesystem behind `--pg-root` determines whether database writes reach persistent storage.

To measure an existing isolated pair of brokers, run the driver directly. Each broker must have its own logged-on exchange session and enough order capacity for warm-up and measurement. The local runner sets the capacity to 320,000 per lane for the default workload.

```sh
python3 run.py --brokers http://127.0.0.1:8080 http://127.0.0.1:8180 \
  --transport http --output /var/tmp/simulator-http-1
```

`run.py` validates client-visible order outcomes and writes exact per-window, median and pooled percentiles for each lane and both lanes together. `local.py` also checks the stopped stores and SQL. Use an idle host for timing and a separate process start for each configuration.
