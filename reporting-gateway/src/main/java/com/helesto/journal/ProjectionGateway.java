package com.helesto.journal;

import io.vertx.core.json.JsonObject;
import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;

/** One JVM, one independent reporting writer per supplied business stream. */
public final class ProjectionGateway {
  static final int BATCH = Integer.getInteger("gateway.batch", 256);
  static final long LINGER = TimeUnit.MILLISECONDS.toNanos(Long.getLong("gateway.lingerMs", 4L));

  record Entry(long index, JsonObject data) {}

  @FunctionalInterface
  interface CommitObserver {
    void observe(String schema, String side, long version);
  }

  public static void main(String[] args) throws Exception {
    run(args, (schema, side, version) -> {});
  }

  static void run(String[] args, CommitObserver observer) throws Exception {
    if (args.length == 0 || BATCH <= 0 || LINGER < 0)
      throw new IllegalArgumentException("Supply schema=journal-directory for each stream");
    var streams = new LinkedHashMap<String, Path>();
    var schemas = new HashSet<String>();
    var directories = new HashSet<Path>();
    for (String arg : args) {
      String[] fields = arg.split("=", 2);
      if (fields.length != 2 || !fields[0].matches("[a-z][a-z0-9_]*") || !schemas.add(fields[0]))
        throw new IllegalArgumentException(arg);
      Path dir = Path.of(fields[1]).toAbsolutePath().normalize();
      if (!directories.add(dir)) throw new IllegalArgumentException("Repeated spool: " + dir);
      streams.put(fields[0], dir);
    }
    var executor = Executors.newFixedThreadPool(streams.size());
    try {
      var tasks = new ExecutorCompletionService<Void>(executor);
      for (var stream : streams.entrySet()) {
        tasks.submit(
            () -> {
              worker(stream.getKey(), stream.getValue(), observer);
              return null;
            });
      }
      tasks.take().get();
    } finally {
      executor.shutdownNow();
    }
  }

  private static void worker(String schema, Path dir, CommitObserver observer) throws Exception {
    while (!BusinessJournal.exists(dir)) Thread.sleep(25);
    while (!Thread.currentThread().isInterrupted()) {
      try (var tail = BusinessJournal.openReader(dir);
          var db =
              DriverManager.getConnection(
                  System.getenv("SIM_DB_URL"),
                  System.getenv("SIM_DB_USER"),
                  System.getenv("SIM_DB_PASSWORD"))) {
        String first = tail.readText();
        if (first == null) {
          Thread.sleep(25);
          continue;
        }
        var genesis = new JsonObject(first);
        if (!"open".equals(genesis.getString("kind"))
            || genesis.getInteger("format") != 1
            || genesis.getLong("n") != 1)
          throw new IllegalStateException("Missing stream genesis: " + dir);
        String stream = genesis.getString("stream");
        boolean broker =
            switch (genesis.getString("role")) {
              case "broker" -> true;
              case "exchange" -> false;
              default -> throw new IllegalStateException("Unknown role");
            };
        // schema validated before this worker starts. A missing schema must not fall back to
        // public.
        db.setSchema(schema);
        try (var s = db.createStatement();
            var r = s.executeQuery("select current_schema()")) {
          r.next();
          if (!schema.equals(r.getString(1)))
            throw new SQLException("Missing reporting schema " + schema);
        }
        db.setAutoCommit(false);
        initialize(db, stream);
        long last, idx;
        try (var s = db.createStatement();
            var r = s.executeQuery("select n,file_offset from projection_cursor where singleton")) {
          if (!r.next()) throw new IllegalStateException("Missing projection cursor");
          last = r.getLong(1);
          idx = r.getLong(2);
        }
        db.commit();
        if (last == 0) tail.seek(0);
        else {
          tail.seek(idx);
          String raw = tail.readText();
          var at = raw == null ? null : new JsonObject(raw);
          if (at == null || at.getLong("n") != last || !stream.equals(at.getString("stream")))
            throw new IllegalStateException("Checkpoint has no matching source");
        }
        try (var projection = new Sink(db, broker, schema, observer)) {
          while (!Thread.currentThread().isInterrupted()) {
            var docs = new ArrayList<Entry>();
            long deadline = 0;
            while (docs.size() < BATCH) {
              String raw = tail.readText();
              if (raw == null) {
                if (docs.isEmpty() || System.nanoTime() >= deadline) break;
                LockSupport.parkNanos(200_000);
                continue;
              }
              var event = new JsonObject(raw);
              if (event.getInteger("format") != 1
                  || !stream.equals(event.getString("stream"))
                  || event.getLong("n") != last + docs.size() + 1)
                throw new IllegalStateException(
                    "Queue gap or incompatible event following " + last + " in " + schema);
              docs.add(new Entry(tail.lastOffset(), event));
              if (docs.size() == 1) deadline = System.nanoTime() + LINGER;
            }
            if (docs.isEmpty()) {
              Thread.sleep(1);
              continue;
            }
            projection.commit(docs, stream, last);
            last += docs.size();
          }
        }
      } catch (SQLException e) {
        String state = Objects.toString(e.getSQLState(), "");
        if (!(state.startsWith("08")
            || state.startsWith("40")
            || state.startsWith("53")
            || state.startsWith("57")
            || state.equals("55P03"))) throw e;
        System.err.println(schema + " projection retry after SQLSTATE " + state);
        Thread.sleep(300);
        // Reopen at the SQL checkpoint even if the previous commit outcome was unknown.
      }
    }
  }

  private static void initialize(Connection db, String stream) throws SQLException {
    try (var s = db.createStatement()) {
      s.execute(
          "create table if not exists projection_cursor(singleton boolean primary key"
              + " check(singleton), stream text not null, n bigint not null,file_offset bigint not"
              + " null)");
      s.execute(
          "create table if not exists business_history(n bigint primary key, kind text not null,"
              + " event jsonb not null)");
      s.execute("lock table projection_cursor in exclusive mode");
      try (var r = s.executeQuery("select stream from projection_cursor where singleton")) {
        if (r.next()) {
          if (!stream.equals(r.getString(1)))
            throw new IllegalStateException("Destination bound to a different business epoch");
          db.commit();
          return;
        }
      }
      try (var r =
          s.executeQuery(
              "select exists(select from orders) or exists(select from business_history)")) {
        r.next();
        if (r.getBoolean(1))
          throw new IllegalStateException(
              "Destination already contains uncheckpointed business state");
      }
      try (var insert = db.prepareStatement("insert into projection_cursor values (true,?,0,0)")) {
        insert.setString(1, stream);
        insert.executeUpdate();
      }
      db.commit();
    }
  }

  static final class Sink implements AutoCloseable {
    final Connection db;
    final boolean broker;
    final PreparedStatement history, upsert, advance;
    final BufferedWriter metrics;
    final String schema;
    final CommitObserver observer;

    Sink(Connection db, boolean broker, String schema, CommitObserver observer) throws Exception {
      this.db = db;
      this.broker = broker;
      this.schema = schema;
      this.observer = observer;
      history =
          db.prepareStatement("insert into business_history(n,kind,event) values(?,?,?::jsonb)");
      String extra = broker ? "" : ",orderid,origclordid";
      upsert =
          db.prepareStatement(
              "insert into orders(clordid,side,symbol,ordstatus,orderqty,cumqty,price"
                  + extra
                  + ") values(?,?,?,?,?,?,?"
                  + (broker ? "" : ",?,?")
                  + ") on conflict(clordid) do update set"
                  + " ordstatus=excluded.ordstatus,cumqty=excluded.cumqty");
      advance =
          db.prepareStatement(
              "update projection_cursor set n=?,file_offset=? where singleton and stream=? and"
                  + " n=?");
      String path = System.getenv("SIM_METRICS_DIR");
      metrics =
          path == null
              ? null
              : Files.newBufferedWriter(
                  Path.of(path, schema + ".tsv"),
                  StandardOpenOption.CREATE,
                  StandardOpenOption.APPEND);
    }

    void commit(List<Entry> docs, String stream, long prev) throws Exception {
      var rows = new LinkedHashMap<Integer, JsonObject>();
      for (var entry : docs) {
        JsonObject e = entry.data();
        history.setLong(1, e.getLong("n"));
        history.setString(2, e.getString("kind"));
        history.setString(3, e.encode());
        history.addBatch();
        var row = e.getJsonObject("row");
        if (row != null) rows.put(row.getInteger("clOrdID"), row);
      }
      for (var r : rows.values()) {
        upsert.setInt(1, r.getInteger("clOrdID"));
        upsert.setString(2, r.getString("side"));
        upsert.setString(3, r.getString("symbol"));
        upsert.setString(4, r.getString("ordStatus"));
        upsert.setLong(5, r.getLong("orderQty"));
        upsert.setLong(6, r.getLong("cumQty"));
        upsert.setBigDecimal(7, java.math.BigDecimal.valueOf(r.getDouble("price")));
        if (!broker) {
          upsert.setInt(8, r.getInteger("orderID"));
          upsert.setInt(9, r.getInteger("origClOrdID"));
        }
        upsert.addBatch();
      }
      var end = docs.get(docs.size() - 1);
      advance.setLong(1, end.data().getLong("n"));
      advance.setLong(2, end.index());
      advance.setString(3, stream);
      advance.setLong(4, prev);
      if (advance.executeUpdate() != 1)
        throw new SQLException("Concurrent checkpoint advance", "40001");
      history.executeBatch();
      upsert.executeBatch();
      observer.observe(schema, "before", end.data().getLong("n"));
      db.commit();
      long visible = System.nanoTime();
      observer.observe(schema, "after", end.data().getLong("n"));
      if (metrics != null) {
        for (var item : docs) {
          var e = item.data();
          var r = e.getJsonObject("row");
          if (r != null && "0".equals(r.getString("ordStatus")))
            metrics.write(
                r.getInteger("clOrdID")
                    + "\t"
                    + r.getString("symbol")
                    + "\t"
                    + e.getLong("at")
                    + "\t"
                    + visible
                    + "\t"
                    + docs.size()
                    + "\n");
        }
        metrics.flush();
      }
    }

    public void close() throws Exception {
      if (metrics != null) metrics.close();
      history.close();
      upsert.close();
      advance.close();
    }
  }
}
