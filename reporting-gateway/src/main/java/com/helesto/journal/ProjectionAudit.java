package com.helesto.journal;

import io.vertx.core.json.JsonObject;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

/** Run against stopped producers; compare each retained envelope and all reporting order fields. */
public final class ProjectionAudit {
  public static void main(String[] args) throws Exception {
    if (args.length == 0) throw new IllegalArgumentException("Supply schema=directory");
    try (var db =
        DriverManager.getConnection(
            System.getenv("SIM_DB_URL"),
            System.getenv("SIM_DB_USER"),
            System.getenv("SIM_DB_PASSWORD"))) {
      for (String arg : args) {
        String[] pair = arg.split("=", 2);
        if (pair.length != 2 || !pair[0].matches("[a-z][a-z0-9_]*"))
          throw new IllegalArgumentException(arg);
        db.setSchema(pair[0]);
        try (var t = BusinessJournal.openReader(Path.of(pair[1]))) {
          long expected = new JsonObject(t.lastText()).getLong("n");
          t.seek(0);
          long limit = System.nanoTime() + 60_000_000_000L;
          while (true) {
            boolean ready;
            try (var s = db.prepareStatement("select to_regclass(?) is not null")) {
              s.setString(1, pair[0] + ".projection_cursor");
              try (var r = s.executeQuery()) {
                r.next();
                ready = r.getBoolean(1);
              }
            }
            if (ready) {
              try (var s = db.createStatement();
                  var r = s.executeQuery("select n from projection_cursor")) {
                if (r.next() && r.getLong(1) == expected) break;
              }
            }
            if (System.nanoTime() > limit)
              throw new IllegalStateException("SQL drain timed out in " + pair[0]);
            Thread.sleep(20);
          }
          long n = 0, changes = 0;
          boolean broker = false;
          var orders = new HashMap<Integer, JsonObject>();
          db.setAutoCommit(false);
          try (var s = db.createStatement()) {
            s.setFetchSize(512);
            try (var r = s.executeQuery("select n,event::text from business_history order by n")) {
              String data;
              while ((data = t.readText()) != null) {
                var e = new JsonObject(data);
                if (!r.next() || r.getLong(1) != ++n || !e.equals(new JsonObject(r.getString(2))))
                  throw new AssertionError("History differs at " + n + " in " + pair[0]);
                if (n == 1) broker = "broker".equals(e.getString("role"));
                if (e.containsKey("row")) {
                  var row = e.getJsonObject("row");
                  orders.put(row.getInteger("clOrdID"), row);
                  changes++;
                }
              }
              if (r.next() || n != expected) throw new AssertionError("Unexpected history size");
            }
          }
          int total = orders.size();
          try (var s = db.createStatement()) {
            s.setFetchSize(512);
            try (var r = s.executeQuery("select * from orders")) {
              while (r.next()) {
                var o = orders.remove(r.getInt("clordid"));
                if (o == null
                    || !o.getString("symbol").equals(r.getString("symbol"))
                    || !o.getString("side").equals(r.getString("side"))
                    || !o.getString("ordStatus").equals(r.getString("ordstatus"))
                    || o.getLong("orderQty") != r.getLong("orderqty")
                    || o.getLong("cumQty") != r.getLong("cumqty")
                    || Double.compare(o.getDouble("price"), r.getDouble("price")) != 0
                    || (!broker
                        && (o.getInteger("orderID") != r.getInt("orderid")
                            || o.getInteger("origClOrdID") != r.getInt("origclordid"))))
                  throw new AssertionError("Reporting row mismatch");
              }
            }
          }
          if (!orders.isEmpty()) throw new AssertionError("Missing SQL orders");
          db.commit();
          db.setAutoCommit(true);
          System.out.printf(
              "%s: %d envelopes, %d order transitions, %d final rows%n",
              pair[0], n, changes, total);
        }
      }
    }
  }
}
