package com.helesto.journal;

import io.vertx.core.*;
import io.vertx.core.http.*;
import io.vertx.core.json.*;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
  static final String role = packagedRole();
  static final boolean broker = role.equals("broker");
  static final String sender = setting("SIM_SENDER", broker ? "BANZAI" : "EXEC"),
      target = setting("SIM_TARGET", broker ? "EXEC" : "BANZAI");
  static final String session = "FIX.4.4:" + sender + "->" + target;
  static final Path store =
      Path.of(System.getProperty("sim.store", setting("SIM_STORE", "data/" + role)))
          .toAbsolutePath();
  static final String startTime = Instant.now().toString();
  static java.util.function.Consumer<String> publisher;
  static Ledger ledger;
  static Fix fix;

  static String setting(String key, String defaultValue) {
    return System.getenv().getOrDefault(key, defaultValue);
  }

  private static String packagedRole() {
    try (var input =
        Thread.currentThread().getContextClassLoader().getResourceAsStream("simulator-role.txt")) {
      if (input == null) throw new IllegalStateException("Missing simulator-role.txt");
      String value =
          new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim();
      if (!Set.of("broker", "exchange").contains(value)
          || !value.equals(setting("SIM_ROLE", value)))
        throw new IllegalArgumentException("Simulator role does not match this application");
      return value;
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  static JsonObject obj(Object... pairs) {
    JsonObject j = new JsonObject();
    for (int i = 0; i < pairs.length; i += 2) j.put((String) pairs[i], pairs[i + 1]);
    return j;
  }

  static void publish(String json) {
    publisher.accept(json);
  }

  public static class Fault extends RuntimeException {
    final int status;

    Fault(int status, String reason) {
      super(reason);
      this.status = status;
    }
  }

  private static final List<String> eventHistory = new ArrayList<>();

  static void event(String text) {
    try {
      synchronized (Main.class) {
        String line = Instant.now() + " " + text;
        eventHistory.add(line);
        Files.writeString(
            store.resolve("events.txt"),
            line + "\n",
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND);
      }
    } catch (Exception e) {
      System.err.println("Event log: " + e);
    }
  }

  public static List<JsonObject> orders() {
    return ledger.list();
  }

  private static int pageLimit(Integer limit) {
    int size = limit == null ? 100 : limit;
    if (size < 1 || size > 500) throw new Fault(400, "limit must be between 1 and 500");
    return size;
  }

  public static List<JsonObject> orders(Integer after, Integer limit) {
    return after == null && limit == null ? orders() : ledger.page(after, pageLimit(limit));
  }

  /**
   * {@code before} or {@code order=desc} selects a newest-first page; otherwise the ascending
   * routes above.
   */
  public static List<JsonObject> orders(
      Integer after, Integer before, String order, Integer limit) {
    if (order != null && !order.equals("asc") && !order.equals("desc"))
      throw new Fault(400, "order must be asc or desc");
    if (before == null && !"desc".equals(order)) return orders(after, limit);
    if (after != null || "asc".equals(order))
      throw new Fault(400, "before selects descending order; omit after");
    if (before != null && before < 1) throw new Fault(400, "before must be a positive order ID");
    return ledger.pageBefore(before, pageLimit(limit));
  }

  public static JsonArray messageLog(boolean incoming, Integer before, Integer limit)
      throws Exception {
    if (before == null && limit == null) return messages(incoming, false);
    if (before != null && before < 1) throw new Fault(400, "before must be a positive message ID");
    return fix.historyPage(incoming, before, pageLimit(limit));
  }

  public static JsonObject getOrder(int id) {
    return ledger.get(id);
  }

  public static JsonObject status() {
    return fix.status();
  }

  public static JsonArray messages(boolean incoming, boolean session) throws Exception {
    return fix.history(incoming ? "incoming" : "outgoing", session);
  }

  public static JsonObject control(String command) throws Exception {
    switch (command) {
      case "start":
        fix.startSession();
        break;
      case "stop":
        fix.stopSession();
        break;
      case "logout":
        fix.logout();
        break;
      default:
        throw new IllegalArgumentException(command);
    }
    return fix.status();
  }

  public static JsonObject order(JsonObject body) throws Exception {
    return (JsonObject) ledger.newOrder(body);
  }

  public static JsonObject change(int id, String method, JsonObject body) throws Exception {
    return (JsonObject) ledger.change(id, method, body);
  }

  public static boolean automaticTrade() {
    return ledger.auto;
  }

  public static void automaticTrade(boolean value) {
    ledger.configure(value, null);
  }

  public static int delay() {
    return ledger.delay;
  }

  public static void delay(int value) {
    if (value < 0) throw new Fault(400, "Negative delay");
    ledger.configure(null, value);
  }

  public static synchronized JsonArray events() throws Exception {
    var a = new JsonArray();
    if (!eventHistory.isEmpty()) {
      var lines = eventHistory;
      for (int i = lines.size() - 1; i >= 0; i--) {
        var pair = lines.get(i).split(" ", 2);
        a.add(obj("id", i + 1, "time", pair[0], "text", pair[1]));
      }
    }
    return a;
  }

  public static void open(java.util.function.Consumer<String> broadcast) throws Exception {
    Files.createDirectories(store);
    if (Files.exists(store.resolve("events.txt")))
      eventHistory.addAll(Files.readAllLines(store.resolve("events.txt")));
    publisher = broadcast;
    ledger = new Ledger();
    fix = new Fix();
    try {
      ledger.recover();
      fix.open();
      ledger.startWorkers();
    } catch (Exception failure) {
      try {
        close();
      } catch (Exception cleanup) {
        failure.addSuppressed(cleanup);
      }
      throw failure;
    }
  }

  public static void close() throws Exception {
    try {
      if (fix != null) fix.close();
    } finally {
      if (ledger != null) {
        try {
          ledger.close();
        } finally {
          ledger = null;
          fix = null;
        }
      }
    }
  }

  public static int errorStatus(Throwable t) {
    return t instanceof Fault f ? f.status : 500;
  }
}
