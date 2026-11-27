package com.helesto.journal;

import static com.helesto.journal.Main.*;

import io.vertx.core.json.JsonObject;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

/**
 * One business journal per FIX session epoch. All mutation and appender access hold this monitor.
 */
final class Ledger {
  private final int orderLimit = Integer.getInteger("sim.maxOrders", 250_000);
  private final NavigableMap<Integer, OrderRow> orders = new TreeMap<>();
  private final NavigableMap<Integer, Integer> exchangeIds = new TreeMap<>();
  // Sized for the admission ceiling. A HashMap that grows rehashes every entry on one insert, under
  // this monitor:
  // at 12,289, 24,577 and 49,153 orders the FIX event loop and every order thread stalled 2, 4 and
  // 8 ms.
  // venueIds/venueOwners hold at most one entry per admitted order. receipts hold one per inbound
  // business
  // message, so fills and cancels can still take them past this size, and then they grow as before.
  private final Map<String, String> receipts = presized(orderLimit);
  private final Map<Integer, String> venueIds = presized(orderLimit);
  private final Map<String, Integer> venueOwners = presized(orderLimit);
  private final Map<Integer, JsonObject> cancels = new HashMap<>();
  private final NavigableMap<Long, JsonObject> outbox = new TreeMap<>();
  private final Map<Long, Long> claimed = new HashMap<>();
  private final NavigableMap<Long, List<JsonObject>> deadlines = new TreeMap<>();

  /** A HashMap that holds {@code entries} without resizing (JDK 17 has no HashMap.newHashMap). */
  private static <K, V> Map<K, V> presized(int entries) {
    return new HashMap<>(
        (int) Math.min(1 << 30, (long) Math.ceil(Math.max(0, Math.min(entries, 250_000)) / 0.75)));
  }

  private final boolean force = Boolean.getBoolean("sim.forceBusinessCommit");
  private final FileChannel owner;
  private final FileLock lock;
  private final FileSpool journal;
  private final DiskHeadroom space;
  private long version;
  private String stream;
  private boolean healthy = true;
  private volatile boolean running;
  volatile boolean auto;
  volatile int delay = 5;
  private Thread senderThread, timerThread;

  Ledger() throws Exception {
    owner =
        FileChannel.open(
            store.resolve("business-writer.lock"),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE);
    lock = owner.tryLock();
    if (lock == null) throw new IllegalStateException("Business store already has a writer");
    Path dir = store.resolve("business-queue");
    if (!Files.exists(dir)
        && (Files.exists(store.resolve("native"))
            || Files.exists(store.resolve("business.received"))))
      throw new IllegalStateException(
          "Existing FIX state requires a reconciled cutover before starting a new business"
              + " journal");
    FileStore filesystem = Files.getFileStore(store);
    journal = new FileSpool(dir);
    space = DiskHeadroom.start(filesystem);
  }

  synchronized void recover() {
    try (var tail = journal.reader()) {
      String value;
      while ((value = tail.readText()) != null) apply(new JsonObject(value));
      journal.finishRecovery(tail);
    }
    if (version > 1 && !Files.isDirectory(store.resolve("native")))
      throw new IllegalStateException("Missing FIX store for existing business journal");
    if (version == 0) {
      requireAdmissionSpace();
      if (Files.exists(store.resolve("native")))
        throw new IllegalStateException("Missing business journal for existing session");
      stream = UUID.randomUUID().toString();
      append(
          obj(
              "kind",
              "open",
              "session",
              session,
              "role",
              role,
              "auto",
              Boolean.parseBoolean(setting("SIM_AUTO_TRADE", "true"))));
    }
  }

  private void requireAdmissionSpace() {
    if (!space.canAdmit())
      throw new Fault(503, "Journal headroom is low or its reading has expired");
  }

  private final StringBuilder envelope = new StringBuilder(192);
  private String quotedFor, quotedStream;

  /** The stream ID exactly as JsonObject.encode() writes it, cached per stream epoch. */
  private String quotedStream() {
    if (quotedFor != stream) {
      String wrapped = new JsonObject().put("s", stream).encode();
      quotedStream = wrapped.substring(5, wrapped.length() - 1);
      quotedFor = stream;
    }
    return quotedStream;
  }

  /**
   * The format-1 text that append(obj("kind","claim","claim",id,"floor",floor)) or
   * append(obj("kind","sent","ack",id)) writes.
   */
  static StringBuilder envelopeText(
      StringBuilder t, boolean claim, long id, long floor, String quotedStream, long n, long at) {
    t.setLength(0);
    if (claim)
      t.append("{\"kind\":\"claim\",\"claim\":").append(id).append(",\"floor\":").append(floor);
    else t.append("{\"kind\":\"sent\",\"ack\":").append(id);
    return t.append(",\"format\":1,\"stream\":")
        .append(quotedStream)
        .append(",\"n\":")
        .append(n)
        .append(",\"at\":")
        .append(at)
        .append('}');
  }

  /**
   * Journal and apply an outbox claim or sent event without building, encoding and re-reading a
   * map.
   */
  private void appendEnvelope(boolean claim, long id, long floor) {
    if (!healthy) throw new Fault(503, "Business journal fenced");
    long n = version + 1, at = System.nanoTime();
    try {
      journal.writeText(envelopeText(envelope, claim, id, floor, quotedStream(), n, at));
      if (force) journal.sync();
      if (claim) claimed.put(id, floor);
      else {
        outbox.remove(id);
        claimed.remove(id);
      }
      version = n;
      notifyAll();
    } catch (Exception e) {
      healthy = false;
      notifyAll();
      throw new IllegalStateException("Business append/apply outcome requires recovery", e);
    }
  }

  private void append(JsonObject event) {
    if (!healthy) throw new Fault(503, "Business journal fenced");
    event.put("format", 1).put("stream", stream).put("n", version + 1).put("at", System.nanoTime());
    try {
      journal.writeText(event.encode());
      if (force) journal.sync();
      apply(event);
      notifyAll();
    } catch (Exception e) {
      healthy = false;
      notifyAll();
      throw new IllegalStateException("Business append/apply outcome requires recovery", e);
    }
    if (event.containsKey("row")) {
      try {
        publish(event.getJsonObject("row").encode());
      } catch (RuntimeException e) {
        Main.event("Subscriber publication failed at " + version + "; reconnect to order snapshot");
      }
    }
  }

  private void apply(JsonObject e) {
    long n = e.getLong("n");
    if (e.getInteger("format") != 1 || n != version + 1)
      throw new IllegalStateException("Noncontiguous journal at " + n);
    if (n == 1) {
      if (!"open".equals(e.getString("kind"))
          || !session.equals(e.getString("session"))
          || !role.equals(e.getString("role")))
        throw new IllegalStateException("Business stream identity mismatch");
      stream = e.getString("stream");
    }
    if (!stream.equals(e.getString("stream")))
      throw new IllegalStateException("Changed stream epoch");
    if (e.containsKey("row")) {
      var row = e.getJsonObject("row");
      int id = row.getInteger("clOrdID");
      orders.put(id, OrderRow.from(row));
      if (!broker) exchangeIds.put(row.getInteger("orderID"), id);
    }
    if (e.containsKey("receipt")) receipts.put(e.getString("receipt"), e.getString("payload"));
    if (e.containsKey("venue")) {
      int id = e.getInteger("orderKey");
      String venue = e.getString("venue");
      venueIds.put(id, venue);
      venueOwners.put(venue, id);
    }
    if (e.containsKey("cancel")) cancels.put(e.getInteger("orderKey"), e.getJsonObject("cancel"));
    if (e.containsKey("auto")) auto = e.getBoolean("auto");
    if (e.containsKey("delay")) delay = e.getInteger("delay");
    if (e.containsKey("due")) {
      var d = e.getJsonObject("due");
      deadlines.computeIfAbsent(d.getLong("when"), k -> new ArrayList<>()).add(d);
    }
    if (e.containsKey("done")) {
      var d = e.getJsonObject("done");
      var ds = deadlines.get(d.getLong("when"));
      if (ds == null || !ds.remove(d))
        throw new IllegalStateException("Unknown scheduled transition");
      if (ds.isEmpty()) deadlines.remove(d.getLong("when"));
    }
    if (e.containsKey("send")) outbox.put(n, e.getJsonObject("send"));
    if (e.containsKey("claim")) claimed.put(e.getLong("claim"), e.getLong("floor"));
    if (e.containsKey("ack")) {
      long id = e.getLong("ack");
      outbox.remove(id);
      claimed.remove(id);
    }
    version = n;
  }

  synchronized void configure(Boolean mode, Integer seconds) {
    requireAdmissionSpace();
    var e = obj("kind", "configure");
    if (mode != null) e.put("auto", mode);
    if (seconds != null) e.put("delay", seconds);
    append(e);
  }

  private record OrderRow(
      int clientId,
      int venueId,
      int originalId,
      char side,
      char status,
      String symbol,
      long quantity,
      long filled,
      double price) {
    static OrderRow from(JsonObject row) {
      return new OrderRow(
          row.getInteger("clOrdID"),
          row.getInteger("orderID", 0),
          row.getInteger("origClOrdID", 0),
          row.getString("side").charAt(0),
          row.getString("ordStatus").charAt(0),
          row.getString("symbol"),
          row.getLong("orderQty"),
          row.getLong("cumQty"),
          row.getDouble("price"));
    }

    JsonObject json() {
      String code = String.valueOf(status);
      JsonObject row =
          new JsonObject()
              .put("clOrdID", clientId)
              .put("side", String.valueOf(side))
              .put("sideText", side == '1' ? "Buy" : "Sell")
              .put("symbol", symbol)
              .put("price", price)
              .put("orderQty", quantity)
              .put("ordStatus", code)
              .put("ordStatusText", label(code))
              .put("cumQty", filled);
      if (venueId != 0) row.put("origClOrdID", originalId).put("orderID", venueId);
      return row;
    }
  }

  List<JsonObject> list() {
    List<OrderRow> snapshot;
    synchronized (this) {
      snapshot = new ArrayList<>(orders.values());
    }
    if (!broker) snapshot.sort(Comparator.comparingInt(OrderRow::venueId));
    return snapshot.stream().map(OrderRow::json).toList();
  }

  List<JsonObject> page(Integer after, int limit) {
    List<OrderRow> snapshot = new ArrayList<>(limit);
    synchronized (this) {
      if (broker) {
        var range = after == null ? orders : orders.tailMap(after, false);
        for (var row : range.values()) {
          snapshot.add(row);
          if (snapshot.size() == limit) break;
        }
      } else {
        var range = after == null ? exchangeIds : exchangeIds.tailMap(after, false);
        for (int clientId : range.values()) {
          snapshot.add(orders.get(clientId));
          if (snapshot.size() == limit) break;
        }
      }
    }
    return snapshot.stream().map(OrderRow::json).toList();
  }

  /**
   * Newest first: public IDs strictly below {@code before} (or the newest when null), descending.
   */
  List<JsonObject> pageBefore(Integer before, int limit) {
    List<OrderRow> snapshot = new ArrayList<>(limit);
    synchronized (this) {
      if (broker) {
        var range = (before == null ? orders : orders.headMap(before, false)).descendingMap();
        for (var row : range.values()) {
          snapshot.add(row);
          if (snapshot.size() == limit) break;
        }
      } else {
        var range =
            (before == null ? exchangeIds : exchangeIds.headMap(before, false)).descendingMap();
        for (int clientId : range.values()) {
          snapshot.add(orders.get(clientId));
          if (snapshot.size() == limit) break;
        }
      }
    }
    return snapshot.stream().map(OrderRow::json).toList();
  }

  synchronized JsonObject get(int externalId) {
    Integer key = broker ? Integer.valueOf(externalId) : exchangeIds.get(externalId);
    OrderRow row = key == null ? null : orders.get(key);
    if (row == null) throw new Fault(404, "Unknown order");
    return row.json();
  }

  private void available() {
    if (!running || !healthy || fix == null || !fix.online())
      throw new Fault(503, "Session unavailable");
  }

  private static boolean terminal(String s) {
    return Set.of("2", "4", "8").contains(s);
  }

  private static String label(String s) {
    return switch (s) {
      case "Z" -> "Pending Confirmation";
      case "0" -> "New";
      case "1" -> "Partially Filled";
      case "2" -> "Filled";
      case "4" -> "Canceled";
      case "6" -> "Pending Cancel";
      case "8" -> "Rejected";
      default -> throw new IllegalArgumentException(s);
    };
  }

  private static JsonObject state(JsonObject row, String status, long filled) {
    return row.copy()
        .put("ordStatus", status)
        .put("ordStatusText", label(status))
        .put("cumQty", filled);
  }

  private static JsonObject row(
      int id, String side, String symbol, long qty, double price, String status) {
    if (!Set.of("1", "2").contains(side)
        || symbol.isBlank()
        || symbol.length() > 20
        || !symbol.chars().allMatch(c -> c >= 32 && c < 127)
        || qty < 1
        || !Double.isFinite(price)
        || price <= 0
        || price >= 1e15
        || java.math.BigDecimal.valueOf(price).stripTrailingZeros().scale() > 2)
      throw new Fault(400, "Invalid order");
    return state(
        obj(
            "clOrdID",
            id,
            "side",
            side,
            "sideText",
            side.equals("1") ? "Buy" : "Sell",
            "symbol",
            symbol,
            "price",
            price,
            "orderQty",
            qty),
        status,
        0);
  }

  private static JsonObject wire(JsonObject row, String type, String id) {
    return obj(
        "type",
        type,
        "cl",
        id,
        "side",
        row.getString("side"),
        "sym",
        row.getString("symbol"),
        "qty",
        row.getLong("orderQty"),
        "px",
        row.getDouble("price"),
        "time",
        System.currentTimeMillis());
  }

  private JsonObject report(JsonObject row, String cancel) {
    String id = row.getInteger("clOrdID").toString(), st = row.getString("ordStatus");
    var m = wire(row, "8", cancel == null ? id : cancel);
    if (cancel != null) m.put("orig", id);
    return m.put("order", row.getInteger("orderID").toString())
        .put("exec", String.valueOf(version + 1))
        .put("status", st)
        .put("cum", row.getLong("cumQty"))
        .put("leaves", terminal(st) ? 0 : row.getLong("orderQty") - row.getLong("cumQty"))
        .put("execType", Set.of("1", "2").contains(st) ? "F" : st);
  }

  private void room() {
    if (orders.size() >= orderLimit) throw new Fault(503, "In-memory order limit reached");
  }

  synchronized JsonObject newOrder(JsonObject request) {
    available();
    requireAdmissionSpace();
    room();
    var quantity = (Number) request.getValue("orderQty");
    var price = (Number) request.getValue("price");
    if (quantity == null || quantity.doubleValue() != quantity.longValue() || price == null)
      throw new Fault(400, "Invalid quantity or price");
    int id = orders.isEmpty() ? 1 : Math.addExact(orders.lastKey(), 1);
    var r =
        row(
            id,
            request.getString("side", ""),
            request.getString("symbol", "IBM"),
            quantity.longValue(),
            price.doubleValue(),
            "Z");
    append(obj("kind", "submit", "row", r, "send", wire(r, "D", Integer.toString(id))));
    return r.copy();
  }

  synchronized JsonObject change(int id, String method, JsonObject body) {
    available();
    requireAdmissionSpace();
    var r = get(id);
    if (terminal(r.getString("ordStatus"))) throw new Fault(409, "Terminal order");
    if (broker) {
      var prev = cancels.get(id);
      if (prev != null && !prev.getBoolean("retry")) return r;
      int attempt = prev == null ? 1 : prev.getInteger("attempt") + 1;
      String cid = id + "CANCEL" + (attempt == 1 ? "" : attempt);
      append(
          obj(
              "kind",
              "requestCancel",
              "orderKey",
              id,
              "cancel",
              obj("attempt", attempt, "cl", cid, "retry", false),
              "send",
              wire(r, "F", cid).put("orig", String.valueOf(id))));
      return r;
    }
    String st = "DELETE".equals(method) ? "4" : body.getString("ordStatus", "");
    Number cum = (Number) body.getMap().getOrDefault("cumQty", r.getLong("cumQty"));
    long old = r.getLong("cumQty"), qty = r.getLong("orderQty"), value = cum.longValue();
    if (!Set.of("1", "2", "4", "8").contains(st)
        || value != cum.doubleValue()
        || value < old
        || value > qty
        || (st.equals("1") && (value <= old || value >= qty))
        || (st.equals("2") && value != qty)
        || (Set.of("4", "8").contains(st) && value != old))
      throw new Fault(400, "Invalid transition");
    var next = state(r, st, value);
    append(obj("kind", "amend", "row", next, "send", report(next, null)));
    return next.copy();
  }

  synchronized void receive(JsonObject msg) {
    try {
      inbound(msg);
    } catch (RuntimeException e) {
      healthy = false;
      notifyAll();
      Main.event("Held business session on inbound failure: " + e);
      throw e;
    }
  }

  private void inbound(JsonObject msg) {
    String type = msg.getString("type"),
        cl = msg.getString("cl"),
        orig = msg.getString("orig", cl),
        st = msg.getString("status", "");
    boolean legacy =
        "2".equals(msg.getString("execType"))
            && Objects.equals(msg.getString("exec"), msg.getString("order"));
    String key =
        switch (type) {
          case "8" ->
              legacy
                  ? "legacy:"
                      + orig
                      + ":"
                      + msg.getString("exec")
                      + ":"
                      + st
                      + ":"
                      + msg.getLong("cum")
                  : "8:" + msg.getString("exec");
          case "9" -> "9:" + cl + ":" + msg.getString("responseTo");
          default -> type + ":" + cl;
        };
    String prior = receipts.get(key);
    if (prior != null) {
      if (!new JsonObject(prior).equals(msg))
        throw new IllegalStateException("Conflicting receipt " + key);
      return;
    }
    var e = obj("kind", "receive", "receipt", key, "payload", msg.encode());
    int id = Integer.parseInt(orig);
    if (!broker && type.equals("D")) {
      room();
      if (orders.containsKey(id)) throw new IllegalStateException("Reused ClOrdID");
      var r =
          row(
                  id,
                  msg.getString("side"),
                  msg.getString("sym"),
                  msg.getLong("qty"),
                  msg.getDouble("px"),
                  "0")
              .put("origClOrdID", id)
              .put("orderID", orders.size() + 1);
      e.put("row", r).put("send", report(r, null));
      if (auto) e.put("due", deadline(id, "fill", delay, null));
    } else if (broker && type.equals("8")) {
      JsonObject r = get(id);
      long filled = msg.getLong("cum"),
          old = r.getLong("cumQty"),
          qty = r.getLong("orderQty"),
          leaves = msg.getLong("leaves");
      String venue = msg.getString("order"), oldVenue = venueIds.get(id);
      boolean fill = Set.of("1", "2").contains(st);
      if (!Set.of("0", "1", "2", "4", "6", "8").contains(st)
          || terminal(r.getString("ordStatus"))
          || venue == null
          || msg.getString("exec") == null
          || (!legacy && !Objects.equals(msg.getString("execType"), fill ? "F" : st))
          || !r.getString("side").equals(msg.getString("side"))
          || !r.getString("symbol").equals(msg.getString("sym"))
          || (msg.containsKey("qty") && msg.getLong("qty") != qty)
          || (msg.containsKey("px")
              && Double.compare(r.getDouble("price"), msg.getDouble("px")) != 0)
          || (oldVenue != null && !venue.equals(oldVenue))
          || !Objects.equals(venueOwners.getOrDefault(venue, id), id)
          || filled < old
          || filled > qty
          || (fill && filled <= old)
          || (st.equals("1") && filled >= qty)
          || (st.equals("2") && filled != qty)
          || (!fill && filled != old)
          || (st.equals("0") && !"Z".equals(r.getString("ordStatus")))
          || (leaves != (terminal(st) ? 0 : qty - filled)
              && !(legacy && terminal(st) && leaves == qty - filled)))
        throw new IllegalStateException("ExecutionReport contradicts order " + id);
      e.put("row", state(r, st, filled)).put("orderKey", id).put("venue", venue);
    } else if (!broker && type.equals("F")) {
      var stored = orders.get(id);
      var r = stored == null ? null : stored.json();
      if (r == null || terminal(r.getString("ordStatus")) || "6".equals(r.getString("ordStatus")))
        e.put(
            "send",
            obj(
                "type",
                "9",
                "cl",
                cl,
                "orig",
                orig,
                "order",
                r == null ? "NONE" : r.getInteger("orderID").toString(),
                "status",
                r == null ? "8" : r.getString("ordStatus"),
                "reason",
                r == null ? 1 : 0));
      else {
        var next = state(r, "6", r.getLong("cumQty"));
        e.put("row", next).put("send", report(next, cl)).put("due", deadline(id, "cancel", 5, cl));
      }
    } else if (broker && type.equals("9")) {
      var latch = cancels.get(id);
      long reason = msg.getLong("reason", -1L);
      if (latch != null
          && cl.equals(latch.getString("cl"))
          && "1".equals(msg.getString("responseTo"))
          && Set.of("0", "1").contains(st)
          && Objects.equals(venueIds.get(id), msg.getString("venue"))
          && !terminal(get(id).getString("ordStatus"))
          && reason != 1
          && reason != 3
          && reason != 6) e.put("orderKey", id).put("cancel", latch.copy().put("retry", true));
    } else throw new IllegalArgumentException("Unexpected FIX " + type);
    append(e);
  }

  private static JsonObject deadline(int id, String kind, int seconds, String cl) {
    return obj(
        "key", id, "op", kind, "when", System.currentTimeMillis() + 1000L * seconds, "cl", cl);
  }

  private synchronized void due() {
    while (healthy && !deadlines.isEmpty() && deadlines.firstKey() <= System.currentTimeMillis()) {
      var d = deadlines.firstEntry().getValue().get(0);
      var r = orders.get(d.getInteger("key")).json();
      String st = r.getString("ordStatus");
      var e = obj("kind", "deadline", "done", d);
      if (!terminal(st) && (d.getString("op").equals("cancel") == "6".equals(st))) {
        boolean cancel = d.getString("op").equals("cancel");
        var next =
            state(r, cancel ? "4" : "2", cancel ? r.getLong("cumQty") : r.getLong("orderQty"));
        e.put("row", next).put("send", report(next, d.getString("cl")));
      }
      append(e);
    }
  }

  synchronized void signal() {
    notifyAll();
  }

  void startWorkers() {
    running = true;
    senderThread = new Thread(this::sendLoop, "business-fix-outbox");
    timerThread =
        new Thread(
            () -> {
              try {
                while (running) {
                  due();
                  fix.flush();
                  Thread.sleep(50);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              } catch (Exception e) {
                fence(e);
              }
            },
            "business-deadlines");
    senderThread.start();
    timerThread.start();
  }

  private synchronized void fence(Exception e) {
    healthy = false;
    notifyAll();
    Main.event("Business processing held: " + e);
  }

  private void sendLoop() {
    try {
      while (true) {
        long id, floor;
        boolean uncertain;
        JsonObject msg;
        synchronized (this) {
          while (running && (outbox.isEmpty() || !fix.online() || !healthy)) wait(100);
          if (!running) return;
          var head = outbox.firstEntry();
          id = head.getKey();
          msg = head.getValue();
          uncertain = claimed.containsKey(id);
          floor = uncertain ? claimed.get(id) : fix.nextOutbound();
          if (!uncertain) appendEnvelope(true, id, floor);
        }
        if (uncertain) {
          if (!fix.recorded(msg, floor))
            throw new IllegalStateException("Ambiguous native handoff " + id);
        } else fix.send(msg);
        synchronized (this) {
          appendEnvelope(false, id, 0);
        }
      }
    } catch (Exception e) {
      fence(e);
    }
  }

  void close() throws Exception {
    synchronized (this) {
      running = false;
      notifyAll();
    }
    if (senderThread != null) senderThread.join();
    if (timerThread != null) timerThread.join();
    space.close();
    synchronized (this) {
      journal.sync();
      journal.close();
      lock.release();
      owner.close();
    }
  }
}
