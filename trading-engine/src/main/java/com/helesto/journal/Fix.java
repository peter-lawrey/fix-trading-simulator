package com.helesto.journal;

import static com.helesto.journal.Main.*;

import io.vertx.core.json.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import quickfix.*;

/** QuickFIX/J 2.3.0 adapter to the same ledger, public API and directional journals. */
public final class Fix extends ApplicationAdapter {
  private final RawHistory receivedBytes = new RawHistory(), sentBytes = new RawHistory();
  private FileChannel incoming, outgoing;
  private Connector connector;
  private final SessionID id = new SessionID("FIX.4.4", sender, target);
  private final SessionSettings settings = new SessionSettings();
  private volatile Session active;
  private volatile boolean started;
  private Message lastOrder, lastReport;

  void open() throws Exception {
    Path dir = store.resolve("native");
    Files.createDirectories(dir);
    Map<String, String> config =
        Map.ofEntries(
            Map.entry("BeginString", "FIX.4.4"),
            Map.entry("SenderCompID", sender),
            Map.entry("TargetCompID", target),
            Map.entry("ConnectionType", broker ? "initiator" : "acceptor"),
            Map.entry("HeartBtInt", "30"),
            Map.entry("StartTime", "00:00:00"),
            Map.entry("EndTime", "00:00:00"),
            Map.entry("ReconnectInterval", "1"),
            Map.entry("FileStorePath", dir.toString()),
            Map.entry("FileStoreSync", "N"),
            Map.entry("PersistMessages", "Y"),
            Map.entry("SocketTcpNoDelay", "Y"),
            Map.entry(
                broker ? "SocketConnectHost" : "SocketAcceptAddress",
                setting("SIM_FIX_HOST", "127.0.0.1")),
            Map.entry(
                broker ? "SocketConnectPort" : "SocketAcceptPort",
                System.getProperty("sim.fixPort", setting("SIM_FIX_PORT", "9880"))));
    config.forEach((k, v) -> settings.setString(id, k, v));
    incoming = openCapture("incoming", receivedBytes);
    outgoing = openCapture("outgoing", sentBytes);
    Log log =
        new Log() {
          public void clear() {
            /* Directional history is retained through session resets. */
          }

          public void onIncoming(String frame) {
            capture(incoming, receivedBytes, frame);
          }

          public void onOutgoing(String frame) {
            capture(outgoing, sentBytes, frame);
          }

          public void onEvent(String msg) {
            event(msg);
          }

          public void onErrorEvent(String msg) {
            event(msg);
          }
        };
    var factory = new FileStoreFactory(settings);
    connector =
        broker
            ? new SocketInitiator(this, factory, settings, s -> log, new DefaultMessageFactory())
            : new SocketAcceptor(this, factory, settings, s -> log, new DefaultMessageFactory());
    startSession();
  }

  FileChannel openCapture(String name, RawHistory history) throws IOException {
    Path path = store.resolve(name + ".fix");
    FileChannel channel =
        FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    try (var input = Files.newInputStream(path)) {
      history.load(input);
    }
    return channel;
  }

  static void capture(FileChannel channel, RawHistory history, String frame) {
    var bytes = ByteBuffer.wrap(frame.getBytes(StandardCharsets.ISO_8859_1));
    synchronized (history) {
      try {
        while (bytes.hasRemaining()) channel.write(bytes);
        history.append(bytes.rewind());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public void onCreate(SessionID sid) {
    active = Session.lookupSession(sid);
  }

  @Override
  public void onLogon(SessionID sid) {
    active = Session.lookupSession(sid);
    event("Logon " + sid);
    ledger.signal();
  }

  @Override
  public void onLogout(SessionID sid) {
    event("Logout " + sid);
  }

  boolean online() {
    var s = active;
    return s != null && s.isLoggedOn();
  }

  synchronized void startSession() throws Exception {
    if (!started) {
      connector.start();
      started = true;
    }
    active = Session.lookupSession(id);
    if (active != null) active.logon();
  }

  synchronized void stopSession() {
    if (started) {
      connector.stop(false);
      started = false;
    }
  }

  synchronized void logout() {
    if (active != null) active.logout();
  }

  synchronized void close() throws Exception {
    try {
      stopSession();
    } finally {
      try {
        flush();
      } finally {
        if (incoming != null) incoming.close();
        if (outgoing != null) outgoing.close();
      }
    }
  }

  void flush() throws IOException {
    if (incoming != null && incoming.isOpen()) incoming.force(false);
    if (outgoing != null && outgoing.isOpen()) outgoing.force(false);
  }

  long nextOutbound() {
    return active.getExpectedSenderNum();
  }

  JsonObject status() {
    var props = new JsonArray();
    var lines = new JsonArray().add("[session]");
    try {
      var p = settings.getSessionProperties(id, true);
      for (String k : new TreeSet<>(p.stringPropertyNames())) {
        props.add(obj("key", k, "value", p.getProperty(k)));
        lines.add(k + "=" + p.getProperty(k));
      }
    } catch (ConfigError e) {
      throw new IllegalStateException(e);
    }
    Session s = active;
    return obj(
        broker ? "initiatorStarted" : "acceptorStarted",
        started,
        "loggedOn",
        online(),
        "sessionID",
        session,
        "startTime",
        startTime,
        "sessionSettingsProperties",
        props,
        "sessionSettingsFile",
        lines,
        "sessionStorage",
        obj(
            "creationTime",
            startTime,
            "incomingSeqnum",
            s == null ? 1 : s.getExpectedTargetNum(),
            "outgoingSeqnum",
            s == null ? 1 : s.getExpectedSenderNum()));
  }

  @Override
  public void fromApp(Message m, SessionID sid) throws FieldNotFound, UnsupportedMessageType {
    String type = m.getHeader().getString(35);
    var j = obj("type", type, "cl", m.getString(11));
    switch (type) {
      case "D", "F" -> {
        j.put("sym", m.getString(55))
            .put("side", m.getString(54))
            .put("qty", (long) m.getDouble(38));
        if (type.equals("D")) j.put("px", m.getDouble(44));
        else j.put("orig", m.getString(41));
      }
      case "8" -> {
        j.put("order", m.getString(37))
            .put("exec", m.getString(17))
            .put("execType", m.getString(150))
            .put("status", m.getString(39))
            .put("cum", (long) m.getDouble(14))
            .put("leaves", (long) m.getDouble(151))
            .put("side", m.getString(54))
            .put("sym", m.getString(55));
        if (m.isSetField(38)) j.put("qty", (long) m.getDouble(38));
        if (m.isSetField(44)) j.put("px", m.getDouble(44));
        if (m.isSetField(41)) j.put("orig", m.getString(41));
      }
      case "9" ->
          j.put("orig", m.getString(41))
              .put("status", m.getString(39))
              .put("reason", m.getInt(102))
              .put("responseTo", m.getString(434))
              .put("venue", m.getString(37));
      default -> throw new UnsupportedMessageType();
    }
    ledger.receive(j);
  }

  private static void setQuantity(Message m, int tag, long value) {
    if (value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) m.setInt(tag, (int) value);
    else m.setString(tag, Long.toString(value));
  }

  void send(JsonObject j) {
    String type = j.getString("type");
    Message m;
    if (type.equals("D")) {
      if (lastOrder == null) lastOrder = new quickfix.fix44.NewOrderSingle();
      m = lastOrder;
    } else if (type.equals("8")) {
      if (lastReport == null) lastReport = new quickfix.fix44.ExecutionReport();
      m = lastReport;
    } else m = new Message();
    // The outbox is single-writer. QuickFIX/J serializes the message to a String before send
    // returns.
    m.clear();
    m.getHeader().setString(35, type);
    m.setString(11, j.getString("cl"));
    if (j.containsKey("orig")) m.setString(41, j.getString("orig"));
    if (!type.equals("9")) {
      m.setString(55, j.getString("sym"));
      m.setString(54, j.getString("side"));
      setQuantity(m, 38, j.getLong("qty"));
    }
    switch (type) {
      case "D", "F" -> {
        m.setString(60, TRANSACTION_TIME.format(Instant.ofEpochMilli(j.getLong("time"))));
        if (type.equals("D")) {
          m.setDouble(44, j.getDouble("px"));
          m.setString(40, "2");
          m.setString(1, "1");
          m.setString(22, "8");
          m.setString(59, "0");
        }
      }
      case "8" -> {
        m.setString(37, j.getString("order"));
        m.setString(17, j.getString("exec"));
        m.setString(39, j.getString("status"));
        m.setString(150, j.getString("execType"));
        m.setDouble(44, j.getDouble("px"));
        setQuantity(m, 14, j.getLong("cum"));
        setQuantity(m, 151, j.getLong("leaves"));
        m.setInt(6, 0);
      }
      case "9" -> {
        m.setString(37, j.getString("order"));
        m.setString(39, j.getString("status"));
        m.setString(434, "1");
        m.setInt(102, j.getInteger("reason"));
      }
      default -> throw new IllegalArgumentException(type);
    }
    if (!active.send(m)) throw new IllegalStateException("QuickFIX/J declined " + type);
  }

  boolean recorded(JsonObject intent, long floor) {
    try {
      int end = active.getExpectedSenderNum();
      for (int seq = Math.toIntExact(floor); seq < end; seq += 1024) {
        List<String> messages = new ArrayList<>();
        active.getStore().get(seq, Math.min(end - 1, seq + 1023), messages);
        for (String frame : messages) if (matches(intent, frame)) return true;
      }
      return false;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Common history and reconciliation predicates are copied unchanged by prepare.py.
  static Map<String, String> fields(String frame) {
    var map = new HashMap<String, String>();
    for (String part : frame.split("\u0001")) {
      int eq = part.indexOf('=');
      if (eq > 0) map.put(part.substring(0, eq), part.substring(eq + 1));
    }
    return map;
  }

  static final java.time.format.DateTimeFormatter TRANSACTION_TIME =
      java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSS")
          .withZone(ZoneOffset.UTC);

  boolean matches(JsonObject j, String frame) {
    var f = fields(frame);
    if (!sender.equals(f.get("49"))
        || !target.equals(f.get("56"))
        || !j.getString("type").equals(f.get("35"))) return false;
    String type = j.getString("type");
    if (type.equals("D") || type.equals("F")) {
      Long time = j.getLong("time");
      if (time == null || !TRANSACTION_TIME.format(Instant.ofEpochMilli(time)).equals(f.get("60")))
        return false;
    }
    if (type.equals("D")
        && !("2".equals(f.get("40"))
            && "1".equals(f.get("1"))
            && "8".equals(f.get("22"))
            && "0".equals(f.get("59")))) return false;
    if (type.equals("9") && !"1".equals(f.get("434"))) return false;
    if (type.equals("8")
        && (f.get("6") == null || new java.math.BigDecimal(f.get("6")).signum() != 0)) return false;
    Map<String, String> ids =
        Map.of(
            "cl",
            "11",
            "orig",
            "41",
            "order",
            "37",
            "exec",
            "17",
            "status",
            "39",
            "execType",
            "150",
            "sym",
            "55",
            "side",
            "54");
    for (var e : ids.entrySet())
      if (j.containsKey(e.getKey()) && !j.getString(e.getKey()).equals(f.get(e.getValue())))
        return false;
    Map<String, String> nums =
        Map.of("qty", "38", "px", "44", "cum", "14", "leaves", "151", "reason", "102");
    for (var e : nums.entrySet())
      if (j.containsKey(e.getKey())
          && !(e.getKey().equals("px") && j.getString("type").equals("F"))) {
        String val = f.get(e.getValue());
        if (val == null
            || new java.math.BigDecimal(val)
                    .compareTo(new java.math.BigDecimal(j.getValue(e.getKey()).toString()))
                != 0) return false;
      }
    return true;
  }

  JsonArray historyPage(boolean incoming, Integer before, int limit) {
    RawHistory history = incoming ? receivedBytes : sentBytes;
    JsonArray page = new JsonArray();
    for (RawHistory.Frame frame : history.page(before, limit)) {
      String text = frame.text();
      page.add(
          obj(
              "id",
              frame.id(),
              "time",
              fields(text).getOrDefault("52", ""),
              "text",
              text.replace("\u0001", " | ")));
    }
    return page;
  }

  JsonArray history(String dir, boolean sessionMessages) throws IOException {
    String bytes = (dir.equals("incoming") ? receivedBytes : sentBytes).asLatin1();
    var a = new ArrayList<JsonObject>();
    int pos = 0;
    while ((pos = bytes.indexOf("8=FIX.", pos)) >= 0) {
      int end = bytes.indexOf("\u000110=", pos), next = bytes.indexOf("8=FIX.", pos + 2);
      if (end < 0) break;
      if (next >= 0 && next < end) {
        pos = next;
        continue;
      }
      end = bytes.indexOf('\u0001', end + 1);
      if (end < 0) break;
      String frame = bytes.substring(pos, end + 1);
      pos = end + 1;
      var f = fields(frame);
      String rendered = frame.replace("\u0001", " | ");
      if (sessionMessages)
        a.add(obj("msgseqnum", Integer.parseInt(f.getOrDefault("34", "0")), "message", rendered));
      else a.add(obj("id", a.size() + 1, "time", f.getOrDefault("52", ""), "text", rendered));
    }
    Collections.reverse(a);
    return new JsonArray(new ArrayList<>(a));
  }
}
