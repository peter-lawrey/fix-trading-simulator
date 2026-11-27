import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.URI;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.chronicle.core.util.NanoSampler;
import net.openhft.chronicle.jlbh.*;

/**
 * Open-loop origin. The broker allocates ClOrdID, so the unique symbol correlates HTTP and
 * WebSocket replies.
 */
public class SimulatorLatency implements JLBHTask {
  static final ObjectMapper JSON = new ObjectMapper();
  final HttpClient[] clients;
  final ExecutorService httpWorkers = Executors.newFixedThreadPool(24);
  final String[] roots;
  final int warmup, count, capacity, rate, runs;
  final String prefix;
  final Path results;
  final boolean queueOffers;
  final ExecutorService orderedDispatch =
      Executors.newSingleThreadExecutor(
          r -> {
            Thread thread = new Thread(r, "round-robin-order-dispatch");
            thread.setDaemon(true);
            return thread;
          });
  final AtomicInteger index = new AtomicInteger();
  final Semaphore[] permits;
  final WebSocket[] sockets;
  final NanoSampler[] successProbes;
  NanoSampler combined;
  final Record[] records;
  final String transport = System.getenv().getOrDefault("ORDER_TRANSPORT", "ws");
  final boolean streamOrders;
  final AtomicInteger httpPosts = new AtomicInteger();
  final AtomicInteger commandMessages = new AtomicInteger();
  volatile boolean stopping;
  final CommandChannel[] commands;

  final class CommandChannel implements WebSocket.Listener {
    final int lane;
    final StringBuilder fragments = new StringBuilder();
    final Deque<Integer> pending = new ArrayDeque<>();
    WebSocket socket;
    boolean disconnected;
    CompletableFuture<Void> writes = CompletableFuture.completedFuture(null);

    CommandChannel(int lane) {
      this.lane = lane;
    }

    synchronized void send(int i, String body) {
      if (disconnected) {
        fail(i, "command socket unavailable");
        return;
      }
      pending.addLast(i);
      // The JDK allows only one unfinished send. The lane permit bounds this queue.
      writes =
          writes
              .thenCompose(
                  ignored -> {
                    commandMessages.incrementAndGet();
                    return socket.sendText(body, true);
                  })
              .thenAccept(sent -> {})
              .whenComplete(
                  (sent, error) -> {
                    if (error != null) disconnect("command write failed: " + error);
                  });
    }

    public synchronized CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
      fragments.append(data);
      if (last) {
        Integer i = pending.pollFirst();
        if (i == null) {
          errors.add("unsolicited command reply on client " + lane);
        } else {
          try {
            admission(i, fragments.toString());
          } catch (Exception invalid) {
            fail(i, "invalid command reply: " + invalid);
          }
        }
        fragments.setLength(0);
      }
      ws.request(1);
      return null;
    }

    private synchronized void disconnect(String reason) {
      if (disconnected) return;
      disconnected = true;
      if (!stopping || !pending.isEmpty()) errors.add(reason);
      while (!pending.isEmpty()) fail(pending.removeFirst(), reason);
    }

    public void onError(WebSocket ws, Throwable error) {
      disconnect("command socket error on client " + lane + ": " + error);
    }

    public CompletionStage<?> onClose(WebSocket ws, int code, String reason) {
      disconnect("command socket closed on client " + lane + ": " + code + " " + reason);
      return null;
    }
  }

  final List<String> errors = Collections.synchronizedList(new ArrayList<>());
  JLBH harness;
  long first, last;
  int wrong, duplicates, foreign;

  record WireEvent(
      String symbol, int id, String status, double qty, double cum, double price, String side) {}

  static class Record {
    long due, jlbhDue, dispatch, handoff, http, ack;
    int httpID, wsID;
    String outcome = "not_scheduled";
  }

  SimulatorLatency(String[] args) {
    if (!transport.equals("ws") && !transport.equals("http"))
      throw new IllegalArgumentException("ORDER_TRANSPORT must be ws or http: " + transport);
    streamOrders = transport.equals("ws");
    roots = args[0].split(",");
    clients = new HttpClient[Integer.parseInt(args[1])];
    if (clients.length % roots.length != 0) throw new IllegalArgumentException("uneven sessions");
    Arrays.setAll(
        clients,
        i ->
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .executor(httpWorkers)
                .connectTimeout(Duration.ofSeconds(5))
                .build());
    sockets = new WebSocket[clients.length];
    commands = new CommandChannel[clients.length];
    successProbes = new NanoSampler[roots.length];
    prefix = args[2];
    warmup = Integer.parseInt(args[3]);
    count = Integer.parseInt(args[4]);
    rate = Integer.parseInt(args[5]);
    capacity = Integer.parseInt(args[6]);
    results = Path.of(args[7]);
    queueOffers = args.length > 8 && args[8].equals("queue");
    runs = Integer.parseInt(args[9]);
    records = new Record[warmup + Math.multiplyExact(count, runs)];
    Arrays.setAll(records, i -> new Record());
    permits = new Semaphore[roots.length];
    Arrays.setAll(permits, i -> new Semaphore(capacity));
  }

  public void init(JLBH jlbh) {
    harness = jlbh;
    combined = jlbh.addProbe("matched_ack_combined");
    Arrays.setAll(successProbes, i -> jlbh.addProbe("matched_ack_" + i));
    for (int subscriber = 0; subscriber < clients.length; subscriber++) {
      final int id = subscriber, lane = subscriber % roots.length;
      if (streamOrders) {
        CommandChannel c = new CommandChannel(subscriber);
        commands[subscriber] = c;
        c.socket =
            clients[subscriber]
                .newWebSocketBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .buildAsync(
                    URI.create(roots[lane].replaceFirst("^http", "ws") + "/socket/order-commands"),
                    c)
                .join();
      }
      sockets[subscriber] =
          clients[subscriber]
              .newWebSocketBuilder()
              .connectTimeout(Duration.ofSeconds(10))
              .buildAsync(
                  URI.create(roots[lane].replaceFirst("^http", "ws") + "/socket/order"),
                  new WebSocket.Listener() {
                    final StringBuilder text = new StringBuilder();

                    public CompletionStage<?> onText(
                        WebSocket ws, CharSequence data, boolean last) {
                      long at = System.nanoTime();
                      text.append(data);
                      if (last) {
                        try {
                          onEvent(id, text.toString(), at);
                        } catch (Exception e) {
                          errors.add("websocket " + e);
                        }
                        text.setLength(0);
                      }
                      ws.request(1);
                      return null;
                    }

                    public void onError(WebSocket ws, Throwable failure) {
                      errors.add("websocket " + failure);
                    }
                  })
              .join();
    }
  }

  public void run(long due) {
    int i = index.getAndIncrement(), lane = i % roots.length;
    if (i >= records.length) throw new IllegalStateException("extra iteration");
    Record r = records[i];
    r.jlbhDue = due;
    if (i < warmup) {
      r.due = i == 0 ? due : records[0].due + i * 1_000_000_000L / rate;
      long remaining;
      while ((remaining = r.due - System.nanoTime()) > 0)
        java.util.concurrent.locks.LockSupport.parkNanos(remaining);
    } else {
      int slot = (i - warmup) % count;
      r.due = slot == 0 ? due : records[i - slot].due + slot * 1_000_000_000L / rate;
    }
    r.dispatch = System.nanoTime();
    if (i == warmup) first = r.dispatch;
    if (i == records.length - 1) last = r.dispatch;
    if (i < warmup || queueOffers) {
      r.outcome = "queued";
      orderedDispatch.execute(
          () -> {
            try {
              permits[lane].acquire();
              submit(i);
            } catch (InterruptedException interrupted) {
              Thread.currentThread().interrupt();
            }
          });
      return;
    }
    if (!permits[lane].tryAcquire()) {
      synchronized (this) {
        r.outcome = "not_admitted";
        harness.sample(System.nanoTime() - r.due);
      }
      return;
    }
    submit(i);
  }

  // Stock JLBH uses the end of its busy wait as the origin for the next slot.
  // Anchor each following interval to the same first measured deadline.
  long nextInterval(long interval) {
    int assigned = index.get();
    if (assigned <= warmup || (assigned - warmup) % count == 0) return interval;
    int slot = (assigned - warmup) % count;
    long ideal = records[assigned - slot].due + slot * 1_000_000_000L / rate;
    return Math.max(0, ideal - records[assigned - 1].jlbhDue);
  }

  void submit(int i) {
    int lane = i % roots.length;
    Record r = records[i];
    r.outcome = "submitted";
    String body =
        "{\"symbol\":\"" + prefix + i + "\",\"side\":\"1\",\"price\":23.5,\"orderQty\":10}";
    if (streamOrders) {
      r.handoff = System.nanoTime();
      commands[i % commands.length].send(i, body);
      return;
    }
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(roots[lane] + "/orders"))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(20))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    r.handoff = System.nanoTime();
    httpPosts.incrementAndGet();
    clients[i % clients.length]
        .sendAsync(req, HttpResponse.BodyHandlers.ofString())
        .whenComplete(
            (res, ex) -> {
              synchronized (this) {
                if (ex != null || res.statusCode() != 200) {
                  fail(i, ex == null ? "HTTP " + res.statusCode() : ex.toString());
                  return;
                }
                try {
                  admission(i, res.body());
                } catch (Exception err) {
                  fail(i, err.toString());
                }
              }
            });
  }

  synchronized void admission(int i, String body) throws IOException {
    WireEvent e = decode(body);
    if (!e.symbol.equals(prefix + i)
        || !valid(e)
        || !"Z".equals(e.status)
        || e.id < 1
        || records[i].http != 0) {
      fail(i, "invalid admission response: " + body);
      return;
    }
    records[i].httpID = e.id;
    records[i].http = System.nanoTime();
    finish(i);
  }

  static WireEvent decode(String value) throws IOException {
    JsonNode n = JSON.readTree(value);
    return new WireEvent(
        n.path("symbol").asText(),
        n.path("clOrdID").asInt(),
        n.path("ordStatus").asText(),
        n.path("orderQty").asDouble(),
        n.path("cumQty").asDouble(),
        n.path("price").asDouble(),
        n.path("side").asText());
  }

  static boolean valid(WireEvent e) {
    return e.qty == 10 && e.cum == 0 && e.price == 23.5 && "1".equals(e.side);
  }

  synchronized void onEvent(int subscriber, String text, long at) throws IOException {
    WireEvent e = decode(text);
    if (!e.symbol.startsWith(prefix)) {
      foreign++;
      return;
    }
    int i = Integer.parseInt(e.symbol.substring(prefix.length()));
    if (i < 0 || i >= records.length || i % roots.length != subscriber % roots.length) {
      wrong++;
      return;
    }
    if (i % clients.length != subscriber)
      return; // each subscriber on a broker receives its broadcast
    if ("Z".equals(e.status))
      return; // local initial status; exchange has not acknowledged the order yet
    if (!"0".equals(e.status) || !valid(e) || e.id < 1) {
      fail(i, "unexpected WS " + text);
      return;
    }
    Record r = records[i];
    if (r.ack != 0) {
      duplicates++;
      return;
    }
    r.ack = at;
    r.wsID = e.id;
    finish(i);
  }

  void finish(int i) {
    Record r = records[i];
    if (r.http == 0 || r.ack == 0 || !"submitted".equals(r.outcome)) return;
    if (r.httpID != r.wsID) {
      fail(i, "mismatched broker ClOrdID");
      return;
    }
    r.outcome = "success";
    permits[i % roots.length].release();
    long latency = r.ack - r.due;
    successProbes[i % roots.length].sampleNanos(latency);
    combined.sampleNanos(latency);
    harness.sample(latency);
  }

  synchronized void fail(int i, String error) {
    Record r = records[i];
    if (errors.size() < 20) errors.add("i=" + i + " " + error);
    if (!"submitted".equals(r.outcome)) return;
    r.outcome = "http_or_protocol_failure";
    permits[i % roots.length].release();
    harness.sample(System.nanoTime() - r.due);
  }

  void close(WebSocket ws) {
    if (ws == null || ws.isOutputClosed()) return;
    try {
      ws.sendClose(WebSocket.NORMAL_CLOSURE, "end").orTimeout(3, TimeUnit.SECONDS).join();
    } catch (CompletionException failure) {
      errors.add("socket close: " + failure);
      ws.abort();
    }
  }

  void save() throws Exception {
    orderedDispatch.shutdownNow();
    Thread.sleep(
        1000); // keep subscriptions alive for duplicate replies after JLBH reaches its count
    stopping = true;
    for (CommandChannel channel : commands) if (channel != null) close(channel.socket);
    for (WebSocket ws : sockets) close(ws);
    synchronized (this) {
      try (var out = Files.newBufferedWriter(results.resolve("orders.tsv"))) {
        out.write(
            "phase\ti\tlane\tsymbol\toutcome\tclOrdID\twsClOrdID\tdue_ns\tjlbh_due_ns\tdispatch_ns"
                + "\thandoff_ns\thttp_ns\tack_ns\n");
        for (int i = 0; i < records.length; i++) {
          Record r = records[i];
          out.write(
              String.join(
                      "\t",
                      i < warmup ? "warmup" : "measured",
                      "" + i,
                      "" + (i % roots.length),
                      prefix + i,
                      r.outcome,
                      "" + r.httpID,
                      "" + r.wsID,
                      "" + r.due,
                      "" + r.jlbhDue,
                      "" + r.dispatch,
                      "" + r.handoff,
                      "" + r.http,
                      "" + r.ack)
                  + "\n");
        }
      }
      Map<String, Object> info = new LinkedHashMap<>();
      info.put("order_transport", transport);
      info.put("http_posts", httpPosts.get());
      info.put("command_messages", commandMessages.get());
      info.put("scheduled_callbacks", index.get());
      info.put("total_expected", records.length);
      info.put("dispatch_span_s", (last - first) * 1e-9);
      info.put("wrong_lane", wrong);
      info.put("duplicates", duplicates);
      info.put("foreign_events", foreign);
      info.put("errors", List.copyOf(errors));
      JSON.writerWithDefaultPrettyPrinter()
          .writeValue(results.resolve("driver.json").toFile(), info);
    }
  }

  public static void main(String[] args) throws Exception {
    var task = new SimulatorLatency(args);
    long deadlineSeconds = 60L + (task.warmup + (long) task.count * task.runs) / task.rate;
    var jlbh =
        new JLBH(
            new JLBHOptions()
                .recordOSJitter(false)
                .warmUpIterations(task.warmup)
                .iterations(task.count)
                .throughput(task.rate)
                .runs(task.runs)
                .latencyDistributor(task::nextInterval)
                .timeout(TimeUnit.SECONDS.toMillis(deadlineSeconds))
                .accountForCoordinatedOmission(true)
                .jlbhTask(task));
    ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor();
    try {
      watchdog.schedule(jlbh::abort, deadlineSeconds, TimeUnit.SECONDS);
      jlbh.start();
    } finally {
      watchdog.shutdownNow();
      try {
        task.save();
      } finally {
        task.httpWorkers.shutdownNow();
      }
    }
  }
}
