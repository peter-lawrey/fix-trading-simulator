import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/** Live broker contract. Run via check-journal.py against real broker and exchange processes. */
public class OrderEntryContract {
    static final ObjectMapper JSON = new ObjectMapper();
    static final HttpClient CLIENT = HttpClient.newHttpClient();
    static final long TIMEOUT = 8;
    static String origin;

    static void check(boolean ok, String message) {
        if (!ok) throw new AssertionError(message);
    }

    static class Channel implements WebSocket.Listener, AutoCloseable {
        final BlockingQueue<String> messages = new LinkedBlockingQueue<>();
        final StringBuilder fragments = new StringBuilder();
        final WebSocket socket;

        Channel(String path) {
            socket = CLIENT.newWebSocketBuilder().buildAsync(URI.create(origin.replace("http:", "ws:") + path), this)
                    .orTimeout(TIMEOUT, TimeUnit.SECONDS).join();
        }

        public CompletionStage<?> onText(WebSocket ws, CharSequence part, boolean last) {
            fragments.append(part);
            if (last) {
                messages.add(fragments.toString());
                fragments.setLength(0);
            }
            ws.request(1);
            return null;
        }

        public void onError(WebSocket ws, Throwable error) { messages.add("socket error: " + error); }

        void send(String s) { socket.sendText(s, true).orTimeout(TIMEOUT, TimeUnit.SECONDS).join(); }
        JsonNode receive() throws Exception {
            String s = messages.poll(TIMEOUT, TimeUnit.SECONDS);
            check(s != null, "missing WebSocket response");
            return JSON.readTree(s);
        }
        public void close() { socket.sendClose(1000, "test complete").orTimeout(TIMEOUT, TimeUnit.SECONDS).join(); }
    }

    static String body(String symbol, String side, int qty) {
        return "{\"symbol\":\"" + symbol + "\",\"side\":\"" + side + "\",\"orderQty\":" + qty + ",\"price\":23.5}";
    }

    static HttpResponse<String> post(String body) throws Exception {
        return CLIENT.send(HttpRequest.newBuilder(URI.create(origin + "/orders"))
                .timeout(java.time.Duration.ofSeconds(TIMEOUT)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    static void accepted(List<JsonNode> rows, JsonNode response, String symbol, String side, int qty) {
        check(response.path("clOrdID").asInt() > 0 && response.path("ordStatus").asText().equals("Z")
                && response.path("symbol").asText().equals(symbol) && response.path("side").asText().equals(side)
                && response.path("orderQty").asInt() == qty && response.path("price").asDouble() == 23.5
                && response.path("cumQty").asDouble(-1) == 0, "wrong admission: " + response);
        rows.add(response);
    }

    static void rejected(Channel channel, String body) throws Exception {
        channel.send(body);
        JsonNode reply = channel.receive();
        check(reply.path("status").asInt() == 400 && !reply.path("error").asText().isEmpty()
                && !reply.has("clOrdID"), "wrong rejection: " + reply);
    }

    public static void main(String[] args) throws Exception {
        origin = args[0];
        List<JsonNode> rows = new ArrayList<>();
        try (Channel updates = new Channel("/socket/order?hello=1");
             Channel a = new Channel("/socket/order-commands");
             Channel b = new Channel("/socket/order-commands")) {
            check(updates.receive().path("subscribed").asBoolean(), "subscription not ready");
            String invalidQty = body("BAD", "1", 0);
            check(post(invalidQty).statusCode() == 400 && post("{bad}").statusCode() == 400, "HTTP rejection");
            for (String invalid : List.of("{bad}", "null", "[]", "", invalidQty,
                    body("BAD", "!", 10), "{\"side\":\"1\",\"orderQty\":2,\"price\":1}",
                    body("BAD", "1", 10) + " {}")) rejected(a, invalid);

            // Same symbol on different channels and two outstanding on one channel.
            a.send(body("SAME", "1", 11));
            b.send(body("SAME", "2", 13));
            a.send(" ".repeat(700) + body("SAME", "2", 12));
            HttpResponse<String> http = post(body("SAME", "1", 14));
            check(http.statusCode() == 200 && http.headers().firstValue("content-type").orElse("").contains("application/json"),
                    "HTTP success contract");
            accepted(rows, JSON.readTree(http.body()), "SAME", "1", 14);
            accepted(rows, b.receive(), "SAME", "2", 13);
            accepted(rows, a.receive(), "SAME", "1", 11);
            accepted(rows, a.receive(), "SAME", "2", 12);
            String fragmented = body("FRAG", "1", 15);
            a.socket.sendText(fragmented.substring(0, 12), false).join();
            a.send(fragmented.substring(12));
            accepted(rows, a.receive(), "FRAG", "1", 15);
            try (Channel fresh = new Channel("/socket/order-commands")) {
                fresh.send(body("REOPEN", "2", 16));
                accepted(rows, fresh.receive(), "REOPEN", "2", 16);
            }

            Map<Integer, JsonNode> waiting = new HashMap<>();
            for (JsonNode order : rows)
                check(waiting.put(order.get("clOrdID").asInt(), order) == null, "duplicate admission ID");
            while (!waiting.isEmpty()) {
                JsonNode event = updates.receive();
                if (event.path("ordStatus").asText().equals("Z")) continue;
                JsonNode order = waiting.remove(event.path("clOrdID").asInt());
                check(order != null && event.path("ordStatus").asText().equals("0"), "unexpected update: " + event);
                for (String field : List.of("side", "symbol"))
                    check(order.path(field).asText().equals(event.path(field).asText()), "wrong update " + field);
                for (String field : List.of("price", "orderQty", "cumQty"))
                    check(event.path(field).isNumber() && order.get(field).decimalValue().compareTo(event.get(field).decimalValue()) == 0,
                            "wrong numeric update " + field);
            }
            check(a.messages.isEmpty() && b.messages.isEmpty(), "another connection's private command reply");
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[1]).toFile(), rows);
    }
}
