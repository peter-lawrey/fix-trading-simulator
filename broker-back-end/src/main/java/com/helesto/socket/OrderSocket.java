package com.helesto.socket;

import com.helesto.dto.OrderDto;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.bind.Jsonb;
import javax.websocket.OnClose;
import javax.websocket.OnError;
import javax.websocket.OnOpen;
import javax.websocket.Session;
import javax.websocket.server.ServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ServerEndpoint("/socket/order")
@ApplicationScoped
public class OrderSocket {

  private static final Logger LOG = LoggerFactory.getLogger(OrderSocket.class.getName());

  private final ConcurrentHashMap<Session, Client> sessions = new ConcurrentHashMap<>();

  // One outstanding container write per client. A stalled connection cannot retain an unbounded
  // queue.
  static final class Client {
    final Session session;
    final ArrayDeque<String> pending = new ArrayDeque<>();
    int bytes;
    boolean closed;

    Client(Session session) {
      this.session = session;
    }

    synchronized boolean offer(String json) {
      if (closed) return false;
      int size = json.length() * 2;
      if (pending.size() >= 512 || bytes + size > 512 * 1024) {
        closed = true;
        pending.clear();
        return false;
      }
      pending.addLast(json);
      bytes += size;
      if (pending.size() == 1) sendHead();
      return !closed;
    }

    synchronized void sendHead() {
      if (closed || pending.isEmpty()) return;
      try {
        session
            .getAsyncRemote()
            .sendText(
                pending.peekFirst(),
                result -> {
                  synchronized (Client.this) {
                    if (closed) return;
                    if (!result.isOK()) {
                      closed = true;
                      pending.clear();
                      closeSocket();
                      return;
                    }
                    bytes -= 2 * pending.removeFirst().length();
                    if (!pending.isEmpty()) sendHead();
                  }
                });
      } catch (RuntimeException transportFailure) {
        closed = true;
        pending.clear();
        closeSocket();
      }
    }

    void closeSocket() {
      try {
        session.close();
      } catch (Exception ignored) {
      }
    }
  }

  // Sent first to a client that connects with ?hello=1, after it is registered and before any later
  // broadcast.
  // A client that waits for this frame before reading a snapshot receives every update the snapshot
  // may miss.
  static final String SUBSCRIBED = "{\"subscribed\":true}";

  @OnOpen
  public void onOpen(Session session) {
    LOG.debug("onOpen " + session.getId());
    Client client = new Client(session);
    var hello = session.getRequestParameterMap().get("hello");
    synchronized (this) {
      sessions.put(session, client);
      if (hello != null && hello.contains("1") && !client.offer(SUBSCRIBED)) {
        sessions.remove(session, client);
        client.closeSocket();
      }
    }
  }

  @OnClose
  public void onClose(Session session) {
    LOG.debug("onClose " + session.getId());
    sessions.remove(session);
  }

  @OnError
  public void onError(Session session, Throwable throwable) {
    LOG.debug("onError " + session.getId() + " left on error: " + throwable);
    sessions.remove(session);
  }

  @Inject Jsonb jsonb;

  public void broadcast(OrderDto orderDto) {
    if (sessions.isEmpty()) return;
    String jsonString = jsonb.toJson(orderDto);
    broadcast(jsonString);
  }

  public synchronized void broadcast(String message) {
    sessions.forEach(
        (session, client) -> {
          if (!client.offer(message)) {
            sessions.remove(session, client);
            client.closeSocket();
          }
        });
  }
}
