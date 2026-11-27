package com.helesto.rest;

import com.helesto.dto.OrderDto;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.annotation.Priority;
import javax.json.bind.Jsonb;
import javax.json.spi.JsonProvider;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParserFactory;
import javax.ws.rs.Consumes;
import javax.ws.rs.Priorities;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.Provider;
import org.eclipse.yasson.YassonJsonb;
import org.jboss.resteasy.plugins.providers.jsonb.JsonBindingProvider;

/**
 * Avoid the decoder's 8 KiB byte buffer for short ASCII order bodies. Use the existing JSON-B
 * mapping.
 */
@Provider
@Priority(Priorities.ENTITY_CODER - 1)
@Consumes(MediaType.APPLICATION_JSON)
public class SmallOrderReader extends JsonBindingProvider {
  private static final int MAX_ASCII_BODY = 512;

  /**
   * Thread-safe by the JSON-P contract. Every parser it creates has exactly one owner, the decode
   * call.
   */
  private static final JsonParserFactory PARSERS =
      JsonProvider.provider().createParserFactory(Collections.emptyMap());

  @Override
  public boolean isReadable(
      Class<?> type, Type generic, Annotation[] annotations, MediaType media) {
    return type == OrderDto.class && super.isReadable(type, generic, annotations, media);
  }

  @Override
  public boolean isWriteable(
      Class<?> type, Type generic, Annotation[] annotations, MediaType media) {
    return false;
  }

  @Override
  public Object readFrom(
      Class<Object> type,
      Type generic,
      Annotation[] annotations,
      MediaType media,
      MultivaluedMap<String, String> headers,
      InputStream input)
      throws IOException {
    // Bounded lookahead also works for chunked bodies with no Content-Length.
    byte[] prefix = input.readNBytes(MAX_ASCII_BODY + 1);
    if (prefix.length > 0 && prefix.length <= MAX_ASCII_BODY && isAscii(prefix)) {
      try {
        return decode(getJsonb(type), new String(prefix, StandardCharsets.US_ASCII), generic);
      } catch (RuntimeException invalid) {
        // Preserve RESTEasy's original empty-body and parse-error handling.
      }
    }
    return super.readFrom(
        type,
        generic,
        annotations,
        media,
        headers,
        new SequenceInputStream(new ByteArrayInputStream(prefix), input));
  }

  /**
   * Map one complete body with the request's own JSON-P parser. Yasson 1.0.9's fromJson(String,
   * Type) creates a parser it never closes, so its pooled 4,096-char buffer is never returned and
   * every request allocates a new one. YassonJsonb.fromJson(JsonParser, Type) runs the same
   * Unmarshaller over a caller-owned parser and leaves closing to the caller. The parser is a
   * local, closed exactly once on every path (a second close would return the same buffer to the
   * pool twice), so concurrent and nested calls never share one.
   */
  static Object decode(Jsonb jsonb, String body, Type generic) {
    if (!(jsonb instanceof YassonJsonb)) {
      return jsonb.fromJson(body, generic);
    }
    try (JsonParser parser = PARSERS.createParser(new StringReader(body))) {
      return ((YassonJsonb) jsonb).fromJson(parser, generic);
    }
  }

  /**
   * A stream frame represents exactly one order. REST retains its existing entity-reader contract.
   */
  static OrderDto decodeCommand(Jsonb jsonb, String text) {
    try (JsonParser parser = PARSERS.createParser(new StringReader(text))) {
      OrderDto order;
      if (jsonb instanceof YassonJsonb) {
        order = ((YassonJsonb) jsonb).fromJson(parser, OrderDto.class);
      } else {
        if (!parser.hasNext() || parser.next() != JsonParser.Event.START_OBJECT)
          throw new javax.json.JsonException("Expected an order object");
        order = jsonb.fromJson(parser.getObject().toString(), OrderDto.class);
      }
      if (parser.hasNext()) throw new javax.json.JsonException("Expected exactly one order");
      return order;
    }
  }

  private static boolean isAscii(byte[] data) {
    for (byte b : data) {
      if (b <= 0) return false; // Preserve encoding detection for BOMs, wide text and NUL.
    }
    return true;
  }
}
