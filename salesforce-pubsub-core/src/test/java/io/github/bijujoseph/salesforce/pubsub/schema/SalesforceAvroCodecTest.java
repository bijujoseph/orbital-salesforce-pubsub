/*
 * Copyright 2026 the Orbital Salesforce Pub/Sub Connector contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.bijujoseph.salesforce.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.EventDecodeException;
import io.github.bijujoseph.salesforce.pubsub.error.EventEncodeException;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.SchemaMetadata;
import io.github.bijujoseph.salesforce.pubsub.transport.TopicMetadata;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SalesforceAvroCodecTest {

  private static final String SCHEMA_ID = "schema-1";
  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              """
              {
                "type": "record",
                "name": "TestEvent",
                "namespace": "example",
                "fields": [
                  {"name": "text", "type": "string"},
                  {"name": "flag", "type": "boolean"},
                  {"name": "count", "type": "int"},
                  {"name": "largeCount", "type": "long"},
                  {"name": "ratio", "type": "float"},
                  {"name": "measurement", "type": "double"},
                  {"name": "businessDate", "type": {"type": "int", "logicalType": "date"}},
                  {"name": "occurredAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                  {"name": "preciseAt", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                  {"name": "alwaysNull", "type": "null"},
                  {"name": "optional", "type": ["null", "string"], "default": null},
                  {"name": "optionalDate", "type": ["null", {"type": "int", "logicalType": "date"}], "default": null},
                  {"name": "tags", "type": {"type": "array", "items": "string"}},
                  {"name": "children", "type": {"type": "array", "items": {
                    "type": "record",
                    "name": "Child",
                    "fields": [
                      {"name": "name", "type": "string"},
                      {"name": "rank", "type": "long"}
                    ]
                  }}},
                  {"name": "binary", "type": "bytes"}
                ]
              }
              """);

  private SalesforceAvroCodec codec;
  private Map<String, Schema> schemas;

  @BeforeEach
  void setUp() {
    schemas = new HashMap<>();
    schemas.put(SCHEMA_ID, SCHEMA);
    codec = new SalesforceAvroCodec(schemas::get);
  }

  @Test
  void publicApiUsesSchemaIdentifiersWithoutExposingAvroSchema() throws NoSuchMethodException {
    assertEquals(
        Map.class,
        SalesforceAvroCodec.class.getMethod("decode", String.class, byte[].class).getReturnType());
    assertEquals(
        byte[].class,
        SalesforceAvroCodec.class.getMethod("encode", String.class, Map.class).getReturnType());
    assertTrue(
        Arrays.stream(SalesforceAvroCodec.class.getMethods())
            .filter(method -> method.getDeclaringClass() == SalesforceAvroCodec.class)
            .flatMap(method -> Arrays.stream(method.getParameterTypes()))
            .noneMatch(Schema.class::equals));
  }

  @Test
  void cacheBackedCodecUsesOnePreviouslyResolvedSchemaWithoutRefetching() {
    AtomicInteger fetches = new AtomicInteger();
    SalesforceEventTransport transport =
        new SalesforceEventTransport() {
          @Override
          public CompletionStage<TopicMetadata> getTopic(String topicName) {
            throw new UnsupportedOperationException();
          }

          @Override
          public CompletionStage<SchemaMetadata> getSchema(String schemaId) {
            fetches.incrementAndGet();
            return CompletableFuture.completedFuture(
                new SchemaMetadata(schemaId, SCHEMA.toString()));
          }

          @Override
          public void updateSession(SalesforceSession session) {}

          @Override
          public boolean isClosed() {
            return false;
          }

          @Override
          public void close() {}
        };
    SalesforceSchemaCache cache = new SalesforceSchemaCache(transport);
    SalesforceAvroCodec cacheBackedCodec = new SalesforceAvroCodec(cache);

    assertThrows(
        EventEncodeException.class, () -> cacheBackedCodec.encode(SCHEMA_ID, completePayload()));
    assertEquals(0, fetches.get());

    cache.resolve(SCHEMA_ID).toCompletableFuture().join();
    byte[] encoded = cacheBackedCodec.encode(SCHEMA_ID, completePayload());
    cacheBackedCodec.decode(SCHEMA_ID, encoded);
    cache.resolve(SCHEMA_ID).toCompletableFuture().join();

    assertEquals(1, fetches.get());
  }

  @Test
  void roundTripNormalizesEverySupportedNeutralValue() {
    Map<String, Object> payload = completePayload();

    Map<String, Object> decoded = codec.decode(SCHEMA_ID, codec.encode(SCHEMA_ID, payload));

    assertEquals("hello", decoded.get("text"));
    assertInstanceOf(String.class, decoded.get("text"));
    assertEquals(true, decoded.get("flag"));
    assertEquals(7, decoded.get("count"));
    assertEquals(9_000_000_000L, decoded.get("largeCount"));
    assertEquals(1.25F, decoded.get("ratio"));
    assertEquals(2.5D, decoded.get("measurement"));
    assertEquals(LocalDate.parse("2026-09-12"), decoded.get("businessDate"));
    assertEquals(Instant.parse("2026-09-12T12:34:56.789Z"), decoded.get("occurredAt"));
    assertEquals(Instant.parse("2026-09-12T12:34:56.123456Z"), decoded.get("preciseAt"));
    assertNull(decoded.get("alwaysNull"));
    assertNull(decoded.get("optional"));
    assertEquals(List.of("alpha", "beta"), decoded.get("tags"));
    assertEquals(List.of(Map.of("name", "first", "rank", 1L)), decoded.get("children"));
    assertArrayEquals(new byte[] {0x00, (byte) 0xFF}, (byte[]) decoded.get("binary"));
  }

  @ParameterizedTest(name = "round trips {0}")
  @MethodSource("nullableValues")
  void nullableUnionRoundTrips(String field, Object value) {
    Map<String, Object> payload = completePayload();
    payload.put(field, value);

    Map<String, Object> decoded = codec.decode(SCHEMA_ID, codec.encode(SCHEMA_ID, payload));

    assertEquals(value, decoded.get(field));
  }

  @Test
  void missingRequiredFieldFailsBeforeAnyNetworkBoundary() {
    Map<String, Object> payload = completePayload();
    payload.remove("text");

    assertThrows(EventEncodeException.class, () -> codec.encode(SCHEMA_ID, payload));
  }

  @Test
  void schemaDefaultsAreAppliedWhenOptionalFieldsAreOmitted() {
    Map<String, Object> payload = completePayload();
    payload.remove("optional");
    payload.remove("optionalDate");

    Map<String, Object> decoded = codec.decode(SCHEMA_ID, codec.encode(SCHEMA_ID, payload));

    assertNull(decoded.get("optional"));
    assertNull(decoded.get("optionalDate"));
  }

  @ParameterizedTest
  @MethodSource("invalidComplexValues")
  void invalidComplexValuesRaiseTypedEncodeFailure(String field, Object value) {
    Map<String, Object> payload = completePayload();
    payload.put(field, value);

    assertThrows(EventEncodeException.class, () -> codec.encode(SCHEMA_ID, payload));
  }

  @Test
  void byteBufferInputsAreCopiedAndEncoded() {
    Map<String, Object> payload = completePayload();
    ByteBuffer bytes = ByteBuffer.wrap(new byte[] {0x01, 0x02});
    payload.put("binary", bytes);

    byte[] encoded = codec.encode(SCHEMA_ID, payload);
    bytes.put(0, (byte) 0x7F);

    assertArrayEquals(
        new byte[] {0x01, 0x02}, (byte[]) codec.decode(SCHEMA_ID, encoded).get("binary"));
  }

  @Test
  void nullArgumentsAndNonRecordSchemasRaiseTypedFailures() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    schemas.put("non-record", stringSchema);

    assertThrows(EventDecodeException.class, () -> codec.decode("missing", new byte[0]));
    assertThrows(EventDecodeException.class, () -> codec.decode(SCHEMA_ID, null));
    assertThrows(EventDecodeException.class, () -> codec.decode("non-record", new byte[0]));
    assertThrows(EventEncodeException.class, () -> codec.encode("missing", Map.of()));
    assertThrows(EventEncodeException.class, () -> codec.encode(SCHEMA_ID, null));
    assertThrows(
        EventEncodeException.class, () -> codec.encode("non-record", Map.of("value", "text")));
  }

  @Test
  void nonNullableUnionRejectsNullAndUnsupportedSchemaTypesFailSafely() {
    Schema unionRecord =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"UnionRecord\",\"fields\":[{\"name\":\"value\",\"type\":[\"string\",\"long\"]}]}");
    Schema enumRecord =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"EnumRecord\",\"fields\":[{\"name\":\"value\",\"type\":{\"type\":\"enum\",\"name\":\"Choice\",\"symbols\":[\"A\"]}}]}");
    schemas.put("union", unionRecord);
    schemas.put("enum", enumRecord);

    assertThrows(
        EventEncodeException.class,
        () -> codec.encode("union", singletonNullableMap("value", null)));
    assertThrows(EventEncodeException.class, () -> codec.encode("enum", Map.of("value", "A")));
  }

  @Test
  void wrongTypeFailsBeforeAnyNetworkBoundaryWithoutRenderingTheValue() {
    Map<String, Object> payload = completePayload();
    String secret = "sensitive-event-payload";
    payload.put("count", new SecretValue(secret));

    EventEncodeException failure =
        assertThrows(EventEncodeException.class, () -> codec.encode(SCHEMA_ID, payload));

    assertTrue(failure.getMessage().contains("schemaId=schema-1"));
    assertTrue(!failure.getMessage().contains(secret));
  }

  @Test
  void malformedAndIncompatiblePayloadsRaiseTypedDecodeFailureWithoutRawBytes() {
    byte[] malformed = "sensitive-event-payload".getBytes(StandardCharsets.UTF_8);
    EventDecodeException malformedFailure =
        assertThrows(EventDecodeException.class, () -> codec.decode(SCHEMA_ID, malformed));

    Schema other =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Other\",\"fields\":[{\"name\":\"only\",\"type\":\"string\"}]}");
    schemas.put("other", other);
    byte[] incompatible = codec.encode("other", Map.of("only", "value"));
    assertThrows(EventDecodeException.class, () -> codec.decode(SCHEMA_ID, incompatible));

    byte[] valid = codec.encode(SCHEMA_ID, completePayload());
    byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
    assertThrows(EventDecodeException.class, () -> codec.decode(SCHEMA_ID, trailing));

    assertTrue(malformedFailure.getMessage().contains("schemaId=schema-1"));
    assertTrue(!malformedFailure.getMessage().contains("sensitive-event-payload"));
  }

  @Test
  void schemaIdentifierIsSanitizedAndBoundedInErrors() {
    String unsafeSchemaId = "schema\r\nsecret" + "x".repeat(300);

    EventDecodeException failure =
        assertThrows(EventDecodeException.class, () -> codec.decode(unsafeSchemaId, new byte[0]));

    assertTrue(!failure.getMessage().contains("\r"));
    assertTrue(!failure.getMessage().contains("\n"));
    assertTrue(failure.getMessage().contains("schema__secret"));
    assertTrue(failure.getMessage().contains("…"));
  }

  @Test
  void decodedByteValuesDoNotAliasTheInputOrSubsequentDecodes() {
    Map<String, Object> payload = completePayload();
    byte[] encoded = codec.encode(SCHEMA_ID, payload);
    Map<String, Object> first = codec.decode(SCHEMA_ID, encoded);
    byte[] bytes = (byte[]) first.get("binary");
    bytes[0] = 0x7F;

    Map<String, Object> second = codec.decode(SCHEMA_ID, encoded);

    assertArrayEquals(new byte[] {0x00, (byte) 0xFF}, (byte[]) second.get("binary"));
  }

  private static Stream<Arguments> nullableValues() {
    return Stream.of(
        Arguments.of("optional", null),
        Arguments.of("optional", "present"),
        Arguments.of("optionalDate", LocalDate.parse("2026-01-02")));
  }

  private static Stream<Arguments> invalidComplexValues() {
    return Stream.of(
        Arguments.of("optional", 5),
        Arguments.of("tags", "not-an-array"),
        Arguments.of("children", List.of("not-a-record")),
        Arguments.of("binary", "not-bytes"));
  }

  private static Map<String, Object> singletonNullableMap(String key, Object value) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put(key, value);
    return map;
  }

  private static Map<String, Object> completePayload() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("text", "hello");
    payload.put("flag", true);
    payload.put("count", 7);
    payload.put("largeCount", 9_000_000_000L);
    payload.put("ratio", 1.25F);
    payload.put("measurement", 2.5D);
    payload.put("businessDate", LocalDate.parse("2026-09-12"));
    payload.put("occurredAt", Instant.parse("2026-09-12T12:34:56.789Z"));
    payload.put("preciseAt", Instant.parse("2026-09-12T12:34:56.123456Z"));
    payload.put("alwaysNull", null);
    payload.put("optional", null);
    payload.put("optionalDate", null);
    payload.put("tags", List.of("alpha", "beta"));
    payload.put("children", List.of(Map.of("name", "first", "rank", 1L)));
    payload.put("binary", new byte[] {0x00, (byte) 0xFF});
    return payload;
  }

  private record SecretValue(String secret) {
    @Override
    public String toString() {
      return secret;
    }
  }
}
