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

package io.github.bijujoseph.salesforce.pubsub.model;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.eventbus.protobuf.FetchResponse;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

class SalesforceEventTest {

  @Test
  void replayIdIsCopiedOnConstructionAndAccess() {
    byte[] supplied = {0x01, (byte) 0xFE};
    SalesforceEvent event = event("salesforce-id", supplied);

    supplied[0] = 0x7F;
    byte[] returned = event.replayId();
    returned[1] = 0x7F;

    assertArrayEquals(new byte[] {0x01, (byte) 0xFE}, event.replayId());
  }

  @Test
  void salesforceEventIdIsTheOnlyDefaultIdentity() {
    SalesforceEvent identified = event("salesforce-id", new byte[] {0x01});
    SalesforceEvent unidentified = event(null, new byte[] {0x02});

    assertEquals("salesforce-id", identified.defaultIdentity().orElseThrow());
    assertFalse(unidentified.defaultIdentity().isPresent());
  }

  @Test
  void payloadMapCannotBeMutatedThroughThePublicBoundary() {
    Map<String, Object> supplied = new LinkedHashMap<>();
    supplied.put("nullable", null);
    SalesforceEvent event =
        new SalesforceEvent("/event/Test__e", null, null, "schema-1", Instant.EPOCH, supplied);

    supplied.put("later", "mutation");

    assertFalse(event.payload().containsKey("later"));
    assertThrows(UnsupportedOperationException.class, () -> event.payload().put("x", "y"));
  }

  @Test
  void absentPayloadIsExposedAsAnEmptyMap() {
    SalesforceEvent event =
        new SalesforceEvent("/event/Test__e", null, null, "schema-1", Instant.EPOCH, null);

    assertEquals(Map.of(), event.payload());
  }

  @Test
  void nestedContainersAndBytesAreSnapshottedAtConstructionAndAccess() {
    byte[] bytes = {0x01, 0x02};
    Map<String, Object> child = new LinkedHashMap<>();
    child.put("bytes", bytes);
    List<Object> children = new ArrayList<>();
    children.add(child);
    Map<String, Object> supplied = new LinkedHashMap<>();
    supplied.put("children", children);
    SalesforceEvent event = eventWithPayload(supplied);

    bytes[0] = 0x7F;
    child.put("later", "mutation");
    children.add("mutation");
    Map<String, Object> first = event.payload();
    List<?> firstChildren = (List<?>) first.get("children");
    Map<?, ?> firstChild = (Map<?, ?>) firstChildren.getFirst();
    ((byte[]) firstChild.get("bytes"))[1] = 0x7F;

    Map<String, Object> second = event.payload();
    List<?> secondChildren = (List<?>) second.get("children");
    Map<?, ?> secondChild = (Map<?, ?>) secondChildren.getFirst();
    assertEquals(1, secondChildren.size());
    assertFalse(secondChild.containsKey("later"));
    assertArrayEquals(new byte[] {0x01, 0x02}, (byte[]) secondChild.get("bytes"));
    assertNotSame(first, second);
  }

  @Test
  void everyReturnedContainerIsDeeplyUnmodifiable() {
    SalesforceEvent event = eventWithPayload(Map.of("children", List.of(Map.of("name", "first"))));
    Map<String, Object> payload = event.payload();
    List<?> children = (List<?>) payload.get("children");
    Map<?, ?> child = (Map<?, ?>) children.getFirst();

    assertThrows(UnsupportedOperationException.class, () -> payload.put("later", true));
    assertThrows(
        UnsupportedOperationException.class, () -> add(children, Map.of("name", "second")));
    assertThrows(UnsupportedOperationException.class, () -> put(child, "later", true));
  }

  @Test
  void byteBufferRemainingBytesBecomeIndependentNeutralByteArray() {
    byte[] backing = {0x00, 0x01, 0x02, 0x03};
    ByteBuffer buffer = ByteBuffer.wrap(backing);
    buffer.position(1);
    buffer.limit(3);
    SalesforceEvent event = eventWithPayload(Map.of("bytes", buffer));

    backing[1] = 0x7F;
    buffer.position(2);

    byte[] first = (byte[]) event.payload().get("bytes");
    first[0] = 0x7F;
    byte[] second = (byte[]) event.payload().get("bytes");

    assertNotSame(first, second);
    assertArrayEquals(new byte[] {0x01, 0x02}, second);
  }

  @Test
  void repeatedAcyclicAliasesAreCopiedIndependently() {
    Map<String, Object> shared = new LinkedHashMap<>();
    shared.put("value", "same");
    SalesforceEvent event = eventWithPayload(Map.of("first", shared, "second", shared));

    Map<String, Object> payload = event.payload();
    Object first = payload.get("first");
    Object second = payload.get("second");

    assertEquals(first, second);
    assertNotSame(first, second);
  }

  @Test
  void cyclesAreRejectedWithoutRejectingImmutableNeutralLeaves() {
    Map<String, Object> cyclicMap = new LinkedHashMap<>();
    cyclicMap.put("secret", "payload-value");
    cyclicMap.put("self", cyclicMap);
    List<Object> cyclicList = new ArrayList<>();
    cyclicList.add(cyclicList);

    IllegalArgumentException cycleFailure =
        assertThrows(IllegalArgumentException.class, () -> eventWithPayload(cyclicMap));
    assertThrows(
        IllegalArgumentException.class, () -> eventWithPayload(Map.of("cycle", cyclicList)));
    assertFalse(cycleFailure.getMessage().contains("payload-value"));

    SalesforceEvent supported =
        eventWithPayload(
            Map.of(
                "boolean",
                true,
                "integer",
                1,
                "long",
                2L,
                "float",
                3F,
                "double",
                4D,
                "date",
                LocalDate.of(2026, 9, 12),
                "instant",
                Instant.EPOCH));
    assertEquals(7, supported.payload().size());
  }

  @Test
  void unsupportedAndAvroSpecificValuesAreRejectedInsteadOfAliased() {
    assertThrows(
        IllegalArgumentException.class,
        () -> eventWithPayload(Map.of("mutable", new StringBuilder("value"))));
    assertThrows(
        IllegalArgumentException.class, () -> eventWithPayload(Map.of("avro", new Utf8("value"))));
    assertThrows(
        IllegalArgumentException.class,
        () -> eventWithPayload(Map.of("generated", FetchResponse.getDefaultInstance())));
    assertThrows(
        IllegalArgumentException.class,
        () -> eventWithPayload(Map.of("objectArray", new Object[] {"value"})));

    Map<Object, Object> nonStringKeys = new LinkedHashMap<>();
    nonStringKeys.put(1, "value");
    assertThrows(
        IllegalArgumentException.class, () -> eventWithPayload(Map.of("nested", nonStringKeys)));
  }

  @Test
  void codecIncompatibleScalarWrappersAreRejectedWithoutRenderingTheirValues() {
    for (Object unsupported : List.of((byte) 7, (short) 8, 'Z')) {
      IllegalArgumentException failure =
          assertThrows(
              IllegalArgumentException.class,
              () -> eventWithPayload(Map.of("unsupported", unsupported)));

      assertFalse(failure.getMessage().contains(String.valueOf(unsupported)));
    }
  }

  private static SalesforceEvent event(String eventId, byte[] replayId) {
    return new SalesforceEvent(
        "/event/Test__e",
        eventId,
        replayId,
        "schema-1",
        Instant.parse("2026-09-12T00:00:00Z"),
        Map.of("Name", "example"));
  }

  private static SalesforceEvent eventWithPayload(Map<String, Object> payload) {
    return new SalesforceEvent(
        "/event/Test__e", "salesforce-id", new byte[] {0x01}, "schema-1", Instant.EPOCH, payload);
  }

  @SuppressWarnings("unchecked")
  private static void add(List<?> values, Object value) {
    ((List<Object>) values).add(value);
  }

  @SuppressWarnings("unchecked")
  private static void put(Map<?, ?> values, Object key, Object value) {
    ((Map<Object, Object>) values).put(key, value);
  }
}
