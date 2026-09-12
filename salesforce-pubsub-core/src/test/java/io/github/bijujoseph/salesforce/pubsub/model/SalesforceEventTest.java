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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
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

  private static SalesforceEvent event(String eventId, byte[] replayId) {
    return new SalesforceEvent(
        "/event/Test__e",
        eventId,
        replayId,
        "schema-1",
        Instant.parse("2026-09-12T00:00:00Z"),
        Map.of("Name", "example"));
  }
}
