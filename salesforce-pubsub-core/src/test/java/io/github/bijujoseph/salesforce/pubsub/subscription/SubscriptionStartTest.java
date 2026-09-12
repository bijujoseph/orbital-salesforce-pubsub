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

package io.github.bijujoseph.salesforce.pubsub.subscription;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SubscriptionStartTest {

  @Test
  void exposesOnlyTheThreeBlueprintStartModes() {
    assertTrue(SubscriptionStart.class.isSealed());
    assertEquals(
        Set.of(Latest.class, Earliest.class, ReplayId.class),
        Set.of(SubscriptionStart.class.getPermittedSubclasses()));
    assertTrue(Latest.class.isRecord());
    assertTrue(Earliest.class.isRecord());
    assertTrue(ReplayId.class.isRecord());
    assertEquals(0, Latest.class.getRecordComponents().length);
    assertEquals(0, Earliest.class.getRecordComponents().length);
    assertEquals(
        Set.of("value:byte[]"),
        Arrays.stream(ReplayId.class.getRecordComponents())
            .map(SubscriptionStartTest::componentSignature)
            .collect(Collectors.toSet()));
  }

  @Test
  void replayIdDefensivelyCopiesAtConstructionAndAccess() {
    byte[] callerBytes = {(byte) 0x80, (byte) 0xff, 0x00, 0x7f};
    ReplayId replayId = new ReplayId(callerBytes);

    callerBytes[0] = 0x01;
    byte[] firstRead = replayId.value();
    assertArrayEquals(new byte[] {(byte) 0x80, (byte) 0xff, 0x00, 0x7f}, firstRead);

    firstRead[1] = 0x02;
    assertArrayEquals(new byte[] {(byte) 0x80, (byte) 0xff, 0x00, 0x7f}, replayId.value());
  }

  @Test
  void replayIdRejectsNullBytes() {
    assertThrows(NullPointerException.class, () -> new ReplayId(null));
  }

  private static String componentSignature(RecordComponent component) {
    return component.getName() + ":" + component.getType().getTypeName().replace("[B", "byte[]");
  }
}
