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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

  @Test
  void replayIdEqualityAndHashCodeUseOpaqueByteContent() {
    ReplayId left = new ReplayId(new byte[] {0x00, 0x7f, (byte) 0x80, (byte) 0xff});
    ReplayId right = new ReplayId(new byte[] {0x00, 0x7f, (byte) 0x80, (byte) 0xff});

    assertEquals(left, left);
    assertEquals(left, right);
    assertEquals(right, left);
    assertEquals(left.hashCode(), right.hashCode());
  }

  @Test
  void replayIdEqualityRejectsDifferentContentLengthAndTypes() {
    ReplayId replayId = new ReplayId(new byte[] {0x01, 0x02});

    assertNotEquals(replayId, new ReplayId(new byte[] {0x01, 0x03}));
    assertNotEquals(replayId, new ReplayId(new byte[] {0x01, 0x02, 0x00}));
    assertNotEquals(replayId, null);
    assertNotEquals(replayId, new byte[] {0x01, 0x02});
  }

  @Test
  void replayIdProvidesValueSemanticsAsMapAndSetKeys() {
    ReplayId first = new ReplayId(new byte[] {0x00, (byte) 0xff});
    ReplayId equalKey = new ReplayId(new byte[] {0x00, (byte) 0xff});
    Set<ReplayId> set = new HashSet<>();
    Map<ReplayId, String> map = new HashMap<>();

    assertTrue(set.add(first));
    assertTrue(set.contains(equalKey));
    assertFalse(set.add(equalKey));
    assertEquals(1, set.size());

    map.put(first, "first");
    assertEquals("first", map.get(equalKey));
    assertEquals("first", map.put(equalKey, "replacement"));
    assertEquals(1, map.size());
    assertEquals("replacement", map.get(first));
  }

  @Test
  void defensiveCopiesKeepEqualityHashAndCollectionLookupStable() {
    byte[] callerBytes = {0x00, 0x7f, (byte) 0x80, (byte) 0xff};
    ReplayId storedKey = new ReplayId(callerBytes);
    ReplayId expectedKey = new ReplayId(callerBytes.clone());
    int originalHash = storedKey.hashCode();
    Set<ReplayId> set = new HashSet<>(Set.of(storedKey));
    Map<ReplayId, String> map = new HashMap<>(Map.of(storedKey, "checkpoint"));

    callerBytes[0] = 0x55;
    byte[] exposedCopy = storedKey.value();
    exposedCopy[1] = 0x66;

    assertEquals(expectedKey, storedKey);
    assertEquals(originalHash, storedKey.hashCode());
    assertTrue(set.contains(expectedKey));
    assertEquals("checkpoint", map.get(expectedKey));
  }

  private static String componentSignature(RecordComponent component) {
    return component.getName() + ":" + component.getType().getTypeName().replace("[B", "byte[]");
  }
}
