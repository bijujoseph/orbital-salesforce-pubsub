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

package io.github.bijujoseph.salesforce.pubsub.replay;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class InMemoryReplayStoreTest {

  private static final String CONNECTION = "salesforce-prod";
  private static final String TOPIC = "/event/Manual_Message__e";
  private static final String CONSUMER = "manual-message-consumer";

  @Test
  void returnsEmptyForAnUnknownNamespace() {
    assertFalse(new InMemoryReplayStore().load(CONNECTION, TOPIC, CONSUMER).isPresent());
  }

  @Test
  void isolatesEachNamespaceComponentAndAvoidsConcatenationCollisions() {
    InMemoryReplayStore store = new InMemoryReplayStore();
    store.save(CONNECTION, TOPIC, CONSUMER, bytes(1));
    store.save("salesforce-test", TOPIC, CONSUMER, bytes(2));
    store.save(CONNECTION, "/event/Other__e", CONSUMER, bytes(3));
    store.save(CONNECTION, TOPIC, "other-consumer", bytes(4));
    store.save("ab", "c", "d", bytes(5));
    store.save("a", "bc", "d", bytes(6));

    assertStored(store, CONNECTION, TOPIC, CONSUMER, bytes(1));
    assertStored(store, "salesforce-test", TOPIC, CONSUMER, bytes(2));
    assertStored(store, CONNECTION, "/event/Other__e", CONSUMER, bytes(3));
    assertStored(store, CONNECTION, TOPIC, "other-consumer", bytes(4));
    assertStored(store, "ab", "c", "d", bytes(5));
    assertStored(store, "a", "bc", "d", bytes(6));
  }

  @Test
  void replacesOnlyTheCheckpointForTheExactNamespace() {
    InMemoryReplayStore store = new InMemoryReplayStore();
    store.save(CONNECTION, TOPIC, CONSUMER, bytes(1));
    store.save(CONNECTION, TOPIC, "other-consumer", bytes(2));

    store.save(CONNECTION, TOPIC, CONSUMER, bytes(3));

    assertStored(store, CONNECTION, TOPIC, CONSUMER, bytes(3));
    assertStored(store, CONNECTION, TOPIC, "other-consumer", bytes(2));
  }

  @Test
  void copiesOpaqueReplayBytesOnSaveAndEveryLoad() {
    InMemoryReplayStore store = new InMemoryReplayStore();
    byte[] opaque = {0x00, 0x7f, (byte) 0x80, (byte) 0xff, 0x00};
    byte[] expected = opaque.clone();

    store.save(CONNECTION, TOPIC, CONSUMER, opaque);
    opaque[0] = 0x55;

    byte[] firstLoad = store.load(CONNECTION, TOPIC, CONSUMER).orElseThrow();
    assertArrayEquals(expected, firstLoad);
    firstLoad[1] = 0x66;

    byte[] secondLoad = store.load(CONNECTION, TOPIC, CONSUMER).orElseThrow();
    assertArrayEquals(expected, secondLoad);
    assertNotSame(firstLoad, secondLoad);
  }

  @Test
  void rejectsNullNamespaceComponentsAndReplayBytes() {
    InMemoryReplayStore store = new InMemoryReplayStore();
    assertThrows(NullPointerException.class, () -> store.load(null, TOPIC, CONSUMER));
    assertThrows(NullPointerException.class, () -> store.load(CONNECTION, null, CONSUMER));
    assertThrows(NullPointerException.class, () -> store.load(CONNECTION, TOPIC, null));
    assertThrows(NullPointerException.class, () -> store.save(null, TOPIC, CONSUMER, bytes(1)));
    assertThrows(
        NullPointerException.class, () -> store.save(CONNECTION, null, CONSUMER, bytes(1)));
    assertThrows(NullPointerException.class, () -> store.save(CONNECTION, TOPIC, null, bytes(1)));
    assertThrows(NullPointerException.class, () -> store.save(CONNECTION, TOPIC, CONSUMER, null));
  }

  private static void assertStored(
      InMemoryReplayStore store,
      String connectionName,
      String topic,
      String consumerName,
      byte[] expected) {
    assertArrayEquals(expected, store.load(connectionName, topic, consumerName).orElseThrow());
  }

  private static byte[] bytes(int value) {
    return new byte[] {(byte) value};
  }
}
