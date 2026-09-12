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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** A protocol-neutral Salesforce event delivered by the core library. */
public record SalesforceEvent(
    String topic,
    String eventId,
    byte[] replayId,
    String schemaId,
    Instant receivedAt,
    Map<String, Object> payload) {

  public SalesforceEvent {
    replayId = copy(replayId);
    payload = snapshotPayload(payload);
  }

  /** Returns a defensive copy of the opaque Salesforce replay position. */
  @Override
  public byte[] replayId() {
    return copy(replayId);
  }

  /** Returns a fresh, deeply immutable snapshot of the neutral event payload. */
  @Override
  public Map<String, Object> payload() {
    return snapshotPayload(payload);
  }

  /**
   * Returns the default business identity supplied by Salesforce, when present.
   *
   * <p>The replay position is deliberately never used as an event identity.
   */
  public Optional<String> defaultIdentity() {
    return Optional.ofNullable(eventId);
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : value.clone();
  }

  private static Map<String, Object> snapshotPayload(Map<String, ?> value) {
    if (value == null) {
      return Map.of();
    }
    return snapshotMap(value, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static Map<String, Object> snapshotMap(Map<?, ?> value, Set<Object> activePath) {
    enter(value, activePath);
    try {
      Map<String, Object> snapshot = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : value.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw new IllegalArgumentException("Neutral payload map keys must be strings");
        }
        snapshot.put(key, snapshotValue(entry.getValue(), activePath));
      }
      return Collections.unmodifiableMap(snapshot);
    } finally {
      activePath.remove(value);
    }
  }

  private static List<Object> snapshotCollection(Collection<?> value, Set<Object> activePath) {
    enter(value, activePath);
    try {
      List<Object> snapshot = new ArrayList<>(value.size());
      for (Object element : value) {
        snapshot.add(snapshotValue(element, activePath));
      }
      return Collections.unmodifiableList(snapshot);
    } finally {
      activePath.remove(value);
    }
  }

  private static Object snapshotValue(Object value, Set<Object> activePath) {
    if (value == null
        || value instanceof String
        || value instanceof Boolean
        || value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
        || value instanceof Character
        || value instanceof LocalDate
        || value instanceof Instant) {
      return value;
    }
    if (value instanceof byte[] bytes) {
      return bytes.clone();
    }
    if (value instanceof ByteBuffer buffer) {
      ByteBuffer remaining = buffer.asReadOnlyBuffer();
      byte[] bytes = new byte[remaining.remaining()];
      remaining.get(bytes);
      return bytes;
    }
    if (value instanceof Map<?, ?> map) {
      return snapshotMap(map, activePath);
    }
    if (value instanceof Collection<?> collection) {
      return snapshotCollection(collection, activePath);
    }
    throw new IllegalArgumentException("Unsupported neutral payload value type");
  }

  private static void enter(Object value, Set<Object> activePath) {
    if (!activePath.add(value)) {
      throw new IllegalArgumentException("Neutral payload must not contain cycles");
    }
  }
}
