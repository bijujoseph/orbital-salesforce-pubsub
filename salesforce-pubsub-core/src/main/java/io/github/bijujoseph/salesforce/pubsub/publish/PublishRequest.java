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

package io.github.bijujoseph.salesforce.pubsub.publish;

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
import java.util.Set;

/** One protocol-neutral custom Platform Event publication. */
public record PublishRequest(String topic, Map<String, Object> payload, String correlationKey) {

  public PublishRequest {
    payload = snapshotPayload(payload);
  }

  /** Returns a fresh defensive snapshot of the outgoing event values. */
  @Override
  public Map<String, Object> payload() {
    return snapshotPayload(payload);
  }

  private static Map<String, Object> snapshotPayload(Map<String, ?> value) {
    if (value == null) {
      return null;
    }
    return snapshotMap(value, Collections.newSetFromMap(new IdentityHashMap<>()));
  }

  private static Map<String, Object> snapshotMap(Map<?, ?> value, Set<Object> activePath) {
    enter(value, activePath);
    try {
      Map<String, Object> snapshot = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : value.entrySet()) {
        if (!(entry.getKey() instanceof String key)) {
          throw new IllegalArgumentException("Publish payload map keys must be strings");
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
        || value instanceof Integer
        || value instanceof Long
        || value instanceof Float
        || value instanceof Double
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
    throw new IllegalArgumentException("Unsupported publish payload value type");
  }

  private static void enter(Object value, Set<Object> activePath) {
    if (!activePath.add(value)) {
      throw new IllegalArgumentException("Publish payload must not contain cycles");
    }
  }
}
