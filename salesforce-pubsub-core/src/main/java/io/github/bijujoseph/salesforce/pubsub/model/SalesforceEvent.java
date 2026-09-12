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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

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
    payload = immutableCopy(payload);
  }

  /** Returns a defensive copy of the opaque Salesforce replay position. */
  @Override
  public byte[] replayId() {
    return copy(replayId);
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

  private static Map<String, Object> immutableCopy(Map<String, Object> value) {
    return value == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(value));
  }
}
