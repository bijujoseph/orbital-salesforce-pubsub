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

package io.github.bijujoseph.salesforce.pubsub.telemetry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The complete metric-label vocabulary exposed by the core.
 *
 * <p>Event IDs, schema IDs, replay IDs, correlation keys, credentials, payloads, customer IDs, and
 * arbitrary extension labels are intentionally not representable.
 */
public record MetricLabels(
    String connectionName, String topic, String outcome, String exceptionType) {

  public static MetricLabels none() {
    return new MetricLabels(null, null, null, null);
  }

  public static MetricLabels connection(String connectionName) {
    return new MetricLabels(connectionName, null, null, null);
  }

  public static MetricLabels topic(String topic) {
    return new MetricLabels(null, topic, null, null);
  }

  public static MetricLabels topicAndException(String topic, String exceptionType) {
    return new MetricLabels(null, topic, null, exceptionType);
  }

  /** Returns only present labels in stable order. */
  public Map<String, String> asMap() {
    Map<String, String> labels = new LinkedHashMap<>(4);
    putIfPresent(labels, "connectionName", connectionName);
    putIfPresent(labels, "topic", topic);
    putIfPresent(labels, "outcome", outcome);
    putIfPresent(labels, "exceptionType", exceptionType);
    return Collections.unmodifiableMap(labels);
  }

  private static void putIfPresent(Map<String, String> labels, String key, String value) {
    if (value != null && !value.isBlank()) {
      labels.put(key, value);
    }
  }
}
