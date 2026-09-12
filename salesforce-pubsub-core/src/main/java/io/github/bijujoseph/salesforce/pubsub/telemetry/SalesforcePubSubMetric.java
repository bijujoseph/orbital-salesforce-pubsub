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

/** Metrics that are always available from the v0.1 core. */
public enum SalesforcePubSubMetric {
  CONNECTION_STATE("salesforce_pubsub_connection_state", Kind.GAUGE),
  SUBSCRIPTION_STATE("salesforce_pubsub_subscription_state", Kind.GAUGE),
  EVENTS_RECEIVED_TOTAL("salesforce_pubsub_events_received_total", Kind.COUNTER),
  EVENTS_EMITTED_TOTAL("salesforce_pubsub_events_emitted_total", Kind.COUNTER),
  DECODE_FAILURES_TOTAL("salesforce_pubsub_decode_failures_total", Kind.COUNTER),
  PUBLISH_SUCCESS_TOTAL("salesforce_pubsub_publish_success_total", Kind.COUNTER),
  PUBLISH_FAILURE_TOTAL("salesforce_pubsub_publish_failure_total", Kind.COUNTER),
  RECONNECTS_TOTAL("salesforce_pubsub_reconnects_total", Kind.COUNTER),
  SCHEMA_CACHE_HITS_TOTAL("salesforce_pubsub_schema_cache_hits_total", Kind.COUNTER),
  SCHEMA_CACHE_MISSES_TOTAL("salesforce_pubsub_schema_cache_misses_total", Kind.COUNTER),
  IN_FLIGHT_EVENTS("salesforce_pubsub_in_flight_events", Kind.GAUGE);

  private final String metricName;
  private final Kind kind;

  SalesforcePubSubMetric(String metricName, Kind kind) {
    this.metricName = metricName;
    this.kind = kind;
  }

  public String metricName() {
    return metricName;
  }

  public Kind kind() {
    return kind;
  }

  public enum Kind {
    COUNTER,
    GAUGE
  }
}
