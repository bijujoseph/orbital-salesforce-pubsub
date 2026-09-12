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

/**
 * Optional lifecycle and metrics bridge supplied by an embedding application.
 *
 * <p>All methods default to no-op behavior. Event IDs and correlation keys are trace correlation
 * values only and must never be used as metric labels. Implementations must not inspect throwable
 * messages because upstream failures can contain remote or sensitive text.
 */
public interface SalesforcePubSubTelemetry {

  default void connected(String connectionName) {}

  default void subscribed(String connectionName, String topic, String consumerName) {}

  default void eventReceived(String topic, String eventId) {
    metric(SalesforcePubSubMetric.EVENTS_RECEIVED_TOTAL, 1, MetricLabels.topic(topic));
  }

  default void reconnecting(String topic, Throwable cause) {
    metric(
        SalesforcePubSubMetric.RECONNECTS_TOTAL,
        1,
        MetricLabels.topicAndException(topic, safeExceptionType(cause)));
  }

  default void published(String topic, String correlationKey) {
    metric(SalesforcePubSubMetric.PUBLISH_SUCCESS_TOTAL, 1, MetricLabels.topic(topic));
  }

  default void connectionState(String connectionName, ConnectorStatus status) {
    metric(
        SalesforcePubSubMetric.CONNECTION_STATE,
        1,
        new MetricLabels(connectionName, null, status.name(), null));
  }

  default void subscriptionState(String connectionName, String topic, ConnectorStatus status) {
    metric(
        SalesforcePubSubMetric.SUBSCRIPTION_STATE,
        1,
        new MetricLabels(connectionName, topic, status.name(), null));
  }

  default void eventEmitted(String topic) {
    metric(SalesforcePubSubMetric.EVENTS_EMITTED_TOTAL, 1, MetricLabels.topic(topic));
  }

  default void decodeFailure(String topic, Throwable cause) {
    metric(
        SalesforcePubSubMetric.DECODE_FAILURES_TOTAL,
        1,
        MetricLabels.topicAndException(topic, safeExceptionType(cause)));
  }

  default void publishFailure(String topic, Throwable cause) {
    metric(
        SalesforcePubSubMetric.PUBLISH_FAILURE_TOTAL,
        1,
        MetricLabels.topicAndException(topic, safeExceptionType(cause)));
  }

  default void inFlightEvents(String topic, long count) {
    metric(SalesforcePubSubMetric.IN_FLIGHT_EVENTS, count, MetricLabels.topic(topic));
  }

  default void schemaCacheHit() {
    metric(SalesforcePubSubMetric.SCHEMA_CACHE_HITS_TOTAL, 1, MetricLabels.none());
  }

  default void schemaCacheMiss() {
    metric(SalesforcePubSubMetric.SCHEMA_CACHE_MISSES_TOTAL, 1, MetricLabels.none());
  }

  default void schemaCacheLoadFailure() {}

  default void schemaCacheEviction() {}

  /** Receives an always-available metric with the closed, low-cardinality label model. */
  default void metric(SalesforcePubSubMetric metric, double value, MetricLabels labels) {}

  private static String safeExceptionType(Throwable cause) {
    return cause == null ? "Unknown" : cause.getClass().getSimpleName();
  }
}
