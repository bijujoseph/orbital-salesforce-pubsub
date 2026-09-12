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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SalesforcePubSubTelemetryTest {

  private static final Set<String> ALLOWED_LABELS =
      Set.of("connectionName", "topic", "outcome", "exceptionType");

  @Test
  void requiredMetricCatalogIsExactAndConditionalMetricsRemainAbsent() {
    assertEquals(
        Set.of(
            "salesforce_pubsub_connection_state",
            "salesforce_pubsub_subscription_state",
            "salesforce_pubsub_events_received_total",
            "salesforce_pubsub_events_emitted_total",
            "salesforce_pubsub_decode_failures_total",
            "salesforce_pubsub_publish_success_total",
            "salesforce_pubsub_publish_failure_total",
            "salesforce_pubsub_reconnects_total",
            "salesforce_pubsub_schema_cache_hits_total",
            "salesforce_pubsub_schema_cache_misses_total",
            "salesforce_pubsub_in_flight_events"),
        java.util.Arrays.stream(SalesforcePubSubMetric.values())
            .map(SalesforcePubSubMetric::metricName)
            .collect(java.util.stream.Collectors.toUnmodifiableSet()));
    for (SalesforcePubSubMetric metric : SalesforcePubSubMetric.values()) {
      assertFalse(metric.metricName().contains("checkpoint_age"));
      assertFalse(metric.metricName().contains("processing_duration"));
    }
    assertEquals(
        Set.of(
            SalesforcePubSubMetric.CONNECTION_STATE,
            SalesforcePubSubMetric.SUBSCRIPTION_STATE,
            SalesforcePubSubMetric.IN_FLIGHT_EVENTS),
        java.util.Arrays.stream(SalesforcePubSubMetric.values())
            .filter(metric -> metric.kind() == SalesforcePubSubMetric.Kind.GAUGE)
            .collect(java.util.stream.Collectors.toUnmodifiableSet()));
    assertTrue(
        java.util.Arrays.stream(SalesforcePubSubMetric.values())
            .filter(metric -> metric.kind() == SalesforcePubSubMetric.Kind.COUNTER)
            .allMatch(metric -> metric.metricName().endsWith("_total")));
  }

  @Test
  void defaultHooksEmitRequiredMetricsWithoutHighCardinalityCorrelationLabels() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    RuntimeException failure = new RuntimeException("credential payload replay customer pii");

    telemetry.eventReceived("/event/Test__e", "event-high-cardinality");
    telemetry.eventEmitted("/event/Test__e");
    telemetry.reconnecting("/event/Test__e", failure);
    telemetry.published("/event/Test__e", "correlation-high-cardinality");
    telemetry.publishFailure("/event/Test__e", failure);
    telemetry.decodeFailure("/event/Test__e", failure);
    telemetry.connectionState("connection", ConnectorStatus.CONNECTED);
    telemetry.subscriptionState("connection", "/event/Test__e", ConnectorStatus.SUBSCRIBED);
    telemetry.inFlightEvents("/event/Test__e", 7);
    telemetry.schemaCacheHit();
    telemetry.schemaCacheMiss();

    assertEquals(25, telemetry.metrics.size());
    assertOneHot(
        telemetry,
        SalesforcePubSubMetric.CONNECTION_STATE,
        ConnectorStatus.CONNECTED,
        ConnectorStatus.values().length);
    assertOneHot(
        telemetry,
        SalesforcePubSubMetric.SUBSCRIPTION_STATE,
        ConnectorStatus.SUBSCRIBED,
        ConnectorStatus.values().length);
    telemetry.metrics.forEach(
        observed -> {
          assertTrue(ALLOWED_LABELS.containsAll(observed.labels().asMap().keySet()));
          String labels = observed.labels().asMap().toString();
          assertFalse(labels.contains("event-high-cardinality"));
          assertFalse(labels.contains("correlation-high-cardinality"));
          assertFalse(labels.contains("credential"));
          assertFalse(labels.contains("payload"));
          assertFalse(labels.contains("replay"));
          assertFalse(labels.contains("customer"));
          assertFalse(labels.contains("pii"));
        });
  }

  @Test
  void labelRecordCannotRepresentArbitraryLabelNames() {
    MetricLabels labels =
        new MetricLabels("connection", "/event/Test__e", "success", "TransportException");

    assertEquals(ALLOWED_LABELS, labels.asMap().keySet());
    assertEquals(Map.of(), MetricLabels.none().asMap());
    assertEquals(
        Map.of("connectionName", "connection"), MetricLabels.connection("connection").asMap());
  }

  @Test
  void noOpTelemetryIsBackendFreeSideEffectFreeAndConcurrent() throws Exception {
    NoOpSalesforcePubSubTelemetry telemetry = NoOpSalesforcePubSubTelemetry.INSTANCE;
    assertSame(telemetry, NoOpSalesforcePubSubTelemetry.INSTANCE);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<?>> invocations = new ArrayList<>();
    try {
      for (int index = 0; index < 1_000; index++) {
        invocations.add(
            executor.submit(
                () -> {
                  telemetry.connected("connection");
                  telemetry.subscribed("connection", "/event/Test__e", "consumer");
                  telemetry.eventReceived("/event/Test__e", "event");
                  telemetry.reconnecting("/event/Test__e", new RuntimeException("secret"));
                  telemetry.published("/event/Test__e", "correlation");
                  telemetry.schemaCacheHit();
                  telemetry.schemaCacheMiss();
                  telemetry.schemaCacheLoadFailure();
                  telemetry.schemaCacheEviction();
                }));
      }
      for (Future<?> invocation : invocations) {
        invocation.get(5, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
    assertDoesNotThrow(() -> new NoOpSalesforcePubSubTelemetry().schemaCacheHit());
    assertDoesNotThrow(
        () -> {
          telemetry.connected(null);
          telemetry.subscribed(null, null, null);
          telemetry.eventReceived(null, null);
          telemetry.eventEmitted(null);
          telemetry.reconnecting(null, null);
          telemetry.decodeFailure(null, null);
          telemetry.published(null, null);
          telemetry.publishFailure(null, null);
          telemetry.connectionState(null, null);
          telemetry.subscriptionState(null, null, null);
          telemetry.inFlightEvents(null, Long.MAX_VALUE);
          telemetry.schemaCacheHit();
          telemetry.schemaCacheMiss();
          telemetry.schemaCacheLoadFailure();
          telemetry.schemaCacheEviction();
          telemetry.metric(null, Double.NaN, null);
        });
  }

  @Test
  void nullFailuresUseOnlyTheLowCardinalityUnknownExceptionLabel() {
    RecordingTelemetry telemetry = new RecordingTelemetry();

    telemetry.reconnecting("/event/Test__e", null);
    telemetry.decodeFailure("/event/Test__e", null);
    telemetry.publishFailure("/event/Test__e", null);

    assertEquals(3, telemetry.metrics.size());
    telemetry.metrics.forEach(
        observed -> {
          assertEquals("Unknown", observed.labels().exceptionType());
          assertEquals(Set.of("topic", "exceptionType"), observed.labels().asMap().keySet());
        });
  }

  private static void assertOneHot(
      RecordingTelemetry telemetry,
      SalesforcePubSubMetric metric,
      ConnectorStatus expected,
      int expectedSeries) {
    List<ObservedMetric> observations =
        telemetry.metrics.stream().filter(observed -> observed.metric() == metric).toList();
    assertEquals(expectedSeries, observations.size());
    assertEquals(1, observations.stream().filter(observed -> observed.value() == 1).count());
    assertEquals(
        expected.name(),
        observations.stream()
            .filter(observed -> observed.value() == 1)
            .findFirst()
            .orElseThrow()
            .labels()
            .outcome());
    assertEquals(
        Set.of(ConnectorStatus.values()).stream()
            .map(Enum::name)
            .collect(java.util.stream.Collectors.toSet()),
        observations.stream()
            .map(observed -> observed.labels().outcome())
            .collect(java.util.stream.Collectors.toSet()));
  }

  private record ObservedMetric(SalesforcePubSubMetric metric, double value, MetricLabels labels) {}

  private static final class RecordingTelemetry implements SalesforcePubSubTelemetry {
    private final List<ObservedMetric> metrics = new ArrayList<>();

    @Override
    public void metric(SalesforcePubSubMetric metric, double value, MetricLabels labels) {
      metrics.add(new ObservedMetric(metric, value, labels));
    }
  }
}
