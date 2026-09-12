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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

class ConnectorHealthTest {

  @Test
  void healthContractExposesExactlyTheBlueprintStates() {
    assertEquals(
        List.of(
            "STARTING",
            "AUTHENTICATING",
            "CONNECTED",
            "SUBSCRIBED",
            "RECONNECTING",
            "DEGRADED",
            "FAILED",
            "STOPPED"),
        Arrays.stream(ConnectorStatus.values()).map(Enum::name).toList());
  }

  @Test
  void lifecyclePublishesApplicationStateAndBlueprintLogLevelsWithoutSensitiveValues() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    RecordingLogger logger = new RecordingLogger();
    ConnectorHealth health = new ConnectorHealth("safe-connection", telemetry, logger.proxy());

    assertEquals(ConnectorStatus.STARTING, health.status());
    assertTrue(health.transitionTo(ConnectorStatus.AUTHENTICATING));
    assertTrue(health.transitionTo(ConnectorStatus.CONNECTED));
    assertTrue(health.transitionTo(ConnectorStatus.SUBSCRIBED));
    assertTrue(health.transitionTo(ConnectorStatus.RECONNECTING));
    assertTrue(health.transitionTo(ConnectorStatus.DEGRADED));
    assertTrue(health.transitionTo(ConnectorStatus.STOPPED));
    assertFalse(health.transitionTo(ConnectorStatus.CONNECTED));

    assertEquals(
        List.of(
            ConnectorStatus.STARTING,
            ConnectorStatus.AUTHENTICATING,
            ConnectorStatus.CONNECTED,
            ConnectorStatus.SUBSCRIBED,
            ConnectorStatus.RECONNECTING,
            ConnectorStatus.DEGRADED,
            ConnectorStatus.STOPPED),
        telemetry.statuses);
    assertEquals(Set.of("DEBUG", "INFO", "WARN"), logger.levels());
    logger.events.forEach(
        event -> {
          assertEquals(Set.of("connectionName", "connectorStatus"), event.keyValues().keySet());
          assertFalse(event.toString().contains("accessToken"));
          assertFalse(event.toString().contains("payload"));
          assertFalse(event.toString().contains("replayId"));
        });

    RecordingLogger failedLogger = new RecordingLogger();
    ConnectorHealth failed =
        new ConnectorHealth(
            "safe-connection", NoOpSalesforcePubSubTelemetry.INSTANCE, failedLogger.proxy());
    assertTrue(failed.transitionTo(ConnectorStatus.FAILED));
    assertTrue(failedLogger.levels().contains("ERROR"));
  }

  @Test
  void aConcurrentTerminalTransitionWinsOnceAndRemainsStable() throws Exception {
    ConnectorHealth health = new ConnectorHealth("connection");
    ExecutorService executor = Executors.newFixedThreadPool(12);
    CountDownLatch start = new CountDownLatch(1);
    AtomicInteger changed = new AtomicInteger();
    try {
      for (int index = 0; index < 100; index++) {
        ConnectorStatus terminal =
            index % 2 == 0 ? ConnectorStatus.FAILED : ConnectorStatus.STOPPED;
        executor.submit(
            () -> {
              await(start);
              if (health.transitionTo(terminal)) {
                changed.incrementAndGet();
              }
            });
      }
      start.countDown();
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertEquals(1, changed.get());
    ConnectorStatus winner = health.status();
    assertTrue(winner == ConnectorStatus.FAILED || winner == ConnectorStatus.STOPPED);
    assertFalse(health.transitionTo(ConnectorStatus.AUTHENTICATING));
    assertEquals(winner, health.status());
  }

  @Test
  void subscriptionAccountingKeepsSameTopicActiveUntilLastExitAndHonorsTerminalPriority() {
    List<ConnectorStatus> topicStates = new ArrayList<>();
    SalesforcePubSubTelemetry telemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void subscriptionState(
              String connectionName, String topic, ConnectorStatus status) {
            topicStates.add(status);
          }
        };
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    Object first = new Object();
    Object second = new Object();

    assertTrue(health.subscriptionSucceeded(first, "/event/Test__e"));
    assertTrue(health.subscriptionSucceeded(second, "/event/Test__e"));
    assertTrue(health.subscriptionEnded(first, ConnectorStatus.CONNECTED));
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertEquals(ConnectorStatus.SUBSCRIBED, topicStates.getLast());
    assertFalse(health.subscriptionEnded(first, ConnectorStatus.CONNECTED));

    assertTrue(health.subscriptionEnded(second, ConnectorStatus.CONNECTED));
    assertEquals(ConnectorStatus.CONNECTED, health.status());
    assertEquals(ConnectorStatus.CONNECTED, topicStates.getLast());

    Object terminal = new Object();
    assertTrue(health.subscriptionSucceeded(terminal, "/event/Terminal__e"));
    assertTrue(health.subscriptionEnded(terminal, ConnectorStatus.FAILED));
    assertEquals(ConnectorStatus.FAILED, health.status());
    assertFalse(health.subscriptionSucceeded(new Object(), "/event/Late__e"));
  }

  @Test
  void nullSubscriptionTopicIsRejectedBeforeStateChangeAndIdentityRemainsReusable() {
    List<String> topics = new ArrayList<>();
    SalesforcePubSubTelemetry telemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void subscriptionState(
              String connectionName, String topic, ConnectorStatus status) {
            topics.add(topic);
          }
        };
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    Object subscription = new Object();

    assertThrows(
        NullPointerException.class, () -> health.subscriptionSucceeded(subscription, null));

    assertEquals(ConnectorStatus.STARTING, health.status());
    assertTrue(topics.isEmpty());
    assertTrue(health.subscriptionSucceeded(subscription, "/event/Test__e"));
    assertEquals(List.of("/event/Test__e"), topics);
  }

  @Test
  void unarySuccessRecoversAccordingToTheActiveSubscriptionRegistry() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    Object subscription = new Object();

    assertTrue(health.transitionTo(ConnectorStatus.DEGRADED));
    assertTrue(health.connectionSucceeded());
    assertEquals(ConnectorStatus.CONNECTED, health.status());

    assertTrue(health.subscriptionSucceeded(subscription, "/event/Test__e"));
    assertTrue(health.transitionTo(ConnectorStatus.DEGRADED));
    assertTrue(health.connectionSucceeded());
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());

    assertTrue(health.transitionTo(ConnectorStatus.FAILED));
    assertFalse(health.connectionSucceeded());
    assertEquals(ConnectorStatus.FAILED, health.status());
    assertEquals(health.status(), telemetry.statuses.getLast());
  }

  @Test
  void partialTransientFailurePreservesRegistryAndRequiresSuccessfulRecovery() {
    List<ConnectorStatus> connectionStates = new ArrayList<>();
    List<String> topicStates = new ArrayList<>();
    SalesforcePubSubTelemetry telemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void connectionState(String connectionName, ConnectorStatus status) {
            connectionStates.add(status);
          }

          @Override
          public void subscriptionState(
              String connectionName, String topic, ConnectorStatus status) {
            topicStates.add(topic + ":" + status);
          }
        };
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    Object first = new Object();
    Object second = new Object();
    Object third = new Object();

    assertTrue(health.subscriptionSucceeded(first, "/event/A__e"));
    assertTrue(health.subscriptionSucceeded(second, "/event/A__e"));
    assertTrue(health.subscriptionSucceeded(third, "/event/B__e"));

    assertTrue(health.subscriptionEnded(first, ConnectorStatus.DEGRADED));
    assertEquals(ConnectorStatus.DEGRADED, health.status());
    assertEquals("/event/A__e:SUBSCRIBED", topicStates.getLast());
    int statesAfterFailure = topicStates.size();
    assertFalse(health.subscriptionEnded(first, ConnectorStatus.DEGRADED));
    assertEquals(statesAfterFailure, topicStates.size());

    assertTrue(health.connectionSucceeded());
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertTrue(health.subscriptionEnded(third, ConnectorStatus.DEGRADED));
    assertEquals(ConnectorStatus.DEGRADED, health.status());
    assertEquals("/event/B__e:DEGRADED", topicStates.getLast());

    assertTrue(health.subscriptionSucceeded(third, "/event/B__e"));
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertTrue(health.subscriptionEnded(second, ConnectorStatus.CONNECTED));
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertTrue(health.subscriptionEnded(third, ConnectorStatus.CONNECTED));
    assertEquals(ConnectorStatus.CONNECTED, health.status());
    assertEquals(health.status(), connectionStates.getLast());
  }

  @Test
  void concurrentTransitionsPublishInStateOrderWithoutStaleTelemetry() throws Exception {
    CountDownLatch authenticatingPublished = new CountDownLatch(1);
    CountDownLatch releaseAuthentication = new CountDownLatch(1);
    List<StatusObservation> observations = new CopyOnWriteArrayList<>();
    java.util.concurrent.atomic.AtomicReference<ConnectorHealth> healthReference =
        new java.util.concurrent.atomic.AtomicReference<>();
    SalesforcePubSubTelemetry blockingTelemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void connectionState(String connectionName, ConnectorStatus published) {
            if (published == ConnectorStatus.STARTING) {
              observations.add(new StatusObservation(published, published));
              return;
            }
            if (published == ConnectorStatus.AUTHENTICATING) {
              authenticatingPublished.countDown();
              await(releaseAuthentication);
            }
            observations.add(new StatusObservation(published, healthReference.get().status()));
          }
        };
    ConnectorHealth health = new ConnectorHealth("connection", blockingTelemetry);
    healthReference.set(health);
    Thread first =
        Thread.ofPlatform()
            .name("health-authenticating-test")
            .start(() -> health.transitionTo(ConnectorStatus.AUTHENTICATING));
    assertTrue(authenticatingPublished.await(5, TimeUnit.SECONDS));
    CountDownLatch secondAttempted = new CountDownLatch(1);
    Thread second =
        Thread.ofPlatform()
            .name("health-connected-test")
            .start(
                () -> {
                  secondAttempted.countDown();
                  health.transitionTo(ConnectorStatus.CONNECTED);
                });
    assertTrue(secondAttempted.await(5, TimeUnit.SECONDS));

    while (second.isAlive() && second.getState() != Thread.State.BLOCKED) {
      Thread.onSpinWait();
    }
    assertEquals(Thread.State.BLOCKED, second.getState());
    assertEquals(ConnectorStatus.AUTHENTICATING, health.status());

    releaseAuthentication.countDown();
    first.join(TimeUnit.SECONDS.toMillis(5));
    second.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(first.isAlive());
    assertFalse(second.isAlive());
    assertEquals(
        List.of(
            new StatusObservation(ConnectorStatus.STARTING, ConnectorStatus.STARTING),
            new StatusObservation(ConnectorStatus.AUTHENTICATING, ConnectorStatus.AUTHENTICATING),
            new StatusObservation(ConnectorStatus.CONNECTED, ConnectorStatus.CONNECTED)),
        observations);
    assertEquals(ConnectorStatus.CONNECTED, health.status());
  }

  @Test
  void invalidHealthInputsFailBeforePublishing() {
    assertThrows(IllegalArgumentException.class, () -> new ConnectorHealth(" "));
    ConnectorHealth health = new ConnectorHealth("connection", null);
    assertThrows(NullPointerException.class, () -> health.transitionTo(null));
  }

  @Test
  void telemetryCallbackFailuresNeverChangeHealthBehaviorOrLeakTheirMessage() {
    RecordingLogger logger = new RecordingLogger();
    SalesforcePubSubTelemetry throwingTelemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void connectionState(String connectionName, ConnectorStatus status) {
            throw new IllegalStateException("credential payload replay customer pii");
          }
        };

    ConnectorHealth health =
        new ConnectorHealth("safe-connection", throwingTelemetry, logger.proxy());
    assertTrue(health.transitionTo(ConnectorStatus.CONNECTED));
    assertEquals(ConnectorStatus.CONNECTED, health.status());
    assertTrue(logger.levels().contains("WARN"));
    logger.events.forEach(
        event -> {
          assertFalse(event.toString().contains("credential"));
          assertFalse(event.toString().contains("payload"));
          assertFalse(event.toString().contains("replay"));
          assertFalse(event.toString().contains("customer"));
          assertFalse(event.toString().contains("pii"));
        });
  }

  private static void await(CountDownLatch latch) {
    try {
      if (!latch.await(5, TimeUnit.SECONDS)) {
        throw new AssertionError("Timed out awaiting concurrent start");
      }
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted awaiting concurrent start", exception);
    }
  }

  private static final class RecordingTelemetry implements SalesforcePubSubTelemetry {
    private final List<ConnectorStatus> statuses = new ArrayList<>();

    @Override
    public void connectionState(String connectionName, ConnectorStatus status) {
      statuses.add(status);
    }
  }

  private record LogEvent(String level, String message, Map<String, Object> keyValues) {}

  private record StatusObservation(ConnectorStatus published, ConnectorStatus current) {}

  private static final class RecordingLogger {
    private final List<LogEvent> events = new ArrayList<>();

    Logger proxy() {
      return (Logger)
          Proxy.newProxyInstance(
              Logger.class.getClassLoader(),
              new Class<?>[] {Logger.class},
              (ignored, method, arguments) -> {
                if (method.getName().startsWith("at")) {
                  return builder(method.getName().substring(2).toUpperCase());
                }
                if (method.getName().startsWith("is")) {
                  return true;
                }
                if (method.getName().equals("getName")) {
                  return "recording";
                }
                return null;
              });
    }

    Set<String> levels() {
      return events.stream().map(LogEvent::level).collect(java.util.stream.Collectors.toSet());
    }

    private LoggingEventBuilder builder(String level) {
      Map<String, Object> keyValues = new LinkedHashMap<>();
      final LoggingEventBuilder[] self = new LoggingEventBuilder[1];
      self[0] =
          (LoggingEventBuilder)
              Proxy.newProxyInstance(
                  LoggingEventBuilder.class.getClassLoader(),
                  new Class<?>[] {LoggingEventBuilder.class},
                  (ignored, method, arguments) -> {
                    if (method.getName().equals("addKeyValue")) {
                      keyValues.put((String) arguments[0], arguments[1]);
                      return self[0];
                    }
                    if (method.getName().equals("log")) {
                      String message =
                          arguments == null || arguments.length == 0
                              ? ""
                              : String.valueOf(arguments[0]);
                      events.add(new LogEvent(level, message, Map.copyOf(keyValues)));
                      return null;
                    }
                    if (method.getReturnType() == LoggingEventBuilder.class) {
                      return self[0];
                    }
                    return null;
                  });
      return self[0];
    }
  }
}
