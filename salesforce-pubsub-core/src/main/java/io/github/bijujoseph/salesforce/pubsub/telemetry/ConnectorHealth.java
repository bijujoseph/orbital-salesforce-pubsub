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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Thread-safe application health publisher for the connector lifecycle. */
public final class ConnectorHealth {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorHealth.class);

  private final String connectionName;
  private final SalesforcePubSubTelemetry telemetry;
  private final Logger logger;
  private final Map<Object, String> activeSubscriptions = new HashMap<>();
  private final AtomicReference<ConnectorStatus> status =
      new AtomicReference<>(ConnectorStatus.STARTING);

  public ConnectorHealth(String connectionName) {
    this(connectionName, NoOpSalesforcePubSubTelemetry.INSTANCE);
  }

  public ConnectorHealth(String connectionName, SalesforcePubSubTelemetry telemetry) {
    this(connectionName, telemetry, LOGGER);
  }

  ConnectorHealth(String connectionName, SalesforcePubSubTelemetry telemetry, Logger logger) {
    if (connectionName == null || connectionName.isBlank()) {
      throw new IllegalArgumentException("Missing connection name");
    }
    this.connectionName = connectionName.trim();
    this.telemetry = telemetry == null ? NoOpSalesforcePubSubTelemetry.INSTANCE : telemetry;
    this.logger = Objects.requireNonNull(logger, "logger");
    publish(ConnectorStatus.STARTING);
  }

  public ConnectorStatus status() {
    ConnectorStatus current = status.get();
    logger
        .atDebug()
        .addKeyValue("connectionName", connectionName)
        .addKeyValue("connectorStatus", current)
        .log("Salesforce Pub/Sub connector health observed");
    return current;
  }

  /**
   * Publishes a caller-observed lifecycle state.
   *
   * <p>The health publisher deliberately does not duplicate the subscription state machine. It
   * accepts lifecycle states until FAILED or STOPPED wins, after which status is stable.
   *
   * @return whether this call changed the status
   */
  public synchronized boolean transitionTo(ConnectorStatus next) {
    Objects.requireNonNull(next, "next status");
    while (true) {
      ConnectorStatus current = status.get();
      if (current == next || current.terminal()) {
        return false;
      }
      if (status.compareAndSet(current, next)) {
        publish(next);
        return true;
      }
    }
  }

  /** Publishes a successful unary connection without downgrading an active subscription. */
  public synchronized boolean connectionSucceeded() {
    if (status.get() == ConnectorStatus.SUBSCRIBED) {
      return false;
    }
    return transitionTo(ConnectorStatus.CONNECTED);
  }

  /** Publishes the successful establishment of one subscription. */
  public synchronized boolean subscriptionSucceeded(Object subscription, String topic) {
    Objects.requireNonNull(subscription, "subscription");
    if (status.get().terminal() || activeSubscriptions.putIfAbsent(subscription, topic) != null) {
      return false;
    }
    boolean transitioned = transitionTo(ConnectorStatus.SUBSCRIBED);
    publishTelemetry(
        () -> telemetry.subscriptionState(connectionName, topic, ConnectorStatus.SUBSCRIBED));
    publishTelemetry(() -> telemetry.subscribed(connectionName, topic, null));
    return transitioned || status.get() == ConnectorStatus.SUBSCRIBED;
  }

  /** Publishes one subscription exit and restores aggregate health when the last stream ends. */
  public synchronized boolean subscriptionEnded(Object subscription, ConnectorStatus exitStatus) {
    Objects.requireNonNull(subscription, "subscription");
    Objects.requireNonNull(exitStatus, "exit status");
    String activeTopic = activeSubscriptions.remove(subscription);
    if (activeTopic == null) {
      if (exitStatus == ConnectorStatus.FAILED || exitStatus == ConnectorStatus.DEGRADED) {
        transitionTo(exitStatus);
      }
      return false;
    }
    ConnectorStatus current = status.get();
    ConnectorStatus effectiveExit = current.terminal() ? current : exitStatus;
    long sameTopicRemaining =
        activeSubscriptions.values().stream().filter(activeTopic::equals).count();
    ConnectorStatus topicStatus =
        sameTopicRemaining > 0 && !effectiveExit.terminal()
            ? ConnectorStatus.SUBSCRIBED
            : effectiveExit;
    publishTelemetry(() -> telemetry.subscriptionState(connectionName, activeTopic, topicStatus));
    if (effectiveExit.terminal()) {
      transitionTo(effectiveExit);
    } else if (activeSubscriptions.isEmpty()) {
      transitionTo(exitStatus);
    } else if (status.get() != ConnectorStatus.SUBSCRIBED) {
      transitionTo(ConnectorStatus.SUBSCRIBED);
    }
    return true;
  }

  private void publish(ConnectorStatus next) {
    publishTelemetry(() -> telemetry.connectionState(connectionName, next));
    if (next == ConnectorStatus.CONNECTED) {
      publishTelemetry(() -> telemetry.connected(connectionName));
    }
    logTransition(next);
  }

  private void publishTelemetry(Runnable callback) {
    try {
      callback.run();
    } catch (RuntimeException telemetryFailure) {
      logger
          .atWarn()
          .addKeyValue("connectionName", connectionName)
          .addKeyValue("exceptionCategory", telemetryFailure.getClass().getSimpleName())
          .log("Salesforce Pub/Sub telemetry callback failed");
    }
  }

  private void logTransition(ConnectorStatus next) {
    switch (next) {
      case RECONNECTING, DEGRADED ->
          logger
              .atWarn()
              .addKeyValue("connectionName", connectionName)
              .addKeyValue("connectorStatus", next)
              .log("Salesforce Pub/Sub connector lifecycle changed");
      case FAILED ->
          logger
              .atError()
              .addKeyValue("connectionName", connectionName)
              .addKeyValue("connectorStatus", next)
              .log("Salesforce Pub/Sub connector lifecycle changed");
      default ->
          logger
              .atInfo()
              .addKeyValue("connectionName", connectionName)
              .addKeyValue("connectorStatus", next)
              .log("Salesforce Pub/Sub connector lifecycle changed");
    }
  }
}
