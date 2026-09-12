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

  private void publish(ConnectorStatus next) {
    try {
      telemetry.connectionState(connectionName, next);
    } catch (RuntimeException telemetryFailure) {
      logger
          .atWarn()
          .addKeyValue("connectionName", connectionName)
          .addKeyValue("exceptionCategory", telemetryFailure.getClass().getSimpleName())
          .log("Salesforce Pub/Sub telemetry callback failed");
    }
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
