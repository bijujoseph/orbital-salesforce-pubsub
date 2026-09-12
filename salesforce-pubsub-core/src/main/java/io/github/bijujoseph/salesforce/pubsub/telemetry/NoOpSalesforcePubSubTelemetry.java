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

/** Backend-free default telemetry implementation. */
public final class NoOpSalesforcePubSubTelemetry implements SalesforcePubSubTelemetry {

  public static final NoOpSalesforcePubSubTelemetry INSTANCE = new NoOpSalesforcePubSubTelemetry();

  public NoOpSalesforcePubSubTelemetry() {}

  @Override
  public void eventReceived(String topic, String eventId) {}

  @Override
  public void reconnecting(String topic, Throwable cause) {}

  @Override
  public void published(String topic, String correlationKey) {}

  @Override
  public void connectionState(String connectionName, ConnectorStatus status) {}

  @Override
  public void subscriptionState(String connectionName, String topic, ConnectorStatus status) {}

  @Override
  public void eventEmitted(String topic) {}

  @Override
  public void decodeFailure(String topic, Throwable cause) {}

  @Override
  public void publishFailure(String topic, Throwable cause) {}

  @Override
  public void inFlightEvents(String topic, long count) {}

  @Override
  public void schemaCacheHit() {}

  @Override
  public void schemaCacheMiss() {}
}
