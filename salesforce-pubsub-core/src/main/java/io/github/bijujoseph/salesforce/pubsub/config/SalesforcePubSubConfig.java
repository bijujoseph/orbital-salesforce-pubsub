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

package io.github.bijujoseph.salesforce.pubsub.config;

import io.github.bijujoseph.salesforce.pubsub.error.ConfigurationException;

/** Immutable, validated settings shared by a Salesforce Pub/Sub client. */
public record SalesforcePubSubConfig(
    String connectionName,
    EndpointConfig endpoint,
    FlowControlOptions flowControl,
    RetryOptions retry) {

  public SalesforcePubSubConfig(String connectionName) {
    this(
        connectionName,
        EndpointConfig.defaults(),
        FlowControlOptions.defaults(),
        RetryOptions.defaults());
  }

  public SalesforcePubSubConfig {
    if (connectionName == null || connectionName.isBlank()) {
      throw new ConfigurationException("Missing connection name");
    }
    connectionName = connectionName.trim();
    if (endpoint == null) {
      throw new ConfigurationException("Missing endpoint configuration");
    }
    if (flowControl == null) {
      throw new ConfigurationException("Missing flow-control configuration");
    }
    if (retry == null) {
      throw new ConfigurationException("Missing retry configuration");
    }
  }

  public static SalesforcePubSubConfig defaults(String connectionName) {
    return new SalesforcePubSubConfig(connectionName);
  }
}
