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

/** Network endpoint used by the Salesforce Pub/Sub gRPC client. */
public record EndpointConfig(String host, int port) {

  public static final int DEFAULT_PORT = 7443;
  public static final String DEFAULT_HOST = "api.pubsub.salesforce.com";

  public EndpointConfig {
    host = requireNonBlank(host, "endpoint host");
    if (port < 1 || port > 65_535) {
      throw new ConfigurationException("Endpoint port must be between 1 and 65535");
    }
  }

  public static EndpointConfig defaults() {
    return new EndpointConfig(DEFAULT_HOST, DEFAULT_PORT);
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new ConfigurationException("Missing " + field);
    }
    return value.trim();
  }
}
