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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bijujoseph.salesforce.pubsub.error.ConfigurationException;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ConfigurationTest {

  @Test
  void defaultsMatchBlueprint() {
    assertEquals(new EndpointConfig("api.pubsub.salesforce.com", 7443), EndpointConfig.defaults());
    assertEquals(new FlowControlOptions(10, 2, 10, 20), FlowControlOptions.defaults());
    assertEquals(
        new RetryOptions(Duration.ofSeconds(1), Duration.ofSeconds(30), 20, 0.20),
        RetryOptions.defaults());
    SalesforcePubSubConfig defaults = SalesforcePubSubConfig.defaults("  connection  ");
    assertEquals("connection", defaults.connectionName());
    assertEquals(EndpointConfig.defaults(), defaults.endpoint());
    assertEquals(FlowControlOptions.defaults(), defaults.flowControl());
    assertEquals(RetryOptions.defaults(), defaults.retry());
  }

  @Test
  void rejectsInvalidFlowControl() {
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(0, 2, 10, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 0, 10, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 2, 0, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 2, 10, 0));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(21, 2, 10, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 20, 10, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 2, 21, 20));
    assertThrows(ConfigurationException.class, () -> new FlowControlOptions(10, 19, 20, 20));
    FlowControlOptions boundary = new FlowControlOptions(10, 10, 10, 20);
    assertEquals(boundary.maxInFlightEvents(), boundary.refillThreshold() + boundary.refillCount());
  }

  @Test
  void rejectsInvalidRetryAndMissingClientConfiguration() {
    assertThrows(
        ConfigurationException.class, () -> new RetryOptions(null, Duration.ofSeconds(1), 1, 0.2));
    assertThrows(
        ConfigurationException.class,
        () -> new RetryOptions(Duration.ZERO, Duration.ofSeconds(1), 1, 0.2));
    assertThrows(
        ConfigurationException.class, () -> new RetryOptions(Duration.ofSeconds(1), null, 1, 0.2));
    assertThrows(
        ConfigurationException.class,
        () -> new RetryOptions(Duration.ofSeconds(1), Duration.ZERO, 1, 0.2));
    assertThrows(
        ConfigurationException.class,
        () -> new RetryOptions(Duration.ofSeconds(2), Duration.ofSeconds(1), 1, 0.2));
    assertThrows(
        ConfigurationException.class,
        () -> new RetryOptions(Duration.ofSeconds(1), Duration.ofSeconds(1), 0, 0.2));
    assertThrows(
        ConfigurationException.class,
        () -> new RetryOptions(Duration.ofSeconds(1), Duration.ofSeconds(1), 1, 1.1));
    assertThrows(
        ConfigurationException.class,
        () ->
            new SalesforcePubSubConfig(
                " ",
                EndpointConfig.defaults(),
                FlowControlOptions.defaults(),
                RetryOptions.defaults()));
    assertThrows(
        ConfigurationException.class,
        () ->
            new SalesforcePubSubConfig(
                "connection", null, FlowControlOptions.defaults(), RetryOptions.defaults()));
    assertThrows(
        ConfigurationException.class,
        () ->
            new SalesforcePubSubConfig(
                "connection", EndpointConfig.defaults(), null, RetryOptions.defaults()));
    assertThrows(
        ConfigurationException.class,
        () ->
            new SalesforcePubSubConfig(
                "connection", EndpointConfig.defaults(), FlowControlOptions.defaults(), null));
  }

  @Test
  void rejectsInvalidEndpoint() {
    assertThrows(ConfigurationException.class, () -> new EndpointConfig(null, 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig(" ", 7443));
    assertThrows(
        ConfigurationException.class, () -> new EndpointConfig("https://example.com", 7443));
    ConfigurationException credentialHost =
        assertThrows(
            ConfigurationException.class,
            () -> new EndpointConfig("diagnostic-user:diagnostic-password@example.com", 7443));
    assertEquals("Endpoint host is invalid", credentialHost.getMessage());
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("example.com/path", 7443));
    assertThrows(
        ConfigurationException.class, () -> new EndpointConfig("example.com?token=x", 7443));
    assertThrows(
        ConfigurationException.class, () -> new EndpointConfig("example.com#fragment", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("example.com:7443", 7443));
    assertThrows(
        ConfigurationException.class, () -> new EndpointConfig("example.invalid:7443", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("dead:beef:7443", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("999.0.0.1", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("2001:db8:::1", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("[example.com]", 7443));
    assertThrows(
        ConfigurationException.class, () -> new EndpointConfig("[2001:db8::1]:7443", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("example\n.com", 7443));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("host", 0));
    assertThrows(ConfigurationException.class, () -> new EndpointConfig("host", 65_536));
  }

  @Test
  void acceptsDnsIpAndLocalEndpointHosts() {
    assertEquals(
        "api.pubsub.salesforce.com", new EndpointConfig("api.pubsub.salesforce.com", 7443).host());
    assertEquals("example.com.", new EndpointConfig("example.com.", 7443).host());
    assertEquals("localhost", new EndpointConfig(" localhost ", 7443).host());
    assertEquals("münich.example", new EndpointConfig("münich.example", 7443).host());
    assertEquals("123", new EndpointConfig("123", 7443).host());
    assertEquals("1.2.3", new EndpointConfig("1.2.3", 7443).host());
    assertEquals("127.0.0.1", new EndpointConfig("127.0.0.1", 7443).host());
    assertEquals("2001:db8::1", new EndpointConfig("2001:db8::1", 7443).host());
    assertEquals("[2001:db8::1]", new EndpointConfig("[2001:db8::1]", 7443).host());
    assertEquals("::ffff:192.0.2.128", new EndpointConfig("::ffff:192.0.2.128", 7443).host());
    assertEquals("fe80::1%eth0", new EndpointConfig("fe80::1%eth0", 7443).host());
  }

  @Test
  void endpointDiagnosticsDoNotExposeCredentialBearingHosts() {
    EndpointConfig endpoint = new EndpointConfig("private-endpoint.internal", 7443);
    SalesforcePubSubConfig config =
        new SalesforcePubSubConfig(
            "connection", endpoint, FlowControlOptions.defaults(), RetryOptions.defaults());

    assertEquals("EndpointConfig[host=<redacted>, port=7443]", endpoint.toString());
    assertFalse(config.toString().contains("private-endpoint.internal"));
  }
}
