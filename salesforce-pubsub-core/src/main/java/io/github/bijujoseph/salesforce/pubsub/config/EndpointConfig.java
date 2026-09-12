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
import java.net.IDN;

/** Network endpoint used by the Salesforce Pub/Sub gRPC client. */
public record EndpointConfig(String host, int port) {

  public static final int DEFAULT_PORT = 7443;
  public static final String DEFAULT_HOST = "api.pubsub.salesforce.com";

  public EndpointConfig {
    host = requireValidHost(host);
    if (port < 1 || port > 65_535) {
      throw new ConfigurationException("Endpoint port must be between 1 and 65535");
    }
  }

  public static EndpointConfig defaults() {
    return new EndpointConfig(DEFAULT_HOST, DEFAULT_PORT);
  }

  @Override
  public String toString() {
    return "EndpointConfig[host=<redacted>, port=" + port + "]";
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new ConfigurationException("Missing " + field);
    }
    return value.trim();
  }

  private static String requireValidHost(String value) {
    if (value != null
        && value.chars().anyMatch(character -> character < 0x20 || character == 0x7f)) {
      throw invalidHost();
    }
    String host = requireNonBlank(value, "endpoint host");
    if (host.chars().anyMatch(character -> Character.isWhitespace(character) || character < 0x20)) {
      throw invalidHost();
    }
    if (host.startsWith("[") || host.endsWith("]")) {
      if (!(host.startsWith("[") && host.endsWith("]"))) {
        throw invalidHost();
      }
      validateIpv6(host.substring(1, host.length() - 1));
      return host;
    }
    if (host.indexOf(':') >= 0) {
      validateIpv6(host);
      return host;
    }
    if (looksLikeIpv4(host)) {
      validateIpv4(host);
      return host;
    }
    validateDnsName(host);
    return host;
  }

  private static void validateIpv6(String host) {
    int scopeSeparator = host.indexOf('%');
    String address = scopeSeparator < 0 ? host : host.substring(0, scopeSeparator);
    if (address.indexOf(':') < 0) {
      throw invalidHost();
    }
    if (scopeSeparator >= 0) {
      String scope = host.substring(scopeSeparator + 1);
      if (scope.isEmpty()
          || scope.indexOf('%') >= 0
          || !scope.chars().allMatch(EndpointConfig::isScopeCharacter)) {
        throw invalidHost();
      }
    }
    if (!isIpv6Literal(address)) {
      throw invalidHost();
    }
  }

  private static void validateIpv4(String host) {
    if (!isIpv4Literal(host)) {
      throw invalidHost();
    }
  }

  private static boolean looksLikeIpv4(String host) {
    return host.chars().allMatch(character -> character == '.' || isAsciiDigit(character))
        && host.split("\\.", -1).length == 4;
  }

  private static boolean isIpv4Literal(String host) {
    String[] octets = host.split("\\.", -1);
    if (octets.length != 4) {
      return false;
    }
    for (String octet : octets) {
      if (octet.isEmpty()
          || octet.length() > 3
          || !octet.chars().allMatch(EndpointConfig::isAsciiDigit)
          || Integer.parseInt(octet) > 255) {
        return false;
      }
    }
    return true;
  }

  private static boolean isIpv6Literal(String address) {
    int compression = address.indexOf("::");
    if (compression != address.lastIndexOf("::")) {
      return false;
    }
    if (compression < 0) {
      return countIpv6Groups(address, true) == 8;
    }
    int leftGroups = countIpv6Groups(address.substring(0, compression), false);
    int rightGroups = countIpv6Groups(address.substring(compression + 2), true);
    return leftGroups >= 0 && rightGroups >= 0 && leftGroups + rightGroups < 8;
  }

  private static int countIpv6Groups(String address, boolean allowTerminalIpv4) {
    if (address.isEmpty()) {
      return 0;
    }
    String[] groups = address.split(":", -1);
    int count = 0;
    for (int index = 0; index < groups.length; index++) {
      String group = groups[index];
      if (group.indexOf('.') >= 0) {
        if (!allowTerminalIpv4 || index != groups.length - 1 || !isIpv4Literal(group)) {
          return -1;
        }
        count += 2;
      } else {
        if (group.isEmpty()
            || group.length() > 4
            || !group.chars().allMatch(EndpointConfig::isAsciiHexDigit)) {
          return -1;
        }
        count++;
      }
    }
    return count;
  }

  private static void validateDnsName(String host) {
    String name = host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
    if (name.isEmpty() || name.endsWith(".")) {
      throw invalidHost();
    }
    try {
      if (IDN.toASCII(name, IDN.USE_STD3_ASCII_RULES).length() > 253) {
        throw invalidHost();
      }
    } catch (IllegalArgumentException exception) {
      throw invalidHost();
    }
  }

  private static boolean isAsciiDigit(int character) {
    return character >= '0' && character <= '9';
  }

  private static boolean isAsciiHexDigit(int character) {
    return isAsciiDigit(character)
        || character >= 'A' && character <= 'F'
        || character >= 'a' && character <= 'f';
  }

  private static boolean isScopeCharacter(int character) {
    return isAsciiDigit(character)
        || character >= 'A' && character <= 'Z'
        || character >= 'a' && character <= 'z'
        || character == '_'
        || character == '-'
        || character == '.'
        || character == '~';
  }

  private static ConfigurationException invalidHost() {
    return new ConfigurationException("Endpoint host is invalid");
  }
}
