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

package io.github.bijujoseph.salesforce.pubsub.auth;

import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;

/**
 * An immutable set of values required to make authenticated Salesforce Pub/Sub calls.
 *
 * <p>Tenant and user identifiers are optional because some OAuth grants do not return identity
 * information. Access tokens and instance URLs are always required. The token is deliberately
 * omitted from the string representation so a session can safely appear in diagnostics.
 */
public record SalesforceSession(
    String accessToken, String instanceUrl, String tenantId, String userId) {

  public SalesforceSession {
    accessToken = requireNonBlank(accessToken, "access token");
    instanceUrl = requireNonBlank(instanceUrl, "instance URL");
    tenantId = optionalValue(tenantId);
    userId = optionalValue(userId);
  }

  @Override
  public String toString() {
    return "SalesforceSession[accessToken=<redacted>, instanceUrl="
        + instanceUrl
        + ", tenantId="
        + redactedOptional(tenantId)
        + ", userId="
        + redactedOptional(userId)
        + "]";
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new AuthenticationException("Missing " + field);
    }
    return value.trim();
  }

  private static String optionalValue(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }

  private static String redactedOptional(String value) {
    return value == null ? "<absent>" : "<present>";
  }
}
