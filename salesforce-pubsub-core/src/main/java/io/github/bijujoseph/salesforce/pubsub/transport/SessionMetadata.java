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

package io.github.bijujoseph.salesforce.pubsub.transport;

import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;

/** A validated, immutable metadata snapshot for exactly one RPC. */
record SessionMetadata(String accessToken, String instanceUrl, String tenantId) {

  static SessionMetadata from(SalesforceSession session) {
    if (session == null) {
      throw new AuthenticationException("Missing Salesforce session");
    }
    if (session.tenantId() == null || session.tenantId().isBlank()) {
      throw new AuthenticationException("Missing tenant ID for Salesforce Pub/Sub RPC");
    }
    if (!isSafeAscii(session.accessToken())
        || !isSafeAscii(session.instanceUrl())
        || !isSafeAscii(session.tenantId())) {
      throw new AuthenticationException("Salesforce session contains invalid RPC metadata");
    }
    return new SessionMetadata(session.accessToken(), session.instanceUrl(), session.tenantId());
  }

  private static boolean isSafeAscii(String value) {
    return value.chars().allMatch(character -> character >= 0x20 && character <= 0x7e);
  }

  @Override
  public String toString() {
    return "SessionMetadata[accessToken=<redacted>, instanceUrl=<redacted>, tenantId=<redacted>]";
  }
}
