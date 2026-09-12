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

package io.github.bijujoseph.salesforce.pubsub.error;

/** Indicates that Salesforce refused an otherwise authenticated operation. */
public final class AuthorizationException extends SalesforcePubSubException {

  private static final long serialVersionUID = 1L;

  private final String topic;
  private final String operation;

  public AuthorizationException(String message) {
    super(message);
    this.topic = null;
    this.operation = null;
  }

  /** Creates a capability failure without including credentials or event data. */
  public AuthorizationException(String topic, String operation) {
    super(
        "Salesforce topic does not permit operation [operation="
            + SafeExceptionContext.value(operation)
            + ", topic="
            + SafeExceptionContext.value(topic)
            + "]");
    this.topic = SafeExceptionContext.value(topic);
    this.operation = SafeExceptionContext.value(operation);
  }

  public String topic() {
    return topic;
  }

  public String operation() {
    return operation;
  }
}
