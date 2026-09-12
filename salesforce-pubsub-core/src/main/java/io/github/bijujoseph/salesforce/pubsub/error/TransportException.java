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

/** Indicates a sanitized Salesforce Pub/Sub transport or protocol failure. */
public final class TransportException extends SalesforcePubSubException {

  private static final long serialVersionUID = 1L;

  private final String code;

  public TransportException(String code) {
    super("Salesforce Pub/Sub RPC failed [" + SafeExceptionContext.value(code) + "]");
    this.code = SafeExceptionContext.value(code);
  }

  public String code() {
    return code;
  }
}
