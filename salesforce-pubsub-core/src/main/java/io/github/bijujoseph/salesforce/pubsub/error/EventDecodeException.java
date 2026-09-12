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

/** Indicates that an event payload could not be decoded with its declared schema. */
public final class EventDecodeException extends SalesforcePubSubException {

  private static final long serialVersionUID = 1L;

  public EventDecodeException(String message) {
    super(message);
  }

  /** Creates a decode failure containing only a sanitized schema identifier. */
  public static EventDecodeException forSchema(String schemaId) {
    return new EventDecodeException(
        "Unable to decode Salesforce event [schemaId="
            + SafeExceptionContext.value(schemaId)
            + "]");
  }
}
