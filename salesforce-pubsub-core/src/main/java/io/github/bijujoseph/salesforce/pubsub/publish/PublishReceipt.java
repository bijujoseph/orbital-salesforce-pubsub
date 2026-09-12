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

package io.github.bijujoseph.salesforce.pubsub.publish;

/** Successful result of one unary custom Platform Event publication. */
public record PublishReceipt(
    String topic, String correlationKey, byte[] replayId, String schemaId, String rpcId) {

  public PublishReceipt {
    replayId = copy(replayId);
  }

  /** Returns a defensive copy of the opaque Salesforce replay position. */
  @Override
  public byte[] replayId() {
    return copy(replayId);
  }

  private static byte[] copy(byte[] value) {
    return value == null ? null : value.clone();
  }
}
