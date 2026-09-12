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

package io.github.bijujoseph.salesforce.pubsub.subscription;

import java.util.Arrays;
import java.util.Objects;

/** Starts immediately after an opaque Salesforce replay position. */
public record ReplayId(byte[] value) implements SubscriptionStart {

  public ReplayId {
    value = Objects.requireNonNull(value, "value").clone();
  }

  /** Returns an isolated copy of the opaque replay position. */
  @Override
  public byte[] value() {
    return value.clone();
  }

  @Override
  public boolean equals(Object other) {
    return this == other
        || other instanceof ReplayId replayId && Arrays.equals(value, replayId.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
