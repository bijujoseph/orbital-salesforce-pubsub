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

import io.github.bijujoseph.salesforce.pubsub.replay.ReplayStore;
import java.util.Objects;

/** Resolves the deterministic initial subscription position without interpreting replay bytes. */
final class ReplayStartSelector {

  private ReplayStartSelector() {}

  static SubscriptionStart select(SubscribeRequest request, ReplayStore replayStore) {
    Objects.requireNonNull(request, "request");
    Objects.requireNonNull(replayStore, "replayStore");

    if (request.start() instanceof ReplayId replayId) {
      return replayId;
    }

    return replayStore
        .load(request.connectionName(), request.topic(), request.consumerName())
        .<SubscriptionStart>map(ReplayId::new)
        .orElseGet(() -> requirePreset(request.start()));
  }

  private static SubscriptionStart requirePreset(SubscriptionStart start) {
    if (start instanceof Latest || start instanceof Earliest) {
      return start;
    }
    throw new IllegalArgumentException("Subscription start must be Latest, Earliest, or ReplayId");
  }
}
