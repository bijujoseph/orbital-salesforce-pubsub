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

package io.github.bijujoseph.salesforce.pubsub.replay;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Process-local replay checkpoint storage. */
public final class InMemoryReplayStore implements ReplayStore {

  private final ConcurrentMap<ReplayNamespace, byte[]> checkpoints = new ConcurrentHashMap<>();

  @Override
  public Optional<byte[]> load(String connectionName, String topic, String consumerName) {
    byte[] checkpoint = checkpoints.get(namespace(connectionName, topic, consumerName));
    return checkpoint == null ? Optional.empty() : Optional.of(checkpoint.clone());
  }

  @Override
  public void save(String connectionName, String topic, String consumerName, byte[] replayId) {
    checkpoints.put(
        namespace(connectionName, topic, consumerName),
        Objects.requireNonNull(replayId, "replayId").clone());
  }

  private static ReplayNamespace namespace(
      String connectionName, String topic, String consumerName) {
    return new ReplayNamespace(
        Objects.requireNonNull(connectionName, "connectionName"),
        Objects.requireNonNull(topic, "topic"),
        Objects.requireNonNull(consumerName, "consumerName"));
  }

  private record ReplayNamespace(String connectionName, String topic, String consumerName) {}
}
