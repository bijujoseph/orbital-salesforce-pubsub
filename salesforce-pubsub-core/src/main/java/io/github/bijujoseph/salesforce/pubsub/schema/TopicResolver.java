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

package io.github.bijujoseph.salesforce.pubsub.schema;

import io.github.bijujoseph.salesforce.pubsub.error.AuthorizationException;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.error.TopicNotFoundException;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.TopicMetadata;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Resolves a topic and applies the capability policy required by an operation. */
public final class TopicResolver {

  private static final String SUBSCRIBE = "subscribe";
  private static final String PUBLISH = "publish";

  private final SalesforceEventTransport transport;

  public TopicResolver(SalesforceEventTransport transport) {
    this.transport = Objects.requireNonNull(transport, "transport");
  }

  /** Resolves a topic for a subsequent Subscribe request. */
  public CompletionStage<ResolvedTopic> resolveForSubscribe(String topicName) {
    return resolve(topicName, SUBSCRIBE);
  }

  /** Resolves a topic for a subsequent Publish request. */
  public CompletionStage<ResolvedTopic> resolveForPublish(String topicName) {
    return resolve(topicName, PUBLISH);
  }

  private CompletionStage<ResolvedTopic> resolve(String topicName, String operation) {
    CompletionStage<TopicMetadata> lookup;
    try {
      // The caller's value is deliberately passed through without normalization or parsing.
      lookup = transport.getTopic(topicName);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    if (lookup == null) {
      return CompletableFuture.failedFuture(new TopicNotFoundException(topicName));
    }
    return lookup.thenApply(metadata -> validate(topicName, operation, metadata));
  }

  private static ResolvedTopic validate(
      String topicName, String operation, TopicMetadata metadata) {
    if (metadata == null) {
      throw new TopicNotFoundException(topicName);
    }
    boolean allowed = SUBSCRIBE.equals(operation) ? metadata.canSubscribe() : metadata.canPublish();
    if (!allowed) {
      throw new AuthorizationException(topicName, operation);
    }
    if (SchemaResolver.isMissing(metadata.schemaId())) {
      throw new SchemaLookupException(metadata.schemaId());
    }
    return new ResolvedTopic(
        topicName, metadata.canPublish(), metadata.canSubscribe(), metadata.schemaId());
  }
}
