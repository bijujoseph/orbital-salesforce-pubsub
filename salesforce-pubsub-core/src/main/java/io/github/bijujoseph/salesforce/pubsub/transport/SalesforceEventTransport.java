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
import java.util.concurrent.CompletionStage;

/** Lifecycle and session contract shared by Salesforce event transports. */
public interface SalesforceEventTransport extends AutoCloseable {

  /**
   * Fetches raw topic information without applying topic policy or caching.
   *
   * <p>The direct result of {@link CompletionStage#toCompletableFuture()} supports caller
   * cancellation of the live RPC. Forced completion, obtrusion, asynchronous completion, and
   * timeout mutation are rejected and cannot complete or unregister the RPC. Cancellation of a
   * stage derived from this result is not an RPC-cancellation mechanism.
   */
  CompletionStage<TopicMetadata> getTopic(String topicName);

  /**
   * Fetches raw schema information without applying schema policy or caching.
   *
   * <p>The direct result of {@link CompletionStage#toCompletableFuture()} supports caller
   * cancellation of the live RPC. Forced completion, obtrusion, asynchronous completion, and
   * timeout mutation are rejected and cannot complete or unregister the RPC. Cancellation of a
   * stage derived from this result is not an RPC-cancellation mechanism.
   */
  CompletionStage<SchemaMetadata> getSchema(String schemaId);

  /** Atomically replaces the session used by RPCs that start after this call. */
  void updateSession(SalesforceSession session);

  /** Returns whether this transport has begun permanent shutdown. */
  boolean isClosed();

  /** Starts channel shutdown and returns without waiting for termination. */
  @Override
  void close();
}
