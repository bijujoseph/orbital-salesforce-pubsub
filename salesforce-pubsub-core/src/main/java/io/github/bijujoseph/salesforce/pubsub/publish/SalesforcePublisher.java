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

import io.github.bijujoseph.salesforce.pubsub.error.PublishException;
import io.github.bijujoseph.salesforce.pubsub.schema.ResolvedTopic;
import io.github.bijujoseph.salesforce.pubsub.schema.SalesforceAvroCodec;
import io.github.bijujoseph.salesforce.pubsub.schema.SalesforceSchemaCache;
import io.github.bijujoseph.salesforce.pubsub.schema.TopicResolver;
import io.github.bijujoseph.salesforce.pubsub.telemetry.NoOpSalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.transport.PubSubApiTransport;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** Asynchronous unary publisher for one custom Platform Event at a time. */
public final class SalesforcePublisher {

  private final TopicResolver topicResolver;
  private final SalesforceSchemaCache schemaCache;
  private final SalesforceAvroCodec codec;
  private final PubSubApiTransport transport;
  private final SalesforcePubSubTelemetry telemetry;

  /** Creates a publisher backed by the supplied transport and no-op telemetry. */
  public SalesforcePublisher(PubSubApiTransport transport) {
    this(transport, null);
  }

  /** Creates a publisher backed by the supplied transport and optional telemetry. */
  public SalesforcePublisher(PubSubApiTransport transport, SalesforcePubSubTelemetry telemetry) {
    PubSubApiTransport checkedTransport = Objects.requireNonNull(transport, "transport");
    SalesforcePubSubTelemetry checkedTelemetry =
        Objects.requireNonNullElse(telemetry, NoOpSalesforcePubSubTelemetry.INSTANCE);
    SalesforceSchemaCache cache = new SalesforceSchemaCache(checkedTransport, checkedTelemetry);
    this.topicResolver = new TopicResolver(checkedTransport);
    this.schemaCache = cache;
    this.codec = new SalesforceAvroCodec(cache);
    this.transport = checkedTransport;
    this.telemetry = checkedTelemetry;
  }

  SalesforcePublisher(
      TopicResolver topicResolver,
      SalesforceSchemaCache schemaCache,
      SalesforceAvroCodec codec,
      PubSubApiTransport transport,
      SalesforcePubSubTelemetry telemetry) {
    this.topicResolver = Objects.requireNonNull(topicResolver, "topic resolver");
    this.schemaCache = Objects.requireNonNull(schemaCache, "schema cache");
    this.codec = Objects.requireNonNull(codec, "codec");
    this.transport = Objects.requireNonNull(transport, "transport");
    this.telemetry = Objects.requireNonNullElse(telemetry, NoOpSalesforcePubSubTelemetry.INSTANCE);
  }

  /**
   * Publishes one request after resolving its exact topic, schema, and encoded value locally.
   *
   * <p>The returned stage completes only for an unambiguous successful Salesforce result.
   */
  public CompletionStage<PublishReceipt> publish(PublishRequest request) {
    if (request == null) {
      return CompletableFuture.failedFuture(new PublishException("Publish request is required"));
    }

    String topic = request.topic();
    Map<String, Object> payload = request.payload();
    String correlationKey = effectiveCorrelationKey(request.correlationKey());

    CompletionStage<PublishReceipt> result;
    try {
      result =
          topicResolver
              .resolveForPublish(topic)
              .thenCompose(
                  resolvedTopic ->
                      schemaCache
                          .resolve(resolvedTopic.schemaId())
                          .thenApply(ignored -> encode(resolvedTopic, payload)))
              .thenCompose(
                  encoded ->
                      transport.publishSingleEvent(
                          topic, correlationKey, encoded.schemaId(), encoded.payload()));
    } catch (RuntimeException failure) {
      result = CompletableFuture.failedFuture(failure);
    }

    result.whenComplete(
        (receipt, failure) -> {
          if (failure == null) {
            recordTelemetry(() -> telemetry.published(topic, correlationKey));
          } else {
            recordTelemetry(() -> telemetry.publishFailure(topic, failure));
          }
        });
    return result;
  }

  private EncodedEvent encode(ResolvedTopic topic, Map<String, Object> payload) {
    return new EncodedEvent(topic.schemaId(), codec.encode(topic.schemaId(), payload));
  }

  private static String effectiveCorrelationKey(String supplied) {
    return supplied == null || supplied.isBlank() ? UUID.randomUUID().toString() : supplied;
  }

  private static void recordTelemetry(Runnable callback) {
    try {
      callback.run();
    } catch (RuntimeException ignored) {
      // Observability is best-effort and must never alter publication behavior.
    }
  }

  private record EncodedEvent(String schemaId, byte[] payload) {}
}
