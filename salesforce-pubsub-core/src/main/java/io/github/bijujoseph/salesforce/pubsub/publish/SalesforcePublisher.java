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
import io.github.bijujoseph.salesforce.pubsub.schema.SalesforceSchemaCache;
import io.github.bijujoseph.salesforce.pubsub.schema.TopicResolver;
import io.github.bijujoseph.salesforce.pubsub.telemetry.NoOpSalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.transport.PubSubApiTransport;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Asynchronous unary publisher for one custom Platform Event at a time. */
public final class SalesforcePublisher {

  private final TopicResolver topicResolver;
  private final SalesforceSchemaCache schemaCache;
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
    this.transport = checkedTransport;
    this.telemetry = checkedTelemetry;
  }

  SalesforcePublisher(
      TopicResolver topicResolver,
      SalesforceSchemaCache schemaCache,
      PubSubApiTransport transport,
      SalesforcePubSubTelemetry telemetry) {
    this.topicResolver = Objects.requireNonNull(topicResolver, "topic resolver");
    this.schemaCache = Objects.requireNonNull(schemaCache, "schema cache");
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
      PublishOperation rejected = new PublishOperation(null, null);
      rejected.fail(new PublishException("Publish request is required"));
      return rejected;
    }

    String topic = request.topic();
    Map<String, Object> payload = request.payload();
    String correlationKey = effectiveCorrelationKey(request.correlationKey());

    PublishOperation operation = new PublishOperation(topic, correlationKey);
    operation.continueWith(
        () -> topicResolver.resolveForPublish(topic),
        resolvedTopic ->
            operation.continueWith(
                () -> schemaCache.resolveAndEncode(resolvedTopic.schemaId(), payload),
                encoded ->
                    operation.continueWith(
                        () ->
                            transport.publishSingleEvent(
                                topic, correlationKey, resolvedTopic.schemaId(), encoded),
                        operation::succeed)));
    return operation;
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

  private static Throwable telemetryCause(Throwable failure) {
    Throwable cause = failure;
    while ((cause instanceof CompletionException || cause instanceof ExecutionException)
        && cause.getCause() != null
        && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  private final class PublishOperation extends CompletableFuture<PublishReceipt> {

    private final Object lock = new Object();
    private final String topic;
    private final String correlationKey;
    private CompletableFuture<?> activeStage;
    private OperationState state = OperationState.ACTIVE;

    private PublishOperation(String topic, String correlationKey) {
      this.topic = topic;
      this.correlationKey = correlationKey;
    }

    private <T> void continueWith(
        Supplier<? extends CompletionStage<T>> stageSupplier, Consumer<T> onSuccess) {
      CompletableFuture<T> stage = null;
      RuntimeException startFailure = null;
      synchronized (lock) {
        if (state != OperationState.ACTIVE) {
          return;
        }
        try {
          CompletionStage<T> supplied =
              Objects.requireNonNull(stageSupplier.get(), "publish operation stage");
          stage = supplied.toCompletableFuture();
          activeStage = stage;
        } catch (RuntimeException failure) {
          startFailure = failure;
        }
      }
      if (startFailure != null) {
        fail(startFailure);
        return;
      }

      CompletableFuture<T> trackedStage = stage;
      trackedStage.whenComplete(
          (value, failure) -> stageCompleted(trackedStage, value, failure, onSuccess));
    }

    private <T> void stageCompleted(
        CompletableFuture<T> completedStage, T value, Throwable failure, Consumer<T> onSuccess) {
      synchronized (lock) {
        if (state != OperationState.ACTIVE || activeStage != completedStage) {
          return;
        }
        activeStage = null;
      }
      if (failure != null) {
        fail(telemetryCause(failure));
        return;
      }
      try {
        onSuccess.accept(value);
      } catch (RuntimeException callbackFailure) {
        fail(callbackFailure);
      }
    }

    private void succeed(PublishReceipt receipt) {
      if (!terminate(OperationState.SUCCEEDED)) {
        return;
      }
      recordTelemetry(() -> telemetry.published(topic, correlationKey));
      super.complete(receipt);
    }

    private void fail(Throwable failure) {
      Throwable cause = telemetryCause(failure);
      if (!terminate(OperationState.FAILED)) {
        return;
      }
      recordTelemetry(() -> telemetry.publishFailure(topic, cause));
      super.completeExceptionally(cause);
    }

    private boolean terminate(OperationState terminalState) {
      synchronized (lock) {
        if (state != OperationState.ACTIVE) {
          return false;
        }
        state = terminalState;
        activeStage = null;
        return true;
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      CompletableFuture<?> stageToCancel;
      synchronized (lock) {
        if (state != OperationState.ACTIVE) {
          return false;
        }
        state = OperationState.CANCELLED;
        stageToCancel = activeStage;
        activeStage = null;
      }
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      if (stageToCancel != null) {
        stageToCancel.cancel(mayInterruptIfRunning);
      }
      recordTelemetry(
          () -> telemetry.publishFailure(topic, new java.util.concurrent.CancellationException()));
      return cancelled;
    }

    @Override
    public boolean complete(PublishReceipt value) {
      return false;
    }

    @Override
    public boolean completeExceptionally(Throwable failure) {
      return false;
    }

    @Override
    public void obtrudeValue(PublishReceipt value) {
      throw readOnlyFailure();
    }

    @Override
    public void obtrudeException(Throwable failure) {
      throw readOnlyFailure();
    }

    @Override
    public CompletableFuture<PublishReceipt> completeAsync(
        Supplier<? extends PublishReceipt> supplier, java.util.concurrent.Executor executor) {
      throw readOnlyFailure();
    }

    @Override
    public CompletableFuture<PublishReceipt> completeAsync(
        Supplier<? extends PublishReceipt> supplier) {
      throw readOnlyFailure();
    }

    @Override
    public CompletableFuture<PublishReceipt> orTimeout(long timeout, TimeUnit unit) {
      throw readOnlyFailure();
    }

    @Override
    public CompletableFuture<PublishReceipt> completeOnTimeout(
        PublishReceipt value, long timeout, TimeUnit unit) {
      throw readOnlyFailure();
    }

    private UnsupportedOperationException readOnlyFailure() {
      return new UnsupportedOperationException("Salesforce publish stage is read-only");
    }
  }

  private enum OperationState {
    ACTIVE,
    CANCELLED,
    SUCCEEDED,
    FAILED
  }
}
