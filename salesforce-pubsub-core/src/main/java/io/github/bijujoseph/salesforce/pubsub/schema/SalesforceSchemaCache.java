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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Ticker;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.telemetry.NoOpSalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.avro.Schema;

/** Bounded, expiring cache of parsed Salesforce Avro schemas keyed by exact schema ID. */
public final class SalesforceSchemaCache {

  static final long MAXIMUM_SIZE = 100;
  static final Duration EXPIRE_AFTER_ACCESS = Duration.ofHours(24);

  private final SchemaResolver resolver;
  private final SalesforcePubSubTelemetry telemetry;
  private final Cache<String, CompletableFuture<Schema>> cache;

  public SalesforceSchemaCache(SalesforceEventTransport transport) {
    this(transport, null);
  }

  public SalesforceSchemaCache(
      SalesforceEventTransport transport, SalesforcePubSubTelemetry telemetry) {
    this(new SchemaResolver(transport), telemetry, Ticker.systemTicker());
  }

  SalesforceSchemaCache(
      SchemaResolver resolver, SalesforcePubSubTelemetry telemetry, Ticker ticker) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
    this.telemetry = Objects.requireNonNullElse(telemetry, NoOpSalesforcePubSubTelemetry.INSTANCE);
    this.cache =
        Caffeine.newBuilder()
            .maximumSize(MAXIMUM_SIZE)
            .expireAfterAccess(EXPIRE_AFTER_ACCESS)
            .ticker(Objects.requireNonNull(ticker, "ticker"))
            .executor(Runnable::run)
            .removalListener(this::recordRemoval)
            .build();
  }

  /** Returns a cached parsed schema or performs one coalesced lookup for the exact schema ID. */
  public CompletionStage<Schema> resolve(String schemaId) {
    if (SchemaResolver.isMissing(schemaId)) {
      return CompletableFuture.failedFuture(new SchemaLookupException(schemaId));
    }

    AtomicBoolean loadStarted = new AtomicBoolean();
    CompletableFuture<Schema> resolved =
        cache.get(
            schemaId,
            key -> {
              loadStarted.set(true);
              recordTelemetry(telemetry::schemaCacheMiss);
              return resolver.resolve(key).toCompletableFuture();
            });
    if (loadStarted.get()) {
      resolved.whenComplete(
          (schema, failure) -> {
            if (failure != null || resolved.isCancelled()) {
              recordTelemetry(telemetry::schemaCacheLoadFailure);
              cache.asMap().remove(schemaId, resolved);
            }
          });
    } else {
      recordTelemetry(telemetry::schemaCacheHit);
    }
    return callerStage(resolved);
  }

  Schema resolvedSchema(String schemaId) {
    CompletableFuture<Schema> resolved = cache.getIfPresent(schemaId);
    if (resolved == null
        || !resolved.isDone()
        || resolved.isCancelled()
        || resolved.isCompletedExceptionally()) {
      return null;
    }
    return resolved.getNow(null);
  }

  long estimatedSize() {
    return cache.estimatedSize();
  }

  void cleanUp() {
    cache.cleanUp();
  }

  private void recordRemoval(String key, CompletableFuture<Schema> schema, RemovalCause cause) {
    if (cause.wasEvicted()) {
      recordTelemetry(telemetry::schemaCacheEviction);
    }
  }

  private static CompletionStage<Schema> callerStage(CompletableFuture<Schema> shared) {
    CompletableFuture<Schema> caller = new CompletableFuture<>();
    shared.whenComplete(
        (schema, failure) -> {
          if (failure == null) {
            caller.complete(schema);
          } else {
            caller.completeExceptionally(failure);
          }
        });
    return caller;
  }

  private static void recordTelemetry(Runnable callback) {
    try {
      callback.run();
    } catch (RuntimeException ignored) {
      // Observability is best-effort and must never alter schema resolution behavior.
    }
  }
}
