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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.benmanes.caffeine.cache.Ticker;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.SchemaMetadata;
import io.github.bijujoseph.salesforce.pubsub.transport.TopicMetadata;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class SalesforceSchemaCacheTest {

  private static final String RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[]}";

  @Test
  void recordsOneMissThenOneHitAndFetchesNewSchemaIdsSeparately() {
    RecordingTransport transport = RecordingTransport.successful();
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    Schema first = cache.resolve("schema-1").toCompletableFuture().join();
    Schema cached = cache.resolve("schema-1").toCompletableFuture().join();
    cache.resolve("schema-2").toCompletableFuture().join();

    assertSame(first, cached);
    assertEquals(2, transport.calls.get());
    assertEquals(2, telemetry.misses.get());
    assertEquals(1, telemetry.hits.get());
    assertEquals(0, telemetry.loadFailures.get());
    assertEquals(0, telemetry.evictions.get());
  }

  @Test
  void cancellingOneWaiterDoesNotCancelOrEvictTheSharedLoad() {
    CompletableFuture<SchemaMetadata> pending = new CompletableFuture<>();
    RecordingTransport transport = new RecordingTransport(ignored -> pending);
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    CompletionStage<Schema> first = cache.resolve("schema-1");
    CompletionStage<Schema> second = cache.resolve("schema-1");
    assertTrue(first.toCompletableFuture().cancel(false));
    CompletionStage<Schema> third = cache.resolve("schema-1");

    assertNotSame(first.toCompletableFuture(), second.toCompletableFuture());
    assertNotSame(first.toCompletableFuture(), third.toCompletableFuture());
    assertNotSame(second.toCompletableFuture(), third.toCompletableFuture());
    assertEquals(1, transport.calls.get());
    assertEquals(1, telemetry.misses.get());
    assertEquals(2, telemetry.hits.get());

    pending.complete(new SchemaMetadata("schema-1", RECORD_SCHEMA));
    assertTrue(first.toCompletableFuture().isCancelled());
    assertSame(second.toCompletableFuture().join(), third.toCompletableFuture().join());
    assertEquals(1, transport.calls.get());
    assertEquals(0, telemetry.loadFailures.get());
  }

  @Test
  void failedLoadIsRemovedAndTheNextRequestRetries() {
    AtomicInteger attempt = new AtomicInteger();
    RecordingTransport transport =
        new RecordingTransport(
            schemaId ->
                attempt.getAndIncrement() == 0
                    ? CompletableFuture.failedFuture(new SchemaLookupException(schemaId))
                    : CompletableFuture.completedFuture(
                        new SchemaMetadata(schemaId, RECORD_SCHEMA)));
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve("schema-1")));
    cache.resolve("schema-1").toCompletableFuture().join();

    assertEquals(2, transport.calls.get());
    assertEquals(2, telemetry.misses.get());
    assertEquals(0, telemetry.hits.get());
    assertEquals(1, telemetry.loadFailures.get());
  }

  @Test
  void malformedAndMismatchedSchemasAreNotCached() {
    for (SchemaMetadata invalid :
        new SchemaMetadata[] {
          new SchemaMetadata("schema-1", "not-avro"), new SchemaMetadata("different", RECORD_SCHEMA)
        }) {
      RecordingTransport transport =
          new RecordingTransport(ignored -> CompletableFuture.completedFuture(invalid));
      RecordingTelemetry telemetry = new RecordingTelemetry();
      SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

      assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve("schema-1")));
      assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve("schema-1")));

      assertEquals(2, transport.calls.get());
      assertEquals(2, telemetry.misses.get());
      assertEquals(2, telemetry.loadFailures.get());
      assertEquals(0, cache.estimatedSize());
    }
  }

  @Test
  void cancellingTheOnlyWaiterDoesNotDiscardAUsefulSharedLoad() {
    CompletableFuture<SchemaMetadata> pending = new CompletableFuture<>();
    RecordingTransport transport = new RecordingTransport(ignored -> pending);
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    CompletableFuture<Schema> cancelled = cache.resolve("schema-1").toCompletableFuture();
    assertTrue(cancelled.cancel(false));
    pending.complete(new SchemaMetadata("schema-1", RECORD_SCHEMA));
    Schema cached = cache.resolve("schema-1").toCompletableFuture().join();

    assertEquals(Schema.Type.RECORD, cached.getType());
    assertEquals(1, transport.calls.get());
    assertEquals(1, telemetry.misses.get());
    assertEquals(1, telemetry.hits.get());
    assertEquals(0, telemetry.loadFailures.get());
    assertTrue(cancelled.isCancelled());
  }

  @Test
  void expiresOnlyAfterTwentyFourHoursWithoutAccess() {
    RecordingTransport transport = RecordingTransport.successful();
    RecordingTelemetry telemetry = new RecordingTelemetry();
    MutableTicker ticker = new MutableTicker();
    SalesforceSchemaCache cache = cache(transport, telemetry, ticker);

    cache.resolve("schema-1").toCompletableFuture().join();
    ticker.advance(Duration.ofHours(23));
    cache.resolve("schema-1").toCompletableFuture().join();
    ticker.advance(Duration.ofHours(23));
    cache.resolve("schema-1").toCompletableFuture().join();
    ticker.advance(Duration.ofHours(25));
    cache.resolve("schema-1").toCompletableFuture().join();

    assertEquals(2, transport.calls.get());
    assertEquals(2, telemetry.misses.get());
    assertEquals(2, telemetry.hits.get());
    assertEquals(1, telemetry.evictions.get());
  }

  @Test
  void neverRetainsMoreThanOneHundredSchemas() {
    RecordingTransport transport = RecordingTransport.successful();
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    for (int index = 0; index <= SalesforceSchemaCache.MAXIMUM_SIZE; index++) {
      cache.resolve("schema-" + index).toCompletableFuture().join();
    }
    cache.cleanUp();

    assertEquals(SalesforceSchemaCache.MAXIMUM_SIZE, cache.estimatedSize());
    assertEquals(101, transport.calls.get());
    assertEquals(101, telemetry.misses.get());
    assertEquals(1, telemetry.evictions.get());
  }

  @Test
  void rejectsMissingIdsWithoutCallingOrCountingTheCacheLoader() {
    RecordingTransport transport = RecordingTransport.successful();
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = cache(transport, telemetry, new MutableTicker());

    assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve(null)));
    assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve(" ")));

    assertEquals(0, transport.calls.get());
    assertEquals(0, telemetry.misses.get());
    assertEquals(0, telemetry.hits.get());
    assertEquals(0, telemetry.loadFailures.get());
    assertFalse(cache.resolve(null).toCompletableFuture().isCancelled());
  }

  @Test
  void omittedTelemetryUsesTheNoOpDefault() {
    RecordingTransport transport = RecordingTransport.successful();
    SalesforceSchemaCache cache = new SalesforceSchemaCache(transport, null);

    Schema first = cache.resolve("schema-1").toCompletableFuture().join();
    Schema second = cache.resolve("schema-1").toCompletableFuture().join();

    assertSame(first, second);
    assertEquals(1, transport.calls.get());
  }

  @Test
  void telemetryFailuresNeverAlterCacheBehavior() {
    AtomicInteger attempt = new AtomicInteger();
    RecordingTransport transport =
        new RecordingTransport(
            schemaId ->
                attempt.getAndIncrement() == 2
                    ? CompletableFuture.failedFuture(new SchemaLookupException(schemaId))
                    : CompletableFuture.completedFuture(
                        new SchemaMetadata(schemaId, RECORD_SCHEMA)));
    SalesforcePubSubTelemetry failingTelemetry =
        new SalesforcePubSubTelemetry() {
          @Override
          public void schemaCacheHit() {
            throw new IllegalStateException("hit telemetry unavailable");
          }

          @Override
          public void schemaCacheMiss() {
            throw new IllegalStateException("miss telemetry unavailable");
          }

          @Override
          public void schemaCacheLoadFailure() {
            throw new IllegalStateException("failure telemetry unavailable");
          }

          @Override
          public void schemaCacheEviction() {
            throw new IllegalStateException("eviction telemetry unavailable");
          }
        };
    SalesforceSchemaCache cache = cache(transport, failingTelemetry, new MutableTicker());

    cache.resolve("schema-0").toCompletableFuture().join();
    cache.resolve("schema-0").toCompletableFuture().join();
    cache.resolve("schema-1").toCompletableFuture().join();
    assertInstanceOf(SchemaLookupException.class, failureOf(cache.resolve("schema-failure")));
    for (int index = 2; index <= SalesforceSchemaCache.MAXIMUM_SIZE; index++) {
      cache.resolve("schema-" + index).toCompletableFuture().join();
    }
    cache.cleanUp();

    assertEquals(SalesforceSchemaCache.MAXIMUM_SIZE, cache.estimatedSize());
  }

  private static SalesforceSchemaCache cache(
      RecordingTransport transport, SalesforcePubSubTelemetry telemetry, Ticker ticker) {
    return new SalesforceSchemaCache(new SchemaResolver(transport), telemetry, ticker);
  }

  private static Throwable failureOf(CompletionStage<?> stage) {
    return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join())
        .getCause();
  }

  private static final class RecordingTransport implements SalesforceEventTransport {

    private final AtomicInteger calls = new AtomicInteger();
    private final Function<String, CompletionStage<SchemaMetadata>> lookup;

    private RecordingTransport(Function<String, CompletionStage<SchemaMetadata>> lookup) {
      this.lookup = lookup;
    }

    private static RecordingTransport successful() {
      return new RecordingTransport(
          schemaId ->
              CompletableFuture.completedFuture(new SchemaMetadata(schemaId, RECORD_SCHEMA)));
    }

    @Override
    public CompletionStage<TopicMetadata> getTopic(String topicName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<SchemaMetadata> getSchema(String schemaId) {
      calls.incrementAndGet();
      return lookup.apply(schemaId);
    }

    @Override
    public void updateSession(SalesforceSession session) {}

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void close() {}
  }

  private static final class RecordingTelemetry implements SalesforcePubSubTelemetry {

    private final AtomicInteger hits = new AtomicInteger();
    private final AtomicInteger misses = new AtomicInteger();
    private final AtomicInteger loadFailures = new AtomicInteger();
    private final AtomicInteger evictions = new AtomicInteger();

    @Override
    public void schemaCacheHit() {
      hits.incrementAndGet();
    }

    @Override
    public void schemaCacheMiss() {
      misses.incrementAndGet();
    }

    @Override
    public void schemaCacheLoadFailure() {
      loadFailures.incrementAndGet();
    }

    @Override
    public void schemaCacheEviction() {
      evictions.incrementAndGet();
    }
  }

  private static final class MutableTicker implements Ticker {

    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long read() {
      return nanos.get();
    }

    private void advance(Duration duration) {
      nanos.addAndGet(duration.toNanos());
    }
  }
}
