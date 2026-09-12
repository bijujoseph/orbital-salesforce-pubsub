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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.AuthorizationException;
import io.github.bijujoseph.salesforce.pubsub.error.TopicNotFoundException;
import io.github.bijujoseph.salesforce.pubsub.error.TransportException;
import io.github.bijujoseph.salesforce.pubsub.schema.ResolvedTopic;
import io.github.bijujoseph.salesforce.pubsub.schema.SalesforceSchemaCache;
import io.github.bijujoseph.salesforce.pubsub.schema.TopicResolver;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TopicSchemaIntegrationTest {

  private static final String RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[]}";

  private RecordingService service;
  private Server server;
  private ManagedChannel channel;
  private PubSubApiTransport transport;

  @BeforeEach
  void startServer() throws Exception {
    String serverName = InProcessServerBuilder.generateName();
    service = new RecordingService();
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    transport =
        new PubSubApiTransport(
            channel,
            new SalesforceSession(
                "access-token", "https://example.my.salesforce.com", "tenant", "user"));
  }

  @AfterEach
  void stopServer() throws Exception {
    transport.close();
    channel.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void resolvesExactTopicStringsAndAppliesOperationSpecificPolicyBeforeDownstreamUse() {
    TopicResolver resolver = new TopicResolver(transport);
    String validSentinel = " /event/Exact_Case__e?do-not-normalize ";

    ResolvedTopic subscribe =
        resolver.resolveForSubscribe(validSentinel).toCompletableFuture().join();
    ResolvedTopic publish = resolver.resolveForPublish(validSentinel).toCompletableFuture().join();
    Throwable missing = failureOf(resolver.resolveForSubscribe("Manual_Message__e"));
    AuthorizationException cannotSubscribe =
        assertInstanceOf(
            AuthorizationException.class,
            failureOf(resolver.resolveForSubscribe("/event/NoSubscribe__e")));
    AuthorizationException cannotPublish =
        assertInstanceOf(
            AuthorizationException.class,
            failureOf(resolver.resolveForPublish("/event/NoPublish__e")));
    Throwable denied = failureOf(resolver.resolveForSubscribe("/event/PermissionDenied__e"));

    assertEquals(validSentinel, subscribe.topicName());
    assertEquals(validSentinel, publish.topicName());
    assertInstanceOf(TopicNotFoundException.class, missing);
    assertEquals("subscribe", cannotSubscribe.operation());
    assertEquals("publish", cannotPublish.operation());
    assertInstanceOf(AuthorizationException.class, denied);
    assertEquals(
        List.of(
            validSentinel,
            validSentinel,
            "Manual_Message__e",
            "/event/NoSubscribe__e",
            "/event/NoPublish__e",
            "/event/PermissionDenied__e"),
        service.topicRequests);
  }

  @Test
  void resolvesAndCachesSchemasWhilePreservingTypedInProcessFailures() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    SalesforceSchemaCache cache = new SalesforceSchemaCache(transport, telemetry);

    cache.resolve("schema-1").toCompletableFuture().join();
    cache.resolve("schema-1").toCompletableFuture().join();
    cache.resolve("schema-2").toCompletableFuture().join();
    assertInstanceOf(AuthorizationException.class, failureOf(cache.resolve("schema-denied")));
    assertInstanceOf(TransportException.class, failureOf(cache.resolve("schema-timeout")));
    assertInstanceOf(TransportException.class, failureOf(cache.resolve("schema-retry")));
    cache.resolve("schema-retry").toCompletableFuture().join();

    assertEquals(1, service.schemaCalls("schema-1"));
    assertEquals(1, service.schemaCalls("schema-2"));
    assertEquals(1, service.schemaCalls("schema-denied"));
    assertEquals(1, service.schemaCalls("schema-timeout"));
    assertEquals(2, service.schemaCalls("schema-retry"));
    assertEquals(1, telemetry.hits.get());
    assertEquals(6, telemetry.misses.get());
    assertEquals(3, telemetry.loadFailures.get());
  }

  private static Throwable failureOf(CompletionStage<?> stage) {
    return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join())
        .getCause();
  }

  private static final class RecordingTelemetry implements SalesforcePubSubTelemetry {

    private final AtomicInteger hits = new AtomicInteger();
    private final AtomicInteger misses = new AtomicInteger();
    private final AtomicInteger loadFailures = new AtomicInteger();

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
  }

  private static final class RecordingService extends PubSubGrpc.PubSubImplBase {

    private final List<String> topicRequests = new ArrayList<>();
    private final Map<String, AtomicInteger> schemaRequests = new ConcurrentHashMap<>();

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      String topic = request.getTopicName();
      topicRequests.add(topic);
      if ("Manual_Message__e".equals(topic)) {
        observer.onError(Status.NOT_FOUND.asRuntimeException());
        return;
      }
      if ("/event/PermissionDenied__e".equals(topic)) {
        observer.onError(Status.PERMISSION_DENIED.asRuntimeException());
        return;
      }
      observer.onNext(
          TopicInfo.newBuilder()
              .setTopicName(topic)
              .setCanSubscribe(!"/event/NoSubscribe__e".equals(topic))
              .setCanPublish(!"/event/NoPublish__e".equals(topic))
              .setSchemaId("schema-1")
              .build());
      observer.onCompleted();
    }

    @Override
    public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> observer) {
      String schemaId = request.getSchemaId();
      int attempt =
          schemaRequests
              .computeIfAbsent(schemaId, ignored -> new AtomicInteger())
              .incrementAndGet();
      if ("schema-denied".equals(schemaId)) {
        observer.onError(Status.PERMISSION_DENIED.asRuntimeException());
        return;
      }
      if ("schema-timeout".equals(schemaId)) {
        observer.onError(Status.DEADLINE_EXCEEDED.asRuntimeException());
        return;
      }
      if ("schema-retry".equals(schemaId) && attempt == 1) {
        observer.onError(Status.INTERNAL.asRuntimeException());
        return;
      }
      observer.onNext(
          SchemaInfo.newBuilder().setSchemaId(schemaId).setSchemaJson(RECORD_SCHEMA).build());
      observer.onCompleted();
    }

    private int schemaCalls(String schemaId) {
      return schemaRequests.get(schemaId).get();
    }
  }
}
