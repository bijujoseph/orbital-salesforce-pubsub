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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.Error;
import com.salesforce.eventbus.protobuf.ErrorCode;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.PublishResult;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;
import io.github.bijujoseph.salesforce.pubsub.error.AuthorizationException;
import io.github.bijujoseph.salesforce.pubsub.error.EventEncodeException;
import io.github.bijujoseph.salesforce.pubsub.error.PublishException;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.error.TopicNotFoundException;
import io.github.bijujoseph.salesforce.pubsub.publish.PublishReceipt;
import io.github.bijujoseph.salesforce.pubsub.publish.PublishRequest;
import io.github.bijujoseph.salesforce.pubsub.publish.SalesforcePublisher;
import io.github.bijujoseph.salesforce.pubsub.telemetry.MetricLabels;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubMetric;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.testing.RecordingLogger;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SalesforcePublisherTest {

  private static final String VALID_TOPIC = " /event/Exact_Case__e ";
  private static final String SCHEMA_ID = "schema-1";
  private static final String SCHEMA_JSON =
      """
      {"type":"record","name":"PlatformEvent","fields":[
        {"name":"Name__c","type":"string"},
        {"name":"Count__c","type":"int"},
        {"name":"Optional__c","type":["null","string"],"default":null}
      ]}
      """;
  private static final byte[] REPLAY_ID = {3, 1, 4, 1, 5};
  private static final String PAYLOAD_SENTINEL = "payload-secret-sentinel";
  private static final String TOKEN_SENTINEL = "access-token-secret-sentinel";
  private static final String REMOTE_SENTINEL = "remote-secret-sentinel";

  private final RecordingLogger logger = new RecordingLogger();
  private RecordingService service;
  private Server server;
  private ManagedChannel channel;
  private PubSubApiTransport transport;
  private SalesforcePublisher publisher;

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
                TOKEN_SENTINEL,
                "https://instance-secret-sentinel.example",
                "tenant-secret-sentinel",
                "user-secret-sentinel"),
            () -> {},
            () -> {},
            null,
            logger.proxy());
    publisher = new SalesforcePublisher(transport);
  }

  @AfterEach
  void stopServer() throws Exception {
    transport.close();
    channel.awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow();
    server.awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void callerCorrelationPublishesOneExpectedAvroEventAndMapsCompleteReceipt() throws Exception {
    PublishReceipt receipt =
        publish(new PublishRequest(VALID_TOPIC, validPayload(), "caller-correlation"));

    assertEquals(List.of("GetTopic", "GetSchema", "Publish"), service.callOrder);
    assertEquals(List.of(VALID_TOPIC), service.topicNames);
    assertEquals(List.of(SCHEMA_ID), service.schemaIds);
    assertEquals(1, service.publishRequests.size());
    com.salesforce.eventbus.protobuf.PublishRequest wireRequest = service.publishRequests.get(0);
    assertEquals(VALID_TOPIC, wireRequest.getTopicName());
    assertEquals(1, wireRequest.getEventsCount());
    ProducerEvent event = wireRequest.getEvents(0);
    assertEquals("caller-correlation", event.getId());
    assertEquals(SCHEMA_ID, event.getSchemaId());
    GenericRecord decoded = decode(event.getPayload().toByteArray());
    assertEquals(PAYLOAD_SENTINEL, decoded.get("Name__c").toString());
    assertEquals(7, decoded.get("Count__c"));
    assertNull(decoded.get("Optional__c"));

    assertEquals(VALID_TOPIC, receipt.topic());
    assertEquals("caller-correlation", receipt.correlationKey());
    assertArrayEquals(REPLAY_ID, receipt.replayId());
    assertEquals("response-schema", receipt.schemaId());
    assertEquals("rpc-123", receipt.rpcId());
  }

  @Test
  void missingAndBlankCorrelationKeysGenerateDistinctUuidsUsedEndToEnd() throws Exception {
    PublishReceipt missing = publish(new PublishRequest(VALID_TOPIC, validPayload(), null));
    PublishReceipt blank = publish(new PublishRequest(VALID_TOPIC, validPayload(), "  "));

    UUID.fromString(missing.correlationKey());
    UUID.fromString(blank.correlationKey());
    assertNotEquals(missing.correlationKey(), blank.correlationKey());
    assertEquals(missing.correlationKey(), service.publishRequests.get(0).getEvents(0).getId());
    assertEquals(blank.correlationKey(), service.publishRequests.get(1).getEvents(0).getId());
    assertEquals(2, service.publishCalls);
    assertEquals(1, service.schemaCalls);
  }

  @Test
  void receiptDefensivelyCopiesOpaqueReplayBytes() {
    byte[] source = REPLAY_ID.clone();
    PublishReceipt receipt =
        new PublishReceipt(VALID_TOPIC, "correlation", source, "schema", "rpc");

    source[0] = 99;
    byte[] returned = receipt.replayId();
    returned[1] = 99;

    assertArrayEquals(REPLAY_ID, receipt.replayId());
  }

  @Test
  void unavailableOptionalResponseMetadataMapsToNullReceiptFields() throws Exception {
    service.responseMode = ResponseMode.SUCCESS_WITHOUT_METADATA;

    PublishReceipt receipt = publish(request(VALID_TOPIC));

    assertNull(receipt.schemaId());
    assertNull(receipt.rpcId());
    assertArrayEquals(REPLAY_ID, receipt.replayId());
  }

  @Test
  void nullRequestFailsBeforeAnyRpc() {
    Throwable failure = failureOf(publisher.publish(null));

    assertInstanceOf(PublishException.class, failure);
    assertTrue(service.callOrder.isEmpty());
  }

  @Test
  void topicAndCapabilityFailuresStopBeforeSchemaAndPublish() {
    Throwable unknown = failureOf(publisher.publish(request("/event/Missing__e")));
    Throwable denied = failureOf(publisher.publish(request("/event/NoPublish__e")));

    assertInstanceOf(TopicNotFoundException.class, unknown);
    AuthorizationException authorization = assertInstanceOf(AuthorizationException.class, denied);
    assertEquals("publish", authorization.operation());
    assertEquals(0, service.schemaCalls);
    assertEquals(0, service.publishCalls);
  }

  @Test
  void invalidTopicAndSchemaResponsesStopBeforePublish() {
    Throwable blankTopic = failureOf(publisher.publish(request(" ")));
    Throwable missingSchema = failureOf(publisher.publish(request("/event/MissingSchema__e")));
    Throwable mismatchedSchema =
        failureOf(publisher.publish(request("/event/MismatchedSchema__e")));
    Throwable malformedSchema = failureOf(publisher.publish(request("/event/MalformedSchema__e")));

    assertInstanceOf(TopicNotFoundException.class, blankTopic);
    assertInstanceOf(SchemaLookupException.class, missingSchema);
    assertInstanceOf(SchemaLookupException.class, mismatchedSchema);
    assertInstanceOf(SchemaLookupException.class, malformedSchema);
    assertEquals(0, service.publishCalls);
  }

  @Test
  void invalidOutgoingValuesFailEncodingBeforePublish() {
    Throwable nullPayload =
        failureOf(publisher.publish(new PublishRequest(VALID_TOPIC, null, "null-payload")));
    Throwable missingRequired =
        failureOf(
            publisher.publish(
                new PublishRequest(VALID_TOPIC, Map.of("Count__c", 7), "missing-required")));
    Throwable wrongType =
        failureOf(
            publisher.publish(
                new PublishRequest(
                    VALID_TOPIC,
                    Map.of("Name__c", PAYLOAD_SENTINEL, "Count__c", "seven"),
                    "wrong-type")));

    assertInstanceOf(EventEncodeException.class, nullPayload);
    assertInstanceOf(EventEncodeException.class, missingRequired);
    assertInstanceOf(EventEncodeException.class, wrongType);
    assertEquals(0, service.publishCalls);
    assertEquals(1, service.schemaCalls);
  }

  @Test
  void nullPayloadFailsBeforePublishEvenWhenSchemaAcceptsAnEmptyRecord() {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);

    Throwable failure =
        failureOf(
            observedPublisher.publish(
                new PublishRequest(
                    "/event/Permissive__e", null, "null-payload-correlation-sentinel")));

    assertInstanceOf(EventEncodeException.class, failure);
    assertEquals(List.of("GetTopic", "GetSchema"), service.callOrder);
    assertEquals(1, service.schemaRequestCount("schema-permissive"));
    assertEquals(0, service.publishCalls);
    assertEquals(0, telemetry.successes.get());
    assertEquals(1, telemetry.failures.size());
    assertSame(failure, telemetry.failures.get(0));
    assertEquals("EventEncodeException", telemetry.failureLabels.get(0).exceptionType());
    String diagnostics = failure + " " + telemetry.failureLabels + " " + logger.events();
    assertFalse(diagnostics.contains("null-payload-correlation-sentinel"));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
  }

  @Test
  void resultErrorAndMalformedResultShapesNeverReturnAReceipt() {
    for (ResponseMode mode :
        List.of(
            ResponseMode.RESULT_ERROR,
            ResponseMode.EMPTY,
            ResponseMode.MULTIPLE,
            ResponseMode.MISMATCHED_CORRELATION)) {
      service.responseMode = mode;
      Throwable failure = failureOf(publisher.publish(request(VALID_TOPIC)));
      assertInstanceOf(PublishException.class, failure, mode.name());
    }

    assertEquals(4, service.publishCalls);
    assertEquals(
        4,
        service.publishRequests.stream()
            .mapToInt(com.salesforce.eventbus.protobuf.PublishRequest::getEventsCount)
            .sum());
  }

  @Test
  void emptyReplayResultFailsWithoutReceiptOrSuccessTelemetry() {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);
    service.responseMode = ResponseMode.EMPTY_REPLAY;

    Throwable failure = failureOf(observedPublisher.publish(request(VALID_TOPIC)));

    assertInstanceOf(PublishException.class, failure);
    assertTrue(failure.getMessage().contains("empty replay ID"));
    assertEquals(1, service.publishCalls);
    assertEquals(0, telemetry.successes.get());
    assertEquals(1, telemetry.failures.size());
    assertSame(failure, telemetry.failures.get(0));
    assertEquals("PublishException", telemetry.failureLabels.get(0).exceptionType());
  }

  @Test
  void telemetryUsesUnderlyingDomainFailuresWithoutChangingReturnedFailures() {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);

    Throwable authorization = failureOf(observedPublisher.publish(request("/event/NoPublish__e")));
    Throwable encoding =
        failureOf(
            observedPublisher.publish(
                new PublishRequest(
                    VALID_TOPIC,
                    Map.of("Name__c", PAYLOAD_SENTINEL, "Count__c", "seven"),
                    "correlation-secret-sentinel")));
    service.responseMode = ResponseMode.RESULT_ERROR;
    Throwable publishing = failureOf(observedPublisher.publish(request(VALID_TOPIC)));

    assertInstanceOf(AuthorizationException.class, authorization);
    assertInstanceOf(EventEncodeException.class, encoding);
    assertInstanceOf(PublishException.class, publishing);
    assertEquals(List.of(authorization, encoding, publishing), telemetry.failures);
    assertEquals(
        List.of("AuthorizationException", "EventEncodeException", "PublishException"),
        telemetry.failureLabels.stream().map(MetricLabels::exceptionType).toList());
    assertEquals(
        List.of("/event/NoPublish__e", VALID_TOPIC, VALID_TOPIC),
        telemetry.failureLabels.stream().map(MetricLabels::topic).toList());
    assertEquals(0, telemetry.successes.get());
    String diagnostics = telemetry.failures + " " + telemetry.failureLabels;
    assertFalse(diagnostics.contains(PAYLOAD_SENTINEL));
    assertFalse(diagnostics.contains(REMOTE_SENTINEL));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
    assertFalse(diagnostics.contains("correlation-secret-sentinel"));
    assertFalse(diagnostics.contains("CompletionException"));
  }

  @Test
  void cancellationBeforePublishRemainsCallerVisibleAndStopsTheFlow() throws Exception {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);
    CompletableFuture<PublishReceipt> publication =
        observedPublisher.publish(request("/event/Held__e")).toCompletableFuture();
    assertFalse(publication.isDone());
    assertTrue(service.awaitFirstHeldSchemaRequest());

    assertTrue(publication.cancel(true));

    assertTrue(publication.isCancelled());
    assertThrows(CancellationException.class, publication::join);
    assertEquals(0, telemetry.successes.get());
    assertEquals(1, telemetry.failures.size());
    assertInstanceOf(CancellationException.class, telemetry.failures.get(0));
    assertEquals("CancellationException", telemetry.failureLabels.get(0).exceptionType());
    assertEquals(0, service.publishCalls);
  }

  @Test
  void cancellationAfterPublishReceiptCancelsServerAndLateResponseCannotWin() throws Exception {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);
    service.responseMode = ResponseMode.HOLD_PUBLISH;
    CompletableFuture<PublishReceipt> publication =
        observedPublisher.publish(request(VALID_TOPIC)).toCompletableFuture();
    assertTrue(service.awaitPublishReceived());
    assertFalse(publication.isDone());
    assertEquals(1, service.publishCalls);

    assertTrue(publication.cancel(true));

    assertTrue(service.awaitPublishCancellation());
    assertTrue(publication.isCancelled());
    assertThrows(CancellationException.class, publication::join);
    service.releaseHeldPublish();
    assertTrue(publication.isCancelled());
    assertEquals(0, telemetry.successes.get());
    assertEquals(1, telemetry.failures.size());
    assertInstanceOf(CancellationException.class, telemetry.failures.get(0));
    assertEquals("CancellationException", telemetry.failureLabels.get(0).exceptionType());
    String diagnostics = telemetry.failures + " " + telemetry.failureLabels + " " + logger.events();
    assertFalse(diagnostics.contains(PAYLOAD_SENTINEL));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
  }

  @Test
  void completedPublishResponseWinsAndCannotBeCancelledAfterward() throws Exception {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);
    service.responseMode = ResponseMode.HOLD_PUBLISH;
    CompletableFuture<PublishReceipt> publication =
        observedPublisher.publish(request(VALID_TOPIC)).toCompletableFuture();
    assertTrue(service.awaitPublishReceived());

    service.releaseHeldPublish();
    PublishReceipt receipt = publication.get(5, TimeUnit.SECONDS);

    assertEquals("caller-correlation", receipt.correlationKey());
    assertFalse(publication.cancel(true));
    assertFalse(publication.isCancelled());
    assertEquals(1L, service.publishCancellationCount());
    assertEquals(1, telemetry.successes.get());
    assertTrue(telemetry.failures.isEmpty());
  }

  @Test
  void mappedTransportPublishStageRejectsExternalMutationAndForwardsCancellation()
      throws Exception {
    service.responseMode = ResponseMode.HOLD_PUBLISH;
    CompletableFuture<PublishReceipt> publication =
        transport
            .publishSingleEvent(VALID_TOPIC, "caller-correlation", SCHEMA_ID, new byte[] {1, 2})
            .toCompletableFuture();
    assertTrue(service.awaitPublishReceived());
    PublishReceipt forged =
        new PublishReceipt("forged", "forged", new byte[] {9}, "forged", "forged");

    assertFalse(publication.complete(forged));
    assertFalse(publication.completeExceptionally(new IllegalStateException("forged")));
    assertThrows(UnsupportedOperationException.class, () -> publication.obtrudeValue(forged));
    assertThrows(
        UnsupportedOperationException.class,
        () -> publication.obtrudeException(new IllegalStateException("forged")));
    assertThrows(
        UnsupportedOperationException.class, () -> publication.completeAsync(() -> forged));
    assertThrows(
        UnsupportedOperationException.class,
        () -> publication.completeAsync(() -> forged, Runnable::run));
    assertThrows(
        UnsupportedOperationException.class, () -> publication.orTimeout(1, TimeUnit.SECONDS));
    assertThrows(
        UnsupportedOperationException.class,
        () -> publication.completeOnTimeout(forged, 1, TimeUnit.SECONDS));
    assertFalse(publication.isDone());
    assertEquals(1L, service.publishCancellationCount());

    assertTrue(publication.cancel(true));

    assertTrue(service.awaitPublishCancellation());
    assertTrue(publication.isCancelled());
    assertTrue(publication.isDone());
    assertTrue(publication.isCompletedExceptionally());
    assertEquals(Future.State.CANCELLED, publication.state());
    assertThrows(CancellationException.class, publication::join);
    assertThrows(CancellationException.class, publication::get);
    assertThrows(CancellationException.class, () -> publication.get(1, TimeUnit.SECONDS));
    assertThrows(CancellationException.class, () -> publication.getNow(forged));
    service.releaseHeldPublish();
    assertTrue(publication.isCancelled());
    String diagnostics = logger.events().toString();
    assertFalse(diagnostics.contains(PAYLOAD_SENTINEL));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
    assertFalse(diagnostics.contains("caller-correlation"));
  }

  @Test
  void resolvedSchemaSurvivesDeterministicBoundedCacheEvictionBeforeEncoding() throws Exception {
    FailureTelemetry telemetry = new FailureTelemetry();
    SalesforcePublisher observedPublisher = new SalesforcePublisher(transport, telemetry);
    CompletableFuture<PublishReceipt> held =
        observedPublisher.publish(request("/event/Held__e")).toCompletableFuture();
    assertFalse(held.isDone());
    assertTrue(service.awaitFirstHeldSchemaRequest());

    int fillerCount = 150;
    for (int index = 0; index < fillerCount; index++) {
      PublishReceipt filler =
          observedPublisher
              .publish(request("/event/Eviction_" + index + "__e"))
              .toCompletableFuture()
              .get(5, TimeUnit.SECONDS);
      assertEquals("caller-correlation", filler.correlationKey());
    }
    assertTrue(telemetry.schemaEvictions.get() > 0);
    assertFalse(held.isDone());
    assertEquals(fillerCount, service.publishCalls);

    CompletableFuture<PublishReceipt> secondHeld =
        observedPublisher
            .publish(
                new PublishRequest("/event/Held__e", validPayload(), "second-held-correlation"))
            .toCompletableFuture();
    assertTrue(service.awaitTwoHeldSchemaRequests());
    assertEquals(2, service.schemaRequestCount("schema-held"));
    assertFalse(secondHeld.isDone());

    service.completeHeldSchemas();
    PublishReceipt heldReceipt = held.get(5, TimeUnit.SECONDS);
    PublishReceipt secondHeldReceipt = secondHeld.get(5, TimeUnit.SECONDS);

    assertEquals("/event/Held__e", heldReceipt.topic());
    assertEquals("second-held-correlation", secondHeldReceipt.correlationKey());
    assertEquals(fillerCount + 2, service.publishCalls);
    assertEquals(2, service.schemaRequestCount("schema-held"));
    com.salesforce.eventbus.protobuf.PublishRequest heldWireRequest =
        service.publishRequests.get(service.publishRequests.size() - 1);
    assertEquals("schema-held", heldWireRequest.getEvents(0).getSchemaId());
    GenericRecord decoded = decode(heldWireRequest.getEvents(0).getPayload().toByteArray());
    assertEquals(PAYLOAD_SENTINEL, decoded.get("Name__c").toString());
  }

  @Test
  void rpcAuthenticationFailureRetainsCategoryAndRedactsRemoteAndLocalSecrets() {
    service.responseMode = ResponseMode.RPC_UNAUTHENTICATED;

    Throwable failure = failureOf(publisher.publish(request(VALID_TOPIC)));

    assertInstanceOf(AuthenticationException.class, failure);
    String diagnostics = failure + " " + logger.events();
    assertFalse(diagnostics.contains(PAYLOAD_SENTINEL));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
    assertFalse(diagnostics.contains(REMOTE_SENTINEL));
    assertFalse(diagnostics.contains("instance-secret-sentinel"));
    assertFalse(diagnostics.contains("tenant-secret-sentinel"));
    assertEquals(1, service.publishCalls);
  }

  @Test
  void resultErrorRedactsRemoteMessageAndPayload() {
    service.responseMode = ResponseMode.RESULT_ERROR;

    Throwable failure = failureOf(publisher.publish(request(VALID_TOPIC)));

    assertInstanceOf(PublishException.class, failure);
    String diagnostics = failure + " " + logger.events();
    assertFalse(diagnostics.contains(PAYLOAD_SENTINEL));
    assertFalse(diagnostics.contains(REMOTE_SENTINEL));
    assertFalse(diagnostics.contains(TOKEN_SENTINEL));
    assertTrue(failure.getMessage().contains("PUBLISH"));
  }

  @Test
  void stablePublicPublishApiIsNeutralUnaryAndHasNoBatchOrStreamSurface() {
    assertRecordComponents(
        PublishRequest.class,
        List.of("topic", "payload", "correlationKey"),
        List.of(String.class, Map.class, String.class));
    assertRecordComponents(
        PublishReceipt.class,
        List.of("topic", "correlationKey", "replayId", "schemaId", "rpcId"),
        List.of(String.class, String.class, byte[].class, String.class, String.class));

    Method publish =
        Arrays.stream(SalesforcePublisher.class.getMethods())
            .filter(method -> method.getName().equals("publish"))
            .findFirst()
            .orElseThrow();
    assertArrayEquals(new Class<?>[] {PublishRequest.class}, publish.getParameterTypes());
    assertEquals(CompletionStage.class, publish.getReturnType());
    ParameterizedType returnType =
        assertInstanceOf(ParameterizedType.class, publish.getGenericReturnType());
    assertEquals(PublishReceipt.class, returnType.getActualTypeArguments()[0]);

    for (Class<?> apiType :
        List.of(
            PublishRequest.class,
            PublishReceipt.class,
            SalesforcePublisher.class,
            PubSubApiTransport.class)) {
      for (Method method : apiType.getMethods()) {
        if (!Modifier.isPublic(method.getModifiers())
            || method.getDeclaringClass() == Object.class) {
          continue;
        }
        assertFalse(method.getName().equals("publishStream"), method.toString());
        assertFalse(method.getName().toLowerCase().contains("batch"), method.toString());
        assertFalse(Collection.class.isAssignableFrom(method.getReturnType()), method.toString());
        assertNeutral(method.getGenericReturnType(), method);
        for (Type parameter : method.getGenericParameterTypes()) {
          assertNeutral(parameter, method);
          if (parameter instanceof Class<?> parameterClass) {
            assertFalse(Collection.class.isAssignableFrom(parameterClass), method.toString());
          }
        }
      }
    }
  }

  private PublishReceipt publish(PublishRequest request) throws Exception {
    return publisher.publish(request).toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private static PublishRequest request(String topic) {
    return new PublishRequest(topic, validPayload(), "caller-correlation");
  }

  private static Map<String, Object> validPayload() {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("Name__c", PAYLOAD_SENTINEL);
    payload.put("Count__c", 7);
    payload.put("Optional__c", null);
    return payload;
  }

  private static Throwable failureOf(CompletionStage<?> stage) {
    return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join())
        .getCause();
  }

  private static void assertRecordComponents(
      Class<?> type, List<String> names, List<Class<?>> types) {
    assertTrue(type.isRecord());
    assertEquals(
        names,
        Arrays.stream(type.getRecordComponents()).map(component -> component.getName()).toList());
    assertEquals(
        types,
        Arrays.stream(type.getRecordComponents()).map(component -> component.getType()).toList());
  }

  private static void assertNeutral(Type type, Method method) {
    assertFalse(type.getTypeName().contains("com.salesforce.eventbus.protobuf"), method.toString());
  }

  private static GenericRecord decode(byte[] payload) throws Exception {
    Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
    return new GenericDatumReader<GenericRecord>(schema)
        .read(null, DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(payload), null));
  }

  private enum ResponseMode {
    SUCCESS,
    SUCCESS_WITHOUT_METADATA,
    RESULT_ERROR,
    EMPTY_REPLAY,
    EMPTY,
    MULTIPLE,
    MISMATCHED_CORRELATION,
    RPC_UNAUTHENTICATED,
    HOLD_PUBLISH
  }

  private static final class FailureTelemetry implements SalesforcePubSubTelemetry {

    private final List<Throwable> failures = new ArrayList<>();
    private final List<MetricLabels> failureLabels = new ArrayList<>();
    private final AtomicInteger successes = new AtomicInteger();
    private final AtomicInteger schemaEvictions = new AtomicInteger();

    @Override
    public void published(String topic, String correlationKey) {
      successes.incrementAndGet();
    }

    @Override
    public void publishFailure(String topic, Throwable cause) {
      failures.add(cause);
      SalesforcePubSubTelemetry.super.publishFailure(topic, cause);
    }

    @Override
    public void metric(SalesforcePubSubMetric metric, double value, MetricLabels labels) {
      if (metric == SalesforcePubSubMetric.PUBLISH_FAILURE_TOTAL) {
        failureLabels.add(labels);
      }
    }

    @Override
    public void schemaCacheEviction() {
      schemaEvictions.incrementAndGet();
    }
  }

  private final class RecordingService extends PubSubGrpc.PubSubImplBase {

    private final List<String> callOrder = new ArrayList<>();
    private final List<String> topicNames = new ArrayList<>();
    private final List<String> schemaIds = new ArrayList<>();
    private final List<com.salesforce.eventbus.protobuf.PublishRequest> publishRequests =
        new ArrayList<>();
    private final Map<String, Integer> schemaRequestCounts = new LinkedHashMap<>();
    private final CountDownLatch firstHeldSchemaRequest = new CountDownLatch(1);
    private final CountDownLatch twoHeldSchemaRequests = new CountDownLatch(2);
    private final CountDownLatch publishReceived = new CountDownLatch(1);
    private final CountDownLatch publishCancelled = new CountDownLatch(1);
    private int schemaCalls;
    private int publishCalls;
    private final List<StreamObserver<SchemaInfo>> heldSchemaObservers = new ArrayList<>();
    private StreamObserver<PublishResponse> heldPublishObserver;
    private PublishResponse heldPublishResponse;
    private ResponseMode responseMode = ResponseMode.SUCCESS;

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      callOrder.add("GetTopic");
      String topic = request.getTopicName();
      topicNames.add(topic);
      if (topic.isBlank() || "/event/Missing__e".equals(topic)) {
        observer.onError(Status.NOT_FOUND.asRuntimeException());
        return;
      }
      boolean canPublish = !"/event/NoPublish__e".equals(topic);
      String schemaId = schemaIdFor(topic);
      observer.onNext(
          TopicInfo.newBuilder()
              .setTopicName(topic)
              .setCanPublish(canPublish)
              .setCanSubscribe(true)
              .setSchemaId(schemaId)
              .build());
      observer.onCompleted();
    }

    @Override
    public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> observer) {
      callOrder.add("GetSchema");
      schemaCalls++;
      schemaIds.add(request.getSchemaId());
      schemaRequestCounts.merge(request.getSchemaId(), 1, Integer::sum);
      if ("schema-held".equals(request.getSchemaId())) {
        heldSchemaObservers.add(observer);
        firstHeldSchemaRequest.countDown();
        twoHeldSchemaRequests.countDown();
        return;
      }
      String topic = topicNames.get(topicNames.size() - 1);
      String responseSchemaId =
          "/event/MismatchedSchema__e".equals(topic) ? "different-schema" : request.getSchemaId();
      String schemaJson = "/event/MalformedSchema__e".equals(topic) ? "not-json" : SCHEMA_JSON;
      if ("schema-permissive".equals(request.getSchemaId())) {
        schemaJson =
            """
            {"type":"record","name":"PermissiveEvent","fields":[
              {"name":"Optional__c","type":["null","string"],"default":null}
            ]}
            """;
      }
      observer.onNext(
          SchemaInfo.newBuilder().setSchemaId(responseSchemaId).setSchemaJson(schemaJson).build());
      observer.onCompleted();
    }

    @Override
    public void publish(
        com.salesforce.eventbus.protobuf.PublishRequest request,
        StreamObserver<PublishResponse> observer) {
      callOrder.add("Publish");
      publishCalls++;
      publishRequests.add(request);
      if (responseMode == ResponseMode.RPC_UNAUTHENTICATED) {
        observer.onError(
            Status.UNAUTHENTICATED
                .withDescription(REMOTE_SENTINEL + " " + TOKEN_SENTINEL)
                .asRuntimeException());
        return;
      }
      PublishResponse.Builder response = PublishResponse.newBuilder();
      if (responseMode != ResponseMode.SUCCESS_WITHOUT_METADATA) {
        response.setSchemaId("response-schema").setRpcId("rpc-123");
      }
      if (responseMode == ResponseMode.EMPTY) {
        observer.onNext(response.build());
        observer.onCompleted();
        return;
      }
      String correlation = request.getEvents(0).getId();
      PublishResult.Builder result =
          PublishResult.newBuilder()
              .setCorrelationKey(
                  responseMode == ResponseMode.MISMATCHED_CORRELATION
                      ? "different-correlation"
                      : correlation);
      if (responseMode != ResponseMode.EMPTY_REPLAY) {
        result.setReplayId(ByteString.copyFrom(REPLAY_ID));
      }
      if (responseMode == ResponseMode.RESULT_ERROR) {
        result.setError(
            Error.newBuilder()
                .setCode(ErrorCode.PUBLISH)
                .setMsg(REMOTE_SENTINEL + " " + PAYLOAD_SENTINEL));
      }
      response.addResults(result);
      if (responseMode == ResponseMode.MULTIPLE) {
        response.addResults(result);
      }
      if (responseMode == ResponseMode.HOLD_PUBLISH) {
        ServerCallStreamObserver<PublishResponse> serverObserver =
            (ServerCallStreamObserver<PublishResponse>) observer;
        serverObserver.setOnCancelHandler(publishCancelled::countDown);
        heldPublishObserver = observer;
        heldPublishResponse = response.build();
        publishReceived.countDown();
        return;
      }
      observer.onNext(response.build());
      observer.onCompleted();
    }

    private boolean awaitPublishReceived() throws InterruptedException {
      return publishReceived.await(5, TimeUnit.SECONDS);
    }

    private boolean awaitPublishCancellation() throws InterruptedException {
      return publishCancelled.await(5, TimeUnit.SECONDS);
    }

    private long publishCancellationCount() {
      return publishCancelled.getCount();
    }

    private void releaseHeldPublish() {
      StreamObserver<PublishResponse> observer = heldPublishObserver;
      heldPublishObserver = null;
      try {
        observer.onNext(heldPublishResponse);
        observer.onCompleted();
      } catch (RuntimeException ignored) {
        // The in-process server may reject a deliberately late response after cancellation.
      }
    }

    private boolean awaitFirstHeldSchemaRequest() throws InterruptedException {
      return firstHeldSchemaRequest.await(5, TimeUnit.SECONDS);
    }

    private boolean awaitTwoHeldSchemaRequests() throws InterruptedException {
      return twoHeldSchemaRequests.await(5, TimeUnit.SECONDS);
    }

    private void completeHeldSchemas() {
      List<StreamObserver<SchemaInfo>> observers = List.copyOf(heldSchemaObservers);
      heldSchemaObservers.clear();
      for (StreamObserver<SchemaInfo> observer : observers) {
        observer.onNext(
            SchemaInfo.newBuilder().setSchemaId("schema-held").setSchemaJson(SCHEMA_JSON).build());
        observer.onCompleted();
      }
    }

    private int schemaRequestCount(String schemaId) {
      return schemaRequestCounts.getOrDefault(schemaId, 0);
    }

    private String schemaIdFor(String topic) {
      if ("/event/MissingSchema__e".equals(topic)) {
        return "";
      }
      if ("/event/Held__e".equals(topic)) {
        return "schema-held";
      }
      if ("/event/Permissive__e".equals(topic)) {
        return "schema-permissive";
      }
      if (topic.startsWith("/event/Eviction_")) {
        return "schema-eviction-"
            + topic.substring("/event/Eviction_".length(), topic.length() - 3);
      }
      return SCHEMA_ID;
    }
  }
}
