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
import io.github.bijujoseph.salesforce.pubsub.testing.RecordingLogger;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
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
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
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
    EMPTY,
    MULTIPLE,
    MISMATCHED_CORRELATION,
    RPC_UNAUTHENTICATED
  }

  private final class RecordingService extends PubSubGrpc.PubSubImplBase {

    private final List<String> callOrder = new ArrayList<>();
    private final List<String> topicNames = new ArrayList<>();
    private final List<String> schemaIds = new ArrayList<>();
    private final List<com.salesforce.eventbus.protobuf.PublishRequest> publishRequests =
        new ArrayList<>();
    private int schemaCalls;
    private int publishCalls;
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
      String schemaId = "/event/MissingSchema__e".equals(topic) ? "" : SCHEMA_ID;
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
      String topic = topicNames.get(topicNames.size() - 1);
      String responseSchemaId =
          "/event/MismatchedSchema__e".equals(topic) ? "different-schema" : request.getSchemaId();
      String schemaJson = "/event/MalformedSchema__e".equals(topic) ? "not-json" : SCHEMA_JSON;
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
              .setReplayId(ByteString.copyFrom(REPLAY_ID))
              .setCorrelationKey(
                  responseMode == ResponseMode.MISMATCHED_CORRELATION
                      ? "different-correlation"
                      : correlation);
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
      observer.onNext(response.build());
      observer.onCompleted();
    }
  }
}
