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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.PublishRequest;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.config.EndpointConfig;
import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;
import io.github.bijujoseph.salesforce.pubsub.error.AuthorizationException;
import io.github.bijujoseph.salesforce.pubsub.error.PublishException;
import io.github.bijujoseph.salesforce.pubsub.error.SalesforcePubSubException;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.error.SubscriptionException;
import io.github.bijujoseph.salesforce.pubsub.error.TopicNotFoundException;
import io.github.bijujoseph.salesforce.pubsub.error.TransportException;
import io.github.bijujoseph.salesforce.pubsub.telemetry.ConnectorHealth;
import io.github.bijujoseph.salesforce.pubsub.telemetry.ConnectorStatus;
import io.github.bijujoseph.salesforce.pubsub.telemetry.SalesforcePubSubTelemetry;
import io.github.bijujoseph.salesforce.pubsub.testing.RecordingLogger;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.SecurityLevel;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class PubSubApiTransportTest {

  private static final Metadata.Key<String> ACCESS_TOKEN =
      Metadata.Key.of("accesstoken", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> INSTANCE_URL =
      Metadata.Key.of("instanceurl", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TENANT_ID =
      Metadata.Key.of("tenantid", Metadata.ASCII_STRING_MARSHALLER);
  private static final Context.Key<ObservedCall> OBSERVED_CALL = Context.key("observed-call");
  private static final MethodDescriptor<StringValue, StringValue> PLAIN_METHOD =
      MethodDescriptor.<StringValue, StringValue>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test.Plain", "Echo"))
          .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
          .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
          .build();

  private final ConcurrentLinkedQueue<ObservedCall> received = new ConcurrentLinkedQueue<>();
  private final AtomicInteger receivedCalls = new AtomicInteger();
  private Server server;
  private ManagedChannel channel;
  private PubSubApiTransport transport;

  @BeforeEach
  void startServer() throws Exception {
    startServer(new RespondingService());
    transport = new PubSubApiTransport(channel, session(0));
  }

  @AfterEach
  void stopServer() throws Exception {
    closeCurrentResources();
  }

  @Test
  void allAllowedRpcShapesReceiveTheExactRequiredMetadata() throws Exception {
    TopicMetadata topic =
        transport.getTopic("/event/Test__e").toCompletableFuture().get(5, TimeUnit.SECONDS);
    SchemaMetadata schema =
        transport.getSchema("schema-1").toCompletableFuture().get(5, TimeUnit.SECONDS);
    PublishResponse publish =
        transport.publish(singlePublish()).toCompletableFuture().get(5, TimeUnit.SECONDS);
    CompletableFuture<FetchResponse> subscriptionResponse = new CompletableFuture<>();
    PubSubApiTransport.SubscriptionRpc subscription =
        transport.subscribe(singleResponseObserver(subscriptionResponse));
    subscription.send(FetchRequest.newBuilder().setTopicName("/event/Test__e").build());
    assertEquals("subscribe", subscriptionResponse.get(5, TimeUnit.SECONDS).getRpcId());
    subscription.complete();
    subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals("/event/Test__e", topic.topicName());
    assertEquals("schema-1", schema.schemaId());
    assertEquals("publish", publish.getRpcId());
    assertEquals(4, receivedCalls.get());
    assertObserved("GetTopic", received.remove(), 0);
    assertObserved("GetSchema", received.remove(), 0);
    assertObserved("Publish", received.remove(), 0);
    assertObserved("Subscribe", received.remove(), 0);
  }

  @Test
  void metadataIsScopedAwayFromAnUnrelatedCallOnTheSameChannel() throws Exception {
    transport.getTopic("salesforce").toCompletableFuture().get(5, TimeUnit.SECONDS);
    StringValue plainResponse = plainCall("plain").get(5, TimeUnit.SECONDS);

    assertEquals("plain", plainResponse.getValue());
    assertObserved("GetTopic", received.remove(), 0);
    ObservedCall plain = received.remove();
    assertTrue(plain.method().endsWith("/Echo"));
    assertNull(plain.headers().accessToken());
    assertNull(plain.headers().instanceUrl());
    assertNull(plain.headers().tenantId());
  }

  @Test
  void rpcStartedAfterRefreshUsesTheReplacementSession() throws Exception {
    transport.getTopic("before").toCompletableFuture().get(5, TimeUnit.SECONDS);
    transport.updateSession(session(1));
    transport.getTopic("after").toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertObserved("GetTopic", received.remove(), 0);
    assertObserved("GetTopic", received.remove(), 1);
  }

  @Test
  void allAllowedRpcShapesReservedBeforeRefreshUseTheSessionCapturedAtStart() throws Exception {
    RpcStartBarrier barrier = new RpcStartBarrier();
    restartServer(new RespondingService(), barrier, () -> {});

    assertRefreshOrderingForAllAllowedRpcShapes(barrier, 1);
  }

  @Test
  void allAllowedRpcShapesStartedBeforeRefreshKeepTheirCapturedSession() throws Exception {
    RpcStartBarrier barrier = new RpcStartBarrier();
    restartServer(new RespondingService(), () -> {}, barrier);

    assertRefreshOrderingForAllAllowedRpcShapes(barrier, 0);
  }

  @RepeatedTest(10)
  void concurrentRefreshAndCallsNeverMixSessionValues() throws Exception {
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<?>> work = new ArrayList<>();
    try {
      for (int generation = 1; generation <= 100; generation++) {
        int currentGeneration = generation;
        work.add(executor.submit(() -> transport.updateSession(session(currentGeneration))));
        work.add(
            executor.submit(
                () -> {
                  try {
                    transport.getTopic("concurrent").toCompletableFuture().get(5, TimeUnit.SECONDS);
                  } catch (Exception exception) {
                    throw new AssertionError("RPC did not complete", exception);
                  }
                }));
      }
      for (Future<?> result : work) {
        result.get(10, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    assertEquals(100, received.size());
    for (ObservedCall call : received) {
      SessionHeaders headers = call.headers();
      String generation = headers.accessToken().substring("token-".length());
      assertEquals("https://instance-" + generation + ".example", headers.instanceUrl());
      assertEquals("tenant-" + generation, headers.tenantId());
    }
  }

  @Test
  void missingTenantIsRejectedBeforeAnyRpcStarts() {
    SalesforceSession incomplete =
        new SalesforceSession("secret-token", "https://secret.example", null, null);

    AuthenticationException failure =
        assertThrows(
            AuthenticationException.class, () -> new PubSubApiTransport(channel, incomplete));

    assertEquals("Missing tenant ID for Salesforce Pub/Sub RPC", failure.getMessage());
    assertFalse(failure.getMessage().contains("secret-token"));
    assertEquals(0, receivedCalls.get());
  }

  @Test
  void rejectedRefreshDoesNotReplaceTheUsableSession() throws Exception {
    SalesforceSession incomplete =
        new SalesforceSession("replacement-secret", "https://replacement.example", null, null);

    assertThrows(AuthenticationException.class, () -> transport.updateSession(incomplete));
    transport.getTopic("still-current").toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertObserved("GetTopic", received.remove(), 0);
  }

  @Test
  void metadataValuesAreAbsentFromDiagnostics() {
    SalesforceSession sensitive =
        new SalesforceSession(
            "highly-secret-token", "https://customer-secret.example", "secret-tenant", null);
    SalesforceCallCredentials credentials = new SalesforceCallCredentials(sensitive);
    SessionMetadata metadata = SessionMetadata.from(sensitive);
    PubSubApiTransport sensitiveTransport = new PubSubApiTransport(channel, sensitive);
    try {
      String diagnostics = credentials + " " + metadata + " " + sensitiveTransport;
      assertFalse(diagnostics.contains("highly-secret-token"));
      assertFalse(diagnostics.contains("customer-secret"));
      assertFalse(diagnostics.contains("secret-tenant"));
    } finally {
      sensitiveTransport.close();
    }
  }

  @Test
  void channelHostNormalizesOnlyTheEndpointIpv6BracketPair() {
    assertEquals(
        "2001:db8::1",
        PubSubApiTransport.channelHost(new EndpointConfig("[2001:db8::1]", 7443).host()));
    assertEquals(
        "fe80::1%eth0",
        PubSubApiTransport.channelHost(new EndpointConfig("[fe80::1%eth0]", 7443).host()));
    assertEquals(
        "2001:db8::1",
        PubSubApiTransport.channelHost(new EndpointConfig("2001:db8::1", 7443).host()));
    assertEquals(
        "api.pubsub.salesforce.com",
        PubSubApiTransport.channelHost(
            new EndpointConfig("api.pubsub.salesforce.com", 7443).host()));
  }

  @Test
  void invalidMetadataIsRejectedWithSanitizedTypedFailure() {
    SalesforceSession invalid =
        new SalesforceSession("secret\nheader", "https://instance.example", "tenant", null);

    AuthenticationException failure =
        assertThrows(AuthenticationException.class, () -> new SalesforceCallCredentials(invalid));

    assertEquals("Salesforce session contains invalid RPC metadata", failure.getMessage());
    assertFalse(failure.getMessage().contains("secret"));
  }

  @Test
  void executorRejectionFailsMetadataApplicationWithoutLeakingValues() {
    SalesforceCallCredentials credentials = new SalesforceCallCredentials(session(0));
    AtomicReference<Metadata> applied = new AtomicReference<>();
    AtomicReference<Status> failed = new AtomicReference<>();

    credentials.applyRequestMetadata(
        requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY),
        command -> {
          throw new RejectedExecutionException("token-0");
        },
        new CallCredentials.MetadataApplier() {
          @Override
          public void apply(Metadata headers) {
            applied.set(headers);
          }

          @Override
          public void fail(Status status) {
            failed.set(status);
          }
        });

    assertNull(applied.get());
    assertNotNull(failed.get());
    assertEquals(Status.Code.UNAUTHENTICATED, failed.get().getCode());
    assertFalse(failed.get().toString().contains("token-0"));
  }

  @Test
  void credentialsRejectTransportsWithoutPrivacyBeforeApplyingMetadata() {
    assertInsecureCredentialsRejected(SecurityLevel.NONE);
    assertInsecureCredentialsRejected(SecurityLevel.INTEGRITY);
  }

  @Test
  void credentialsRejectMissingOrThrowingSecurityInformationWithoutApplyingMetadata() {
    assertCredentialsRejected(null);
    assertCredentialsRejected(
        new CallCredentials.RequestInfo() {
          @Override
          public MethodDescriptor<?, ?> getMethodDescriptor() {
            return PLAIN_METHOD;
          }

          @Override
          public SecurityLevel getSecurityLevel() {
            throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
          }

          @Override
          public String getAuthority() {
            return "server-sentinel-authority";
          }

          @Override
          public Attributes getTransportAttrs() {
            return Attributes.EMPTY;
          }
        });
  }

  @Test
  void credentialsRejectUnrelatedMissingAndThrowingMethodDescriptors() {
    assertCredentialsRejected(requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY, PLAIN_METHOD));
    assertCredentialsRejected(
        requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY, PubSubGrpc.getManagedSubscribeMethod()));
    assertCredentialsRejected(
        requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY, PubSubGrpc.getPublishStreamMethod()));
    assertCredentialsRejected(requestInfo(SecurityLevel.PRIVACY_AND_INTEGRITY, null));
    assertCredentialsRejected(
        new CallCredentials.RequestInfo() {
          @Override
          public MethodDescriptor<?, ?> getMethodDescriptor() {
            throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
          }

          @Override
          public SecurityLevel getSecurityLevel() {
            return SecurityLevel.PRIVACY_AND_INTEGRITY;
          }

          @Override
          public String getAuthority() {
            return "server-sentinel-authority";
          }

          @Override
          public Attributes getTransportAttrs() {
            return Attributes.EMPTY;
          }
        });
  }

  @Test
  void publicCredentialsRefuseAnActualPlaintextChannelBeforeServerInvocation() throws Exception {
    AtomicInteger invocations = new AtomicInteger();
    Server plaintextServer =
        NettyServerBuilder.forPort(0)
            .addService(
                new PubSubGrpc.PubSubImplBase() {
                  @Override
                  public void getTopic(
                      TopicRequest request, StreamObserver<TopicInfo> responseObserver) {
                    invocations.incrementAndGet();
                    responseObserver.onNext(TopicInfo.getDefaultInstance());
                    responseObserver.onCompleted();
                  }
                })
            .build()
            .start();
    ManagedChannel plaintextChannel =
        NettyChannelBuilder.forAddress("127.0.0.1", plaintextServer.getPort())
            .usePlaintext()
            .build();
    try {
      CompletableFuture<TopicInfo> result = new CompletableFuture<>();
      PubSubGrpc.newStub(plaintextChannel)
          .withCallCredentials(new SalesforceCallCredentials(session(0)))
          .getTopic(TopicRequest.getDefaultInstance(), singleResponseObserver(result));

      Exception wrapper = assertThrows(Exception.class, () -> result.get(5, TimeUnit.SECONDS));
      assertEquals(Status.Code.UNAUTHENTICATED, Status.fromThrowable(wrapper.getCause()).getCode());
      assertSafe(wrapper.toString());
      assertSafe(wrapper.getCause().toString());
      assertEquals(0, invocations.get());
    } finally {
      plaintextChannel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      plaintextServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void serverAndCauseDescriptionsAreSanitizedAtTheAsyncBoundary() throws Exception {
    restartServer(new FailingService());
    CompletableFuture<TopicMetadata> response = transport.getTopic("failure").toCompletableFuture();

    Exception wrapper = assertThrows(Exception.class, () -> response.get(5, TimeUnit.SECONDS));
    TransportException failure = assertInstanceOf(TransportException.class, wrapper.getCause());

    assertEquals("Salesforce Pub/Sub RPC failed [INTERNAL]", failure.getMessage());
    assertNull(failure.getCause());
    assertSafe(failure.toString());
  }

  @Test
  void grpcFailuresMapToTypedCategoriesWithoutRemoteDescriptionsOrCauses() throws Exception {
    assertTopicFailure(Status.Code.UNAUTHENTICATED, AuthenticationException.class);
    assertTopicFailure(Status.Code.PERMISSION_DENIED, AuthorizationException.class);
    assertTopicFailure(Status.Code.UNAVAILABLE, TransportException.class);

    restartServer(new CategorizedFailingService(Status.Code.NOT_FOUND));
    Exception topicWrapper =
        assertThrows(
            Exception.class,
            () -> transport.getTopic("/event/Missing__e").get(5, TimeUnit.SECONDS));
    TopicNotFoundException topic =
        assertInstanceOf(TopicNotFoundException.class, topicWrapper.getCause());
    assertEquals("/event/Missing__e", topic.topic());
    assertNull(topic.getCause());
    assertSafe(topic.toString());

    Exception schemaWrapper =
        assertThrows(
            Exception.class, () -> transport.getSchema("missing-schema").get(5, TimeUnit.SECONDS));
    SchemaLookupException schema =
        assertInstanceOf(SchemaLookupException.class, schemaWrapper.getCause());
    assertEquals("missing-schema", schema.schemaId());
    assertNull(schema.getCause());
    assertSafe(schema.toString());
  }

  @Test
  void missingTopicInputsAreRejectedLocallyBeforeHealthLoggingOrRpcStart() throws Exception {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    RecordingLogger logger = new RecordingLogger();
    restartServer(new RespondingService(), health, logger);

    for (String missingTopic : new String[] {null, "", "   ", "\u2003"}) {
      TopicNotFoundException failure =
          assertThrows(TopicNotFoundException.class, () -> transport.getTopic(missingTopic));
      assertNull(failure.getCause());
    }

    assertEquals(0, receivedCalls.get());
    assertEquals(ConnectorStatus.STARTING, health.status());
    assertEquals(List.of(ConnectorStatus.STARTING), telemetry.statuses);
    assertTrue(logger.events().isEmpty());

    TopicMetadata valid = transport.getTopic("/event/Exact__e").get(5, TimeUnit.SECONDS);
    assertEquals("/event/Exact__e", valid.topicName());
    assertEquals(1, receivedCalls.get());
  }

  @Test
  void publishRpcFailuresUsePublishCategoryWithoutMaskingAuthCategories() throws Exception {
    assertPublishFailure(Status.Code.INVALID_ARGUMENT, PublishException.class);
    assertPublishFailure(Status.Code.NOT_FOUND, PublishException.class);
    assertPublishFailure(Status.Code.UNAVAILABLE, PublishException.class);
    assertPublishFailure(Status.Code.UNAUTHENTICATED, AuthenticationException.class);
    assertPublishFailure(Status.Code.PERMISSION_DENIED, AuthorizationException.class);
  }

  @Test
  void subscribeRpcFailuresUseSubscriptionCategoryWithoutMaskingAuthCategories() throws Exception {
    assertSubscriptionFailure(
        Status.Code.UNAVAILABLE, SubscriptionException.class, "WARN", ConnectorStatus.DEGRADED);
    assertSubscriptionFailure(
        Status.Code.DEADLINE_EXCEEDED,
        SubscriptionException.class,
        "WARN",
        ConnectorStatus.DEGRADED);
    assertSubscriptionFailure(
        Status.Code.RESOURCE_EXHAUSTED,
        SubscriptionException.class,
        "WARN",
        ConnectorStatus.DEGRADED);
    assertSubscriptionFailure(
        Status.Code.ABORTED, SubscriptionException.class, "WARN", ConnectorStatus.DEGRADED);
    assertSubscriptionFailure(
        Status.Code.INTERNAL, SubscriptionException.class, "ERROR", ConnectorStatus.FAILED);
    assertSubscriptionFailure(
        Status.Code.UNAUTHENTICATED,
        AuthenticationException.class,
        "ERROR",
        ConnectorStatus.FAILED);
    assertSubscriptionFailure(
        Status.Code.PERMISSION_DENIED,
        AuthorizationException.class,
        "ERROR",
        ConnectorStatus.FAILED);
  }

  @Test
  void representativeGrpcFailuresLogOnceAtSafeSeverityWithOnlySafeFields() throws Exception {
    assertFailureLog(Status.Code.CANCELLED, "DEBUG", TransportException.class);
    assertFailureLog(Status.Code.UNAVAILABLE, "WARN", TransportException.class);
    assertFailureLog(Status.Code.UNAUTHENTICATED, "ERROR", AuthenticationException.class);
    assertFailureLog(Status.Code.PERMISSION_DENIED, "ERROR", AuthorizationException.class);
    assertFailureLog(Status.Code.INTERNAL, "ERROR", TransportException.class);
  }

  @Test
  void successfulTransportBoundariesPublishConnectedAndStoppedHealth() throws Exception {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    RecordingLogger logger = new RecordingLogger();
    restartServer(new RespondingService(), health, logger);

    transport.getTopic("/event/Test__e").get(5, TimeUnit.SECONDS);

    assertEquals(ConnectorStatus.CONNECTED, health.status());
    assertEquals(1, telemetry.connected.get());
    assertEquals(List.of(ConnectorStatus.STARTING, ConnectorStatus.CONNECTED), telemetry.statuses);

    transport.close();

    assertEquals(ConnectorStatus.STOPPED, health.status());
    assertEquals(
        List.of(ConnectorStatus.STARTING, ConnectorStatus.CONNECTED, ConnectorStatus.STOPPED),
        telemetry.statuses);
  }

  @Test
  void firstAcceptedSubscriptionResponsePublishesExactSubscriptionContextOnce() throws Exception {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    RecordingLogger logger = new RecordingLogger();
    restartServer(new RespondingService(), health, logger);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>(2);
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    FetchRequest request = FetchRequest.newBuilder().setTopicName("/event/Exact_Topic__e").build();

    subscription.send(request);
    subscription.send(request);

    assertTrue(downstream.nextReceived.await(5, TimeUnit.SECONDS));
    assertEquals(2, downstream.next.get());
    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertEquals(List.of(ConnectorStatus.STARTING, ConnectorStatus.SUBSCRIBED), telemetry.statuses);
    assertEquals(0, telemetry.connected.get());
    assertEquals(
        List.of(
            new SubscriptionStateObservation(
                "connection", "/event/Exact_Topic__e", ConnectorStatus.SUBSCRIBED)),
        telemetry.subscriptionStates);
    assertEquals(
        List.of(new SubscribedObservation("connection", "/event/Exact_Topic__e", null)),
        telemetry.subscriptions);

    transport.getTopic("/event/Exact_Topic__e").get(5, TimeUnit.SECONDS);

    assertEquals(ConnectorStatus.SUBSCRIBED, health.status());
    assertEquals(0, telemetry.connected.get());
    assertEquals(1, telemetry.subscriptionStates.size());
    assertEquals(1, telemetry.subscriptions.size());

    subscription.complete();
    subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(ConnectorStatus.CONNECTED, health.status());
    assertEquals(1, telemetry.connected.get());
    assertEquals(
        new SubscriptionStateObservation(
            "connection", "/event/Exact_Topic__e", ConnectorStatus.CONNECTED),
        telemetry.subscriptionStates.getLast());
  }

  @Test
  void terminalHealthBlocksLateSubscriptionSuccessTelemetry() {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    assertTrue(health.transitionTo(ConnectorStatus.FAILED));

    assertFalse(health.subscriptionSucceeded(new Object(), "/event/Late__e"));

    assertEquals(ConnectorStatus.FAILED, health.status());
    assertTrue(telemetry.subscriptionStates.isEmpty());
    assertTrue(telemetry.subscriptions.isEmpty());
  }

  @Test
  void missingSchemaIdIsATypedLookupFailureBeforeRpcStart() {
    assertInstanceOf(
        SchemaLookupException.class,
        assertThrows(Exception.class, () -> transport.getSchema(null)));
    assertInstanceOf(
        SchemaLookupException.class, assertThrows(Exception.class, () -> transport.getSchema(" ")));
    assertEquals(0, receivedCalls.get());
  }

  @Test
  void synchronousChannelFailureIsSanitizedAtTheAsyncBoundary() throws Exception {
    ThrowingManagedChannel throwingChannel = new ThrowingManagedChannel();
    PubSubApiTransport failingTransport = new PubSubApiTransport(throwingChannel, session(0));
    try {
      CompletableFuture<TopicMetadata> response =
          failingTransport.getTopic("failure").toCompletableFuture();
      Exception wrapper = assertThrows(Exception.class, () -> response.get(5, TimeUnit.SECONDS));
      SalesforcePubSubException failure =
          assertInstanceOf(SalesforcePubSubException.class, wrapper.getCause());
      assertEquals("Salesforce Pub/Sub RPC failed [UNKNOWN]", failure.getMessage());
      assertSafe(failure.toString());
    } finally {
      failingTransport.close();
    }
  }

  @Test
  void synchronousSubscriptionFailureIsSanitizedForCompletionAndObserver() throws Exception {
    ThrowingManagedChannel throwingChannel = new ThrowingManagedChannel();
    PubSubApiTransport failingTransport = new PubSubApiTransport(throwingChannel, session(0));
    CompletableFuture<FetchResponse> observerResult = new CompletableFuture<>();
    try {
      PubSubApiTransport.SubscriptionRpc subscription =
          failingTransport.subscribe(singleResponseObserver(observerResult));

      Exception completionWrapper =
          assertThrows(
              Exception.class,
              () -> subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS));
      Exception observerWrapper =
          assertThrows(Exception.class, () -> observerResult.get(5, TimeUnit.SECONDS));
      assertInstanceOf(SubscriptionException.class, completionWrapper.getCause());
      assertInstanceOf(SubscriptionException.class, observerWrapper.getCause());
      assertSafe(completionWrapper.toString());
      assertSafe(completionWrapper.getCause().toString());
      assertSafe(observerWrapper.toString());
      assertSafe(observerWrapper.getCause().toString());
    } finally {
      failingTransport.close();
    }
  }

  @Test
  void synchronousUnaryStartFailureCancelsThePartialCallAndPreservesSanitizedFailure()
      throws Exception {
    FailingStartManagedChannel failingChannel = new FailingStartManagedChannel(true);
    PubSubApiTransport failingTransport = new PubSubApiTransport(failingChannel, session(0));
    try {
      CompletableFuture<TopicMetadata> response = failingTransport.getTopic("failure");

      Exception wrapper = assertThrows(Exception.class, () -> response.get(5, TimeUnit.SECONDS));
      SalesforcePubSubException failure =
          assertInstanceOf(SalesforcePubSubException.class, wrapper.getCause());
      assertEquals("Salesforce Pub/Sub RPC failed [UNKNOWN]", failure.getMessage());
      assertEquals(1, failingChannel.starts.get());
      assertEquals(1, failingChannel.cancels.get());
      assertSafe(wrapper.toString());
      assertSafe(failure.toString());
    } finally {
      failingTransport.close();
    }
  }

  @Test
  void synchronousSubscriptionStartFailureCancelsThePartialCall() throws Exception {
    FailingStartManagedChannel failingChannel = new FailingStartManagedChannel(false);
    PubSubApiTransport failingTransport = new PubSubApiTransport(failingChannel, session(0));
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    try {
      PubSubApiTransport.SubscriptionRpc subscription = failingTransport.subscribe(downstream);

      Exception failure =
          assertThrows(
              Exception.class,
              () -> subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS));
      assertInstanceOf(SubscriptionException.class, failure.getCause());
      assertEquals(1, failingChannel.starts.get());
      assertEquals(1, failingChannel.cancels.get());
      assertEquals(1, downstream.errors.get());
      assertSafe(failure.toString());
      assertSafe(failure.getCause().toString());
    } finally {
      failingTransport.close();
    }
  }

  @Test
  void unaryCleanupFailuresCannotEscapeOrReplaceTheSanitizedFailure() {
    PubSubApiTransport.CancellableRpcFuture<String> result =
        new PubSubApiTransport.CancellableRpcFuture<>();
    ThrowingCancelClientCall<String, String> call = new ThrowingCancelClientCall<>();
    ThrowingClientRequestObserver<Object> stream =
        new ThrowingClientRequestObserver<>(true, false, true);
    PubSubApiTransport.UnaryResponseObserver<String, String> observer =
        new PubSubApiTransport.UnaryResponseObserver<>(
            result,
            ignored -> {
              throw Status.INTERNAL
                  .withDescription("server-sentinel token-0 instance-0 tenant-0")
                  .asRuntimeException();
            });
    result.attachClientCall(call);
    observer.beforeStart(stream);

    assertDoesNotThrow(() -> observer.onNext("invalid"));

    Exception failure = assertThrows(Exception.class, result::join);
    assertEquals(1, call.cancels.get());
    assertEquals(1, stream.cancels.get());
    assertEquals("Salesforce Pub/Sub RPC failed [INTERNAL]", failure.getCause().getMessage());
    assertSafe(failure.toString());
  }

  @Test
  void unaryResponseDoesNotCompleteBeforeTerminalSignal() throws Exception {
    HoldingUnaryService service = new HoldingUnaryService();
    restartServer(service);

    CompletableFuture<TopicMetadata> response = transport.getTopic("held").toCompletableFuture();
    assertTrue(service.responseSent.await(5, TimeUnit.SECONDS));

    assertFalse(response.isDone());
    transport.close();
    assertTrue(service.cancelled.await(5, TimeUnit.SECONDS));
  }

  @Test
  void closeWinningAfterUnaryRegistrationPreventsChannelCallStart() throws Exception {
    CountingManagedChannel countingChannel = new CountingManagedChannel();
    CountDownLatch registered = new CountDownLatch(1);
    CountDownLatch releaseStart = new CountDownLatch(1);
    PubSubApiTransport racingTransport =
        new PubSubApiTransport(
            countingChannel,
            session(0),
            () -> {
              registered.countDown();
              await(releaseStart);
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<CompletionStage<TopicMetadata>> call =
          executor.submit(() -> racingTransport.getTopic("registered"));
      assertTrue(registered.await(5, TimeUnit.SECONDS));

      racingTransport.close();
      releaseStart.countDown();
      call.get(5, TimeUnit.SECONDS);

      assertEquals(0, countingChannel.newCalls.get());
      assertTrue(countingChannel.isShutdown());
    } finally {
      releaseStart.countDown();
      racingTransport.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void closeWinningAfterSubscriptionRegistrationPreventsChannelCallStart() throws Exception {
    CountingManagedChannel countingChannel = new CountingManagedChannel();
    CountDownLatch registered = new CountDownLatch(1);
    CountDownLatch releaseStart = new CountDownLatch(1);
    PubSubApiTransport racingTransport =
        new PubSubApiTransport(
            countingChannel,
            session(0),
            () -> {
              registered.countDown();
              await(releaseStart);
            });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<PubSubApiTransport.SubscriptionRpc> call =
          executor.submit(() -> racingTransport.subscribe(new RecordingObserver<>()));
      assertTrue(registered.await(5, TimeUnit.SECONDS));

      racingTransport.close();
      releaseStart.countDown();
      PubSubApiTransport.SubscriptionRpc subscription = call.get(5, TimeUnit.SECONDS);

      assertTrue(subscription.isCancelled());
      assertEquals(0, countingChannel.newCalls.get());
      assertTrue(countingChannel.isShutdown());
    } finally {
      releaseStart.countDown();
      racingTransport.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void closeAfterUnaryStartClaimPreventsDelayedCallFromReachingServer() throws Exception {
    DelayedService service = new DelayedService();
    CountDownLatch startClaimed = new CountDownLatch(1);
    CountDownLatch releaseInvocation = new CountDownLatch(1);
    restartServer(
        service,
        () -> {},
        () -> {
          startClaimed.countDown();
          await(releaseInvocation);
        });
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<CompletionStage<TopicMetadata>> call =
          executor.submit(() -> transport.getTopic("claimed"));
      assertTrue(startClaimed.await(5, TimeUnit.SECONDS));

      assertTimeout(Duration.ofSeconds(1), transport::close);
      releaseInvocation.countDown();
      CompletableFuture<TopicMetadata> view = call.get(5, TimeUnit.SECONDS).toCompletableFuture();

      awaitCancellation(view);
      assertFalse(service.started.await(100, TimeUnit.MILLISECONDS));
      assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseInvocation.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void closeAfterSubscriptionStartClaimPreventsDelayedCallFromReachingServerOrObserver()
      throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(false);
    CountDownLatch startClaimed = new CountDownLatch(1);
    CountDownLatch releaseInvocation = new CountDownLatch(1);
    restartServer(
        service,
        () -> {},
        () -> {
          startClaimed.countDown();
          await(releaseInvocation);
        });
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<PubSubApiTransport.SubscriptionRpc> call =
          executor.submit(() -> transport.subscribe(downstream));
      assertTrue(startClaimed.await(5, TimeUnit.SECONDS));

      assertTimeout(Duration.ofSeconds(1), transport::close);
      releaseInvocation.countDown();
      PubSubApiTransport.SubscriptionRpc subscription = call.get(5, TimeUnit.SECONDS);

      assertTrue(subscription.isCancelled());
      assertFalse(service.subscribed.await(100, TimeUnit.MILLISECONDS));
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
      assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseInvocation.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @RepeatedTest(10)
  void blockedUnaryNewCallCannotDelayCloseOrStartAfterCloseWins() throws Exception {
    assertBlockedNewCallCannotDelayClose(false);
  }

  @RepeatedTest(10)
  void blockedSubscriptionNewCallCannotDelayCloseOrStartAfterCloseWins() throws Exception {
    assertBlockedNewCallCannotDelayClose(true);
  }

  @RepeatedTest(10)
  void blockedClientCallStartCannotDelayClose() throws Exception {
    BlockingLifecycleManagedChannel blockingChannel =
        new BlockingLifecycleManagedChannel(false, true);
    PubSubApiTransport racingTransport = new PubSubApiTransport(blockingChannel, session(0));
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<PubSubApiTransport.SubscriptionRpc> invocation =
          executor.submit(() -> racingTransport.subscribe(downstream));
      assertTrue(blockingChannel.startEntered.await(5, TimeUnit.SECONDS));

      assertTimeoutPreemptively(Duration.ofSeconds(1), racingTransport::close);
      assertTrue(blockingChannel.isShutdown());
      assertTrue(blockingChannel.cancelled.await(1, TimeUnit.SECONDS));

      blockingChannel.release.countDown();
      PubSubApiTransport.SubscriptionRpc subscription = invocation.get(5, TimeUnit.SECONDS);
      assertTrue(subscription.isCancelled());
      assertEquals(1, blockingChannel.starts.get());
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      blockingChannel.release.countDown();
      racingTransport.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @RepeatedTest(10)
  void synchronousStartCallbackCanCloseReentrantlyBeforeBlockedStartReturns() throws Exception {
    BlockingLifecycleManagedChannel blockingChannel =
        new BlockingLifecycleManagedChannel(false, true, true);
    AtomicReference<PubSubApiTransport> racingTransport = new AtomicReference<>();
    CountDownLatch closeReturned = new CountDownLatch(1);
    AtomicInteger next = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();
    AtomicInteger completed = new AtomicInteger();
    StreamObserver<FetchResponse> downstream =
        new StreamObserver<>() {
          @Override
          public void onNext(FetchResponse response) {
            next.incrementAndGet();
            racingTransport.get().close();
            closeReturned.countDown();
          }

          @Override
          public void onError(Throwable failure) {
            errors.incrementAndGet();
          }

          @Override
          public void onCompleted() {
            completed.incrementAndGet();
          }
        };
    racingTransport.set(new PubSubApiTransport(blockingChannel, session(0)));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<PubSubApiTransport.SubscriptionRpc> invocation =
          executor.submit(() -> racingTransport.get().subscribe(downstream));

      assertTrue(closeReturned.await(1, TimeUnit.SECONDS));
      assertTrue(blockingChannel.isShutdown());
      assertTrue(blockingChannel.cancelled.await(1, TimeUnit.SECONDS));
      assertEquals(1, next.get());
      assertFalse(invocation.isDone());

      blockingChannel.release.countDown();
      assertTrue(invocation.get(5, TimeUnit.SECONDS).isCancelled());
      assertEquals(0, errors.get());
      assertEquals(0, completed.get());
    } finally {
      blockingChannel.release.countDown();
      racingTransport.get().close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void publicUnaryStageMutationCannotForgeOrUnregisterTheLiveRpc() throws Exception {
    assertPublicMutationDoesNotOwnRpc(true);
    assertPublicMutationDoesNotOwnRpc(false);
  }

  @Test
  void concreteUnaryReturnDescriptorsRemainCompletableFuture() throws Exception {
    assertEquals(
        CompletableFuture.class,
        PubSubApiTransport.class.getMethod("getTopic", String.class).getReturnType());
    assertEquals(
        CompletableFuture.class,
        PubSubApiTransport.class.getMethod("getSchema", String.class).getReturnType());
    assertEquals(
        CompletionStage.class,
        SalesforceEventTransport.class.getMethod("getTopic", String.class).getReturnType());
    assertEquals(
        CompletionStage.class,
        SalesforceEventTransport.class.getMethod("getSchema", String.class).getReturnType());
  }

  @Test
  void unaryLateErrorWinsOverBufferedResponseAndIsSanitized() throws Exception {
    restartServer(new ResponseThenErrorService());

    CompletableFuture<TopicMetadata> response =
        transport.getTopic("late-error").toCompletableFuture();
    Exception wrapper = assertThrows(Exception.class, () -> response.get(5, TimeUnit.SECONDS));
    SalesforcePubSubException failure =
        assertInstanceOf(SalesforcePubSubException.class, wrapper.getCause());

    assertEquals("Salesforce Pub/Sub RPC failed [INTERNAL]", failure.getMessage());
    assertSafe(failure.toString());
  }

  @Test
  void unaryObserverRejectsZeroAndMultipleResponses() {
    PubSubApiTransport.CancellableRpcFuture<String> zero =
        new PubSubApiTransport.CancellableRpcFuture<>();
    PubSubApiTransport.UnaryResponseObserver<String, String> zeroObserver =
        new PubSubApiTransport.UnaryResponseObserver<>(zero, value -> value);

    zeroObserver.onCompleted();

    assertTrue(zero.isCompletedExceptionally());
    assertSafe(assertThrows(Exception.class, zero::join).toString());

    PubSubApiTransport.CancellableRpcFuture<String> multiple =
        new PubSubApiTransport.CancellableRpcFuture<>();
    PubSubApiTransport.UnaryResponseObserver<String, String> multipleObserver =
        new PubSubApiTransport.UnaryResponseObserver<>(multiple, value -> value);
    multipleObserver.onNext("first");
    assertFalse(multiple.isDone());

    multipleObserver.onNext("second");

    assertTrue(multiple.isCompletedExceptionally());
    assertSafe(assertThrows(Exception.class, multiple::join).toString());
  }

  @Test
  void bufferedUnaryCancellationRemainsStableAcrossLateTerminalSignals() {
    PubSubApiTransport.CancellableRpcFuture<String> result =
        new PubSubApiTransport.CancellableRpcFuture<>();
    PubSubApiTransport.UnaryResponseObserver<String, String> observer =
        new PubSubApiTransport.UnaryResponseObserver<>(result, value -> value);

    observer.onNext("buffered");
    assertTrue(result.cancel(false));
    observer.onCompleted();
    observer.onError(
        Status.INTERNAL.withDescription("server-sentinel token-0").asRuntimeException());

    assertTrue(result.isCancelled());
    assertThrows(java.util.concurrent.CancellationException.class, result::join);
  }

  @Test
  void onlyTheWinningUnaryTerminalFailureIsMappedAndReported() {
    List<Status.Code> reported = new ArrayList<>();
    PubSubApiTransport.CancellableRpcFuture<String> result =
        new PubSubApiTransport.CancellableRpcFuture<>((code, ignoredFailure) -> reported.add(code));
    PubSubApiTransport.UnaryResponseObserver<String, String> observer =
        new PubSubApiTransport.UnaryResponseObserver<>(result, value -> value);

    observer.onError(Status.UNAVAILABLE.asRuntimeException());
    observer.onError(Status.INTERNAL.asRuntimeException());

    assertEquals(List.of(Status.Code.UNAVAILABLE), reported);
    TransportException failure =
        assertInstanceOf(
            TransportException.class, assertThrows(Exception.class, result::join).getCause());
    assertEquals("Salesforce Pub/Sub RPC failed [UNAVAILABLE]", failure.getMessage());
  }

  @Test
  void closeIsImmediateIdempotentAndCancelsAnActiveRpc() throws Exception {
    DelayedService delayedService = new DelayedService();
    restartServer(delayedService);
    CompletableFuture<TopicMetadata> response = transport.getTopic("blocked").toCompletableFuture();
    assertTrue(delayedService.started.await(5, TimeUnit.SECONDS));

    assertTimeout(Duration.ofSeconds(1), transport::close);
    assertTimeout(Duration.ofSeconds(1), transport::close);

    assertTrue(transport.isClosed());
    assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
    Exception failure = assertThrows(Exception.class, () -> response.get(5, TimeUnit.SECONDS));
    assertSafe(failure.toString());
    assertThrows(IllegalStateException.class, () -> transport.getTopic("closed"));
    assertThrows(IllegalStateException.class, () -> transport.updateSession(session(2)));
  }

  @Test
  void closeCancelsABufferedUnaryAndLateServerTerminalCannotChangeIt() throws Exception {
    HoldingUnaryService service = new HoldingUnaryService();
    restartServer(service);
    CompletableFuture<TopicMetadata> response =
        transport.getTopic("buffered").toCompletableFuture();
    assertTrue(service.responseSent.await(5, TimeUnit.SECONDS));

    transport.close();
    awaitCancellation(response);
    service.completeLate();
    service.errorLate();

    assertTrue(response.isCompletedExceptionally());
  }

  @Test
  void closeDoesNotRunOrWaitForUnaryCompletionDependents() throws Exception {
    DelayedService service = new DelayedService();
    restartServer(service);
    CompletableFuture<TopicMetadata> response =
        transport.getTopic("blocked-dependent").toCompletableFuture();
    assertTrue(service.started.await(5, TimeUnit.SECONDS));
    CountDownLatch dependentEntered = new CountDownLatch(1);
    CountDownLatch releaseDependent = new CountDownLatch(1);
    response.whenComplete(
        (ignored, failure) -> {
          dependentEntered.countDown();
          await(releaseDependent);
        });

    assertTimeout(Duration.ofSeconds(1), transport::close);
    assertTrue(channel.isShutdown());
    assertTrue(dependentEntered.await(5, TimeUnit.SECONDS));
    releaseDependent.countDown();
    awaitCancellation(response);
  }

  @Test
  void explicitUnaryCancellationDoesNotRunOrWaitForCallerDependents() throws Exception {
    DelayedService service = new DelayedService();
    restartServer(service);
    CompletableFuture<TopicMetadata> response = transport.getTopic("blocking-cancel-dependent");
    assertTrue(service.started.await(5, TimeUnit.SECONDS));
    CountDownLatch dependentEntered = new CountDownLatch(1);
    CountDownLatch releaseDependent = new CountDownLatch(1);
    CountDownLatch dependentExited = new CountDownLatch(1);
    AtomicReference<Thread> dependentThread = new AtomicReference<>();
    response.whenComplete(
        (ignored, failure) -> {
          dependentThread.set(Thread.currentThread());
          dependentEntered.countDown();
          try {
            await(releaseDependent);
          } finally {
            dependentExited.countDown();
          }
        });
    Thread cancellingThread = Thread.currentThread();

    try {
      assertTimeout(Duration.ofSeconds(1), () -> assertTrue(response.cancel(true)));

      assertTrue(response.isCancelled());
      assertTrue(response.isDone());
      assertTrue(response.isCompletedExceptionally());
      assertEquals(java.util.concurrent.Future.State.CANCELLED, response.state());
      assertThrows(CancellationException.class, response::join);
      assertThrows(CancellationException.class, response::get);
      assertThrows(CancellationException.class, () -> response.getNow(null));
      assertTrue(service.unaryCancelled.await(5, TimeUnit.SECONDS));
      assertTrue(dependentEntered.await(5, TimeUnit.SECONDS));
      assertFalse(cancellingThread.equals(dependentThread.get()));
      assertTrue(dependentThread.get().isVirtual());
    } finally {
      releaseDependent.countDown();
      assertTrue(dependentExited.await(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void cancellationCompletionFallsBackFromRejectionWithoutStarvingOtherCancellations()
      throws Exception {
    java.util.concurrent.Executor rejectingExecutor =
        task -> {
          throw new RejectedExecutionException("expected test rejection");
        };
    PubSubApiTransport.CancellableRpcFuture<String> firstSource =
        new PubSubApiTransport.CancellableRpcFuture<>(rejectingExecutor);
    PubSubApiTransport.CancellableRpcFuture<String> secondSource =
        new PubSubApiTransport.CancellableRpcFuture<>(rejectingExecutor);
    CompletableFuture<String> first = firstSource.readOnlyStage();
    CompletableFuture<String> second = secondSource.readOnlyStage();
    CountDownLatch firstDependentEntered = new CountDownLatch(1);
    CountDownLatch releaseFirstDependent = new CountDownLatch(1);
    CountDownLatch firstDependentExited = new CountDownLatch(1);
    CountDownLatch secondDependentEntered = new CountDownLatch(1);
    first.whenComplete(
        (ignored, failure) -> {
          firstDependentEntered.countDown();
          try {
            await(releaseFirstDependent);
          } finally {
            firstDependentExited.countDown();
          }
        });
    second.whenComplete((ignored, failure) -> secondDependentEntered.countDown());

    try {
      assertTimeout(Duration.ofSeconds(1), () -> assertTrue(first.cancel(false)));
      assertTrue(first.isCancelled());
      assertTrue(firstDependentEntered.await(5, TimeUnit.SECONDS));

      assertTimeout(Duration.ofSeconds(1), () -> assertTrue(second.cancel(false)));
      assertTrue(second.isCancelled());
      assertTrue(secondDependentEntered.await(5, TimeUnit.SECONDS));
    } finally {
      releaseFirstDependent.countDown();
      assertTrue(firstDependentExited.await(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void unaryCancellationOwnsStateAndCleanupExactlyOnce() {
    PubSubApiTransport.CancellableRpcFuture<String> source =
        new PubSubApiTransport.CancellableRpcFuture<>();
    ThrowingCancelClientCall<String, String> call = new ThrowingCancelClientCall<>();
    ThrowingClientRequestObserver<Object> stream =
        new ThrowingClientRequestObserver<>(false, false, true);
    PubSubApiTransport.UnaryResponseObserver<String, String> observer =
        new PubSubApiTransport.UnaryResponseObserver<>(source, value -> value);
    source.attachClientCall(call);
    observer.beforeStart(stream);
    CompletableFuture<String> response = source.readOnlyStage();

    assertTimeout(Duration.ofSeconds(1), () -> assertTrue(response.cancel(true)));
    assertFalse(response.cancel(true));

    assertTrue(response.isCancelled());
    assertEquals(1, call.cancels.get());
    assertEquals(1, stream.cancels.get());
    assertThrows(CancellationException.class, response::join);
  }

  @RepeatedTest(20)
  void explicitUnaryCancellationAndTransportCloseHaveOneCancellationOutcome() throws Exception {
    DelayedService service = new DelayedService();
    restartServer(service);
    CompletableFuture<TopicMetadata> response = transport.getTopic("cancel-close-race");
    assertTrue(service.started.await(5, TimeUnit.SECONDS));
    CountDownLatch race = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<Boolean> explicitCancel =
          executor.submit(
              () -> {
                await(race);
                return response.cancel(true);
              });
      Future<?> transportClose =
          executor.submit(
              () -> {
                await(race);
                transport.close();
              });

      race.countDown();
      explicitCancel.get(5, TimeUnit.SECONDS);
      transportClose.get(5, TimeUnit.SECONDS);

      assertTrue(response.isCancelled());
      assertTrue(response.isDone());
      assertTrue(service.unaryCancelled.await(5, TimeUnit.SECONDS));
      assertFalse(response.cancel(true));
      assertThrows(CancellationException.class, response::join);
    } finally {
      race.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void unaryTerminalBeforeCloseRemainsSuccessful() throws Exception {
    CompletableFuture<TopicMetadata> response =
        transport.getTopic("complete-first").toCompletableFuture();
    TopicMetadata completed = response.get(5, TimeUnit.SECONDS);

    transport.close();

    assertFalse(response.isCancelled());
    assertEquals(completed, response.join());
  }

  @Test
  void cancellingSubscriptionCancelsItsGrpcCall() throws Exception {
    DelayedService delayedService = new DelayedService();
    restartServer(delayedService);
    CompletableFuture<FetchResponse> response = new CompletableFuture<>();
    PubSubApiTransport.SubscriptionRpc subscription =
        transport.subscribe(singleResponseObserver(response));
    subscription.send(FetchRequest.newBuilder().setTopicName("cancelled").build());
    assertTrue(delayedService.subscriptionStarted.await(5, TimeUnit.SECONDS));

    subscription.cancel();

    assertTrue(subscription.isCancelled());
    assertTrue(delayedService.subscriptionCancelled.await(5, TimeUnit.SECONDS));
  }

  @Test
  void subscriptionCancelReachesNetworkWithoutWaitingForCompletionDependent() throws Exception {
    DelayedService delayedService = new DelayedService();
    restartServer(delayedService);
    PubSubApiTransport.SubscriptionRpc subscription =
        transport.subscribe(new RecordingObserver<>());
    subscription.send(FetchRequest.newBuilder().setTopicName("cancelled").build());
    assertTrue(delayedService.subscriptionStarted.await(5, TimeUnit.SECONDS));
    CountDownLatch dependentEntered = new CountDownLatch(1);
    CountDownLatch releaseDependent = new CountDownLatch(1);
    subscription
        .completion()
        .whenComplete(
            (ignored, failure) -> {
              dependentEntered.countDown();
              await(releaseDependent);
            });

    assertTimeout(Duration.ofSeconds(1), subscription::cancel);
    assertTrue(delayedService.subscriptionCancelled.await(5, TimeUnit.SECONDS));
    assertTrue(dependentEntered.await(5, TimeUnit.SECONDS));
    assertTrue(subscription.isCancelled());
    releaseDependent.countDown();
  }

  @Test
  void subscriptionCancelWinsBeforeSendAndSuppressesLaterCallbacks() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(false);
    restartServer(service);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    assertTrue(service.subscribed.await(5, TimeUnit.SECONDS));

    subscription.cancel();
    assertThrows(
        SalesforcePubSubException.class,
        () -> subscription.send(FetchRequest.getDefaultInstance()));
    assertThrows(SalesforcePubSubException.class, subscription::complete);
    service.emitAfterCancellation();

    assertTrue(subscription.isCancelled());
    assertEquals(0, service.requests.get());
    assertEquals(0, downstream.next.get());
    assertEquals(0, downstream.errors.get());
    assertEquals(0, downstream.completed.get());
  }

  @RepeatedTest(10)
  void concurrentCancelPromptlyCancelsAnInProgressSend() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(true);
    restartServer(service);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> send = executor.submit(() -> subscription.send(FetchRequest.getDefaultInstance()));
      assertTrue(service.requestEntered.await(5, TimeUnit.SECONDS));
      java.util.concurrent.CountDownLatch cancelAttempted =
          new java.util.concurrent.CountDownLatch(1);
      Future<?> cancel =
          executor.submit(
              () -> {
                cancelAttempted.countDown();
                subscription.cancel();
              });
      assertTrue(cancelAttempted.await(5, TimeUnit.SECONDS));
      cancel.get(1, TimeUnit.SECONDS);

      service.releaseRequest.countDown();
      send.get(5, TimeUnit.SECONDS);

      assertEquals(1, service.requests.get());
      assertTrue(subscription.isCancelled());
      assertThrows(
          SalesforcePubSubException.class,
          () -> subscription.send(FetchRequest.getDefaultInstance()));
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      service.releaseRequest.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void cancellationIsPromptWhileSendOrHalfCloseIsStalled() throws Exception {
    for (boolean stallSend : List.of(true, false)) {
      RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
      PubSubApiTransport.SubscriptionRpc subscription =
          new PubSubApiTransport.SubscriptionRpc(downstream);
      BlockingClientRequestObserver requests = new BlockingClientRequestObserver(stallSend);
      subscription.attachFallback(requests);
      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> outbound =
            executor.submit(
                () -> {
                  if (stallSend) {
                    subscription.send(FetchRequest.getDefaultInstance());
                  } else {
                    subscription.complete();
                  }
                });
        assertTrue(requests.outboundEntered.await(5, TimeUnit.SECONDS));

        assertTimeout(Duration.ofSeconds(1), subscription::cancel);
        assertTrue(requests.cancelled.await(1, TimeUnit.SECONDS));
        assertThrows(
            SalesforcePubSubException.class,
            () -> subscription.send(FetchRequest.getDefaultInstance()));

        requests.releaseOutbound.countDown();
        outbound.get(5, TimeUnit.SECONDS);
        assertTrue(subscription.isCancelled());
        assertEquals(0, downstream.next.get());
        assertEquals(0, downstream.errors.get());
        assertEquals(0, downstream.completed.get());
      } finally {
        requests.releaseOutbound.countDown();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      }
    }
  }

  @Test
  void outboundFailuresCancelTheSubscriptionCallAndRequestStream() {
    for (boolean failSend : List.of(true, false)) {
      RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
      PubSubApiTransport.SubscriptionRpc subscription =
          new PubSubApiTransport.SubscriptionRpc(downstream);
      ThrowingCancelClientCall<FetchRequest, FetchResponse> call = new ThrowingCancelClientCall<>();
      ThrowingClientRequestObserver<FetchRequest> stream =
          new ThrowingClientRequestObserver<>(failSend, !failSend, true);
      subscription.attachClientCall(call);
      subscription.attachFallback(stream);

      SubscriptionException failure =
          assertThrows(
              SubscriptionException.class,
              () -> {
                if (failSend) {
                  subscription.send(FetchRequest.getDefaultInstance());
                } else {
                  subscription.complete();
                }
              });

      assertEquals(1, call.cancels.get());
      assertEquals(1, stream.cancels.get());
      assertEquals(1, downstream.errors.get());
      assertSafe(failure.toString());
      assertSanitizedExceptionalCompletion(subscription);
    }
  }

  @Test
  void outboundCallsDuringSynchronousClientStartAreRejectedInsteadOfDropped() throws Exception {
    BlockingStartClientCall<String, String> delegate = new BlockingStartClientCall<>();
    PubSubApiTransport.OperationClientCall<String, String> call =
        new PubSubApiTransport.OperationClientCall<>(delegate);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> start =
          executor.submit(() -> call.start(new ClientCall.Listener<>() {}, new Metadata()));
      assertTrue(delegate.startEntered.await(5, TimeUnit.SECONDS));

      IllegalStateException sendFailure =
          assertThrows(IllegalStateException.class, () -> call.sendMessage("early"));
      assertThrows(IllegalStateException.class, () -> call.request(1));
      assertThrows(IllegalStateException.class, call::halfClose);
      assertThrows(IllegalStateException.class, () -> call.setMessageCompression(true));
      assertSafe(sendFailure.toString());
      assertEquals(0, delegate.sends.get());
      assertEquals(0, delegate.requests.get());
      assertEquals(0, delegate.halfCloses.get());

      delegate.releaseStart.countDown();
      start.get(5, TimeUnit.SECONDS);
      call.request(1);
      call.sendMessage("started");
      call.halfClose();
      call.setMessageCompression(true);
      assertEquals(1, delegate.requests.get());
      assertEquals(1, delegate.sends.get());
      assertEquals(1, delegate.halfCloses.get());
      assertEquals(1, delegate.compressions.get());
    } finally {
      delegate.releaseStart.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void reentrantCancelFromOutboundSendDoesNotWaitOnItsOwnPermit() {
    AtomicReference<PubSubApiTransport.SubscriptionRpc> subscriptionReference =
        new AtomicReference<>();
    AtomicInteger outboundCalls = new AtomicInteger();
    PubSubApiTransport.SubscriptionRpc subscription =
        new PubSubApiTransport.SubscriptionRpc(new RecordingObserver<>());
    subscription.attachFallback(
        new StreamObserver<>() {
          @Override
          public void onNext(FetchRequest request) {
            outboundCalls.incrementAndGet();
            subscriptionReference.get().cancel();
          }

          @Override
          public void onError(Throwable failure) {}

          @Override
          public void onCompleted() {}
        });
    subscriptionReference.set(subscription);

    assertTimeout(
        Duration.ofSeconds(1), () -> subscription.send(FetchRequest.getDefaultInstance()));

    assertTrue(subscription.isCancelled());
    assertEquals(1, outboundCalls.get());
    assertThrows(
        SalesforcePubSubException.class,
        () -> subscription.send(FetchRequest.getDefaultInstance()));
  }

  @Test
  void inProgressSendLinearizesBeforeCloseAndNoCallbackFollowsClose() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(true);
    restartServer(service);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> send = executor.submit(() -> subscription.send(FetchRequest.getDefaultInstance()));
      assertTrue(service.requestEntered.await(5, TimeUnit.SECONDS));
      CountDownLatch closeAttempted = new CountDownLatch(1);
      Future<?> close =
          executor.submit(
              () -> {
                closeAttempted.countDown();
                transport.close();
              });
      assertTrue(closeAttempted.await(5, TimeUnit.SECONDS));
      close.get(1, TimeUnit.SECONDS);
      assertTrue(channel.isShutdown());

      service.releaseRequest.countDown();
      send.get(5, TimeUnit.SECONDS);
      service.emitAfterCancellation();

      assertEquals(1, service.requests.get());
      assertTrue(subscription.isCancelled());
      assertThrows(
          SalesforcePubSubException.class,
          () -> subscription.send(FetchRequest.getDefaultInstance()));
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      service.releaseRequest.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void inProgressCallbackLinearizesBeforeCloseAndNoLaterCallbackIsDelivered() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(false);
    restartServer(service);
    BlockingObserver downstream = new BlockingObserver();
    transport.subscribe(downstream);
    assertTrue(service.subscribed.await(5, TimeUnit.SECONDS));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> callback = executor.submit(service::emitNext);
      assertTrue(downstream.entered.await(5, TimeUnit.SECONDS));
      CountDownLatch closeAttempted = new CountDownLatch(1);
      Future<?> close =
          executor.submit(
              () -> {
                closeAttempted.countDown();
                transport.close();
              });
      assertTrue(closeAttempted.await(5, TimeUnit.SECONDS));
      close.get(1, TimeUnit.SECONDS);
      assertTrue(channel.isShutdown());

      downstream.release.countDown();
      callback.get(5, TimeUnit.SECONDS);
      service.emitAfterCancellation();

      assertEquals(1, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      downstream.release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void earlySynchronousCallbackCanRefreshAndCloseWithoutLifecycleDeadlock() throws Exception {
    restartServer(new EarlyCallbackService());
    CountDownLatch callbackReturned = new CountDownLatch(1);
    StreamObserver<FetchResponse> downstream =
        new StreamObserver<>() {
          @Override
          public void onNext(FetchResponse value) {
            transport.updateSession(session(1));
            transport.close();
            callbackReturned.countDown();
          }

          @Override
          public void onError(Throwable failure) {}

          @Override
          public void onCompleted() {}
        };

    assertTimeout(Duration.ofSeconds(1), () -> transport.subscribe(downstream));

    assertTrue(callbackReturned.await(5, TimeUnit.SECONDS));
    assertTrue(transport.isClosed());
    assertTrue(channel.isShutdown());
  }

  @RepeatedTest(20)
  void concurrentCloseSendAndCancelAlwaysEndsWithoutPostCloseActivity() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(false);
    restartServer(service);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    assertTrue(service.subscribed.await(5, TimeUnit.SECONDS));
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      Future<?> send =
          executor.submit(
              () -> {
                await(start);
                try {
                  subscription.send(FetchRequest.getDefaultInstance());
                } catch (SalesforcePubSubException expected) {
                  // Close or cancellation won the linearization race.
                }
              });
      Future<?> cancel =
          executor.submit(
              () -> {
                await(start);
                subscription.cancel();
              });
      Future<?> close =
          executor.submit(
              () -> {
                await(start);
                transport.close();
              });

      start.countDown();
      send.get(5, TimeUnit.SECONDS);
      cancel.get(5, TimeUnit.SECONDS);
      close.get(5, TimeUnit.SECONDS);
      service.emitAfterCancellation();

      assertTrue(transport.isClosed());
      assertTrue(subscription.isCancelled());
      assertTrue(service.requests.get() <= 1);
      assertThrows(
          SalesforcePubSubException.class,
          () -> subscription.send(FetchRequest.getDefaultInstance()));
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void subscriptionForwardsExactlyOneServerTerminalSignal() throws Exception {
    RacingSubscriptionService service = new RacingSubscriptionService(false);
    restartServer(service);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    assertTrue(service.subscribed.await(5, TimeUnit.SECONDS));

    service.completeThenError();
    subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS);
    subscription.cancel();

    assertFalse(subscription.isCancelled());
    assertEquals(0, downstream.next.get());
    assertEquals(0, downstream.errors.get());
    assertEquals(1, downstream.completed.get());
  }

  @Test
  void forcedServerCallbacksAreSuppressedAfterCancellationWins() {
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    SubscriptionHarness harness = subscriptionHarness(downstream);

    harness.subscription().cancel();
    harness.responses().onNext(FetchResponse.getDefaultInstance());
    harness
        .responses()
        .onError(Status.INTERNAL.withDescription("server-sentinel token-0").asRuntimeException());
    harness.responses().onCompleted();
    harness.subscription().cancel();

    assertTrue(harness.subscription().isCancelled());
    assertEquals(0, downstream.next.get());
    assertEquals(0, downstream.errors.get());
    assertEquals(0, downstream.completed.get());
  }

  @Test
  void callbackBeforeCancellationIsDeliveredButLaterCallbacksAreSuppressed() {
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    SubscriptionHarness harness = subscriptionHarness(downstream);

    harness.responses().onNext(FetchResponse.getDefaultInstance());
    harness.subscription().cancel();
    harness.responses().onNext(FetchResponse.getDefaultInstance());
    harness.responses().onError(Status.INTERNAL.asRuntimeException());
    harness.responses().onCompleted();

    assertTrue(harness.subscription().isCancelled());
    assertEquals(1, downstream.next.get());
    assertEquals(0, downstream.errors.get());
    assertEquals(0, downstream.completed.get());
  }

  @Test
  void serverErrorBeforeCancellationRemainsTheSingleSanitizedTerminalSignal() {
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    SubscriptionHarness harness = subscriptionHarness(downstream);

    harness
        .responses()
        .onError(
            Status.INTERNAL
                .withDescription("server-sentinel token-0 instance-0 tenant-0")
                .asRuntimeException());
    harness.subscription().cancel();
    harness.responses().onCompleted();

    Exception failure =
        assertThrows(
            Exception.class,
            () -> harness.subscription().completion().toCompletableFuture().join());
    assertInstanceOf(SubscriptionException.class, failure.getCause());
    assertFalse(harness.subscription().isCancelled());
    assertSafe(failure.toString());
    assertEquals(0, downstream.next.get());
    assertEquals(1, downstream.errors.get());
    assertEquals(0, downstream.completed.get());
  }

  @Test
  void halfCloseThenCancelAndRepeatedTerminalCallsStayStable() {
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    SubscriptionHarness halfClosed = subscriptionHarness(downstream);

    halfClosed.subscription().complete();
    halfClosed.subscription().cancel();
    halfClosed.subscription().cancel();
    assertThrows(SalesforcePubSubException.class, halfClosed.subscription()::complete);
    halfClosed.responses().onCompleted();

    assertTrue(halfClosed.subscription().isCancelled());
    assertEquals(0, downstream.completed.get());

    RecordingObserver<FetchResponse> terminalDownstream = new RecordingObserver<>();
    SubscriptionHarness terminal = subscriptionHarness(terminalDownstream);
    terminal.responses().onCompleted();
    terminal.responses().onCompleted();
    terminal.responses().onError(Status.INTERNAL.asRuntimeException());
    terminal.subscription().cancel();

    terminal.subscription().completion().toCompletableFuture().join();
    assertFalse(terminal.subscription().isCancelled());
    assertEquals(1, terminalDownstream.completed.get());
    assertEquals(0, terminalDownstream.errors.get());
  }

  @Test
  void cancelledAndTerminatedSubscriptionMisuseIsTypedAsSubscriptionFailure() {
    SubscriptionHarness cancelled = subscriptionHarness(new RecordingObserver<>());
    cancelled.subscription().cancel();

    SubscriptionException cancelledFailure =
        assertThrows(
            SubscriptionException.class,
            () -> cancelled.subscription().send(FetchRequest.getDefaultInstance()));
    assertEquals("Salesforce Pub/Sub subscription is not active", cancelledFailure.getMessage());

    SubscriptionHarness terminated = subscriptionHarness(new RecordingObserver<>());
    terminated.responses().onCompleted();

    SubscriptionException terminatedFailure =
        assertThrows(SubscriptionException.class, terminated.subscription()::complete);
    assertEquals("Salesforce Pub/Sub subscription is not active", terminatedFailure.getMessage());
  }

  @Test
  void throwingDownstreamCallbacksBecomeSanitizedTerminalFailures() {
    ThrowingObserver onNext = new ThrowingObserver(true, true, false);
    SubscriptionHarness nextHarness = subscriptionHarness(onNext);
    nextHarness.responses().onNext(FetchResponse.getDefaultInstance());
    assertSanitizedExceptionalCompletion(nextHarness.subscription());
    assertEquals(1, onNext.next.get());
    assertEquals(1, onNext.errors.get());

    ThrowingObserver onError = new ThrowingObserver(false, true, false);
    SubscriptionHarness errorHarness = subscriptionHarness(onError);
    errorHarness
        .responses()
        .onError(Status.INTERNAL.withDescription("server-sentinel token-0").asRuntimeException());
    assertSanitizedExceptionalCompletion(errorHarness.subscription());
    assertEquals(1, onError.errors.get());

    ThrowingObserver onCompleted = new ThrowingObserver(false, false, true);
    SubscriptionHarness completedHarness = subscriptionHarness(onCompleted);
    completedHarness.responses().onCompleted();
    assertSanitizedExceptionalCompletion(completedHarness.subscription());
    assertEquals(1, onCompleted.completed.get());
  }

  @Test
  void invalidPublishRequestsUsePublishCategoryWithoutStartingAnRpc() {
    PublishRequest batch =
        PublishRequest.newBuilder()
            .addEvents(ProducerEvent.getDefaultInstance())
            .addEvents(ProducerEvent.getDefaultInstance())
            .build();

    PublishException nullFailure =
        assertThrows(PublishException.class, () -> transport.publish(null));
    PublishException emptyFailure =
        assertThrows(
            PublishException.class, () -> transport.publish(PublishRequest.getDefaultInstance()));
    PublishException batchFailure =
        assertThrows(PublishException.class, () -> transport.publish(batch));

    for (PublishException failure : List.of(nullFailure, emptyFailure, batchFailure)) {
      assertEquals(
          "Salesforce Pub/Sub unary Publish requires exactly one event", failure.getMessage());
      assertNull(failure.getCause());
    }
    assertEquals(0, receivedCalls.get());
    assertTrue(
        Arrays.stream(PubSubApiTransport.class.getDeclaredMethods())
            .map(java.lang.reflect.Method::getName)
            .noneMatch(name -> name.equals("managedSubscribe") || name.equals("publishStream")));
  }

  private void assertBlockedNewCallCannotDelayClose(boolean subscription) throws Exception {
    BlockingLifecycleManagedChannel blockingChannel =
        new BlockingLifecycleManagedChannel(true, false);
    PubSubApiTransport racingTransport = new PubSubApiTransport(blockingChannel, session(0));
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> invocation =
          subscription
              ? executor.submit(() -> racingTransport.subscribe(downstream))
              : executor.submit(() -> racingTransport.getTopic("blocked-new-call"));
      assertTrue(blockingChannel.newCallEntered.await(5, TimeUnit.SECONDS));

      assertTimeoutPreemptively(Duration.ofSeconds(1), racingTransport::close);
      assertTrue(blockingChannel.isShutdown());

      blockingChannel.release.countDown();
      Object operation = invocation.get(5, TimeUnit.SECONDS);
      assertTrue(blockingChannel.cancelled.await(1, TimeUnit.SECONDS));
      assertEquals(0, blockingChannel.starts.get());
      if (operation instanceof PubSubApiTransport.SubscriptionRpc stream) {
        assertTrue(stream.isCancelled());
      } else {
        awaitCancellation((CompletableFuture<?>) operation);
      }
      assertEquals(0, downstream.next.get());
      assertEquals(0, downstream.errors.get());
      assertEquals(0, downstream.completed.get());
    } finally {
      blockingChannel.release.countDown();
      racingTransport.close();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private void assertSanitizedExceptionalCompletion(
      PubSubApiTransport.SubscriptionRpc subscription) {
    Exception failure =
        assertThrows(Exception.class, () -> subscription.completion().toCompletableFuture().join());
    assertSafe(failure.toString());
    assertNotNull(failure.getCause());
    assertInstanceOf(SubscriptionException.class, failure.getCause());
    assertSafe(failure.getCause().toString());
  }

  private void assertPublicMutationDoesNotOwnRpc(boolean completeNormally) throws Exception {
    HoldingUnaryService service = new HoldingUnaryService();
    restartServer(service);
    CompletionStage<TopicMetadata> stage = transport.getTopic("held");
    assertTrue(service.responseSent.await(5, TimeUnit.SECONDS));
    CompletableFuture<TopicMetadata> publicView = stage.toCompletableFuture();

    if (completeNormally) {
      assertFalse(publicView.complete(new TopicMetadata("forged", false, false, "forged")));
    } else {
      assertFalse(publicView.completeExceptionally(new IllegalStateException("forged")));
    }
    assertThrows(
        UnsupportedOperationException.class,
        () -> publicView.obtrudeValue(new TopicMetadata("forged", false, false, "forged")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> publicView.obtrudeException(new IllegalStateException("forged")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> publicView.completeAsync(() -> new TopicMetadata("forged", false, false, "forged")));
    assertThrows(
        UnsupportedOperationException.class, () -> publicView.orTimeout(1, TimeUnit.MILLISECONDS));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            publicView.completeOnTimeout(
                new TopicMetadata("forged", false, false, "forged"), 1, TimeUnit.MILLISECONDS));

    assertFalse(service.cancelled.await(100, TimeUnit.MILLISECONDS));
    assertTrue(publicView.cancel(true));
    assertTrue(service.cancelled.await(5, TimeUnit.SECONDS));
    assertTrue(publicView.isCancelled());
  }

  private static SubscriptionHarness subscriptionHarness(StreamObserver<FetchResponse> downstream) {
    PubSubApiTransport.SubscriptionRpc subscription =
        new PubSubApiTransport.SubscriptionRpc(downstream);
    subscription.attachFallback(
        new StreamObserver<>() {
          @Override
          public void onNext(FetchRequest value) {}

          @Override
          public void onError(Throwable failure) {}

          @Override
          public void onCompleted() {}
        });
    return new SubscriptionHarness(subscription, subscription.responseObserver());
  }

  private static void await(CountDownLatch latch) {
    try {
      assertTrue(latch.await(5, TimeUnit.SECONDS));
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
      throw new AssertionError("Interrupted while awaiting race start", exception);
    }
  }

  private static void awaitCancellation(CompletableFuture<?> future) throws Exception {
    assertThrows(
        java.util.concurrent.CancellationException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertTrue(future.isCancelled());
  }

  private void assertTopicFailure(
      Status.Code code, Class<? extends SalesforcePubSubException> expectedType) throws Exception {
    restartServer(new CategorizedFailingService(code));
    Exception wrapper =
        assertThrows(
            Exception.class, () -> transport.getTopic("/event/Test__e").get(5, TimeUnit.SECONDS));
    SalesforcePubSubException failure = assertInstanceOf(expectedType, wrapper.getCause());
    assertNull(failure.getCause());
    assertSafe(failure.toString());
  }

  private void assertPublishFailure(
      Status.Code code, Class<? extends SalesforcePubSubException> expectedType) throws Exception {
    restartServer(new CategorizedFailingService(code));
    Exception wrapper =
        assertThrows(
            Exception.class,
            () ->
                transport.publish(singlePublish()).toCompletableFuture().get(5, TimeUnit.SECONDS));
    SalesforcePubSubException failure = assertInstanceOf(expectedType, wrapper.getCause());
    assertNull(failure.getCause());
    assertSafe(failure.toString());
    if (expectedType == PublishException.class) {
      assertEquals("Salesforce Pub/Sub publish failed [" + code.name() + "]", failure.getMessage());
    }
  }

  private void assertFailureLog(
      Status.Code code,
      String expectedLevel,
      Class<? extends SalesforcePubSubException> expectedType)
      throws Exception {
    RecordingLogger logger = new RecordingLogger();
    restartServer(new CategorizedFailingService(code), null, logger);

    Exception wrapper =
        assertThrows(
            Exception.class, () -> transport.getTopic("/event/Test__e").get(5, TimeUnit.SECONDS));
    assertInstanceOf(expectedType, wrapper.getCause());
    assertEquals(1, logger.events().size());
    RecordingLogger.LogEvent event = logger.events().getFirst();
    assertEquals(expectedLevel, event.level());
    assertEquals(
        java.util.Map.of("grpcStatus", code, "exceptionCategory", expectedType.getSimpleName()),
        event.keyValues());
    assertSafe(event.toString());
    assertFalse(event.toString().contains("payload"));
    assertFalse(event.toString().contains("replay"));
    assertFalse(event.toString().contains("PII"));
  }

  private void assertSubscriptionFailure(
      Status.Code code,
      Class<? extends SalesforcePubSubException> expectedType,
      String expectedLevel,
      ConnectorStatus expectedHealth)
      throws Exception {
    RecordingTelemetry telemetry = new RecordingTelemetry();
    ConnectorHealth health = new ConnectorHealth("connection", telemetry);
    RecordingLogger logger = new RecordingLogger();
    restartServer(new CategorizedFailingService(code), health, logger);
    RecordingObserver<FetchResponse> downstream = new RecordingObserver<>();

    PubSubApiTransport.SubscriptionRpc subscription = transport.subscribe(downstream);
    Exception wrapper =
        assertThrows(
            Exception.class,
            () -> subscription.completion().toCompletableFuture().get(5, TimeUnit.SECONDS));

    SalesforcePubSubException failure = assertInstanceOf(expectedType, wrapper.getCause());
    assertSame(failure, downstream.failure.get());
    assertNull(failure.getCause());
    if (expectedType == SubscriptionException.class) {
      assertEquals(
          "Salesforce Pub/Sub subscription failed [" + code.name() + "]", failure.getMessage());
    }
    assertSafe(failure.toString());
    assertEquals(expectedHealth, health.status());
    assertEquals(1, logger.events().size());
    RecordingLogger.LogEvent event = logger.events().getFirst();
    assertEquals(expectedLevel, event.level());
    assertEquals(
        java.util.Map.of("grpcStatus", code, "exceptionCategory", expectedType.getSimpleName()),
        event.keyValues());
    assertSafe(event.toString());
    assertFalse(event.toString().contains("payload"));
    assertFalse(event.toString().contains("replay"));
    assertFalse(event.toString().contains("PII"));
  }

  private void restartServer(PubSubGrpc.PubSubImplBase service) throws Exception {
    closeCurrentResources();
    received.clear();
    receivedCalls.set(0);
    startServer(service);
    transport = new PubSubApiTransport(channel, session(0));
  }

  private void restartServer(
      PubSubGrpc.PubSubImplBase service, ConnectorHealth health, RecordingLogger logger)
      throws Exception {
    closeCurrentResources();
    received.clear();
    receivedCalls.set(0);
    startServer(service);
    transport =
        new PubSubApiTransport(channel, session(0), () -> {}, () -> {}, health, logger.proxy());
  }

  private void restartServer(
      PubSubGrpc.PubSubImplBase service, Runnable beforeRpcStart, Runnable afterRpcStart)
      throws Exception {
    closeCurrentResources();
    received.clear();
    receivedCalls.set(0);
    startServer(service);
    transport = new PubSubApiTransport(channel, session(0), beforeRpcStart, afterRpcStart);
  }

  private void startServer(PubSubGrpc.PubSubImplBase service) throws Exception {
    String name = InProcessServerBuilder.generateName();
    ServerInterceptor metadataInterceptor =
        new ServerInterceptor() {
          @Override
          public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
              ServerCall<RequestT, ResponseT> call,
              Metadata headers,
              ServerCallHandler<RequestT, ResponseT> next) {
            ObservedCall captured =
                new ObservedCall(
                    call.getMethodDescriptor().getFullMethodName(),
                    new SessionHeaders(
                        headers.get(ACCESS_TOKEN),
                        headers.get(INSTANCE_URL),
                        headers.get(TENANT_ID)));
            received.add(captured);
            return Contexts.interceptCall(
                Context.current().withValue(OBSERVED_CALL, captured), call, headers, next);
          }
        };
    server =
        InProcessServerBuilder.forName(name)
            .directExecutor()
            .addService(ServerInterceptors.intercept(service, metadataInterceptor))
            .addService(ServerInterceptors.intercept(plainService(), metadataInterceptor))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(name).directExecutor().build();
  }

  private void closeCurrentResources() throws Exception {
    if (transport != null) {
      transport.close();
      transport = null;
    }
    if (channel != null) {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      channel = null;
    }
    if (server != null) {
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server = null;
    }
  }

  private CompletableFuture<StringValue> plainCall(String value) {
    CompletableFuture<StringValue> result = new CompletableFuture<>();
    ClientCalls.asyncUnaryCall(
        channel.newCall(PLAIN_METHOD, CallOptions.DEFAULT),
        StringValue.of(value),
        singleResponseObserver(result));
    return result;
  }

  private void assertInsecureCredentialsRejected(SecurityLevel securityLevel) {
    assertCredentialsRejected(requestInfo(securityLevel));
  }

  private void assertCredentialsRejected(CallCredentials.RequestInfo requestInfo) {
    SalesforceCallCredentials credentials = new SalesforceCallCredentials(session(0));
    AtomicReference<Metadata> applied = new AtomicReference<>();
    AtomicReference<Status> failed = new AtomicReference<>();

    credentials.applyRequestMetadata(
        requestInfo,
        Runnable::run,
        new CallCredentials.MetadataApplier() {
          @Override
          public void apply(Metadata headers) {
            applied.set(headers);
          }

          @Override
          public void fail(Status status) {
            failed.set(status);
          }
        });

    assertNull(applied.get());
    assertNotNull(failed.get());
    assertEquals(Status.Code.UNAUTHENTICATED, failed.get().getCode());
    assertSafe(failed.get().toString());
  }

  private static CallCredentials.RequestInfo requestInfo(SecurityLevel securityLevel) {
    return requestInfo(securityLevel, PubSubGrpc.getGetTopicMethod());
  }

  private static CallCredentials.RequestInfo requestInfo(
      SecurityLevel securityLevel, MethodDescriptor<?, ?> method) {
    return new CallCredentials.RequestInfo() {
      @Override
      public MethodDescriptor<?, ?> getMethodDescriptor() {
        return method;
      }

      @Override
      public SecurityLevel getSecurityLevel() {
        return securityLevel;
      }

      @Override
      public String getAuthority() {
        return "server-sentinel-authority";
      }

      @Override
      public Attributes getTransportAttrs() {
        return Attributes.EMPTY;
      }
    };
  }

  private static ServerServiceDefinition plainService() {
    return ServerServiceDefinition.builder("test.Plain")
        .addMethod(
            PLAIN_METHOD,
            ServerCalls.asyncUnaryCall(
                (StringValue request, StreamObserver<StringValue> observer) -> {
                  observer.onNext(request);
                  observer.onCompleted();
                }))
        .build();
  }

  private PublishRequest singlePublish() {
    return PublishRequest.newBuilder()
        .setTopicName("/event/Test__e")
        .addEvents(ProducerEvent.newBuilder().setId("one").build())
        .build();
  }

  private static <T> StreamObserver<T> singleResponseObserver(CompletableFuture<T> result) {
    return new StreamObserver<>() {
      @Override
      public void onNext(T value) {
        result.complete(value);
      }

      @Override
      public void onError(Throwable failure) {
        result.completeExceptionally(failure);
      }

      @Override
      public void onCompleted() {
        // Unary-style result completes in onNext.
      }
    };
  }

  private void assertObserved(String method, ObservedCall call, int generation) {
    assertTrue(call.method().endsWith("/" + method));
    assertEquals(
        new SessionHeaders(
            "token-" + generation,
            "https://instance-" + generation + ".example",
            "tenant-" + generation),
        call.headers());
  }

  private void assertRefreshOrderingForAllAllowedRpcShapes(
      RpcStartBarrier barrier, int expectedGenerationOffset) throws Exception {
    List<Runnable> calls =
        List.of(
            () -> transport.getTopic("refresh-race").join(),
            () -> transport.getSchema("refresh-race").join(),
            () -> transport.publish(singlePublish()).toCompletableFuture().join(),
            () -> {
              CompletableFuture<FetchResponse> response = new CompletableFuture<>();
              PubSubApiTransport.SubscriptionRpc subscription =
                  transport.subscribe(singleResponseObserver(response));
              subscription.send(
                  FetchRequest.newBuilder().setTopicName("/event/RefreshRace__e").build());
              response.join();
              subscription.complete();
              subscription.completion().toCompletableFuture().join();
            });
    List<String> methods = List.of("GetTopic", "GetSchema", "Publish", "Subscribe");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      for (int index = 0; index < calls.size(); index++) {
        RpcStartGate gate = barrier.arm();
        Future<?> call = executor.submit(calls.get(index));
        assertTrue(gate.entered().await(5, TimeUnit.SECONDS));
        try {
          transport.updateSession(session(index + 1));
        } finally {
          gate.release().countDown();
        }
        call.get(5, TimeUnit.SECONDS);
        assertObserved(methods.get(index), received.remove(), index + expectedGenerationOffset);
      }
    } finally {
      barrier.releaseCurrent();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private void assertSafe(String value) {
    assertFalse(value.contains("token-0"));
    assertFalse(value.contains("instance-0"));
    assertFalse(value.contains("tenant-0"));
    assertFalse(value.contains("server-sentinel"));
  }

  private SalesforceSession session(int generation) {
    return new SalesforceSession(
        "token-" + generation,
        "https://instance-" + generation + ".example",
        "tenant-" + generation,
        "user-" + generation);
  }

  private record SessionHeaders(String accessToken, String instanceUrl, String tenantId) {}

  private record ObservedCall(String method, SessionHeaders headers) {}

  private record SubscriptionStateObservation(
      String connectionName, String topic, ConnectorStatus status) {}

  private record SubscribedObservation(String connectionName, String topic, String consumerName) {}

  private static final class RecordingTelemetry implements SalesforcePubSubTelemetry {
    private final List<ConnectorStatus> statuses = new ArrayList<>();
    private final List<SubscriptionStateObservation> subscriptionStates = new ArrayList<>();
    private final List<SubscribedObservation> subscriptions = new ArrayList<>();
    private final AtomicInteger connected = new AtomicInteger();

    @Override
    public void connectionState(String connectionName, ConnectorStatus status) {
      statuses.add(status);
    }

    @Override
    public void connected(String connectionName) {
      connected.incrementAndGet();
    }

    @Override
    public void subscriptionState(String connectionName, String topic, ConnectorStatus status) {
      subscriptionStates.add(new SubscriptionStateObservation(connectionName, topic, status));
    }

    @Override
    public void subscribed(String connectionName, String topic, String consumerName) {
      subscriptions.add(new SubscribedObservation(connectionName, topic, consumerName));
    }
  }

  private record SubscriptionHarness(
      PubSubApiTransport.SubscriptionRpc subscription,
      ClientResponseObserver<FetchRequest, FetchResponse> responses) {}

  private record RpcStartGate(CountDownLatch entered, CountDownLatch release) {}

  private static final class RpcStartBarrier implements Runnable {

    private final AtomicReference<RpcStartGate> current = new AtomicReference<>();

    private RpcStartGate arm() {
      RpcStartGate gate = new RpcStartGate(new CountDownLatch(1), new CountDownLatch(1));
      assertTrue(current.compareAndSet(null, gate));
      return gate;
    }

    @Override
    public void run() {
      RpcStartGate gate = current.get();
      if (gate == null) {
        return;
      }
      gate.entered().countDown();
      await(gate.release());
      current.compareAndSet(gate, null);
    }

    private void releaseCurrent() {
      RpcStartGate gate = current.get();
      if (gate != null) {
        gate.release().countDown();
      }
    }
  }

  private final class RespondingService extends PubSubGrpc.PubSubImplBase {

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      receivedCalls.incrementAndGet();
      observer.onNext(
          TopicInfo.newBuilder()
              .setTopicName(request.getTopicName())
              .setTenantGuid("org")
              .setCanPublish(true)
              .setCanSubscribe(true)
              .setSchemaId("schema-1")
              .build());
      observer.onCompleted();
    }

    @Override
    public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> observer) {
      receivedCalls.incrementAndGet();
      observer.onNext(
          SchemaInfo.newBuilder().setSchemaId(request.getSchemaId()).setSchemaJson("{}").build());
      observer.onCompleted();
    }

    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> observer) {
      assertEquals(1, request.getEventsCount());
      receivedCalls.incrementAndGet();
      observer.onNext(PublishResponse.newBuilder().setRpcId("publish").build());
      observer.onCompleted();
    }

    @Override
    public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> observer) {
      receivedCalls.incrementAndGet();
      return new StreamObserver<>() {
        @Override
        public void onNext(FetchRequest request) {
          observer.onNext(FetchResponse.newBuilder().setRpcId("subscribe").build());
        }

        @Override
        public void onError(Throwable failure) {
          // Client cancellation ends this fake stream.
        }

        @Override
        public void onCompleted() {
          observer.onCompleted();
        }
      };
    }
  }

  private static final class HoldingUnaryService extends PubSubGrpc.PubSubImplBase {

    private final java.util.concurrent.CountDownLatch responseSent =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch cancelled =
        new java.util.concurrent.CountDownLatch(1);
    private ServerCallStreamObserver<TopicInfo> responses;

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      responses = (ServerCallStreamObserver<TopicInfo>) observer;
      responses.setOnCancelHandler(cancelled::countDown);
      observer.onNext(TopicInfo.newBuilder().setTopicName(request.getTopicName()).build());
      responseSent.countDown();
    }

    private void completeLate() {
      try {
        responses.onCompleted();
      } catch (RuntimeException ignored) {
        // The in-process server may reject writes after client cancellation.
      }
    }

    private void errorLate() {
      try {
        responses.onError(Status.INTERNAL.withDescription("server-sentinel").asRuntimeException());
      } catch (RuntimeException ignored) {
        // The in-process server may reject a duplicate terminal signal.
      }
    }
  }

  private static final class ResponseThenErrorService extends PubSubGrpc.PubSubImplBase {

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      observer.onNext(TopicInfo.newBuilder().setTopicName(request.getTopicName()).build());
      observer.onError(
          Status.INTERNAL
              .withDescription("server-sentinel token-0 instance-0 tenant-0")
              .asRuntimeException());
    }
  }

  private static final class EarlyCallbackService extends PubSubGrpc.PubSubImplBase {

    @Override
    public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> observer) {
      observer.onNext(FetchResponse.newBuilder().setRpcId("early").build());
      return new StreamObserver<>() {
        @Override
        public void onNext(FetchRequest request) {}

        @Override
        public void onError(Throwable failure) {}

        @Override
        public void onCompleted() {}
      };
    }
  }

  private static final class RacingSubscriptionService extends PubSubGrpc.PubSubImplBase {

    private final boolean blockRequest;
    private final java.util.concurrent.CountDownLatch subscribed =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch requestEntered =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch releaseRequest =
        new java.util.concurrent.CountDownLatch(1);
    private final AtomicInteger requests = new AtomicInteger();
    private ServerCallStreamObserver<FetchResponse> responses;

    private RacingSubscriptionService(boolean blockRequest) {
      this.blockRequest = blockRequest;
    }

    @Override
    public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> observer) {
      responses = (ServerCallStreamObserver<FetchResponse>) observer;
      subscribed.countDown();
      return new StreamObserver<>() {
        @Override
        public void onNext(FetchRequest request) {
          requests.incrementAndGet();
          requestEntered.countDown();
          if (blockRequest) {
            try {
              assertTrue(releaseRequest.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
              throw new AssertionError("Interrupted while holding request", exception);
            }
          }
        }

        @Override
        public void onError(Throwable failure) {
          // Expected for client cancellation.
        }

        @Override
        public void onCompleted() {
          // Tests control the server terminal callback directly.
        }
      };
    }

    private void emitAfterCancellation() {
      try {
        responses.onNext(FetchResponse.newBuilder().setRpcId("late").build());
        responses.onCompleted();
      } catch (RuntimeException ignored) {
        // The in-process server may reject writes after cancellation.
      }
    }

    private void emitNext() {
      responses.onNext(FetchResponse.newBuilder().setRpcId("next").build());
    }

    private void completeThenError() {
      responses.onCompleted();
      try {
        responses.onError(Status.INTERNAL.withDescription("server-sentinel").asRuntimeException());
      } catch (RuntimeException ignored) {
        // The server API may reject a duplicate terminal signal itself.
      }
    }
  }

  private static final class RecordingObserver<T> implements StreamObserver<T> {

    private final AtomicInteger next = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final CountDownLatch nextReceived;

    private RecordingObserver() {
      this(0);
    }

    private RecordingObserver(int expectedResponses) {
      nextReceived = new CountDownLatch(expectedResponses);
    }

    @Override
    public void onNext(T value) {
      next.incrementAndGet();
      nextReceived.countDown();
    }

    @Override
    public void onError(Throwable failure) {
      errors.incrementAndGet();
      this.failure.set(failure);
    }

    @Override
    public void onCompleted() {
      completed.incrementAndGet();
    }
  }

  private static final class BlockingObserver implements StreamObserver<FetchResponse> {

    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger next = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();

    @Override
    public void onNext(FetchResponse value) {
      next.incrementAndGet();
      entered.countDown();
      await(release);
    }

    @Override
    public void onError(Throwable failure) {
      errors.incrementAndGet();
    }

    @Override
    public void onCompleted() {
      completed.incrementAndGet();
    }
  }

  private static final class BlockingClientRequestObserver
      extends ClientCallStreamObserver<FetchRequest> {

    private final boolean stallSend;
    private final CountDownLatch outboundEntered = new CountDownLatch(1);
    private final CountDownLatch releaseOutbound = new CountDownLatch(1);
    private final CountDownLatch cancelled = new CountDownLatch(1);

    private BlockingClientRequestObserver(boolean stallSend) {
      this.stallSend = stallSend;
    }

    @Override
    public void cancel(String message, Throwable cause) {
      cancelled.countDown();
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int count) {}

    @Override
    public void setMessageCompression(boolean enabled) {}

    @Override
    public void onNext(FetchRequest request) {
      if (stallSend) {
        blockOutbound();
      }
    }

    @Override
    public void onError(Throwable failure) {}

    @Override
    public void onCompleted() {
      if (!stallSend) {
        blockOutbound();
      }
    }

    private void blockOutbound() {
      outboundEntered.countDown();
      await(releaseOutbound);
    }
  }

  private static final class ThrowingClientRequestObserver<RequestT>
      extends ClientCallStreamObserver<RequestT> {

    private final boolean throwOnNext;
    private final boolean throwOnCompleted;
    private final boolean throwOnCancel;
    private final AtomicInteger cancels = new AtomicInteger();

    private ThrowingClientRequestObserver(
        boolean throwOnNext, boolean throwOnCompleted, boolean throwOnCancel) {
      this.throwOnNext = throwOnNext;
      this.throwOnCompleted = throwOnCompleted;
      this.throwOnCancel = throwOnCancel;
    }

    @Override
    public void cancel(String message, Throwable cause) {
      cancels.incrementAndGet();
      if (throwOnCancel) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }

    @Override
    public boolean isReady() {
      return true;
    }

    @Override
    public void setOnReadyHandler(Runnable handler) {}

    @Override
    public void disableAutoInboundFlowControl() {}

    @Override
    public void request(int count) {}

    @Override
    public void setMessageCompression(boolean enabled) {}

    @Override
    public void onNext(RequestT request) {
      if (throwOnNext) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }

    @Override
    public void onError(Throwable failure) {}

    @Override
    public void onCompleted() {
      if (throwOnCompleted) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }
  }

  private static final class ThrowingCancelClientCall<RequestT, ResponseT>
      extends ClientCall<RequestT, ResponseT> {

    private final AtomicInteger cancels = new AtomicInteger();

    @Override
    public void start(Listener<ResponseT> responseListener, Metadata headers) {}

    @Override
    public void request(int count) {}

    @Override
    public void cancel(String message, Throwable cause) {
      cancels.incrementAndGet();
      throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
    }

    @Override
    public void halfClose() {}

    @Override
    public void sendMessage(RequestT message) {}
  }

  private static final class BlockingStartClientCall<RequestT, ResponseT>
      extends ClientCall<RequestT, ResponseT> {

    private final CountDownLatch startEntered = new CountDownLatch(1);
    private final CountDownLatch releaseStart = new CountDownLatch(1);
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger sends = new AtomicInteger();
    private final AtomicInteger halfCloses = new AtomicInteger();
    private final AtomicInteger compressions = new AtomicInteger();

    @Override
    public void start(Listener<ResponseT> responseListener, Metadata headers) {
      startEntered.countDown();
      await(releaseStart);
    }

    @Override
    public void request(int count) {
      requests.incrementAndGet();
    }

    @Override
    public void cancel(String message, Throwable cause) {}

    @Override
    public void halfClose() {
      halfCloses.incrementAndGet();
    }

    @Override
    public void sendMessage(RequestT message) {
      sends.incrementAndGet();
    }

    @Override
    public void setMessageCompression(boolean enabled) {
      compressions.incrementAndGet();
    }
  }

  private static final class ThrowingObserver implements StreamObserver<FetchResponse> {

    private final boolean throwOnNext;
    private final boolean throwOnError;
    private final boolean throwOnCompleted;
    private final AtomicInteger next = new AtomicInteger();
    private final AtomicInteger errors = new AtomicInteger();
    private final AtomicInteger completed = new AtomicInteger();

    private ThrowingObserver(boolean throwOnNext, boolean throwOnError, boolean throwOnCompleted) {
      this.throwOnNext = throwOnNext;
      this.throwOnError = throwOnError;
      this.throwOnCompleted = throwOnCompleted;
    }

    @Override
    public void onNext(FetchResponse value) {
      next.incrementAndGet();
      if (throwOnNext) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }

    @Override
    public void onError(Throwable failure) {
      errors.incrementAndGet();
      if (throwOnError) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }

    @Override
    public void onCompleted() {
      completed.incrementAndGet();
      if (throwOnCompleted) {
        throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
      }
    }
  }

  private static final class FailingService extends PubSubGrpc.PubSubImplBase {

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      observer.onError(
          Status.INTERNAL
              .withDescription("server-sentinel token-0 instance-0 tenant-0")
              .withCause(new IllegalStateException("server-sentinel cause"))
              .asRuntimeException());
    }
  }

  private static final class CategorizedFailingService extends PubSubGrpc.PubSubImplBase {

    private final Status.Code code;

    private CategorizedFailingService(Status.Code code) {
      this.code = code;
    }

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      observer.onError(failure());
    }

    @Override
    public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> observer) {
      observer.onError(failure());
    }

    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> observer) {
      observer.onError(failure());
    }

    @Override
    public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> observer) {
      observer.onError(failure());
      return new StreamObserver<>() {
        @Override
        public void onNext(FetchRequest request) {}

        @Override
        public void onError(Throwable failure) {}

        @Override
        public void onCompleted() {}
      };
    }

    private RuntimeException failure() {
      return Status.fromCode(code)
          .withDescription("server-sentinel token-0 instance-0 tenant-0 payload replay PII")
          .withCause(new IllegalStateException("server-sentinel cause"))
          .asRuntimeException();
    }
  }

  private static final class DelayedService extends PubSubGrpc.PubSubImplBase {

    private final java.util.concurrent.CountDownLatch started =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch subscriptionStarted =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch unaryCancelled =
        new java.util.concurrent.CountDownLatch(1);
    private final java.util.concurrent.CountDownLatch subscriptionCancelled =
        new java.util.concurrent.CountDownLatch(1);

    @Override
    public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
      ((ServerCallStreamObserver<TopicInfo>) observer)
          .setOnCancelHandler(unaryCancelled::countDown);
      started.countDown();
    }

    @Override
    public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> observer) {
      ((ServerCallStreamObserver<FetchResponse>) observer)
          .setOnCancelHandler(subscriptionCancelled::countDown);
      return new StreamObserver<>() {
        @Override
        public void onNext(FetchRequest request) {
          subscriptionStarted.countDown();
        }

        @Override
        public void onError(Throwable failure) {
          // Expected when the client cancels.
        }

        @Override
        public void onCompleted() {
          // This stream deliberately stays open.
        }
      };
    }
  }

  private static final class ThrowingManagedChannel extends ManagedChannel {

    private boolean shutdown;

    @Override
    public ManagedChannel shutdown() {
      shutdown = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public ManagedChannel shutdownNow() {
      return shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
      throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
    }

    @Override
    public String authority() {
      return "safe-authority";
    }
  }

  private static final class FailingStartManagedChannel extends ManagedChannel {

    private final boolean throwOnCancel;
    private final AtomicInteger starts = new AtomicInteger();
    private final AtomicInteger cancels = new AtomicInteger();
    private volatile boolean shutdown;

    private FailingStartManagedChannel(boolean throwOnCancel) {
      this.throwOnCancel = throwOnCancel;
    }

    @Override
    public ManagedChannel shutdown() {
      shutdown = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public ManagedChannel shutdownNow() {
      return shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
      return new ClientCall<>() {
        @Override
        public void start(Listener<ResponseT> responseListener, Metadata headers) {
          starts.incrementAndGet();
          throw new IllegalStateException("server-sentinel token-0 instance-0 tenant-0");
        }

        @Override
        public void request(int count) {}

        @Override
        public void cancel(String message, Throwable cause) {
          cancels.incrementAndGet();
          if (throwOnCancel) {
            throw new IllegalStateException("cleanup-sentinel token-0 instance-0 tenant-0");
          }
        }

        @Override
        public void halfClose() {}

        @Override
        public void sendMessage(RequestT message) {}
      };
    }

    @Override
    public String authority() {
      return "safe-authority";
    }
  }

  private static final class CountingManagedChannel extends ManagedChannel {

    private final AtomicInteger newCalls = new AtomicInteger();
    private volatile boolean shutdown;

    @Override
    public ManagedChannel shutdown() {
      shutdown = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public ManagedChannel shutdownNow() {
      return shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
      newCalls.incrementAndGet();
      throw new AssertionError("RPC started after close won registration race");
    }

    @Override
    public String authority() {
      return "safe-authority";
    }
  }

  private static final class BlockingLifecycleManagedChannel extends ManagedChannel {

    private final boolean blockNewCall;
    private final boolean blockStart;
    private final boolean callbackOnStart;
    private final CountDownLatch newCallEntered = new CountDownLatch(1);
    private final CountDownLatch startEntered = new CountDownLatch(1);
    private final CountDownLatch cancelled = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger starts = new AtomicInteger();
    private volatile boolean shutdown;

    private BlockingLifecycleManagedChannel(boolean blockNewCall, boolean blockStart) {
      this(blockNewCall, blockStart, false);
    }

    private BlockingLifecycleManagedChannel(
        boolean blockNewCall, boolean blockStart, boolean callbackOnStart) {
      this.blockNewCall = blockNewCall;
      this.blockStart = blockStart;
      this.callbackOnStart = callbackOnStart;
    }

    @Override
    public ManagedChannel shutdown() {
      shutdown = true;
      return this;
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public ManagedChannel shutdownNow() {
      return shutdown();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return shutdown;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
        MethodDescriptor<RequestT, ResponseT> method, CallOptions callOptions) {
      newCallEntered.countDown();
      if (blockNewCall) {
        await(release);
      }
      return new ClientCall<>() {
        @Override
        public void start(Listener<ResponseT> responseListener, Metadata headers) {
          starts.incrementAndGet();
          startEntered.countDown();
          if (callbackOnStart) {
            emitFetchResponse(responseListener);
          }
          if (blockStart) {
            await(release);
          }
        }

        @Override
        public void request(int count) {}

        @Override
        public void cancel(String message, Throwable cause) {
          cancelled.countDown();
        }

        @Override
        public void halfClose() {}

        @Override
        public void sendMessage(RequestT message) {}
      };
    }

    @Override
    public String authority() {
      return "safe-authority";
    }

    @SuppressWarnings("unchecked")
    private static <ResponseT> void emitFetchResponse(
        ClientCall.Listener<ResponseT> responseListener) {
      responseListener.onMessage((ResponseT) FetchResponse.getDefaultInstance());
    }
  }
}
