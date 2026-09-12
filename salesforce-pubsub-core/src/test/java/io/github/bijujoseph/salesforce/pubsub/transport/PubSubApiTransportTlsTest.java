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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.StringValue;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.config.EndpointConfig;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.SelfSignedCertificate;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.RepeatedTest;

class PubSubApiTransportTlsTest {

  private static final Metadata.Key<String> ACCESS_TOKEN =
      Metadata.Key.of("accesstoken", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> INSTANCE_URL =
      Metadata.Key.of("instanceurl", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TENANT_ID =
      Metadata.Key.of("tenantid", Metadata.ASCII_STRING_MARSHALLER);
  private static final MethodDescriptor<StringValue, StringValue> PLAIN_METHOD =
      MethodDescriptor.<StringValue, StringValue>newBuilder()
          .setType(MethodDescriptor.MethodType.UNARY)
          .setFullMethodName(MethodDescriptor.generateFullMethodName("test.Plain", "Echo"))
          .setRequestMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
          .setResponseMarshaller(ProtoUtils.marshaller(StringValue.getDefaultInstance()))
          .build();

  @RepeatedTest(3)
  void publicConstructorUsesTlsAndScopesCredentialsToSalesforceMethods() throws Exception {
    String previousStore = System.getProperty("javax.net.ssl.trustStore");
    String previousPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String previousType = System.getProperty("javax.net.ssl.trustStoreType");
    SelfSignedCertificate trustedCertificate = null;
    SelfSignedCertificate untrustedCertificate = null;
    Path trustStore = null;
    Server trustedServer = null;
    Server hostnameMismatchServer = null;
    Server untrustedServer = null;
    Server plaintextServer = null;
    PubSubApiTransport transport = null;
    ManagedChannel unrelatedChannel = null;
    PubSubApiTransport hostnameMismatchTransport = null;
    PubSubApiTransport untrustedTransport = null;
    PubSubApiTransport plaintextTransport = null;
    try {
      Instant now = Instant.now();
      Date notBefore = Date.from(now.minus(Duration.ofDays(2)));
      Date notAfter = Date.from(now.plus(Duration.ofDays(7)));
      trustedCertificate = new SelfSignedCertificate("localhost", notBefore, notAfter);
      untrustedCertificate = new SelfSignedCertificate("localhost", notBefore, notAfter);
      trustStore = createTrustStore(trustedCertificate);
      System.setProperty("javax.net.ssl.trustStore", trustStore.toString());
      System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
      System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());

      AtomicReference<Metadata> salesforceHeaders = new AtomicReference<>();
      AtomicInteger trustedInvocations = new AtomicInteger();
      AtomicInteger unrelatedInvocations = new AtomicInteger();
      trustedServer =
          tlsServer(trustedCertificate, salesforceHeaders, trustedInvocations, unrelatedInvocations)
              .build()
              .start();
      transport =
          new PubSubApiTransport(
              new EndpointConfig("localhost", trustedServer.getPort()), session());

      TopicMetadata topic = transport.getTopic("/event/Tls__e").get(5, TimeUnit.SECONDS);

      assertEquals("/event/Tls__e", topic.topicName());
      Metadata headers = salesforceHeaders.get();
      assertNotNull(headers);
      assertEquals("tls-token", headers.get(ACCESS_TOKEN));
      assertEquals("https://tls-instance.example", headers.get(INSTANCE_URL));
      assertEquals("tls-tenant", headers.get(TENANT_ID));
      assertEquals(1, trustedInvocations.get());

      unrelatedChannel =
          NettyChannelBuilder.forAddress("localhost", trustedServer.getPort())
              .useTransportSecurity()
              .build();
      CompletableFuture<StringValue> unrelated = new CompletableFuture<>();
      ClientCall<StringValue, StringValue> call =
          unrelatedChannel.newCall(
              PLAIN_METHOD,
              CallOptions.DEFAULT.withCallCredentials(new SalesforceCallCredentials(session())));
      ClientCalls.asyncUnaryCall(call, StringValue.of("plain"), observer(unrelated));
      assertThrows(Exception.class, () -> unrelated.get(5, TimeUnit.SECONDS));
      assertEquals(0, unrelatedInvocations.get());

      AtomicInteger hostnameMismatchInvocations = new AtomicInteger();
      hostnameMismatchServer =
          tlsServer(
                  trustedCertificate,
                  new AtomicReference<>(),
                  hostnameMismatchInvocations,
                  new AtomicInteger())
              .build()
              .start();
      hostnameMismatchTransport =
          new PubSubApiTransport(
              new EndpointConfig("127.0.0.1", hostnameMismatchServer.getPort()), session());
      PubSubApiTransport rejectedHostname = hostnameMismatchTransport;
      assertThrows(
          Exception.class,
          () -> rejectedHostname.getTopic("hostname-mismatch").get(5, TimeUnit.SECONDS));
      assertEquals(0, hostnameMismatchInvocations.get());

      AtomicInteger untrustedInvocations = new AtomicInteger();
      untrustedServer =
          tlsServer(
                  untrustedCertificate,
                  new AtomicReference<>(),
                  untrustedInvocations,
                  new AtomicInteger())
              .build()
              .start();
      untrustedTransport =
          new PubSubApiTransport(
              new EndpointConfig("localhost", untrustedServer.getPort()), session());
      PubSubApiTransport rejectedUntrusted = untrustedTransport;
      assertThrows(
          Exception.class, () -> rejectedUntrusted.getTopic("untrusted").get(5, TimeUnit.SECONDS));
      assertEquals(0, untrustedInvocations.get());

      AtomicInteger plaintextInvocations = new AtomicInteger();
      plaintextServer = plaintextServer(plaintextInvocations).build().start();
      plaintextTransport =
          new PubSubApiTransport(
              new EndpointConfig("localhost", plaintextServer.getPort()), session());
      PubSubApiTransport rejectedPlaintext = plaintextTransport;
      assertThrows(
          Exception.class, () -> rejectedPlaintext.getTopic("plaintext").get(5, TimeUnit.SECONDS));
      assertEquals(0, plaintextInvocations.get());
    } finally {
      close(transport);
      close(hostnameMismatchTransport);
      close(untrustedTransport);
      close(plaintextTransport);
      shutdown(unrelatedChannel);
      shutdown(trustedServer);
      shutdown(hostnameMismatchServer);
      shutdown(untrustedServer);
      shutdown(plaintextServer);
      restoreProperty("javax.net.ssl.trustStore", previousStore);
      restoreProperty("javax.net.ssl.trustStorePassword", previousPassword);
      restoreProperty("javax.net.ssl.trustStoreType", previousType);
      if (trustStore != null) {
        Files.deleteIfExists(trustStore);
      }
      if (trustedCertificate != null) {
        trustedCertificate.delete();
      }
      if (untrustedCertificate != null) {
        untrustedCertificate.delete();
      }
    }
  }

  private static NettyServerBuilder tlsServer(
      SelfSignedCertificate certificate,
      AtomicReference<Metadata> salesforceHeaders,
      AtomicInteger salesforceInvocations,
      AtomicInteger unrelatedInvocations)
      throws Exception {
    return NettyServerBuilder.forPort(0)
        .sslContext(
            GrpcSslContexts.forServer(certificate.certificate(), certificate.privateKey()).build())
        .addService(
            ServerInterceptors.intercept(
                topicService(salesforceInvocations), metadataInterceptor(salesforceHeaders)))
        .addService(plainService(unrelatedInvocations));
  }

  private static NettyServerBuilder plaintextServer(AtomicInteger invocations) {
    return NettyServerBuilder.forPort(0).addService(topicService(invocations));
  }

  private static PubSubGrpc.PubSubImplBase topicService(AtomicInteger invocations) {
    return new PubSubGrpc.PubSubImplBase() {
      @Override
      public void getTopic(TopicRequest request, StreamObserver<TopicInfo> observer) {
        invocations.incrementAndGet();
        observer.onNext(
            TopicInfo.newBuilder()
                .setTopicName(request.getTopicName())
                .setSchemaId("schema")
                .build());
        observer.onCompleted();
      }
    };
  }

  private static ServerInterceptor metadataInterceptor(AtomicReference<Metadata> captured) {
    return new ServerInterceptor() {
      @Override
      public <RequestT, ResponseT> ServerCall.Listener<RequestT> interceptCall(
          ServerCall<RequestT, ResponseT> call,
          Metadata headers,
          ServerCallHandler<RequestT, ResponseT> next) {
        captured.set(headers);
        return next.startCall(call, headers);
      }
    };
  }

  private static ServerServiceDefinition plainService(AtomicInteger invocations) {
    return ServerServiceDefinition.builder("test.Plain")
        .addMethod(
            PLAIN_METHOD,
            ServerCalls.asyncUnaryCall(
                (StringValue request, StreamObserver<StringValue> observer) -> {
                  invocations.incrementAndGet();
                  observer.onNext(request);
                  observer.onCompleted();
                }))
        .build();
  }

  private static Path createTrustStore(SelfSignedCertificate certificate) throws Exception {
    char[] password = "changeit".toCharArray();
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, password);
    keyStore.setCertificateEntry("local-test", certificate.cert());
    Path path = Files.createTempFile("osp-022-trust-", ".p12");
    try {
      try (OutputStream output = Files.newOutputStream(path)) {
        keyStore.store(output, password);
      }
      return path;
    } catch (Exception exception) {
      Files.deleteIfExists(path);
      throw exception;
    }
  }

  private static SalesforceSession session() {
    return new SalesforceSession(
        "tls-token", "https://tls-instance.example", "tls-tenant", "tls-user");
  }

  private static <T> StreamObserver<T> observer(CompletableFuture<T> result) {
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
      public void onCompleted() {}
    };
  }

  private static void close(PubSubApiTransport transport) {
    if (transport != null) {
      transport.close();
    }
  }

  private static void shutdown(ManagedChannel channel) throws InterruptedException {
    if (channel != null) {
      channel.shutdownNow();
      assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void shutdown(Server server) throws InterruptedException {
    if (server != null) {
      server.shutdownNow();
      assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void restoreProperty(String name, String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }
}
