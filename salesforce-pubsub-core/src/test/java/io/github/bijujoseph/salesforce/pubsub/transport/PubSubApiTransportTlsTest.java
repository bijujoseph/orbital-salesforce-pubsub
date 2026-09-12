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
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

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

  @Test
  void publicConstructorUsesTlsAndScopesCredentialsToSalesforceMethods() throws Exception {
    Path trustedCertificate = resource("tls/localhost.crt");
    Path trustedKey = materializePrivateKey("tls/localhost-key.b64");
    Path trustStore = createTrustStore(trustedCertificate);
    String previousStore = System.getProperty("javax.net.ssl.trustStore");
    String previousPassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String previousType = System.getProperty("javax.net.ssl.trustStoreType");
    Server trustedServer = null;
    Server plaintextServer = null;
    PubSubApiTransport transport = null;
    ManagedChannel unrelatedChannel = null;
    PubSubApiTransport untrustedTransport = null;
    PubSubApiTransport plaintextTransport = null;
    try {
      System.setProperty("javax.net.ssl.trustStore", trustStore.toString());
      System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
      System.setProperty("javax.net.ssl.trustStoreType", KeyStore.getDefaultType());

      AtomicReference<Metadata> salesforceHeaders = new AtomicReference<>();
      AtomicInteger unrelatedInvocations = new AtomicInteger();
      trustedServer =
          tlsServer(trustedCertificate, trustedKey, salesforceHeaders, unrelatedInvocations)
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

      untrustedTransport =
          new PubSubApiTransport(
              new EndpointConfig("127.0.0.1", trustedServer.getPort()), session());
      PubSubApiTransport rejectedUntrusted = untrustedTransport;
      assertThrows(
          Exception.class, () -> rejectedUntrusted.getTopic("untrusted").get(5, TimeUnit.SECONDS));

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
      close(untrustedTransport);
      close(plaintextTransport);
      shutdown(unrelatedChannel);
      shutdown(trustedServer);
      shutdown(plaintextServer);
      restoreProperty("javax.net.ssl.trustStore", previousStore);
      restoreProperty("javax.net.ssl.trustStorePassword", previousPassword);
      restoreProperty("javax.net.ssl.trustStoreType", previousType);
      Files.deleteIfExists(trustStore);
      Files.deleteIfExists(trustedKey);
    }
  }

  private static NettyServerBuilder tlsServer(
      Path certificate,
      Path privateKey,
      AtomicReference<Metadata> salesforceHeaders,
      AtomicInteger unrelatedInvocations)
      throws Exception {
    return NettyServerBuilder.forPort(0)
        .sslContext(GrpcSslContexts.forServer(certificate.toFile(), privateKey.toFile()).build())
        .addService(
            ServerInterceptors.intercept(topicService(), metadataInterceptor(salesforceHeaders)))
        .addService(plainService(unrelatedInvocations));
  }

  private static NettyServerBuilder plaintextServer(AtomicInteger invocations) {
    return NettyServerBuilder.forPort(0).addService(topicService(invocations));
  }

  private static PubSubGrpc.PubSubImplBase topicService() {
    return topicService(new AtomicInteger());
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

  private static Path createTrustStore(Path certificate) throws Exception {
    char[] password = "changeit".toCharArray();
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, password);
    try (InputStream input = Files.newInputStream(certificate)) {
      keyStore.setCertificateEntry(
          "local-test", CertificateFactory.getInstance("X.509").generateCertificate(input));
    }
    Path path = Files.createTempFile("osp-022-trust-", ".p12");
    try (OutputStream output = Files.newOutputStream(path)) {
      keyStore.store(output, password);
    }
    return path;
  }

  private static Path resource(String name) throws Exception {
    return Path.of(
        Objects.requireNonNull(PubSubApiTransportTlsTest.class.getClassLoader().getResource(name))
            .toURI());
  }

  private static Path materializePrivateKey(String resource) throws Exception {
    String body = Files.readString(resource(resource), StandardCharsets.US_ASCII).trim();
    Path privateKey = Files.createTempFile("osp-022-key-", ".pem");
    Files.writeString(
        privateKey,
        "-----BEGIN PRIVATE KEY-----\n" + body + "\n-----END PRIVATE KEY-----\n",
        StandardCharsets.US_ASCII);
    return privateKey;
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
