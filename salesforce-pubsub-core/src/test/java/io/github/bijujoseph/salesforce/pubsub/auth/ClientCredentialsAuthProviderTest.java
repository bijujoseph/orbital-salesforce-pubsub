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

package io.github.bijujoseph.salesforce.pubsub.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClientCredentialsAuthProviderTest {

  private HttpServer server;
  private AtomicInteger requestCount;

  @BeforeEach
  void startServer() throws IOException {
    requestCount = new AtomicInteger();
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/services/oauth2/token",
        exchange -> {
          requestCount.incrementAndGet();
          respond(
              exchange,
              200,
              "{\"access_token\":\"access-secret\",\"instance_url\":\"https://instance.example\",\"id\":\"https://login.salesforce.com/id/tenant-1/user-1\"}");
        });
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  @Test
  void acquiresSessionAndCoalescesConcurrentRequests() throws Exception {
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "client-secret");

    var first = provider.authenticate();
    var second = provider.authenticate();
    assertSame(first, second);
    SalesforceSession session = first.toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals("access-secret", session.accessToken());
    assertEquals("https://instance.example", session.instanceUrl());
    assertEquals("tenant-1", session.tenantId());
    assertEquals("user-1", session.userId());
    assertEquals(1, requestCount.get());
    assertFalse(provider.toString().contains("client-secret"));
    assertEquals(
        "user-1",
        provider.refresh(session).toCompletableFuture().get(5, TimeUnit.SECONDS).userId());
    assertEquals(2, requestCount.get());
  }

  @Test
  void mapsUnsuccessfulResponseWithoutLeakingResponseBody() {
    server.removeContext("/services/oauth2/token");
    server.createContext(
        "/services/oauth2/token",
        exchange ->
            respond(
                exchange,
                400,
                "{\"error\":\"invalid_client\",\"error_description\":\"client-secret-is-secret\"}"));
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "client-secret");

    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    AuthenticationException authenticationException =
        assertInstanceOf(AuthenticationException.class, failure.getCause());
    assertFalse(authenticationException.getMessage().contains("client-secret-is-secret"));
    assertFalse(authenticationException.getMessage().contains("client-secret"));
  }

  @Test
  void rejectsMissingOrInvalidAuthenticationConfiguration() {
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider((String) null, "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider(" ", "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("not-a-url", "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("http://[invalid", "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider(URI.create("/relative"), "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("http://localhost", null, "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("http://localhost", "client", " "));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("http://localhost", "client", "secret", null));
  }

  @Test
  void rejectsMissingSessionRefreshAndInvalidResponseWithoutRawBody() {
    CompletionException missingCurrent =
        assertThrows(
            CompletionException.class,
            () ->
                new ClientCredentialsAuthProvider(
                        "http://localhost:" + server.getAddress().getPort(), "client", "secret")
                    .refresh(null)
                    .toCompletableFuture()
                    .join());
    assertInstanceOf(AuthenticationException.class, missingCurrent.getCause());

    replaceResponse(200, "{\"instance_url\":\"https://instance.example\"}");
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    CompletionException invalid =
        assertThrows(
            CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    AuthenticationException authenticationException =
        assertInstanceOf(AuthenticationException.class, invalid.getCause());
    assertFalse(authenticationException.getMessage().contains("instance.example"));
  }

  @Test
  void diagnosticsExcludeCredentialBearingEndpointComponents() {
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://diagnostic-user:diagnostic-password@localhost:"
                + server.getAddress().getPort()
                + "/login?client_secret=diagnostic-query-secret",
            "client",
            "secret");
    String diagnostics = provider.toString();
    assertFalse(diagnostics.contains("diagnostic-user"));
    assertFalse(diagnostics.contains("diagnostic-password"));
    assertFalse(diagnostics.contains("diagnostic-query-secret"));
    assertEquals(
        "http://localhost:" + server.getAddress().getPort() + "/login/services/oauth2/token",
        diagnostics.substring(diagnostics.indexOf("http://"), diagnostics.indexOf(", clientId=")));
  }

  @Test
  void staleCompletionCannotClearANewerInFlightRequest() throws Exception {
    CountDownLatch secondRequestStarted = new CountDownLatch(1);
    CountDownLatch releaseSecondRequest = new CountDownLatch(1);
    server.removeContext("/services/oauth2/token");
    server.createContext(
        "/services/oauth2/token",
        exchange -> {
          int requestNumber = requestCount.incrementAndGet();
          if (requestNumber == 2) {
            secondRequestStarted.countDown();
            try {
              assertTrue(releaseSecondRequest.await(5, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
              Thread.currentThread().interrupt();
              throw new IOException("Test request interrupted", exception);
            }
          }
          respond(
              exchange,
              200,
              "{\"access_token\":\"access-secret\",\"instance_url\":\"https://instance.example\"}");
        });
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    var first = provider.authenticate();
    first.toCompletableFuture().get(5, TimeUnit.SECONDS);
    var second = provider.authenticate();
    try {
      assertTrue(secondRequestStarted.await(5, TimeUnit.SECONDS));
      provider.clearInFlight(first.toCompletableFuture(), null, null);
      assertSame(second, provider.authenticate());
    } finally {
      releaseSecondRequest.countDown();
    }
    second.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  @Test
  void acceptsExplicitIdentityFieldsAndDirectTokenEndpoint() throws Exception {
    replaceResponse(
        200,
        "{\"access_token\":\"access-secret\",\"instance_url\":\"https://instance.example\",\"tenant_id\":\"tenant-explicit\",\"user_id\":\"user-explicit\"}");
    String endpoint =
        "http://localhost:"
            + server.getAddress().getPort()
            + "/services/oauth2/token?query-must-not-be-retained";
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(URI.create(endpoint), "client", "secret");
    SalesforceSession session =
        provider.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("tenant-explicit", session.tenantId());
    assertEquals("user-explicit", session.userId());

    ClientCredentialsAuthProvider loginUrlConstructor =
        new ClientCredentialsAuthProvider(
            URI.create("http://localhost:" + server.getAddress().getPort()), "client", "secret");
    assertEquals(
        "tenant-explicit",
        loginUrlConstructor
            .authenticate()
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS)
            .tenantId());
  }

  @Test
  void networkFailureIsMappedWithoutTransportDetails() throws Exception {
    int unusedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      unusedPort = socket.getLocalPort();
    }
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider("http://localhost:" + unusedPort, "client", "secret");
    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    AuthenticationException authenticationException =
        assertInstanceOf(AuthenticationException.class, failure.getCause());
    assertEquals("Salesforce authentication request failed", authenticationException.getMessage());
  }

  @Test
  void supportsEscapedJsonAndOptionalIdentityMetadata() throws Exception {
    replaceResponse(
        200,
        "{\"access_token\":\"a\\\"b\\\\c\\/d\\be\\ff\\ng\\rh\\ti\\u006a\",\"instance_url\":\"https://instance.example\",\"id\":\"urn:identity\"}");
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    SalesforceSession session =
        provider.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("a\"b\\c/d\be\ff\ng\rh\tij", session.accessToken());
    org.junit.jupiter.api.Assertions.assertEquals(null, session.tenantId());
    org.junit.jupiter.api.Assertions.assertEquals(null, session.userId());
  }

  @Test
  void invalidJsonEscapesAndIdentityAreNotExposed() {
    replaceResponse(
        200, "{\"access_token\":\"token\\q\",\"instance_url\":\"https://instance.example\"}");
    ClientCredentialsAuthProvider invalidProvider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () -> invalidProvider.authenticate().toCompletableFuture().join());
    assertInstanceOf(AuthenticationException.class, failure.getCause());

    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"id\":\":\"}");
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    assertEquals(null, provider.authenticate().toCompletableFuture().join().tenantId());
  }

  @Test
  void handlesNullFieldsAndIdentityValuesIndependently() throws Exception {
    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"tenant_id\":\"tenant\",\"id\":\"https://login.salesforce.com/id/tenant-from-id/user-from-id\"}");
    ClientCredentialsAuthProvider missingUser =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    SalesforceSession userFromIdentity =
        missingUser.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("tenant", userFromIdentity.tenantId());
    assertEquals("user-from-id", userFromIdentity.userId());

    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"user_id\":\"user\",\"id\":\"https://login.salesforce.com/id/tenant-from-id/user-from-id\"}");
    ClientCredentialsAuthProvider missingTenant =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    SalesforceSession tenantFromIdentity =
        missingTenant.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("tenant-from-id", tenantFromIdentity.tenantId());
    assertEquals("user", tenantFromIdentity.userId());

    replaceResponse(
        200, "{\"access_token\":null,\"instance_url\":\"https://instance.example\",\"id\":null}");
    ClientCredentialsAuthProvider nullFields =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    assertInstanceOf(
        AuthenticationException.class,
        assertThrows(
                CompletionException.class,
                () -> nullFields.authenticate().toCompletableFuture().join())
            .getCause());

    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"id\":\"https://login.salesforce.com/id//user-from-id\"}");
    ClientCredentialsAuthProvider blankTenant =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    SalesforceSession blankTenantSession =
        blankTenant.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals(null, blankTenantSession.tenantId());
    assertEquals("user-from-id", blankTenantSession.userId());
  }

  @Test
  void acceptsEndpointPathVariantsAndEmptyResponseAsTypedFailure() {
    assertEquals(
        "ClientCredentialsAuthProvider[tokenEndpoint=http://localhost:"
            + server.getAddress().getPort()
            + "/services/oauth2/token, clientId=<redacted>]",
        new ClientCredentialsAuthProvider(
                "http://localhost:" + server.getAddress().getPort() + "/", "client", "secret")
            .toString());
    assertEquals(
        "ClientCredentialsAuthProvider[tokenEndpoint=http://localhost:"
            + server.getAddress().getPort()
            + "/base/services/oauth2/token, clientId=<redacted>]",
        new ClientCredentialsAuthProvider(
                "http://localhost:" + server.getAddress().getPort() + "/base/", "client", "secret")
            .toString());

    replaceResponse(200, "");
    ClientCredentialsAuthProvider emptyResponse =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    assertInstanceOf(
        AuthenticationException.class,
        assertThrows(
                CompletionException.class,
                () -> emptyResponse.authenticate().toCompletableFuture().join())
            .getCause());
  }

  private void replaceResponse(int status, String body) {
    server.removeContext("/services/oauth2/token");
    server.createContext("/services/oauth2/token", exchange -> respond(exchange, status, body));
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }
}
