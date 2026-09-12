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
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.InetSocketAddress;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
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
  void postsExactClientCredentialsWithFormEncoding() throws Exception {
    AtomicReference<String> requestBody = new AtomicReference<>();
    server.removeContext("/services/oauth2/token");
    server.createContext(
        "/services/oauth2/token",
        exchange -> {
          requestBody.set(
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
          respond(
              exchange,
              200,
              "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\"}");
        });
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client +&=", " secret +&= ");

    provider.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals(
        "grant_type=client_credentials&client_id=client+%2B%26%3D&client_secret=+secret+%2B%26%3D+",
        requestBody.get());
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
        () -> new ClientCredentialsAuthProvider("ftp://localhost", "client", "secret"));
    assertThrows(
        AuthenticationException.class,
        () -> new ClientCredentialsAuthProvider("http://example.com", "client", "secret"));
    assertEquals(
        "ClientCredentialsAuthProvider[tokenEndpoint=<redacted>, clientId=<redacted>]",
        new ClientCredentialsAuthProvider("https://example.com", "client", "secret").toString());
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
                + "/path-secret/login?client_secret=diagnostic-query-secret",
            "client",
            "secret");
    String diagnostics = provider.toString();
    assertFalse(diagnostics.contains("diagnostic-user"));
    assertFalse(diagnostics.contains("diagnostic-password"));
    assertFalse(diagnostics.contains("diagnostic-query-secret"));
    assertFalse(diagnostics.contains("path-secret"));
    assertEquals(
        "ClientCredentialsAuthProvider[tokenEndpoint=<redacted>, clientId=<redacted>]",
        diagnostics);
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
      provider.clearInFlight(first.toCompletableFuture());
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
  void synchronousClientFailureReturnsSanitizedFailedStage() {
    SynchronouslyFailingHttpClient httpClient = new SynchronouslyFailingHttpClient();
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider("http://localhost", "client", "secret", httpClient);

    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    AuthenticationException authenticationException =
        assertInstanceOf(AuthenticationException.class, failure.getCause());
    assertEquals("Salesforce authentication request failed", authenticationException.getMessage());
    assertFalse(authenticationException.getMessage().contains("synchronous-client-secret"));
    assertEquals(Duration.ofSeconds(30), httpClient.lastRequest.timeout().orElseThrow());
    assertThrows(
        CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    assertEquals(2, httpClient.attempts.get());
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
  void rejectsNestedOrTrailingOAuthResponseContent() {
    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"payload\":{\"instance_url\":\"https://wrong-instance.example\"}}");
    ClientCredentialsAuthProvider nestedFields =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    AuthenticationException nestedFailure =
        assertInstanceOf(
            AuthenticationException.class,
            assertThrows(
                    CompletionException.class,
                    () -> nestedFields.authenticate().toCompletableFuture().join())
                .getCause());
    assertFalse(nestedFailure.getMessage().contains("wrong-instance.example"));

    replaceResponse(
        200, "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\"} trailing");
    ClientCredentialsAuthProvider trailingContent =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    assertInstanceOf(
        AuthenticationException.class,
        assertThrows(
                CompletionException.class,
                () -> trailingContent.authenticate().toCompletableFuture().join())
            .getCause());
  }

  @Test
  void acceptsAndIgnoresValidNonStringOAuthMetadata() throws Exception {
    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"expires_in\":3600,\"zero\":0,\"negative\":-1,\"decimal\":1.5,\"positive_exponent\":1E+2,\"negative_exponent\":1e-2,\"active\":true,\"metadata\":{\"instance_url\":\"https://wrong-instance.example\",\"enabled\":false},\"scopes\":[\"one\",\"two\",null,{},[]]}");
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");

    SalesforceSession session =
        provider.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);

    assertEquals("token", session.accessToken());
    assertEquals("https://instance.example", session.instanceUrl());
  }

  @Test
  void rejectsInvalidTopLevelJsonShapesAndNumbers() {
    assertInvalidOAuthResponse("{}");
    assertInvalidOAuthResponse(
        "{\"access_token\":\"first\",\"access_token\":\"second\",\"instance_url\":\"https://instance.example\"}");
    assertInvalidOAuthResponse("{true:null}");
    assertInvalidOAuthResponse("{\"access_token\":\"unterminated}");
    assertInvalidOAuthResponse(
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"invalid_number\":01}");
    assertInvalidOAuthResponse(
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"invalid_number\":١}");
  }

  @Test
  void rejectsOversizedOAuthResponseWithoutBufferingOrExposingIt() {
    String oversizedSecret = "response-secret-" + "x".repeat(64 * 1024);
    replaceResponse(200, oversizedSecret);
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");

    AuthenticationException failure =
        assertInstanceOf(
            AuthenticationException.class,
            assertThrows(
                    CompletionException.class,
                    () -> provider.authenticate().toCompletableFuture().join())
                .getCause());

    assertEquals("Salesforce authentication response was invalid", failure.getMessage());
    assertFalse(failure.getMessage().contains("response-secret"));
  }

  @Test
  void rejectsExcessivelyNestedOAuthMetadata() {
    String nestedValue = "[".repeat(33) + "null" + "]".repeat(33);
    assertInvalidOAuthResponse(
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"metadata\":"
            + nestedValue
            + "}");
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

    replaceResponse(
        200,
        "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\",\"tenant_id\":\" \",\"user_id\":\"\",\"id\":\"https://login.salesforce.com/id/tenant-from-id/user-from-id\"}");
    ClientCredentialsAuthProvider blankExplicitIdentity =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    SalesforceSession identityFromUrl =
        blankExplicitIdentity.authenticate().toCompletableFuture().get(5, TimeUnit.SECONDS);
    assertEquals("tenant-from-id", identityFromUrl.tenantId());
    assertEquals("user-from-id", identityFromUrl.userId());
  }

  @Test
  void acceptsEndpointPathVariantsAndEmptyResponseAsTypedFailure() {
    new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort() + "/", "client", "secret")
        .authenticate()
        .toCompletableFuture()
        .join();
    AtomicInteger baseRequestCount = new AtomicInteger();
    server.createContext(
        "/base/services/oauth2/token",
        exchange -> {
          baseRequestCount.incrementAndGet();
          respond(
              exchange,
              200,
              "{\"access_token\":\"token\",\"instance_url\":\"https://instance.example\"}");
        });
    new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort() + "/base/", "client", "secret")
        .authenticate()
        .toCompletableFuture()
        .join();
    new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort() + "/services/oauth2/token/",
            "client",
            "secret")
        .authenticate()
        .toCompletableFuture()
        .join();
    assertEquals(2, requestCount.get());
    assertEquals(1, baseRequestCount.get());

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

  private void assertInvalidOAuthResponse(String body) {
    replaceResponse(200, body);
    ClientCredentialsAuthProvider provider =
        new ClientCredentialsAuthProvider(
            "http://localhost:" + server.getAddress().getPort(), "client", "secret");
    assertInstanceOf(
        AuthenticationException.class,
        assertThrows(
                CompletionException.class,
                () -> provider.authenticate().toCompletableFuture().join())
            .getCause());
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(status, bytes.length);
    try (var output = exchange.getResponseBody()) {
      output.write(bytes);
    }
  }

  private static final class SynchronouslyFailingHttpClient extends HttpClient {
    private final HttpClient delegate = HttpClient.newHttpClient();
    private final AtomicInteger attempts = new AtomicInteger();
    private HttpRequest lastRequest;

    @Override
    public Optional<CookieHandler> cookieHandler() {
      return delegate.cookieHandler();
    }

    @Override
    public Optional<Duration> connectTimeout() {
      return delegate.connectTimeout();
    }

    @Override
    public Redirect followRedirects() {
      return delegate.followRedirects();
    }

    @Override
    public Optional<ProxySelector> proxy() {
      return delegate.proxy();
    }

    @Override
    public SSLContext sslContext() {
      return delegate.sslContext();
    }

    @Override
    public SSLParameters sslParameters() {
      return delegate.sslParameters();
    }

    @Override
    public Optional<Authenticator> authenticator() {
      return delegate.authenticator();
    }

    @Override
    public Version version() {
      return delegate.version();
    }

    @Override
    public Optional<Executor> executor() {
      return delegate.executor();
    }

    @Override
    public <T> HttpResponse<T> send(
        HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException {
      return delegate.send(request, responseBodyHandler);
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
        HttpRequest request, HttpResponse.BodyHandler<T> responseBodyHandler) {
      lastRequest = request;
      attempts.incrementAndGet();
      throw new IllegalArgumentException("synchronous-client-secret");
    }

    @Override
    public <T> CompletableFuture<HttpResponse<T>> sendAsync(
        HttpRequest request,
        HttpResponse.BodyHandler<T> responseBodyHandler,
        HttpResponse.PushPromiseHandler<T> pushPromiseHandler) {
      throw new IllegalArgumentException("synchronous-client-secret");
    }
  }
}
