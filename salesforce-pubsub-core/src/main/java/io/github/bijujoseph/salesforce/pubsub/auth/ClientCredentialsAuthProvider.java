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

import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Acquires Salesforce sessions with the OAuth 2.0 client-credentials grant.
 *
 * <p>The provider uses the JDK asynchronous HTTP client and coalesces concurrent acquisition or
 * refresh requests into one in-flight request. OAuth response bodies and client exceptions are
 * never copied into an exception message because they can contain credentials.
 */
public final class ClientCredentialsAuthProvider implements SalesforceAuthProvider {

  private static final String TOKEN_PATH = "/services/oauth2/token";
  private static final Duration AUTH_REQUEST_TIMEOUT = Duration.ofSeconds(30);

  private final URI tokenEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final HttpClient httpClient;
  private final Object requestLock = new Object();
  private CompletableFuture<SalesforceSession> inFlight;

  public ClientCredentialsAuthProvider(String loginUrl, String clientId, String clientSecret) {
    this(loginUrl, clientId, clientSecret, defaultHttpClient());
  }

  public ClientCredentialsAuthProvider(
      String loginUrl, String clientId, String clientSecret, HttpClient httpClient) {
    this(toUri(loginUrl), clientId, clientSecret, httpClient);
  }

  public ClientCredentialsAuthProvider(URI loginUrl, String clientId, String clientSecret) {
    this(loginUrl, clientId, clientSecret, defaultHttpClient());
  }

  public ClientCredentialsAuthProvider(
      URI loginUrl, String clientId, String clientSecret, HttpClient httpClient) {
    this.tokenEndpoint = tokenEndpoint(loginUrl);
    this.clientId = requireNonBlank(clientId, "client ID");
    this.clientSecret = requireNonBlankExact(clientSecret, "client secret");
    if (httpClient == null) {
      throw new AuthenticationException("Missing HTTP client");
    }
    this.httpClient = httpClient;
  }

  @Override
  public CompletionStage<SalesforceSession> authenticate() {
    synchronized (requestLock) {
      if (inFlight != null && !inFlight.isDone()) {
        return inFlight;
      }

      CompletableFuture<SalesforceSession> request;
      try {
        request = sendTokenRequest();
      } catch (RuntimeException exception) {
        return failedFuture(
            new AuthenticationException("Salesforce authentication request failed"));
      }
      inFlight = request;
      request.whenComplete((session, failure) -> clearInFlight(request));
      return request;
    }
  }

  @Override
  public CompletionStage<SalesforceSession> refresh(SalesforceSession current) {
    if (current == null) {
      return failedFuture(
          new AuthenticationException("Cannot refresh a missing Salesforce session"));
    }
    return authenticate();
  }

  @Override
  public String toString() {
    return "ClientCredentialsAuthProvider[tokenEndpoint=<redacted>, clientId=<redacted>]";
  }

  private CompletableFuture<SalesforceSession> sendTokenRequest() {
    String form =
        "grant_type=client_credentials"
            + "&client_id="
            + formEncode(clientId)
            + "&client_secret="
            + formEncode(clientSecret);
    HttpRequest request =
        HttpRequest.newBuilder(tokenEndpoint)
            .timeout(AUTH_REQUEST_TIMEOUT)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(form, StandardCharsets.UTF_8))
            .build();

    return httpClient
        .sendAsync(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8))
        .handle(this::mapResponse)
        .toCompletableFuture();
  }

  private SalesforceSession mapResponse(HttpResponse<String> response, Throwable failure) {
    if (failure != null) {
      throw new AuthenticationException("Salesforce authentication request failed");
    }
    if (response == null || response.statusCode() < 200 || response.statusCode() >= 300) {
      throw new AuthenticationException(
          "Salesforce authentication request failed with an unsuccessful response");
    }
    try {
      Map<String, String> fields = parseTopLevelStringFields(response.body());
      String accessToken = firstValue(fields, "access_token", "accessToken");
      String instanceUrl = firstValue(fields, "instance_url", "instanceUrl");
      String tenantId = firstValue(fields, "tenant_id", "tenantId", "organization_id", "org_id");
      String userId = firstValue(fields, "user_id", "userId");

      String identity = firstValue(fields, "id");
      if (isBlank(tenantId) || isBlank(userId)) {
        String[] identityParts = identityParts(identity);
        if (isBlank(tenantId)) {
          tenantId = identityParts[0];
        }
        if (isBlank(userId)) {
          userId = identityParts[1];
        }
      }
      return new SalesforceSession(accessToken, instanceUrl, tenantId, userId);
    } catch (AuthenticationException exception) {
      throw exception;
    } catch (RuntimeException exception) {
      throw new AuthenticationException("Salesforce authentication response was invalid");
    }
  }

  void clearInFlight(CompletableFuture<SalesforceSession> completedRequest) {
    synchronized (requestLock) {
      if (inFlight == completedRequest) {
        inFlight = null;
      }
    }
  }

  private static URI tokenEndpoint(URI loginUrl) {
    String scheme = loginUrl == null ? null : loginUrl.getScheme();
    String host = loginUrl == null ? null : loginUrl.getHost();
    if (loginUrl == null
        || !("https".equalsIgnoreCase(scheme)
            || ("http".equalsIgnoreCase(scheme) && "localhost".equalsIgnoreCase(host)))
        || host == null) {
      throw new AuthenticationException(
          "Salesforce login URL must use HTTPS or HTTP on localhost for testing");
    }
    String path = loginUrl.getPath();
    while (path != null && path.length() > 1 && path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (path != null && path.endsWith(TOKEN_PATH)) {
      try {
        return new URI(
            loginUrl.getScheme(), null, loginUrl.getHost(), loginUrl.getPort(), path, null, null);
      } catch (URISyntaxException exception) {
        throw new AuthenticationException("Salesforce login URL is invalid");
      }
    }
    String basePath = path == null || path.isBlank() || "/".equals(path) ? "" : path;
    try {
      return new URI(
          loginUrl.getScheme(),
          null,
          loginUrl.getHost(),
          loginUrl.getPort(),
          basePath + TOKEN_PATH,
          null,
          null);
    } catch (URISyntaxException exception) {
      throw new AuthenticationException("Salesforce login URL is invalid");
    }
  }

  private static URI toUri(String value) {
    if (value == null || value.isBlank()) {
      throw new AuthenticationException("Missing Salesforce login URL");
    }
    try {
      return URI.create(value.trim());
    } catch (IllegalArgumentException exception) {
      throw new AuthenticationException("Salesforce login URL is invalid");
    }
  }

  private static String formEncode(String value) {
    return java.net.URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static HttpClient defaultHttpClient() {
    return HttpClient.newBuilder().connectTimeout(AUTH_REQUEST_TIMEOUT).build();
  }

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new AuthenticationException("Missing " + field);
    }
    return value.trim();
  }

  private static String requireNonBlankExact(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new AuthenticationException("Missing " + field);
    }
    return value;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private static String firstValue(Map<String, String> fields, String... names) {
    for (String name : names) {
      if (fields.containsKey(name)) {
        return fields.get(name);
      }
    }
    return null;
  }

  private static Map<String, String> parseTopLevelStringFields(String json) {
    return new FlatJsonObjectParser(json).parse();
  }

  private static final class FlatJsonObjectParser {
    private final String json;
    private int position;

    private FlatJsonObjectParser(String json) {
      if (json == null) {
        throw new IllegalArgumentException("Missing JSON response");
      }
      this.json = json;
    }

    private Map<String, String> parse() {
      Map<String, String> fields = new HashMap<>();
      skipWhitespace();
      expect('{');
      skipWhitespace();
      if (consume('}')) {
        requireEnd();
        return fields;
      }
      while (true) {
        String name = parseString();
        skipWhitespace();
        expect(':');
        skipWhitespace();
        String value;
        if (position < json.length() && json.charAt(position) == '"') {
          value = parseString();
        } else if (consumeLiteral("null")) {
          value = null;
        } else {
          skipValue();
          value = null;
        }
        if (fields.containsKey(name)) {
          throw new IllegalArgumentException("Duplicate JSON field");
        }
        fields.put(name, value);
        skipWhitespace();
        if (consume('}')) {
          requireEnd();
          return fields;
        }
        expect(',');
        skipWhitespace();
      }
    }

    private String parseString() {
      if (position >= json.length() || json.charAt(position) != '"') {
        throw new IllegalArgumentException("Expected JSON string");
      }
      int start = position++;
      boolean escaped = false;
      while (position < json.length()) {
        char character = json.charAt(position++);
        if (!escaped && character == '"') {
          return decodeJsonString(json.substring(start, position));
        }
        if (!escaped && character < 0x20) {
          throw new IllegalArgumentException("Invalid JSON string");
        }
        if (escaped) {
          escaped = false;
        } else if (character == '\\') {
          escaped = true;
        }
      }
      throw new IllegalArgumentException("Unterminated JSON string");
    }

    private boolean consumeLiteral(String literal) {
      if (!json.startsWith(literal, position)) {
        return false;
      }
      position += literal.length();
      return true;
    }

    private void skipValue() {
      if (position >= json.length()) {
        throw new IllegalArgumentException("Missing JSON value");
      }
      switch (json.charAt(position)) {
        case '"' -> parseString();
        case '{' -> skipObject();
        case '[' -> skipArray();
        case 't' -> requireLiteral("true");
        case 'f' -> requireLiteral("false");
        case 'n' -> requireLiteral("null");
        default -> skipNumber();
      }
    }

    private void skipObject() {
      expect('{');
      skipWhitespace();
      if (consume('}')) {
        return;
      }
      while (true) {
        parseString();
        skipWhitespace();
        expect(':');
        skipWhitespace();
        skipValue();
        skipWhitespace();
        if (consume('}')) {
          return;
        }
        expect(',');
        skipWhitespace();
      }
    }

    private void skipArray() {
      expect('[');
      skipWhitespace();
      if (consume(']')) {
        return;
      }
      while (true) {
        skipValue();
        skipWhitespace();
        if (consume(']')) {
          return;
        }
        expect(',');
        skipWhitespace();
      }
    }

    private void requireLiteral(String literal) {
      if (!consumeLiteral(literal)) {
        throw new IllegalArgumentException("Invalid JSON literal");
      }
    }

    private void skipNumber() {
      int start = position;
      consume('-');
      if (consume('0')) {
        if (position < json.length() && isAsciiDigit(json.charAt(position))) {
          throw new IllegalArgumentException("Invalid JSON number");
        }
      } else {
        requireDigits();
      }
      if (consume('.')) {
        requireDigits();
      }
      if (position < json.length()
          && (json.charAt(position) == 'e' || json.charAt(position) == 'E')) {
        position++;
        if (!consume('+')) {
          consume('-');
        }
        requireDigits();
      }
      if (position == start) {
        throw new IllegalArgumentException("Invalid JSON value");
      }
    }

    private void requireDigits() {
      int start = position;
      while (position < json.length() && isAsciiDigit(json.charAt(position))) {
        position++;
      }
      if (position == start) {
        throw new IllegalArgumentException("Invalid JSON number");
      }
    }

    private static boolean isAsciiDigit(char value) {
      return value >= '0' && value <= '9';
    }

    private boolean consume(char expected) {
      if (position < json.length() && json.charAt(position) == expected) {
        position++;
        return true;
      }
      return false;
    }

    private void expect(char expected) {
      if (!consume(expected)) {
        throw new IllegalArgumentException("Invalid JSON response");
      }
    }

    private void skipWhitespace() {
      while (position < json.length() && Character.isWhitespace(json.charAt(position))) {
        position++;
      }
    }

    private void requireEnd() {
      skipWhitespace();
      if (position != json.length()) {
        throw new IllegalArgumentException("Unexpected content after JSON object");
      }
    }
  }

  private static String decodeJsonString(String encodedValue) {
    String value = encodedValue.substring(1, encodedValue.length() - 1);
    StringBuilder decoded = new StringBuilder(value.length());
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      if (character != '\\' || index + 1 >= value.length()) {
        decoded.append(character);
        continue;
      }
      char escaped = value.charAt(++index);
      switch (escaped) {
        case '"' -> decoded.append('"');
        case '\\' -> decoded.append('\\');
        case '/' -> decoded.append('/');
        case 'b' -> decoded.append('\b');
        case 'f' -> decoded.append('\f');
        case 'n' -> decoded.append('\n');
        case 'r' -> decoded.append('\r');
        case 't' -> decoded.append('\t');
        case 'u' -> {
          if (index + 4 >= value.length()) {
            throw new IllegalArgumentException("Invalid JSON escape");
          }
          decoded.append((char) Integer.parseInt(value.substring(index + 1, index + 5), 16));
          index += 4;
        }
        default -> throw new IllegalArgumentException("Invalid JSON escape");
      }
    }
    return decoded.toString();
  }

  private static String[] identityParts(String identity) {
    if (identity == null || identity.isBlank()) {
      return new String[] {null, null};
    }
    try {
      String path = URI.create(identity).getPath();
      if (path == null) {
        return new String[] {null, null};
      }
      String[] segments = path.split("/");
      for (int index = 0; index + 2 < segments.length; index++) {
        if ("id".equals(segments[index])) {
          return new String[] {
            nonBlankOrNull(segments[index + 1]), nonBlankOrNull(segments[index + 2])
          };
        }
      }
    } catch (IllegalArgumentException ignored) {
      // An identity URL is optional; malformed identity metadata must not hide a valid token.
    }
    return new String[] {null, null};
  }

  private static String nonBlankOrNull(String value) {
    return value == null || value.isBlank() ? null : value;
  }

  private static <T> CompletionStage<T> failedFuture(Throwable failure) {
    CompletableFuture<T> result = new CompletableFuture<>();
    result.completeExceptionally(failure);
    return result;
  }
}
