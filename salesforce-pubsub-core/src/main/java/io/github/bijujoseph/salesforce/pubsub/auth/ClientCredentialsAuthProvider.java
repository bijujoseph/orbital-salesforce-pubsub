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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Acquires Salesforce sessions with the OAuth 2.0 client-credentials grant.
 *
 * <p>The provider uses the JDK asynchronous HTTP client and coalesces concurrent acquisition or
 * refresh requests into one in-flight request. OAuth response bodies and client exceptions are
 * never copied into an exception message because they can contain credentials.
 */
public final class ClientCredentialsAuthProvider implements SalesforceAuthProvider {

  private static final String TOKEN_PATH = "/services/oauth2/token";
  private static final Pattern JSON_STRING_PATTERN =
      Pattern.compile("\\\"([^\\\"]+)\\\"\\s*:\\s*(\\\"(?:\\\\.|[^\\\"\\\\])*\\\"|null)");

  private final URI tokenEndpoint;
  private final String clientId;
  private final String clientSecret;
  private final HttpClient httpClient;
  private final Object requestLock = new Object();
  private CompletableFuture<SalesforceSession> inFlight;

  public ClientCredentialsAuthProvider(String loginUrl, String clientId, String clientSecret) {
    this(loginUrl, clientId, clientSecret, HttpClient.newHttpClient());
  }

  public ClientCredentialsAuthProvider(
      String loginUrl, String clientId, String clientSecret, HttpClient httpClient) {
    this(toUri(loginUrl), clientId, clientSecret, httpClient);
  }

  public ClientCredentialsAuthProvider(URI loginUrl, String clientId, String clientSecret) {
    this(loginUrl, clientId, clientSecret, HttpClient.newHttpClient());
  }

  public ClientCredentialsAuthProvider(
      URI loginUrl, String clientId, String clientSecret, HttpClient httpClient) {
    this.tokenEndpoint = tokenEndpoint(loginUrl);
    this.clientId = requireNonBlank(clientId, "client ID");
    this.clientSecret = requireNonBlank(clientSecret, "client secret");
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

      CompletableFuture<SalesforceSession> request = sendTokenRequest();
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
    return "ClientCredentialsAuthProvider[tokenEndpoint="
        + tokenEndpoint
        + ", clientId=<redacted>]";
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
      String body = response.body();
      String accessToken = firstValue(body, "access_token", "accessToken");
      String instanceUrl = firstValue(body, "instance_url", "instanceUrl");
      String tenantId = firstValue(body, "tenant_id", "tenantId", "organization_id", "org_id");
      String userId = firstValue(body, "user_id", "userId");

      String identity = firstValue(body, "id");
      if (tenantId == null || userId == null) {
        String[] identityParts = identityParts(identity);
        if (tenantId == null) {
          tenantId = identityParts[0];
        }
        if (userId == null) {
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
    if (loginUrl == null || loginUrl.getScheme() == null || loginUrl.getHost() == null) {
      throw new AuthenticationException("Salesforce login URL must be an absolute URL");
    }
    String path = loginUrl.getPath();
    if (path != null && path.endsWith(TOKEN_PATH)) {
      try {
        return new URI(
            loginUrl.getScheme(), null, loginUrl.getHost(), loginUrl.getPort(), path, null, null);
      } catch (URISyntaxException exception) {
        throw new AuthenticationException("Salesforce login URL is invalid");
      }
    }
    String basePath = path == null || path.isBlank() || "/".equals(path) ? "" : path;
    if (basePath.endsWith("/")) {
      basePath = basePath.substring(0, basePath.length() - 1);
    }
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

  private static String requireNonBlank(String value, String field) {
    if (value == null || value.isBlank()) {
      throw new AuthenticationException("Missing " + field);
    }
    return value.trim();
  }

  private static String firstValue(String json, String... names) {
    if (json == null || json.isBlank()) {
      return null;
    }
    for (String name : names) {
      Matcher matcher = JSON_STRING_PATTERN.matcher(json);
      while (matcher.find()) {
        if (name.equals(matcher.group(1))) {
          String encodedValue = matcher.group(2);
          return "null".equals(encodedValue) ? null : decodeJsonString(encodedValue);
        }
      }
    }
    return null;
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
