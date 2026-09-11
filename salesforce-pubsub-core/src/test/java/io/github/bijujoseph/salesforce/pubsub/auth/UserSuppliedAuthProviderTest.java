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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bijujoseph.salesforce.pubsub.error.AuthenticationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class UserSuppliedAuthProviderTest {

  @Test
  void acceptsAndAtomicallyUpdatesCallerSession() {
    SalesforceSession initial =
        new SalesforceSession("token-1", "https://instance.example", "tenant", "user");
    SalesforceSession replacement =
        new SalesforceSession("token-2", "https://instance.example", "tenant", "user");
    UserSuppliedAuthProvider provider = new UserSuppliedAuthProvider(initial);

    assertSame(initial, provider.authenticate().toCompletableFuture().join());
    provider.updateSession(replacement);
    assertSame(replacement, provider.authenticate().toCompletableFuture().join());
    assertSame(replacement, provider.refresh(initial).toCompletableFuture().join());
  }

  @Test
  void supplierIsReadAsOneCompleteImmutableSession() {
    AtomicReference<SalesforceSession> current =
        new AtomicReference<>(
            new SalesforceSession("token-1", "https://instance.example", null, null));
    UserSuppliedAuthProvider provider = new UserSuppliedAuthProvider(current::get);

    assertEquals("token-1", provider.authenticate().toCompletableFuture().join().accessToken());
    current.set(new SalesforceSession("token-2", "https://instance.example", "tenant", "user"));
    assertEquals("token-2", provider.refresh(null).toCompletableFuture().join().accessToken());
  }

  @Test
  void missingSessionFailsWithTypedAuthenticationError() {
    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () ->
                new UserSuppliedAuthProvider(() -> null)
                    .authenticate()
                    .toCompletableFuture()
                    .join());
    assertInstanceOf(AuthenticationException.class, failure.getCause());
  }

  @Test
  void rejectsNullInputsAndSanitizesSupplierFailure() {
    assertThrows(
        AuthenticationException.class,
        () -> new UserSuppliedAuthProvider((SalesforceSession) null));
    assertThrows(
        AuthenticationException.class,
        () -> new UserSuppliedAuthProvider((java.util.function.Supplier<SalesforceSession>) null));
    UserSuppliedAuthProvider provider =
        new UserSuppliedAuthProvider(
            () -> {
              throw new IllegalStateException("secret-from-supplier");
            });
    CompletionException failure =
        assertThrows(
            CompletionException.class, () -> provider.authenticate().toCompletableFuture().join());
    AuthenticationException authenticationException =
        assertInstanceOf(AuthenticationException.class, failure.getCause());
    org.junit.jupiter.api.Assertions.assertFalse(
        authenticationException.getMessage().contains("secret-from-supplier"));
  }

  @Test
  void setAliasAndInvalidUpdateRemainTyped() {
    UserSuppliedAuthProvider provider =
        new UserSuppliedAuthProvider(
            new SalesforceSession("token", "https://instance.example", null, null));
    SalesforceSession replacement =
        new SalesforceSession("replacement", "https://instance.example", null, null);
    provider.setSession(replacement);
    assertSame(replacement, provider.authenticate().toCompletableFuture().join());
    assertThrows(AuthenticationException.class, () -> provider.updateSession(null));
  }

  @Test
  void sessionRequiresAccessTokenAndInstanceUrl() {
    assertThrows(
        AuthenticationException.class,
        () -> new SalesforceSession(null, "https://instance.example", null, null));
    assertThrows(
        AuthenticationException.class, () -> new SalesforceSession("token", " ", null, null));
  }

  @Test
  void sessionStringNeverContainsAccessToken() {
    SalesforceSession session =
        new SalesforceSession(
            "secret-access-token",
            "https://instance.example",
            "tenant-credential",
            "user-credential");
    String text = session.toString();
    org.junit.jupiter.api.Assertions.assertFalse(text.contains("secret-access-token"));
    org.junit.jupiter.api.Assertions.assertFalse(text.contains("tenant-credential"));
    org.junit.jupiter.api.Assertions.assertFalse(text.contains("user-credential"));
  }
}
