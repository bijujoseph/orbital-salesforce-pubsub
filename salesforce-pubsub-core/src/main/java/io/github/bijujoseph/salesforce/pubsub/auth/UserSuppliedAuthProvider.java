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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Authentication provider for an embedding application that already owns a Salesforce session.
 *
 * <p>A fixed session can be updated atomically with {@link #updateSession(SalesforceSession)}. A
 * supplier is useful when the embedding application rotates sessions externally; it is invoked only
 * while the provider's lock is held, and the resulting immutable session is published in one atomic
 * operation.
 */
public final class UserSuppliedAuthProvider implements SalesforceAuthProvider {

  private final AtomicReference<SalesforceSession> session;
  private final Supplier<SalesforceSession> sessionSupplier;
  private final Object sessionLock = new Object();

  public UserSuppliedAuthProvider(SalesforceSession session) {
    this.session = new AtomicReference<>(requireSession(session));
    this.sessionSupplier = null;
  }

  public UserSuppliedAuthProvider(Supplier<SalesforceSession> sessionSupplier) {
    this.session = new AtomicReference<>();
    if (sessionSupplier == null) {
      throw new AuthenticationException("Missing session supplier");
    }
    this.sessionSupplier = sessionSupplier;
  }

  @Override
  public CompletionStage<SalesforceSession> authenticate() {
    try {
      return CompletableFuture.completedFuture(loadSession());
    } catch (AuthenticationException exception) {
      return failedFuture(exception);
    } catch (RuntimeException exception) {
      return failedFuture(new AuthenticationException("Unable to obtain caller-supplied session"));
    }
  }

  @Override
  public CompletionStage<SalesforceSession> refresh(SalesforceSession current) {
    return authenticate();
  }

  /** Replaces the current session as one immutable, atomic value. */
  public void updateSession(SalesforceSession updatedSession) {
    session.set(requireSession(updatedSession));
  }

  /** Alias for integrations that refer to the operation as setting the current session. */
  public void setSession(SalesforceSession updatedSession) {
    updateSession(updatedSession);
  }

  private SalesforceSession loadSession() {
    if (sessionSupplier == null) {
      SalesforceSession current = session.get();
      if (current == null) {
        throw new AuthenticationException("No caller-supplied Salesforce session is available");
      }
      return current;
    }
    synchronized (sessionLock) {
      SalesforceSession supplied = requireSession(sessionSupplier.get());
      session.set(supplied);
      return supplied;
    }
  }

  private static SalesforceSession requireSession(SalesforceSession value) {
    if (value == null) {
      throw new AuthenticationException("No caller-supplied Salesforce session is available");
    }
    return value;
  }

  private static <T> CompletionStage<T> failedFuture(Throwable failure) {
    CompletableFuture<T> result = new CompletableFuture<>();
    result.completeExceptionally(failure);
    return result;
  }
}
