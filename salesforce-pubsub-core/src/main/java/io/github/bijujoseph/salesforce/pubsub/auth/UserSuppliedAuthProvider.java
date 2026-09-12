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
import io.github.bijujoseph.salesforce.pubsub.telemetry.ConnectorHealth;
import io.github.bijujoseph.salesforce.pubsub.telemetry.ConnectorStatus;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authentication provider for an embedding application that already owns a Salesforce session.
 *
 * <p>A fixed session can be updated atomically with {@link #updateSession(SalesforceSession)}. A
 * supplier is useful when the embedding application rotates sessions externally; it is invoked only
 * while the provider's lock is held, and the resulting immutable session is published in one atomic
 * operation.
 */
public final class UserSuppliedAuthProvider implements SalesforceAuthProvider {

  private static final Logger DEFAULT_LOGGER =
      LoggerFactory.getLogger(UserSuppliedAuthProvider.class);

  private final AtomicReference<SalesforceSession> session;
  private final Supplier<SalesforceSession> sessionSupplier;
  private final ConnectorHealth health;
  private final Logger logger;
  private final Object sessionLock = new Object();

  public UserSuppliedAuthProvider(SalesforceSession session) {
    this(session, null);
  }

  public UserSuppliedAuthProvider(SalesforceSession session, ConnectorHealth health) {
    this.session = new AtomicReference<>(requireSession(session));
    this.sessionSupplier = null;
    this.health = health;
    this.logger = DEFAULT_LOGGER;
  }

  public UserSuppliedAuthProvider(Supplier<SalesforceSession> sessionSupplier) {
    this(sessionSupplier, null);
  }

  public UserSuppliedAuthProvider(
      Supplier<SalesforceSession> sessionSupplier, ConnectorHealth health) {
    this.session = new AtomicReference<>();
    if (sessionSupplier == null) {
      throw new AuthenticationException("Missing session supplier");
    }
    this.sessionSupplier = sessionSupplier;
    this.health = health;
    this.logger = DEFAULT_LOGGER;
  }

  @Override
  public CompletionStage<SalesforceSession> authenticate() {
    transitionHealth(ConnectorStatus.AUTHENTICATING);
    try {
      SalesforceSession loaded = loadSession();
      logger.atInfo().log("Caller-supplied Salesforce session acquired");
      return CompletableFuture.completedFuture(loaded);
    } catch (AuthenticationException exception) {
      logAuthenticationFailure();
      return failedFuture(exception);
    } catch (RuntimeException exception) {
      logAuthenticationFailure();
      return failedFuture(new AuthenticationException("Unable to obtain caller-supplied session"));
    }
  }

  @Override
  public CompletionStage<SalesforceSession> refresh(SalesforceSession current) {
    return authenticate();
  }

  /** Replaces the current session as one immutable, atomic value in fixed-session mode. */
  public void updateSession(SalesforceSession updatedSession) {
    if (sessionSupplier != null) {
      throw new AuthenticationException(
          "Supplier-backed sessions must be updated through their supplier");
    }
    SalesforceSession replacement = requireSession(updatedSession);
    synchronized (sessionLock) {
      session.set(replacement);
    }
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
      SalesforceSession supplied;
      try {
        supplied = sessionSupplier.get();
      } catch (RuntimeException exception) {
        throw new AuthenticationException("Unable to obtain caller-supplied session");
      }
      supplied = requireSession(supplied);
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

  private void logAuthenticationFailure() {
    logger
        .atError()
        .addKeyValue("exceptionCategory", AuthenticationException.class.getSimpleName())
        .log("Caller-supplied Salesforce session acquisition failed");
    transitionHealth(ConnectorStatus.FAILED);
  }

  private void transitionHealth(ConnectorStatus status) {
    if (health != null) {
      health.transitionTo(status);
    }
  }
}
