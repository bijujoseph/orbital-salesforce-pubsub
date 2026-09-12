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

import com.salesforce.eventbus.protobuf.PubSubGrpc;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.SecurityLevel;
import io.grpc.Status;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

/** Attaches one immutable Salesforce session snapshot to one gRPC call. */
public final class SalesforceCallCredentials extends CallCredentials {

  private static final Metadata.Key<String> ACCESS_TOKEN =
      Metadata.Key.of("accesstoken", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> INSTANCE_URL =
      Metadata.Key.of("instanceurl", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> TENANT_ID =
      Metadata.Key.of("tenantid", Metadata.ASCII_STRING_MARSHALLER);
  private static final Set<String> ALLOWED_METHODS =
      Set.of(
          PubSubGrpc.getGetTopicMethod().getFullMethodName(),
          PubSubGrpc.getGetSchemaMethod().getFullMethodName(),
          PubSubGrpc.getPublishMethod().getFullMethodName(),
          PubSubGrpc.getSubscribeMethod().getFullMethodName());

  private final SessionMetadata session;

  /** Creates credentials bound to one immutable session snapshot. */
  public SalesforceCallCredentials(SalesforceSession session) {
    this(SessionMetadata.from(session));
  }

  SalesforceCallCredentials(SessionMetadata session) {
    this.session = Objects.requireNonNull(session, "session metadata");
  }

  @Override
  public void applyRequestMetadata(
      RequestInfo requestInfo, Executor applicationExecutor, MetadataApplier applier) {
    Objects.requireNonNull(applier, "metadata applier");
    if (!hasTransportPrivacy(requestInfo)) {
      applier.fail(
          Status.UNAUTHENTICATED.withDescription(
              "Salesforce session metadata requires transport security"));
      return;
    }
    if (!isAllowedMethod(requestInfo)) {
      applier.fail(
          Status.UNAUTHENTICATED.withDescription(
              "Salesforce session metadata is unavailable for this RPC"));
      return;
    }
    Objects.requireNonNull(applicationExecutor, "application executor");
    try {
      applicationExecutor.execute(
          () -> {
            Metadata headers = new Metadata();
            headers.put(ACCESS_TOKEN, session.accessToken());
            headers.put(INSTANCE_URL, session.instanceUrl());
            headers.put(TENANT_ID, session.tenantId());
            applier.apply(headers);
          });
    } catch (RuntimeException exception) {
      applier.fail(
          Status.UNAUTHENTICATED.withDescription("Salesforce session metadata unavailable"));
    }
  }

  @Override
  public String toString() {
    return "SalesforceCallCredentials[session=<redacted>]";
  }

  private static boolean hasTransportPrivacy(RequestInfo requestInfo) {
    try {
      return requestInfo != null
          && requestInfo.getSecurityLevel() == SecurityLevel.PRIVACY_AND_INTEGRITY;
    } catch (RuntimeException exception) {
      return false;
    }
  }

  private static boolean isAllowedMethod(RequestInfo requestInfo) {
    try {
      MethodDescriptor<?, ?> method = requestInfo.getMethodDescriptor();
      return method != null && ALLOWED_METHODS.contains(method.getFullMethodName());
    } catch (RuntimeException exception) {
      return false;
    }
  }
}
