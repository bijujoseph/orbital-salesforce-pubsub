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

import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.PublishRequest;
import com.salesforce.eventbus.protobuf.PublishResponse;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import com.salesforce.eventbus.protobuf.TopicInfo;
import com.salesforce.eventbus.protobuf.TopicRequest;
import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.config.EndpointConfig;
import io.github.bijujoseph.salesforce.pubsub.error.SalesforcePubSubException;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Secure asynchronous transport for Salesforce Pub/Sub API calls.
 *
 * <p>Authentication acquisition and refresh scheduling are deliberately owned by the higher-level
 * client. The transport receives a completed immutable session and publishes replacements
 * atomically. Automatic provider invocation, provider failure handling, and refresh/reconnect
 * orchestration are deferred to OSP-035.
 */
public final class PubSubApiTransport implements SalesforceEventTransport {

  private final ManagedChannel channel;
  private final PubSubGrpc.PubSubStub asyncStub;
  private final AtomicReference<SessionMetadata> session;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final Object lifecycleLock = new Object();
  private final Set<ActiveOperation> activeOperations = new HashSet<>();

  /**
   * Creates a TLS-protected HTTP/2 channel using an already-acquired Salesforce session.
   *
   * <p>This constructor does not invoke an authentication provider or block for authentication.
   */
  public PubSubApiTransport(EndpointConfig endpoint, SalesforceSession initialSession) {
    this(SessionMetadata.from(initialSession), newSecureChannel(endpoint));
  }

  PubSubApiTransport(ManagedChannel channel, SalesforceSession initialSession) {
    this(SessionMetadata.from(initialSession), channel);
  }

  private PubSubApiTransport(SessionMetadata initialSession, ManagedChannel channel) {
    this.channel = Objects.requireNonNull(channel, "channel");
    this.asyncStub = PubSubGrpc.newStub(channel);
    this.session = new AtomicReference<>(initialSession);
  }

  @Override
  public CompletableFuture<TopicMetadata> getTopic(String topicName) {
    if (topicName == null) {
      throw new SalesforcePubSubException("Missing topic name for Salesforce Pub/Sub RPC");
    }
    TopicRequest request = TopicRequest.newBuilder().setTopicName(topicName).build();
    return invokeUnary(
        (stub, observer) -> stub.getTopic(request, observer), PubSubApiTransport::mapTopic);
  }

  @Override
  public CompletableFuture<SchemaMetadata> getSchema(String schemaId) {
    if (schemaId == null) {
      throw new SalesforcePubSubException("Missing schema ID for Salesforce Pub/Sub RPC");
    }
    SchemaRequest request = SchemaRequest.newBuilder().setSchemaId(schemaId).build();
    return invokeUnary(
        (stub, observer) -> stub.getSchema(request, observer), PubSubApiTransport::mapSchema);
  }

  @Override
  public void updateSession(SalesforceSession updatedSession) {
    SessionMetadata replacement = SessionMetadata.from(updatedSession);
    synchronized (lifecycleLock) {
      if (closed.get()) {
        throw new IllegalStateException("Salesforce Pub/Sub transport is closed");
      }
      session.set(replacement);
    }
  }

  @Override
  public boolean isClosed() {
    return closed.get();
  }

  @Override
  public void close() {
    List<ActiveOperation> operations;
    synchronized (lifecycleLock) {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      operations = new ArrayList<>(activeOperations);
      activeOperations.clear();
    }
    for (ActiveOperation operation : operations) {
      operation.closeFromTransport();
    }
    channel.shutdownNow();
  }

  CompletableFuture<PublishResponse> publish(PublishRequest request) {
    if (request == null || request.getEventsCount() != 1) {
      throw new SalesforcePubSubException(
          "Salesforce Pub/Sub unary Publish requires exactly one event");
    }
    return invokeUnary((stub, observer) -> stub.publish(request, observer), Function.identity());
  }

  SubscriptionRpc subscribe(StreamObserver<FetchResponse> responseObserver) {
    Objects.requireNonNull(responseObserver, "response observer");
    SubscriptionRpc result = new SubscriptionRpc(responseObserver, this::unregister);
    PubSubGrpc.PubSubStub stub = registerForNewRpc(result);
    if (result.canStart()) {
      try {
        StreamObserver<FetchRequest> requestObserver = stub.subscribe(result.responseObserver());
        result.attachFallback(requestObserver);
      } catch (RuntimeException exception) {
        result.failBeforeStart(exception);
      }
    }
    return result;
  }

  private <RawResponseT, ResponseT> CompletableFuture<ResponseT> invokeUnary(
      BiConsumer<PubSubGrpc.PubSubStub, StreamObserver<RawResponseT>> invocation,
      Function<RawResponseT, ResponseT> responseMapper) {
    Objects.requireNonNull(invocation, "RPC invocation");
    Objects.requireNonNull(responseMapper, "response mapper");
    CancellableRpcFuture<ResponseT> result = new CancellableRpcFuture<>(this::unregister);
    PubSubGrpc.PubSubStub stub = registerForNewRpc(result);
    if (result.canStart()) {
      try {
        invocation.accept(stub, new UnaryResponseObserver<>(result, responseMapper));
      } catch (RuntimeException exception) {
        result.fail(exception);
      }
    }
    return result;
  }

  private PubSubGrpc.PubSubStub registerForNewRpc(ActiveOperation operation) {
    synchronized (lifecycleLock) {
      if (closed.get()) {
        throw new IllegalStateException("Salesforce Pub/Sub transport is closed");
      }
      activeOperations.add(operation);
      SessionMetadata snapshot = session.get();
      return asyncStub.withCallCredentials(new SalesforceCallCredentials(snapshot));
    }
  }

  private void unregister(ActiveOperation operation) {
    synchronized (lifecycleLock) {
      activeOperations.remove(operation);
    }
  }

  @Override
  public String toString() {
    return "PubSubApiTransport[closed=" + closed.get() + "]";
  }

  private static ManagedChannel newSecureChannel(EndpointConfig endpoint) {
    EndpointConfig validated = Objects.requireNonNull(endpoint, "endpoint");
    return NettyChannelBuilder.forAddress(validated.host(), validated.port())
        .useTransportSecurity()
        .build();
  }

  private static TopicMetadata mapTopic(TopicInfo response) {
    return new TopicMetadata(
        response.getTopicName(),
        response.getCanPublish(),
        response.getCanSubscribe(),
        response.getSchemaId());
  }

  private static SchemaMetadata mapSchema(SchemaInfo response) {
    return new SchemaMetadata(response.getSchemaId(), response.getSchemaJson());
  }

  private static RpcFailure sanitize(Throwable failure) {
    Status.Code code =
        failure == null ? Status.Code.UNKNOWN : Status.fromThrowable(failure).getCode();
    return new RpcFailure(code);
  }

  private interface ActiveOperation {

    void closeFromTransport();
  }

  static final class UnaryResponseObserver<RawResponseT, ResponseT>
      implements ClientResponseObserver<Object, RawResponseT> {

    private final CancellableRpcFuture<ResponseT> result;
    private final Function<RawResponseT, ResponseT> responseMapper;
    private boolean receivedResponse;
    private ResponseT response;

    UnaryResponseObserver(
        CancellableRpcFuture<ResponseT> result, Function<RawResponseT, ResponseT> responseMapper) {
      this.result = result;
      this.responseMapper = responseMapper;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<Object> requestStream) {
      result.attach(requestStream);
    }

    @Override
    public void onNext(RawResponseT response) {
      if (receivedResponse) {
        result.failAndCancel(null, "Invalid unary RPC response count");
        return;
      }
      try {
        this.response = responseMapper.apply(response);
        receivedResponse = true;
      } catch (RuntimeException exception) {
        result.failAndCancel(exception, "Invalid unary RPC response");
      }
    }

    @Override
    public void onError(Throwable failure) {
      result.fail(failure);
    }

    @Override
    public void onCompleted() {
      if (result.isDone()) {
        return;
      }
      if (receivedResponse) {
        result.complete(response);
      } else {
        result.fail(null);
      }
    }
  }

  static final class CancellableRpcFuture<ResponseT> extends CompletableFuture<ResponseT>
      implements ActiveOperation {

    private final AtomicReference<ClientCallStreamObserver<?>> requestStream =
        new AtomicReference<>();
    private final AtomicReference<RpcState> state = new AtomicReference<>(RpcState.ACTIVE);
    private final Consumer<ActiveOperation> onTerminal;
    private final AtomicBoolean terminalNotified = new AtomicBoolean();

    CancellableRpcFuture() {
      this(ignored -> {});
    }

    private CancellableRpcFuture(Consumer<ActiveOperation> onTerminal) {
      this.onTerminal = onTerminal;
    }

    @Override
    public boolean complete(ResponseT response) {
      if (!state.compareAndSet(RpcState.ACTIVE, RpcState.TERMINATED)) {
        return false;
      }
      notifyTerminal();
      return super.complete(response);
    }

    @Override
    public boolean completeExceptionally(Throwable failure) {
      if (!state.compareAndSet(RpcState.ACTIVE, RpcState.TERMINATED)) {
        return false;
      }
      notifyTerminal();
      return super.completeExceptionally(failure);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      if (!state.compareAndSet(RpcState.ACTIVE, RpcState.CANCELLED)) {
        return false;
      }
      notifyTerminal();
      boolean cancelled = super.cancel(mayInterruptIfRunning);
      cancelRequest(requestStream.get());
      return cancelled;
    }

    @Override
    public void closeFromTransport() {
      if (state.compareAndSet(RpcState.ACTIVE, RpcState.CANCELLED)) {
        CompletableFuture.runAsync(this::cancelCompletionFromTransport);
      }
    }

    private boolean canStart() {
      return state.get() == RpcState.ACTIVE;
    }

    private void attach(ClientCallStreamObserver<?> stream) {
      if (!requestStream.compareAndSet(null, stream)) {
        stream.cancel("Duplicate RPC stream", null);
        return;
      }
      if (state.get() == RpcState.CANCELLED) {
        cancelRequest(stream);
      }
    }

    private void fail(Throwable failure) {
      completeExceptionally(sanitize(failure));
    }

    private void failAndCancel(Throwable failure, String reason) {
      if (completeExceptionally(sanitize(failure))) {
        ClientCallStreamObserver<?> stream = requestStream.get();
        if (stream != null) {
          stream.cancel(reason, null);
        }
      }
    }

    private static void cancelRequest(ClientCallStreamObserver<?> stream) {
      if (stream != null) {
        try {
          stream.cancel("RPC cancelled", null);
        } catch (RuntimeException ignored) {
          // Cancellation is already reflected by the future's terminal state.
        }
      }
    }

    private void notifyTerminal() {
      if (terminalNotified.compareAndSet(false, true)) {
        onTerminal.accept(this);
      }
    }

    private void cancelCompletionFromTransport() {
      super.cancel(false);
    }
  }

  static final class SubscriptionRpc implements ActiveOperation {

    private final StreamObserver<FetchResponse> downstream;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final Consumer<ActiveOperation> onTerminal;
    private final AtomicBoolean terminalNotified = new AtomicBoolean();
    private final AtomicBoolean callbackActive = new AtomicBoolean();
    private final AtomicReference<StreamObserver<FetchRequest>> requests = new AtomicReference<>();
    private final AtomicReference<SubscriptionState> state =
        new AtomicReference<>(SubscriptionState.STARTING);

    private SubscriptionRpc(
        StreamObserver<FetchResponse> downstream, Consumer<ActiveOperation> onTerminal) {
      this.downstream = downstream;
      this.onTerminal = onTerminal;
    }

    SubscriptionRpc(StreamObserver<FetchResponse> downstream) {
      this(downstream, ignored -> {});
    }

    void send(FetchRequest request) {
      Objects.requireNonNull(request, "fetch request");
      StreamObserver<FetchRequest> requestObserver = requireRequestObserver();
      if (!state.compareAndSet(SubscriptionState.ACTIVE, SubscriptionState.SENDING)) {
        throw inactiveSubscription();
      }
      try {
        requestObserver.onNext(request);
      } catch (RuntimeException exception) {
        failOutbound(SubscriptionState.SENDING, exception);
        throw sanitize(exception);
      } finally {
        state.compareAndSet(SubscriptionState.SENDING, SubscriptionState.ACTIVE);
      }
    }

    void complete() {
      StreamObserver<FetchRequest> requestObserver = requireRequestObserver();
      if (!state.compareAndSet(SubscriptionState.ACTIVE, SubscriptionState.HALF_CLOSING)) {
        throw inactiveSubscription();
      }
      try {
        requestObserver.onCompleted();
      } catch (RuntimeException exception) {
        if (failOutbound(SubscriptionState.HALF_CLOSING, exception)) {
          throw sanitize(exception);
        }
      } finally {
        state.compareAndSet(SubscriptionState.HALF_CLOSING, SubscriptionState.HALF_CLOSED);
      }
    }

    void cancel() {
      if (!cancelState()) {
        return;
      }
      notifyTerminal();
      completion.cancel(false);
      StreamObserver<FetchRequest> requestObserver = requests.get();
      if (requestObserver instanceof ClientCallStreamObserver<?> clientStream) {
        cancelClientStream(clientStream, "RPC cancelled");
      }
    }

    @Override
    public void closeFromTransport() {
      if (cancelState()) {
        CompletableFuture.runAsync(() -> completion.cancel(false));
      }
    }

    private boolean canStart() {
      return isLive(state.get());
    }

    java.util.concurrent.CompletionStage<Void> completion() {
      return completion.minimalCompletionStage();
    }

    boolean isCancelled() {
      return state.get() == SubscriptionState.CANCELLED;
    }

    ClientResponseObserver<FetchRequest, FetchResponse> responseObserver() {
      return new ClientResponseObserver<>() {
        @Override
        public void beforeStart(ClientCallStreamObserver<FetchRequest> requestStream) {
          attach(requestStream);
        }

        @Override
        public void onNext(FetchResponse response) {
          if (!callbackActive.compareAndSet(false, true)) {
            return;
          }
          try {
            if (!isLive(state.get())) {
              return;
            }
            downstream.onNext(response);
          } catch (RuntimeException exception) {
            failCallback(exception);
          } finally {
            callbackActive.set(false);
          }
        }

        @Override
        public void onError(Throwable failure) {
          terminateWithError(failure);
        }

        @Override
        public void onCompleted() {
          if (!terminateState()) {
            return;
          }
          notifyTerminal();
          try {
            downstream.onCompleted();
            completion.complete(null);
          } catch (RuntimeException exception) {
            completion.completeExceptionally(sanitize(exception));
          }
        }
      };
    }

    void attachFallback(StreamObserver<FetchRequest> requestObserver) {
      attach(requestObserver);
    }

    private void attach(StreamObserver<FetchRequest> requestObserver) {
      if (!requests.compareAndSet(null, requestObserver)) {
        return;
      }
      state.compareAndSet(SubscriptionState.STARTING, SubscriptionState.ACTIVE);
      if (state.get() == SubscriptionState.CANCELLED
          && requestObserver instanceof ClientCallStreamObserver<?> clientStream) {
        cancelClientStream(clientStream, "RPC cancelled");
      }
    }

    private StreamObserver<FetchRequest> requireRequestObserver() {
      StreamObserver<FetchRequest> requestObserver = requests.get();
      if (requestObserver == null) {
        throw inactiveSubscription();
      }
      return requestObserver;
    }

    private boolean cancelState() {
      while (true) {
        SubscriptionState current = state.get();
        if (current == SubscriptionState.CANCELLED || current == SubscriptionState.TERMINATED) {
          return false;
        }
        if (state.compareAndSet(current, SubscriptionState.CANCELLED)) {
          return true;
        }
      }
    }

    private boolean terminateState() {
      while (true) {
        SubscriptionState current = state.get();
        if (!isLive(current)) {
          return false;
        }
        if (state.compareAndSet(current, SubscriptionState.TERMINATED)) {
          return true;
        }
      }
    }

    private boolean failOutbound(SubscriptionState expected, Throwable failure) {
      if (!state.compareAndSet(expected, SubscriptionState.TERMINATED)) {
        return false;
      }
      notifyTerminal();
      completion.completeExceptionally(sanitize(failure));
      notifyError(failure);
      return true;
    }

    private void failBeforeStart(Throwable failure) {
      terminateWithError(failure);
    }

    private void failCallback(Throwable failure) {
      if (!terminateState()) {
        return;
      }
      notifyTerminal();
      completion.completeExceptionally(sanitize(failure));
      notifyError(failure);
      StreamObserver<FetchRequest> requestObserver = requests.get();
      if (requestObserver instanceof ClientCallStreamObserver<?> clientStream) {
        cancelClientStream(clientStream, "Subscription callback failed");
      }
    }

    private void terminateWithError(Throwable failure) {
      if (!terminateState()) {
        return;
      }
      notifyTerminal();
      completion.completeExceptionally(sanitize(failure));
      notifyError(failure);
    }

    private void notifyError(Throwable failure) {
      try {
        downstream.onError(sanitize(failure));
      } catch (RuntimeException ignored) {
        // A broken observer cannot receive any safer notification.
      }
    }

    private void notifyTerminal() {
      if (terminalNotified.compareAndSet(false, true)) {
        onTerminal.accept(this);
      }
    }

    private static boolean isLive(SubscriptionState state) {
      return state != SubscriptionState.CANCELLED && state != SubscriptionState.TERMINATED;
    }

    private static SalesforcePubSubException inactiveSubscription() {
      return new SalesforcePubSubException("Salesforce Pub/Sub subscription is not active");
    }

    private static void cancelClientStream(
        ClientCallStreamObserver<?> clientStream, String reason) {
      try {
        clientStream.cancel(reason, null);
      } catch (RuntimeException ignored) {
        // The local terminal state already prevents further transport activity.
      }
    }
  }

  private enum SubscriptionState {
    STARTING,
    ACTIVE,
    SENDING,
    HALF_CLOSING,
    HALF_CLOSED,
    CANCELLED,
    TERMINATED
  }

  private enum RpcState {
    ACTIVE,
    CANCELLED,
    TERMINATED
  }

  private static final class RpcFailure extends SalesforcePubSubException {

    private static final long serialVersionUID = 1L;

    private RpcFailure(Status.Code code) {
      super("Salesforce Pub/Sub RPC failed [" + code + "]");
    }
  }
}
