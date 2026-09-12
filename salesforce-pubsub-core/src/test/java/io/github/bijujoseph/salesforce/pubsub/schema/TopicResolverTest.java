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

package io.github.bijujoseph.salesforce.pubsub.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import io.github.bijujoseph.salesforce.pubsub.error.AuthorizationException;
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.error.TopicNotFoundException;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.SchemaMetadata;
import io.github.bijujoseph.salesforce.pubsub.transport.TopicMetadata;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class TopicResolverTest {

  @Test
  void passesCallerTopicThroughExactlyAndRetainsItForTheDownstreamOperation() {
    RecordingTransport transport =
        new RecordingTransport(
            ignored ->
                CompletableFuture.completedFuture(
                    new TopicMetadata("server-returned-name", true, true, "schema-1")));
    TopicResolver resolver = new TopicResolver(transport);
    String sentinel = " /event/Exact_Case__e?do-not-normalize ";

    ResolvedTopic resolved = resolver.resolveForSubscribe(sentinel).toCompletableFuture().join();

    assertEquals(List.of(sentinel), transport.topics);
    assertEquals(sentinel, resolved.topicName());
    assertEquals("schema-1", resolved.schemaId());
  }

  @Test
  void passesInvalidTopicThroughWithoutInferringAnObjectName() {
    TopicNotFoundException notFound = new TopicNotFoundException("Manual_Message__e");
    RecordingTransport transport =
        new RecordingTransport(ignored -> CompletableFuture.failedFuture(notFound));
    TopicResolver resolver = new TopicResolver(transport);

    CompletionException failure =
        assertThrows(
            CompletionException.class,
            () -> resolver.resolveForSubscribe("Manual_Message__e").toCompletableFuture().join());

    assertSame(notFound, failure.getCause());
    assertEquals(List.of("Manual_Message__e"), transport.topics);
  }

  @Test
  void rejectsSubscribeAndPublishCapabilitiesForTheSpecificOperation() {
    RecordingTransport noSubscribe =
        new RecordingTransport(
            ignored ->
                CompletableFuture.completedFuture(
                    new TopicMetadata("ignored", true, false, "schema-1")));
    RecordingTransport noPublish =
        new RecordingTransport(
            ignored ->
                CompletableFuture.completedFuture(
                    new TopicMetadata("ignored", false, true, "schema-1")));

    AuthorizationException subscribeFailure =
        assertInstanceOf(
            AuthorizationException.class,
            failureOf(new TopicResolver(noSubscribe).resolveForSubscribe("/event/Subscribe__e")));
    AuthorizationException publishFailure =
        assertInstanceOf(
            AuthorizationException.class,
            failureOf(new TopicResolver(noPublish).resolveForPublish("/event/Publish__e")));

    assertEquals("subscribe", subscribeFailure.operation());
    assertEquals("/event/Subscribe__e", subscribeFailure.topic());
    assertEquals("publish", publishFailure.operation());
    assertEquals("/event/Publish__e", publishFailure.topic());
  }

  @Test
  void requiresSchemaIdAfterCapabilityValidation() {
    RecordingTransport transport =
        new RecordingTransport(
            ignored ->
                CompletableFuture.completedFuture(new TopicMetadata("ignored", true, true, " ")));

    Throwable failure =
        failureOf(new TopicResolver(transport).resolveForPublish("/event/MissingSchema__e"));

    assertInstanceOf(SchemaLookupException.class, failure);
    assertEquals(List.of("/event/MissingSchema__e"), transport.topics);
  }

  @Test
  void preservesSynchronousTransportFailureAsAnAsynchronousResult() {
    TopicNotFoundException notFound = new TopicNotFoundException(null);
    RecordingTransport transport =
        new RecordingTransport(
            ignored -> {
              throw notFound;
            });

    Throwable failure = failureOf(new TopicResolver(transport).resolveForSubscribe(null));

    assertSame(notFound, failure);
    assertEquals(1, transport.topics.size());
    assertEquals(null, transport.topics.getFirst());
  }

  @Test
  void mapsMissingLookupResultsToTopicNotFound() {
    RecordingTransport nullStage = new RecordingTransport(ignored -> null);
    RecordingTransport nullMetadata =
        new RecordingTransport(ignored -> CompletableFuture.completedFuture(null));

    assertInstanceOf(
        TopicNotFoundException.class,
        failureOf(new TopicResolver(nullStage).resolveForPublish("missing-stage")));
    assertInstanceOf(
        TopicNotFoundException.class,
        failureOf(new TopicResolver(nullMetadata).resolveForPublish("missing-metadata")));
  }

  @Test
  void preservesTypedPermissionFailureFromTransport() {
    AuthorizationException denied = new AuthorizationException("/event/Denied__e", "getTopic");
    RecordingTransport transport =
        new RecordingTransport(ignored -> CompletableFuture.failedFuture(denied));

    Throwable failure =
        failureOf(new TopicResolver(transport).resolveForPublish("/event/Denied__e"));

    assertSame(denied, failure);
  }

  private static Throwable failureOf(CompletionStage<?> stage) {
    return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join())
        .getCause();
  }

  private static final class RecordingTransport implements SalesforceEventTransport {

    private final Function<String, CompletionStage<TopicMetadata>> topicLookup;
    private final List<String> topics = new ArrayList<>();

    private RecordingTransport(Function<String, CompletionStage<TopicMetadata>> topicLookup) {
      this.topicLookup = topicLookup;
    }

    @Override
    public CompletionStage<TopicMetadata> getTopic(String topicName) {
      topics.add(topicName);
      return topicLookup.apply(topicName);
    }

    @Override
    public CompletionStage<SchemaMetadata> getSchema(String schemaId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void updateSession(SalesforceSession session) {}

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void close() {}
  }
}
