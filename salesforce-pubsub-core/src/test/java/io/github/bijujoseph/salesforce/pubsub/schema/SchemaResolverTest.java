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
import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.error.TransportException;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.SchemaMetadata;
import io.github.bijujoseph.salesforce.pubsub.transport.TopicMetadata;
import io.grpc.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

class SchemaResolverTest {

  private static final String RECORD_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"value\",\"type\":\"string\"}]}";

  @Test
  void resolvesTheExactSchemaIdAndParsesAvro() {
    RecordingTransport transport =
        new RecordingTransport(
            schemaId ->
                CompletableFuture.completedFuture(new SchemaMetadata(schemaId, RECORD_SCHEMA)));

    Schema schema = new SchemaResolver(transport).resolve("schema-1").toCompletableFuture().join();

    assertEquals(Schema.Type.RECORD, schema.getType());
    assertEquals("Event", schema.getName());
    assertEquals(List.of("schema-1"), transport.schemaIds);
  }

  @Test
  void rejectsAbsentSchemaIdBeforeTransportAccess() {
    RecordingTransport transport =
        new RecordingTransport(ignored -> CompletableFuture.failedFuture(new AssertionError()));

    Throwable nullFailure = failureOf(new SchemaResolver(transport).resolve(null));
    Throwable blankFailure = failureOf(new SchemaResolver(transport).resolve(" \t"));

    assertInstanceOf(SchemaLookupException.class, nullFailure);
    assertInstanceOf(SchemaLookupException.class, blankFailure);
    assertEquals(List.of(), transport.schemaIds);
  }

  @Test
  void rejectsMismatchedIdsMissingContentAndMalformedAvroWithSafeTypedFailure() {
    SchemaMetadata[] invalid = {
      null,
      new SchemaMetadata(" ", RECORD_SCHEMA),
      new SchemaMetadata("different", RECORD_SCHEMA),
      new SchemaMetadata("schema-1", " "),
      new SchemaMetadata("schema-1", "not-avro")
    };

    for (SchemaMetadata metadata : invalid) {
      RecordingTransport transport =
          new RecordingTransport(ignored -> CompletableFuture.completedFuture(metadata));

      SchemaLookupException failure =
          assertInstanceOf(
              SchemaLookupException.class,
              failureOf(new SchemaResolver(transport).resolve("schema-1")));

      assertEquals("schema-1", failure.schemaId());
      assertEquals("Salesforce schema lookup failed [schemaId=schema-1]", failure.getMessage());
    }
  }

  @Test
  void preservesTypedTransportFailures() {
    TransportException lookupFailure = new TransportException(Status.Code.DEADLINE_EXCEEDED);
    RecordingTransport transport =
        new RecordingTransport(ignored -> CompletableFuture.failedFuture(lookupFailure));

    Throwable failure = failureOf(new SchemaResolver(transport).resolve("schema-1"));

    assertSame(lookupFailure, failure);
  }

  @Test
  void preservesSynchronousFailuresAndRejectsMissingLookupStage() {
    TransportException synchronousFailure = new TransportException(Status.Code.UNAVAILABLE);
    RecordingTransport throwing =
        new RecordingTransport(
            ignored -> {
              throw synchronousFailure;
            });
    RecordingTransport nullStage = new RecordingTransport(ignored -> null);

    assertSame(synchronousFailure, failureOf(new SchemaResolver(throwing).resolve("schema-1")));
    assertInstanceOf(
        SchemaLookupException.class, failureOf(new SchemaResolver(nullStage).resolve("schema-1")));
  }

  private static Throwable failureOf(CompletionStage<?> stage) {
    return assertThrows(CompletionException.class, () -> stage.toCompletableFuture().join())
        .getCause();
  }

  private static final class RecordingTransport implements SalesforceEventTransport {

    private final Function<String, CompletionStage<SchemaMetadata>> schemaLookup;
    private final List<String> schemaIds = new ArrayList<>();

    private RecordingTransport(Function<String, CompletionStage<SchemaMetadata>> schemaLookup) {
      this.schemaLookup = schemaLookup;
    }

    @Override
    public CompletionStage<TopicMetadata> getTopic(String topicName) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompletionStage<SchemaMetadata> getSchema(String schemaId) {
      schemaIds.add(schemaId);
      return schemaLookup.apply(schemaId);
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
