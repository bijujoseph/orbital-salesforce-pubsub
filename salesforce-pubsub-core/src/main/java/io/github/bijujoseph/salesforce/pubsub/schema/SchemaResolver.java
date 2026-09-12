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

import io.github.bijujoseph.salesforce.pubsub.error.SchemaLookupException;
import io.github.bijujoseph.salesforce.pubsub.transport.SalesforceEventTransport;
import io.github.bijujoseph.salesforce.pubsub.transport.SchemaMetadata;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.avro.Schema;

/** Fetches and parses Salesforce Avro schemas without applying a cache policy. */
public final class SchemaResolver {

  private final SalesforceEventTransport transport;

  public SchemaResolver(SalesforceEventTransport transport) {
    this.transport = Objects.requireNonNull(transport, "transport");
  }

  /** Resolves one exact schema ID into its parsed Avro representation. */
  public CompletionStage<Schema> resolve(String schemaId) {
    if (isMissing(schemaId)) {
      return CompletableFuture.failedFuture(new SchemaLookupException(schemaId));
    }

    CompletionStage<SchemaMetadata> lookup;
    try {
      lookup = transport.getSchema(schemaId);
    } catch (RuntimeException failure) {
      return CompletableFuture.failedFuture(failure);
    }
    if (lookup == null) {
      return CompletableFuture.failedFuture(new SchemaLookupException(schemaId));
    }
    return lookup.thenApply(metadata -> parse(schemaId, metadata));
  }

  static boolean isMissing(String value) {
    return value == null || value.isBlank();
  }

  private static Schema parse(String requestedSchemaId, SchemaMetadata metadata) {
    if (metadata == null
        || isMissing(metadata.schemaId())
        || !requestedSchemaId.equals(metadata.schemaId())
        || isMissing(metadata.schemaJson())) {
      throw new SchemaLookupException(requestedSchemaId);
    }
    try {
      return new Schema.Parser().parse(metadata.schemaJson());
    } catch (RuntimeException failure) {
      throw new SchemaLookupException(requestedSchemaId);
    }
  }
}
