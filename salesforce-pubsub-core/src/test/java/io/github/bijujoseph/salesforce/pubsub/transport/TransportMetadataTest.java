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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.github.bijujoseph.salesforce.pubsub.auth.SalesforceSession;
import org.junit.jupiter.api.Test;

class TransportMetadataTest {

  @Test
  void schemaDiagnosticsRetainOnlyTheSafeIdentifier() {
    SchemaMetadata metadata =
        new SchemaMetadata("schema-1", "sensitive schema payload and customer fields");

    assertEquals("SchemaMetadata[schemaId=schema-1, schemaJson=<redacted>]", metadata.toString());
    assertFalse(metadata.toString().contains("sensitive"));
    assertFalse(metadata.toString().contains("customer"));
  }

  @Test
  void sessionMetadataDiagnosticsRedactEveryGrpcHeaderValue() {
    SalesforceSession session =
        new SalesforceSession(
            "secret-access-token",
            "https://customer-secret.example",
            "secret-tenant",
            "secret-user");

    SessionMetadata metadata = SessionMetadata.from(session);

    assertEquals(
        "SessionMetadata[accessToken=<redacted>, instanceUrl=<redacted>, tenantId=<redacted>]",
        metadata.toString());
    assertFalse(metadata.toString().contains("secret"));
  }
}
