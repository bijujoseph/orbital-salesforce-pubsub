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

package io.github.bijujoseph.salesforce.pubsub.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class TypedExceptionTest {

  @Test
  void blueprintFailureCategoriesHaveDistinctRuntimeTypesWithoutCauses() {
    List<SalesforcePubSubException> failures =
        List.of(
            new AuthenticationException("authentication failed"),
            new AuthorizationException("authorization failed"),
            new TopicNotFoundException("/event/Test__e"),
            new SubscriptionException("subscription failed"),
            new InvalidReplayException("invalid replay"),
            new SchemaLookupException("schema-1"),
            new EventDecodeException("decode failed"),
            new EventEncodeException("encode failed"),
            new PublishException("publish failed"),
            new ReplayStoreException("replay store failed"),
            new TransportException("UNAVAILABLE"));

    assertEquals(11, failures.stream().map(Object::getClass).distinct().count());
    failures.forEach(
        failure -> {
          assertInstanceOf(SalesforcePubSubException.class, failure);
          assertNull(failure.getCause());
        });
  }

  @Test
  void topicCapabilityContextUsesFixedSanitizedOutput() {
    AuthorizationException failure =
        new AuthorizationException("/event/Test__e\r\nforged", "subscribe\nforged");

    assertEquals("/event/Test__e__forged", failure.topic());
    assertEquals("subscribe_forged", failure.operation());
    assertEquals(
        "Salesforce topic does not permit operation [operation=subscribe_forged, topic=/event/Test__e__forged]",
        failure.getMessage());
    assertNull(failure.getCause());
  }

  @Test
  void topicAndSchemaFailuresExposeOnlyBoundedSafeContext() {
    String longValue = "x".repeat(300) + "credential-sentinel";
    TopicNotFoundException topic = new TopicNotFoundException(longValue);
    SchemaLookupException schema = new SchemaLookupException(null);

    assertTrue(topic.topic().length() <= 257);
    assertTrue(topic.topic().endsWith("…"));
    assertTrue(!topic.toString().contains("credential-sentinel"));
    assertEquals("<absent>", schema.schemaId());
    assertEquals("Salesforce schema lookup failed [schemaId=<absent>]", schema.getMessage());
  }

  @Test
  void transportFailureCarriesOnlyStatusCode() {
    TransportException failure = new TransportException("INTERNAL\r\nremote payload");

    assertEquals("INTERNAL__remote payload", failure.code());
    assertEquals("Salesforce Pub/Sub RPC failed [INTERNAL__remote payload]", failure.getMessage());
    assertNull(failure.getCause());
  }
}
