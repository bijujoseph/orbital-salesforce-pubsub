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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Status;
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
            new TransportException(Status.Code.UNAVAILABLE));

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
  void exceptionContextNormalizesUnsafeUnicodeBeforeCodePointSafeTruncation() {
    String supplementaryFormat = new String(Character.toChars(0xE0001));
    String unsafe = "safe\t\0\u001b\u2028\u2029\u202e" + supplementaryFormat + "context";

    String sanitized = SafeExceptionContext.value(unsafe);

    assertEquals("safe_______context", sanitized);
    assertFalse(sanitized.codePoints().anyMatch(TypedExceptionTest::unsafeDiagnosticCharacter));
    assertEquals("safe context", SafeExceptionContext.value("safe context"));
    assertEquals("<absent>", SafeExceptionContext.value(null));

    String boundary = SafeExceptionContext.value("x".repeat(255) + "😀tail");
    assertEquals("x".repeat(255) + "…", boundary);
    assertFalse(Character.isSurrogate(boundary.charAt(boundary.length() - 1)));

    String normalizedBoundary =
        SafeExceptionContext.value("x".repeat(255) + supplementaryFormat + "tail");
    assertEquals("x".repeat(255) + "_…", normalizedBoundary);
    assertFalse(
        normalizedBoundary.codePoints().anyMatch(TypedExceptionTest::unsafeDiagnosticCharacter));
  }

  @Test
  void transportFailureCarriesOnlyStatusCode() {
    TransportException failure = new TransportException(Status.Code.INTERNAL);

    assertEquals("INTERNAL", failure.code());
    assertEquals("Salesforce Pub/Sub RPC failed [INTERNAL]", failure.getMessage());
    assertNull(failure.getCause());
    IllegalArgumentException missing =
        assertThrows(IllegalArgumentException.class, () -> new TransportException(null));
    assertFalse(missing.getMessage().contains("payload"));
    assertThrows(
        NoSuchMethodException.class, () -> TransportException.class.getConstructor(String.class));
  }

  private static boolean unsafeDiagnosticCharacter(int codePoint) {
    int type = Character.getType(codePoint);
    return type == Character.CONTROL
        || type == Character.FORMAT
        || type == Character.LINE_SEPARATOR
        || type == Character.PARAGRAPH_SEPARATOR
        || type == Character.SURROGATE;
  }
}
