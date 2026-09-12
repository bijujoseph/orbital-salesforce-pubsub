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

package io.github.bijujoseph.salesforce.pubsub.subscription;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.bijujoseph.salesforce.pubsub.config.FlowControlOptions;
import io.github.bijujoseph.salesforce.pubsub.replay.InvalidReplayPolicy;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class SubscribeRequestTest {

  @Test
  void exposesTheExactBlueprintRecordComponents() {
    assertTrue(SubscribeRequest.class.isRecord());
    assertEquals(
        List.of(
            "connectionName:java.lang.String",
            "consumerName:java.lang.String",
            "topic:java.lang.String",
            "start:io.github.bijujoseph.salesforce.pubsub.subscription.SubscriptionStart",
            "invalidReplayPolicy:io.github.bijujoseph.salesforce.pubsub.replay.InvalidReplayPolicy",
            "flowControl:io.github.bijujoseph.salesforce.pubsub.config.FlowControlOptions"),
        Arrays.stream(SubscribeRequest.class.getRecordComponents())
            .map(SubscribeRequestTest::componentSignature)
            .toList());
  }

  @Test
  void retainsStableCallerSuppliedConsumerNameAndConfiguration() {
    Latest start = new Latest();
    FlowControlOptions flowControl = FlowControlOptions.defaults();
    SubscribeRequest request =
        new SubscribeRequest(
            "salesforce-prod",
            "manual-message-consumer",
            "/event/Manual_Message__e",
            start,
            InvalidReplayPolicy.ERROR,
            flowControl);

    assertEquals("salesforce-prod", request.connectionName());
    assertEquals("manual-message-consumer", request.consumerName());
    assertEquals("/event/Manual_Message__e", request.topic());
    assertSame(start, request.start());
    assertEquals(InvalidReplayPolicy.ERROR, request.invalidReplayPolicy());
    assertSame(flowControl, request.flowControl());
  }

  @Test
  void exposesAllInvalidReplayPoliciesWithoutImplementingFallback() {
    assertArrayEquals(
        new InvalidReplayPolicy[] {
          InvalidReplayPolicy.ERROR, InvalidReplayPolicy.EARLIEST, InvalidReplayPolicy.LATEST
        },
        InvalidReplayPolicy.values());

    SubscribeRequest request =
        new SubscribeRequest("connection", "consumer", "/event/Test__e", new Latest(), null, null);
    assertNull(request.invalidReplayPolicy());
    assertNull(request.flowControl());
  }

  private static String componentSignature(RecordComponent component) {
    return component.getName() + ":" + component.getType().getTypeName();
  }
}
