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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.bijujoseph.salesforce.pubsub.config.FlowControlOptions;
import io.github.bijujoseph.salesforce.pubsub.replay.InvalidReplayPolicy;
import io.github.bijujoseph.salesforce.pubsub.replay.ReplayStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ReplayStartSelectorTest {

  private static final String CONNECTION = "salesforce-prod";
  private static final String TOPIC = "/event/Manual_Message__e";
  private static final String CONSUMER = "manual-message-consumer";

  @ParameterizedTest(name = "{0}")
  @MethodSource("precedenceScenarios")
  void selectsStartUsingDeterministicPrecedence(
      String description,
      SubscriptionStart requested,
      byte[] stored,
      Class<? extends SubscriptionStart> expectedType,
      byte[] expectedReplay,
      int expectedLoads) {
    RecordingReplayStore store = new RecordingReplayStore(stored);
    SubscribeRequest request = request(requested);

    SubscriptionStart selected = ReplayStartSelector.select(request, store);

    assertInstanceOf(expectedType, selected);
    assertEquals(expectedLoads, store.loads.size());
    if (expectedReplay == null) {
      assertSame(requested, selected);
    } else {
      assertArrayEquals(expectedReplay, assertInstanceOf(ReplayId.class, selected).value());
    }
  }

  @Test
  void usesTheExactStableNamespaceOnEveryResumeSelection() {
    RecordingReplayStore store = new RecordingReplayStore(null);
    SubscribeRequest request = request(new Earliest());

    assertSame(request.start(), ReplayStartSelector.select(request, store));
    assertSame(request.start(), ReplayStartSelector.select(request, store));

    assertEquals(
        List.of(
            new LoadCall(CONNECTION, TOPIC, CONSUMER), new LoadCall(CONNECTION, TOPIC, CONSUMER)),
        store.loads);
  }

  @Test
  void isolatesSelectorOutputFromMutableStoreBytes() {
    byte[] stored = {0x00, 0x7f, (byte) 0x80, (byte) 0xff};
    RecordingReplayStore store = new RecordingReplayStore(stored);

    ReplayId selected =
        assertInstanceOf(ReplayId.class, ReplayStartSelector.select(request(new Latest()), store));
    stored[0] = 0x55;
    byte[] exposed = selected.value();
    exposed[1] = 0x66;

    assertArrayEquals(new byte[] {0x00, 0x7f, (byte) 0x80, (byte) 0xff}, selected.value());
  }

  @Test
  void rejectsMissingSelectorInputs() {
    RecordingReplayStore store = new RecordingReplayStore(null);
    assertThrows(NullPointerException.class, () -> ReplayStartSelector.select(null, store));
    assertThrows(
        NullPointerException.class, () -> ReplayStartSelector.select(request(new Latest()), null));
  }

  @ParameterizedTest(name = "null start is rejected before consulting {0}")
  @MethodSource("invalidStartStoreScenarios")
  void rejectsMissingStartBeforeLoadingReplayState(String description, byte[] stored) {
    RecordingReplayStore store = new RecordingReplayStore(stored);

    IllegalArgumentException failure =
        assertThrows(
            IllegalArgumentException.class, () -> ReplayStartSelector.select(request(null), store));

    assertEquals(0, store.loads.size());
    String message = failure.getMessage();
    assertNotNull(message);
    assertFalse(message.isBlank());
    assertFalse(message.contains(CONNECTION));
    assertFalse(message.contains(TOPIC));
    assertFalse(message.contains(CONSUMER));
    if (stored != null) {
      assertFalse(message.contains(Arrays.toString(stored)));
    }
  }

  private static Stream<Arguments> precedenceScenarios() {
    byte[] explicit = {0x01, (byte) 0xff};
    byte[] checkpoint = {0x02, (byte) 0x80};
    return Stream.of(
        Arguments.of(
            "explicit replay overrides stored checkpoint",
            new ReplayId(explicit),
            checkpoint,
            ReplayId.class,
            explicit,
            0),
        Arguments.of(
            "explicit replay bypasses empty store",
            new ReplayId(explicit),
            null,
            ReplayId.class,
            explicit,
            0),
        Arguments.of(
            "stored checkpoint overrides latest",
            new Latest(),
            checkpoint,
            ReplayId.class,
            checkpoint,
            1),
        Arguments.of(
            "stored checkpoint overrides earliest",
            new Earliest(),
            checkpoint,
            ReplayId.class,
            checkpoint,
            1),
        Arguments.of(
            "latest applies without checkpoint", new Latest(), null, Latest.class, null, 1),
        Arguments.of(
            "earliest applies without checkpoint", new Earliest(), null, Earliest.class, null, 1));
  }

  private static Stream<Arguments> invalidStartStoreScenarios() {
    return Stream.of(
        Arguments.of("empty store", null),
        Arguments.of("store containing a checkpoint", new byte[] {0x01, (byte) 0xff}));
  }

  private static SubscribeRequest request(SubscriptionStart start) {
    return new SubscribeRequest(
        CONNECTION,
        CONSUMER,
        TOPIC,
        start,
        InvalidReplayPolicy.ERROR,
        FlowControlOptions.defaults());
  }

  private static final class RecordingReplayStore implements ReplayStore {
    private final byte[] checkpoint;
    private final List<LoadCall> loads = new ArrayList<>();

    private RecordingReplayStore(byte[] checkpoint) {
      this.checkpoint = checkpoint;
    }

    @Override
    public Optional<byte[]> load(String connectionName, String topic, String consumerName) {
      loads.add(new LoadCall(connectionName, topic, consumerName));
      return Optional.ofNullable(checkpoint);
    }

    @Override
    public void save(String connectionName, String topic, String consumerName, byte[] replayId) {
      throw new AssertionError("Start selection must not write checkpoints");
    }
  }

  private record LoadCall(String connectionName, String topic, String consumerName) {}
}
