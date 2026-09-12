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

package io.github.bijujoseph.salesforce.pubsub.config;

import io.github.bijujoseph.salesforce.pubsub.error.ConfigurationException;

/** Bounded client-side demand settings for a Salesforce subscription. */
public record FlowControlOptions(
    int initialRequestCount, int refillThreshold, int refillCount, int maxInFlightEvents) {

  public FlowControlOptions {
    if (initialRequestCount < 1) {
      throw new ConfigurationException("initialRequestCount must be positive");
    }
    if (refillThreshold < 1) {
      throw new ConfigurationException("refillThreshold must be positive");
    }
    if (refillCount < 1) {
      throw new ConfigurationException("refillCount must be positive");
    }
    if (maxInFlightEvents < 1) {
      throw new ConfigurationException("maxInFlightEvents must be positive");
    }
    if (initialRequestCount > maxInFlightEvents) {
      throw new ConfigurationException("initialRequestCount cannot exceed maxInFlightEvents");
    }
    if (refillThreshold >= maxInFlightEvents) {
      throw new ConfigurationException("refillThreshold must be less than maxInFlightEvents");
    }
    if (refillCount > maxInFlightEvents) {
      throw new ConfigurationException("refillCount cannot exceed maxInFlightEvents");
    }
    if (refillThreshold > maxInFlightEvents - refillCount) {
      throw new ConfigurationException(
          "refillThreshold plus refillCount cannot exceed maxInFlightEvents");
    }
  }

  public static FlowControlOptions defaults() {
    return new FlowControlOptions(10, 2, 10, 20);
  }
}
