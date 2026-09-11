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
import java.time.Duration;

/** Exponential-backoff and jitter settings for transient Salesforce failures. */
public record RetryOptions(
    Duration initialBackoff, Duration maxBackoff, int maxConsecutiveFailures, double jitter) {

  public RetryOptions {
    if (initialBackoff == null || initialBackoff.isZero() || initialBackoff.isNegative()) {
      throw new ConfigurationException("initialBackoff must be positive");
    }
    if (maxBackoff == null || maxBackoff.isZero() || maxBackoff.isNegative()) {
      throw new ConfigurationException("maxBackoff must be positive");
    }
    if (maxBackoff.compareTo(initialBackoff) < 0) {
      throw new ConfigurationException("maxBackoff cannot be less than initialBackoff");
    }
    if (maxConsecutiveFailures < 1) {
      throw new ConfigurationException("maxConsecutiveFailures must be positive");
    }
    if (!Double.isFinite(jitter) || jitter < 0.0 || jitter > 1.0) {
      throw new ConfigurationException("jitter must be between 0.0 and 1.0");
    }
  }

  public static RetryOptions defaults() {
    return new RetryOptions(Duration.ofSeconds(1), Duration.ofSeconds(30), 20, 0.20);
  }
}
