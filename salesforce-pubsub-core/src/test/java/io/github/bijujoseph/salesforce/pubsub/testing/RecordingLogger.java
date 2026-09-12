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

package io.github.bijujoseph.salesforce.pubsub.testing;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.spi.LoggingEventBuilder;

/** In-memory SLF4J facade used to verify structured diagnostics without a runtime binding. */
public final class RecordingLogger {

  private final List<LogEvent> events = new ArrayList<>();

  public Logger proxy() {
    return (Logger)
        Proxy.newProxyInstance(
            Logger.class.getClassLoader(),
            new Class<?>[] {Logger.class},
            (ignored, method, arguments) -> {
              if (method.getName().startsWith("at")) {
                return builder(method.getName().substring(2).toUpperCase());
              }
              if (method.getName().startsWith("is")) {
                return true;
              }
              if (method.getName().equals("getName")) {
                return "recording";
              }
              return null;
            });
  }

  public List<LogEvent> events() {
    return List.copyOf(events);
  }

  private LoggingEventBuilder builder(String level) {
    Map<String, Object> keyValues = new LinkedHashMap<>();
    final LoggingEventBuilder[] self = new LoggingEventBuilder[1];
    self[0] =
        (LoggingEventBuilder)
            Proxy.newProxyInstance(
                LoggingEventBuilder.class.getClassLoader(),
                new Class<?>[] {LoggingEventBuilder.class},
                (ignored, method, arguments) -> {
                  if (method.getName().equals("addKeyValue")) {
                    keyValues.put((String) arguments[0], arguments[1]);
                    return self[0];
                  }
                  if (method.getName().equals("log")) {
                    String message =
                        arguments == null || arguments.length == 0
                            ? ""
                            : String.valueOf(arguments[0]);
                    events.add(new LogEvent(level, message, Map.copyOf(keyValues)));
                    return null;
                  }
                  if (method.getReturnType() == LoggingEventBuilder.class) {
                    return self[0];
                  }
                  return null;
                });
    return self[0];
  }

  public record LogEvent(String level, String message, Map<String, Object> keyValues) {}
}
