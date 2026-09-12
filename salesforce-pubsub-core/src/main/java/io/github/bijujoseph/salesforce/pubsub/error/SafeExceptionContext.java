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

final class SafeExceptionContext {

  private static final int MAX_VALUE_LENGTH = 256;

  private SafeExceptionContext() {}

  static String value(String value) {
    if (value == null || value.isBlank()) {
      return "<absent>";
    }
    StringBuilder sanitized = new StringBuilder(Math.min(value.length(), MAX_VALUE_LENGTH + 1));
    int offset = 0;
    while (offset < value.length()) {
      int codePoint = value.codePointAt(offset);
      int normalized = unsafeDiagnosticCharacter(codePoint) ? '_' : codePoint;
      if (sanitized.length() + Character.charCount(normalized) > MAX_VALUE_LENGTH) {
        break;
      }
      sanitized.appendCodePoint(normalized);
      offset += Character.charCount(codePoint);
    }
    if (offset < value.length()) {
      sanitized.append('…');
    }
    return sanitized.toString();
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
