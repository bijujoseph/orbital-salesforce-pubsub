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

import io.github.bijujoseph.salesforce.pubsub.error.EventDecodeException;
import io.github.bijujoseph.salesforce.pubsub.error.EventEncodeException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

/** Encodes and decodes Salesforce Avro data using protocol-neutral Java values. */
public final class SalesforceAvroCodec {

  private final GenericData genericData;
  private final ResolvedSchemaLookup schemaLookup;

  public SalesforceAvroCodec(SalesforceSchemaCache schemaCache) {
    this(Objects.requireNonNull(schemaCache, "schemaCache")::resolvedSchema);
  }

  SalesforceAvroCodec(ResolvedSchemaLookup schemaLookup) {
    this.schemaLookup = Objects.requireNonNull(schemaLookup, "schemaLookup");
    this.genericData = configuredGenericData();
  }

  /** Decodes one complete Avro datum with an already-resolved Salesforce schema. */
  public Map<String, Object> decode(String schemaId, byte[] payload) {
    try {
      Schema schema = requireResolvedSchema(schemaId);
      Objects.requireNonNull(payload, "payload");
      if (schema.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException("event schema must be a record");
      }

      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
      Object decoded = new GenericDatumReader<>(schema, schema, genericData).read(null, decoder);
      if (!decoder.isEnd()) {
        throw new IOException("trailing bytes after Avro datum");
      }
      return normalizeRecord(schema, decoded);
    } catch (IOException | RuntimeException failure) {
      throw EventDecodeException.forSchema(schemaId);
    }
  }

  /** Encodes a neutral event map with an already-resolved Salesforce schema. */
  public byte[] encode(String schemaId, Map<String, Object> payload) {
    try {
      return encode(schemaId, requireResolvedSchema(schemaId), payload);
    } catch (EventEncodeException failure) {
      throw failure;
    } catch (RuntimeException failure) {
      throw EventEncodeException.forSchema(schemaId);
    }
  }

  byte[] encode(String schemaId, Schema schema, Map<String, Object> payload) {
    try {
      Objects.requireNonNull(payload, "payload");
      if (schema.getType() != Schema.Type.RECORD) {
        throw new IllegalArgumentException("event schema must be a record");
      }

      Object datum = toAvroValue(schema, payload);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(output, null);
      new GenericDatumWriter<>(schema, genericData).write(datum, encoder);
      encoder.flush();
      return output.toByteArray();
    } catch (IOException | RuntimeException failure) {
      throw EventEncodeException.forSchema(schemaId);
    }
  }

  private static GenericData configuredGenericData() {
    GenericData configured = new GenericData();
    configured.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.DateConversion());
    configured.addLogicalTypeConversion(
        new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
    configured.addLogicalTypeConversion(
        new org.apache.avro.data.TimeConversions.TimestampMicrosConversion());
    return configured;
  }

  private Schema requireResolvedSchema(String schemaId) {
    Schema schema = schemaLookup.find(schemaId);
    if (schema == null) {
      throw new IllegalStateException("schema has not been resolved");
    }
    return schema;
  }

  private Map<String, Object> normalizeRecord(Schema schema, Object datum) {
    if (!(datum instanceof GenericRecord record)) {
      throw new IllegalArgumentException("decoded datum is not a record");
    }
    Map<String, Object> normalized = new LinkedHashMap<>();
    for (Schema.Field field : schema.getFields()) {
      normalized.put(field.name(), normalizeValue(field.schema(), record.get(field.name())));
    }
    return normalized;
  }

  private Object normalizeValue(Schema schema, Object datum) {
    if (datum == null) {
      return null;
    }
    if (schema.getType() == Schema.Type.UNION) {
      return normalizeValue(schema.getTypes().get(genericData.resolveUnion(schema, datum)), datum);
    }
    return switch (schema.getType()) {
      case RECORD -> normalizeRecord(schema, datum);
      case ARRAY -> normalizeArray(schema.getElementType(), datum);
      case STRING -> datum instanceof Utf8 ? datum.toString() : datum;
      case BYTES -> copyBytes(datum);
      case BOOLEAN, INT, LONG, FLOAT, DOUBLE, NULL -> datum;
      default -> throw new IllegalArgumentException("unsupported Avro value type");
    };
  }

  private List<Object> normalizeArray(Schema elementSchema, Object datum) {
    if (!(datum instanceof Iterable<?> values)) {
      throw new IllegalArgumentException("decoded datum is not an array");
    }
    List<Object> normalized = new ArrayList<>();
    for (Object value : values) {
      normalized.add(normalizeValue(elementSchema, value));
    }
    return normalized;
  }

  private Object toAvroValue(Schema schema, Object value) {
    if (schema.getType() == Schema.Type.UNION) {
      return toAvroUnion(schema, value);
    }
    if (value == null) {
      if (schema.getType() == Schema.Type.NULL) {
        return null;
      }
      throw new IllegalArgumentException("required value is missing");
    }

    LogicalType logicalType = schema.getLogicalType();
    if (logicalType instanceof LogicalTypes.Date) {
      return requireType(value, LocalDate.class);
    }
    if (logicalType instanceof LogicalTypes.TimestampMillis
        || logicalType instanceof LogicalTypes.TimestampMicros) {
      return requireType(value, Instant.class);
    }

    return switch (schema.getType()) {
      case RECORD -> toAvroRecord(schema, value);
      case ARRAY -> toAvroArray(schema.getElementType(), value);
      case STRING -> requireType(value, String.class);
      case BOOLEAN -> requireType(value, Boolean.class);
      case INT -> requireType(value, Integer.class);
      case LONG -> requireType(value, Long.class);
      case FLOAT -> requireType(value, Float.class);
      case DOUBLE -> requireType(value, Double.class);
      case BYTES -> toByteBuffer(value);
      case NULL -> throw new IllegalArgumentException("null schema requires null");
      default -> throw new IllegalArgumentException("unsupported Avro value type");
    };
  }

  private Object toAvroUnion(Schema union, Object value) {
    if (value == null) {
      if (union.getTypes().stream().anyMatch(branch -> branch.getType() == Schema.Type.NULL)) {
        return null;
      }
      throw new IllegalArgumentException("required union value is missing");
    }
    for (Schema branch : union.getTypes()) {
      if (branch.getType() == Schema.Type.NULL) {
        continue;
      }
      try {
        return toAvroValue(branch, value);
      } catch (IllegalArgumentException ignored) {
        // Try the next compatible branch before reporting one typed encode failure.
      }
    }
    throw new IllegalArgumentException("value does not match a union branch");
  }

  private GenericRecord toAvroRecord(Schema schema, Object value) {
    if (!(value instanceof Map<?, ?> values)) {
      throw new IllegalArgumentException("record value must be a map");
    }
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      if (!values.containsKey(field.name())) {
        if (field.hasDefaultValue()) {
          record.put(field.name(), genericData.getDefaultValue(field));
          continue;
        }
        throw new IllegalArgumentException("required field is missing");
      }
      record.put(field.name(), toAvroValue(field.schema(), values.get(field.name())));
    }
    return record;
  }

  private List<Object> toAvroArray(Schema elementSchema, Object value) {
    if (!(value instanceof Iterable<?> values)) {
      throw new IllegalArgumentException("array value must be iterable");
    }
    List<Object> converted = new ArrayList<>();
    for (Object element : values) {
      converted.add(toAvroValue(elementSchema, element));
    }
    return converted;
  }

  private static <T> T requireType(Object value, Class<T> type) {
    if (!type.isInstance(value)) {
      throw new IllegalArgumentException("value has the wrong type");
    }
    return type.cast(value);
  }

  private static ByteBuffer toByteBuffer(Object value) {
    if (value instanceof byte[] bytes) {
      return ByteBuffer.wrap(bytes.clone());
    }
    if (value instanceof ByteBuffer buffer) {
      return buffer.asReadOnlyBuffer();
    }
    throw new IllegalArgumentException("bytes value has the wrong type");
  }

  private static byte[] copyBytes(Object datum) {
    if (!(datum instanceof ByteBuffer buffer)) {
      throw new IllegalArgumentException("decoded datum is not bytes");
    }
    ByteBuffer copy = buffer.asReadOnlyBuffer();
    byte[] bytes = new byte[copy.remaining()];
    copy.get(bytes);
    return bytes;
  }

  @FunctionalInterface
  interface ResolvedSchemaLookup {
    Schema find(String schemaId);
  }
}
