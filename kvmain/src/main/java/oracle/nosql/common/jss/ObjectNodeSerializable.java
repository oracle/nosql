/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */

package oracle.nosql.common.jss;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

/**
 * An abstract class for implementing a {@link JsonSerializable} that can be
 * serialized into a json {@link ObjectNode}.
 *
 * <p>
 * A typical implementation class follows the below paradigm:
 * <pre>
 * {@code
 *   public class Perf extends ObjectNodeSerializable {
 *       public static final Map<String, Field<?, Perf>> FIELDS =
 *           new HashMap<>();
 *       public static final String LONG_VALUE =
 *           new LongField<Perf>(FIELDS, "longValue", o -> o.longValue)
 *           .getName();
 *
 *       private final long longValue;
 *
 *       public Perf(long longValue) {
 *           this.longValue = longValue;
 *       }
 *
 *       public Perf(JsonNode payload) {
 *           this(readField(FIELDS, payload, LONG_VALUE));
 *       }
 *
 *       @Override
 *       public ObjectNode toJson() {
 *           return writeFields(FIELDS, this);
 *       }
 *   }
 * }
 * </pre>
 * In general, the implementation set up the FIELDS member variable and
 * specifies the {@linkplain Field fields} of the implementation object. This
 * class provides convenient field types including primitive field types
 * (integer, string, etc.), as well as JsonNode types , JsonSerializable and
 * list-of and map-of JsonSerializable types.
 *
 * <p>
 * A typical inheritance hierarchy of the implementations can be done with the
 * following paradigm:
 * <pre>
 * {@code
 *   public class ExtendPerf extends Perf {
 *       public static final Map<String, Field<?, ExtendPerf>> FIELDS =
 *           new HashMap<>();
 *       public static final String EXTEND_LONG_VALUE =
 *           new LongField<ExtendPerf>(
 *               FIELDS, "extendLongValue", o -> o.extendLongValue)
 *           .getName();
 *
 *       private final long extendLongValue;
 *
 *       public Subclass(long longValue,
 *                       long extendLongValue) {
 *           super(longValue);
 *           this.extendLongValue = extendLongValue;
 *       }
 *
 *       public Subclass(JsonNode payload) {
 *           super(payload);
 *           this.extendLongValue = readField(
 *               FIELDS, payload, EXTEND_LONG_VALUE);
 *       }
 *
 *       @Override
 *       public JsonNode toJson() {
 *           final ObjectNode result = writeFields(Perf.FIELDS, this);
 *           return writeFields(FIELDS, this, result);
 *       }
 *   }
 * }
 * </pre>
 */
public abstract class ObjectNodeSerializable extends AbstractJsonSerializable {

    @Override
    public abstract ObjectNode toJson();

    @Override
    public boolean isDefault() {
        return toJson().isEmpty();
    }

    /**
     * A writer for writing a field of {@link JsonSerializable}.
     */
    public interface FieldWriter<F, T extends ObjectNodeSerializable> {

        /**
         * Writes the field of {@code obj} with {@code name} into {@code
         * payload}. The default value of the field is {@code defaultValue}.
         * The writer should filter out the field (i.e., skipping writing the
         * field) if it is empty and/or equals to the default value.
         */
        void write(T obj, ObjectNode payload, String name, F defaultValue);
    }

    /**
     * Returns a {@link FieldWriter} which first extracts the field from the
     * {@link JsonSerializable} object with the {@code extractor} and then
     * writes the extracted field with the {@code writer}.  This method is used
     * to combine the extractor and writer logic to simplify implementation.
     */
    public static <F, T extends ObjectNodeSerializable>
        FieldWriter<F, T> wrapped(FieldExtractor<F, T> extractor,
                                  ExtractedFieldWriter<F> extractedWriter) {
        Objects.requireNonNull(extractor, "extractor");
        Objects.requireNonNull(extractedWriter, "extractedWriter");
        return (o, p, n, d) -> {
            final F value = extractor.extract(o);
            extractedWriter.write(p, n, value, d);
        };
    }

    /**
     * An extractor to return a field given the {@link ObjectNodeSerializable}
     * object.
     */
    public interface FieldExtractor<F, T extends ObjectNodeSerializable> {

        /**
         * Returns an object that represents the field extracted from {@code
         * obj}.
         */
        F extract(T obj);
    }

    /**
     * A writer to write the extracted field.
     */
    public interface ExtractedFieldWriter<F> {

        /**
         * Writes the {@code extracted} field into the provided {@code payload}.
         */
        void write(ObjectNode payload,
                   String name,
                   F extracted,
                   F defaultValue);
    }

    /**
     * A reader for reading a {@link JsonNode} to return a field.
     */
    public interface FieldReader<F> {

        /**
         * Reads the field which is represented as {@code payload}. Returns
         * {@code defaultValue} if the payload type is incompatible.
         */
        F read(JsonNode payload, F defaultValue);
    }

    /**
     * Returns a {@link FieldReader} which first checks the compatibility with
     * the {@code checker} and then reads the content with {@code innerReader}.
     * The default value is returned if the check failed. This method is used
     * to combine the checker and reader logic to simplify implementation.
     */
    public static <F> FieldReader<F> wrapped(FieldTypeChecker checker,
                                             FieldReader<F> innerReader) {
        Objects.requireNonNull(checker, "checker");
        Objects.requireNonNull(innerReader, "innerReader");
        return (p, d) -> {
            if (!checker.isCompatible(p)) {
                return d;
            }
            return innerReader.read(p, d);
        };
    }

    /**
     * A checker for the compatiblity of a payload type w.r.t. a field.
     */
    public interface FieldTypeChecker {

        /**
         * Returns {@code true} if the payload type is compatible to the
         * field.
         */
        boolean isCompatible(JsonNode payload);
    }

    /**
     * A field of ObjectNodeSerializable.
     */
    public static class Field<F, T extends ObjectNodeSerializable> {

        /** The name of the field. */
        protected final String name;
        /** The field writer. */
        protected final FieldWriter<F, T> fieldWriter;
        /** The json node reader. */
        protected final FieldReader<F> fieldReader;
        /** The default value of the field. */
        protected final F defaultValue;

        /**
         * Construct a field.
         *
         * @param fields the object map of fields that this field belongs
         * to
         * @param name the name of the field
         * @param fieldExtractor extracts a field from a {@link
         * ObjectNodeSerializable} to be serialized.  Returns {@code null} if
         * the field should not be serialized. The {@link #wrapped(FieldFilter,
         * FieldExtractor)} method can also be used to combine a serialization
         * checker and a extractor implementation as a convenience.
         * @param fieldReader reads the {@link JsonNode} corresponding to the
         * {@code name} in the serialized {@link ObjectNode}. The reader must
         * check for type compatibility and returns the supplied {@code
         * defaultValue} if not compatible. The {@link JsonSerializationUtils}
         * provides utility methods to read common data types which includes
         * the compatibility check. The {@link #wrapped(FieldTypeChecker,
         * FieldReader)} method can also be used to combine a type checker and
         * a reader implementation as a convenience.
         * @param defaultValue the default value of the field
         */
        public static <F, T extends ObjectNodeSerializable>
            String create(Map<String, Field<?,T>> fields,
                          String name,
                          FieldWriter<F, T> fieldWriter,
                          FieldReader<F> fieldReader,
                          F defaultValue) {
            return new Field<>(
                fields, name, fieldWriter, fieldReader, defaultValue)
                .getName();

        }

        protected Field(Map<String, Field<?,T>> fields,
                        String name,
                        FieldWriter<F, T> fieldWriter,
                        FieldReader<F> fieldReader,
                        F defaultValue) {
            Objects.requireNonNull(fields, "fields");
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(fieldWriter, "fieldWriter");
            Objects.requireNonNull(fieldReader, "fieldReader");
            Objects.requireNonNull(defaultValue, "defaultValue");
            this.name = name;
            this.fieldWriter = fieldWriter;
            this.fieldReader = fieldReader;
            this.defaultValue = defaultValue;
            fields.put(name, this);
        }

        public String getName() {
            return name;
        }

        public F getDefaultValue() {
            return defaultValue;
        }

        /**
         * Reads a field from a serialized {@code payload} by calling the
         * {@code fieldReader}. Returns the {@code defaultValue} if the payload
         * does not have the corresponding field.
         *
         * Note that we only need to check for missing field here. The
         * type-checking logic is inside {@link FieldReader}.
         */
        public F read(ObjectNode payload) {
            /*
             * Note that we only need to check for missing field here. The
             * type-checking logic is inside the fieldReader.
             */
            final JsonNode e = payload.get(name);
            if (e == null) {
                return defaultValue;
            }
            return fieldReader.read(e, defaultValue);
        }

        /**
         * Extracts the field from {@code object} with {@code fieldExtractor}
         * and writes to the provided {@code payload} in the proper format. Do
         * nothing if the extracted value returns {@code null}.
         */
        public void write(ObjectNode payload,
                          T object) {
            if (object == null) {
                return;
            }
            fieldWriter.write(object, payload, name, defaultValue);
        }
    }

    /**
     * Given a schema of fields, write the ObjectNodeSerializable into a
     * ObjectNode.
     */
    public static <T extends ObjectNodeSerializable>
        ObjectNode writeFields(Map<String, Field<?, T>> fields,
                               T obj) {
        final ObjectNode result = JsonUtils.createObjectNode();
        return writeFields(fields, result, obj);
    }

    /**
     * Given a schema of fields, write the ObjectNodeSerializable into the
     * provided ObjectNode.
     */
    public static <T extends ObjectNodeSerializable>
        ObjectNode writeFields(Map<String, Field<?, T>> fields,
                               ObjectNode result,
                               T obj) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(obj, "obj");
        fields.values().forEach((f) -> f.write(result, obj));
        return result;
    }

    /**
     * Given a schema of fields, read the field corresponding to the name.
     * Returns the default value of the field if the payload is not a
     * ObjectNode.
     *
     * Note that the field itself does checks for reading non-existing or
     * incompatible values.
     */
    @SuppressWarnings("unchecked")
    public static <F, T extends ObjectNodeSerializable>
    F readField(Map<String, Field<?, T>> fields,
                JsonNode payload,
                String name) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(payload, "payload");
        final Field<F, T> field = (Field<F, T>) fields.get(name);
        if (!payload.isObject()) {
            return field.getDefaultValue();
        }
        return field.read(payload.asObject());
    }

    /**
     * A boolean field.
     */
    public static class BooleanField<T extends ObjectNodeSerializable>
        extends Field<Boolean, T>
    {

        public static final boolean DEFAULT = false;

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Boolean, T> fieldExtractor) {
            return new BooleanField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Boolean, T> fieldExtractor,
                          boolean defaultValue) {
            return new BooleanField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected BooleanField(Map<String, Field<?, T>> fields,
                               String name,
                               FieldExtractor<Boolean, T> fieldExtractor,
                               boolean defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeBoolean),
                  JsonSerializationUtils::readBoolean,
                  defaultValue);
        }
    }

    /**
     * An integer field.
     */
    public static class IntegerField<T extends ObjectNodeSerializable>
        extends Field<Integer, T>
    {
        public static final int DEFAULT = 0;

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Integer, T> fieldExtractor) {
            return new IntegerField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Integer, T> fieldExtractor,
                          int defaultValue) {
            return new IntegerField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected IntegerField(Map<String, Field<?, T>> fields,
                               String name,
                               FieldExtractor<Integer, T> fieldExtractor,
                               int defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeInteger),
                  JsonSerializationUtils::readInteger,
                  defaultValue);
        }
    }

    /**
     * A long field.
     */
    public static class LongField<T extends ObjectNodeSerializable>
        extends Field<Long, T>
    {
        public static final long DEFAULT = 0L;

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Long, T> fieldExtractor) {
            return new LongField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Long, T> fieldExtractor,
                          long defaultValue) {
            return new LongField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected LongField(Map<String, Field<?, T>> fields,
                            String name,
                            FieldExtractor<Long, T> fieldExtractor,
                            long defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeLong),
                  JsonSerializationUtils::readLong,
                  defaultValue);
        }
    }

    /**
     * A double field.
     */
    public static class DoubleField<T extends ObjectNodeSerializable>
        extends Field<Double, T>
    {
        public static final double DEFAULT = 0.0;

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Double, T> fieldExtractor) {
            return new DoubleField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Double, T> fieldExtractor,
                          double defaultValue) {
            return new DoubleField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected DoubleField(Map<String, Field<?, T>> fields,
                              String name,
                              FieldExtractor<Double, T> fieldExtractor,
                              double defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeDouble),
                  JsonSerializationUtils::readDouble,
                  defaultValue);
        }
    }

    /**
     * A string field.
     */
    public static class StringField<T extends ObjectNodeSerializable>
        extends Field<String, T>
    {
        public static final String DEFAULT = "";

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<String, T> fieldExtractor) {
            return new StringField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<String, T> fieldExtractor,
                          String defaultValue) {
            return new StringField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected StringField(Map<String, Field<?, T>> fields,
                              String name,
                              FieldExtractor<String, T> fieldExtractor,
                              String defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeString),
                  JsonSerializationUtils::readString,
                  defaultValue);
        }
    }

    /**
     * An abstract {@link JsonNode} field.
     */
    public static abstract class JsonNodeField
        <J extends JsonNode, T extends ObjectNodeSerializable>
        extends Field<J, T>
    {
        protected JsonNodeField(Map<String, Field<?, T>> fields,
                                String name,
                                FieldExtractor<J, T> fieldExtractor,
                                Function<JsonNode, J> converter,
                                J defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializationUtils::writeJsonNode),
                  (p, d) -> converter.apply(p),
                  defaultValue);
        }
    }

    /**
     * A field of primitive {@link JsonNode}.
     */
    public static class JsonPrimitiveField<T extends ObjectNodeSerializable>
        extends JsonNodeField<JsonNode, T>
    {
        public static final JsonNode DEFAULT = JsonUtils.createJsonNull();

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<JsonNode, T> fieldExtractor) {
            return new JsonPrimitiveField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<JsonNode, T> fieldExtractor,
                          JsonNode defaultValue) {
            return new JsonPrimitiveField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected JsonPrimitiveField(
            Map<String, Field<?, T>> fields,
            String name,
            FieldExtractor<JsonNode, T> fieldExtractor,
            JsonNode defaultValue)
        {
            super(fields, name,
                  fieldExtractor, Function.identity(), defaultValue);
        }
    }

    /**
     * A field of {@link ObjectNode}.
     */
    public static class ObjectNodeField<T extends ObjectNodeSerializable>
        extends JsonNodeField<ObjectNode, T>
    {
        public static final ObjectNode DEFAULT = JsonUtils.createObjectNode();

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<ObjectNode, T> fieldExtractor) {
            return new ObjectNodeField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<ObjectNode, T> fieldExtractor,
                          ObjectNode defaultValue) {
            return new ObjectNodeField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected ObjectNodeField(Map<String, Field<?, T>> fields,
                                  String name,
                                  FieldExtractor<ObjectNode, T> fieldExtractor,
                                  ObjectNode defaultValue) {
            super(fields, name,
                  fieldExtractor, JsonNode::asObject, defaultValue);
        }
    }

    /**
     * A field of {@link ArrayNode}.
     */
    public static class ArrayNodeField<T extends ObjectNodeSerializable>
        extends JsonNodeField<ArrayNode, T>
    {
        public static final ArrayNode DEFAULT = JsonUtils.createArrayNode();

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<ArrayNode, T> fieldExtractor) {
            return new ArrayNodeField<>(
                fields, name, fieldExtractor, DEFAULT)
                .getName();
        }

        public static <T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<ArrayNode, T> fieldExtractor,
                          ArrayNode defaultValue) {
            return new ArrayNodeField<>(
                fields, name, fieldExtractor, defaultValue)
                .getName();
        }

        protected ArrayNodeField(Map<String, Field<?, T>> fields,
                                 String name,
                                 FieldExtractor<ArrayNode, T> fieldExtractor,
                                 ArrayNode defaultValue) {
            super(fields, name,
                  fieldExtractor, JsonNode::asArray, defaultValue);
        }
    }

    /**
     * A {@link JsonSerializable} field.
     */
    public static class JsonSerializableField
        <F extends JsonSerializable,
         T extends ObjectNodeSerializable> extends Field<F, T>
    {

        public static <F extends JsonSerializable,
                       T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<F, T> fieldExtractor,
                          FieldReader<F> fieldReader,
                          F defaultValue) {
            return new JsonSerializableField<>(
                fields, name, fieldExtractor, fieldReader, defaultValue)
                .getName();
        }

        protected JsonSerializableField(Map<String, Field<?, T>> fields,
                                        String name,
                                        FieldExtractor<F, T> fieldExtractor,
                                        FieldReader<F> fieldReader,
                                        F defaultValue) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          JsonSerializableField::writeExtracted),
                  fieldReader,
                  defaultValue);
            if (!defaultValue.isDefault()) {
                throw new IllegalArgumentException(String.format(
                    "default value should be empty: %s",
                    defaultValue));
            }
        }

        private static <F extends JsonSerializable>
            void writeExtracted(ObjectNode payload,
                                String name,
                                F extracted,
                                F defaultValue)
        {
            if (extracted.isDefault()) {
                return;
            }
            payload.put(name, extracted.toJson());
        }
    }

    /**
     * A field of a list of {@link JsonSerializable} objects.
     *
     * Empty and incompatible entries are filtered out. This implies that the
     * indices of the list entries are not important.
     */
    public static class FilteredListField
        <E extends JsonSerializable,
         T extends ObjectNodeSerializable> extends Field<List<E>, T>
    {
        /**
         * Returns the default value.
         */
        public static <E> List<E> getDefault() {
            return Collections.emptyList();
        }

        public static <E extends JsonSerializable,
                       T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<List<E>, T> fieldExtractor,
                          Function<JsonNode, E> entryReader) {
            return new FilteredListField<>(
                fields, name, fieldExtractor, entryReader)
                .getName();
        }

        protected FilteredListField(Map<String, Field<?, T>> fields,
                                    String name,
                                    FieldExtractor<List<E>, T> fieldExtractor,
                                    Function<JsonNode, E> entryReader) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          (p, n, f, d) ->
                          JsonSerializationUtils.writeList(p, n, f)),
                  (p, d) -> JsonSerializationUtils.readList(p, entryReader, d),
                  Collections.emptyList());
        }
    }

    /**
     * A field of a map of String and {@link JsonSerializable} objects.
     *
     * Empty and incompatible entries are filtered out. This implies that the
     * existence of the keys of the map is not important.
     */
    public static class FilteredMapField
        <E extends JsonSerializable,
         T extends ObjectNodeSerializable>
        extends Field<Map<String, E>, T>
    {
        /**
         * Returns the default value.
         */
        public static <E> Map<String, E> getDefault() {
            return Collections.emptyMap();
        }

        public static <E extends JsonSerializable,
                       T extends ObjectNodeSerializable>
            String create(Map<String, Field<?, T>> fields,
                          String name,
                          FieldExtractor<Map<String, E>, T> fieldExtractor,
                          Function<JsonNode, E> entryReader) {
            return new FilteredMapField<>(
                fields, name, fieldExtractor, entryReader)
                .getName();
        }

        protected FilteredMapField(Map<String, Field<?, T>> fields,
                                   String name,
                                   FieldExtractor<Map<String, E>, T> fieldExtractor,
                                   Function<JsonNode, E> entryReader) {
            super(fields, name,
                  wrapped(fieldExtractor,
                          (p, n, f, d) ->
                          JsonSerializationUtils.writeMap(p, n, f)),
                  (p, d) -> JsonSerializationUtils.readMap(p, entryReader, d),
                  Collections.emptyMap());
        }
    }
}
