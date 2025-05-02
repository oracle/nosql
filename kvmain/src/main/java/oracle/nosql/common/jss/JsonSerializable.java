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

import oracle.nosql.common.json.JsonNode;

/**
 * An object that can be serialized to a {@link JsonNode}.
 *
 * <p>This interface (along with the json serialization format) is designed for
 * objects such that using default in the place of missing values should not
 * result in correctness issue. Performance metrics, for example, are a good
 * candidate. Under such assumption, we use the json format and a paradigm with
 * default values to mitigate the complexity of backward compatibility.
 *
 * <p>An implementing class should follow the rules below:
 * <ul>
 * <li>A field of a {@link JsonSerializable} object must always have a default
 * value.</li>
 * <li>The {@link JsonSerializable#toJson} method must consider filtering.
 * Fields with default values or nested fields of JsonSerializable types that
 * are {@linkplain #isDefault() default} should be filtered.  The {@link
 * JsonSerializationUtils} provides write methods for primitive data types as
 * well as JsonNode, JsonSerializable and lists or maps of JsonSerializable
 * which supports filtering.  It is highly recommended that a {@code null}
 * field value is always treated as a default value and filtered out.</li>
 * <li>The reading of the {@link JsonSerializable} fields (e.g., when
 * implementing the constructor) must provide default value if the field is
 * missing or has incompatible type in the serialized json payload. We provide
 * convenient methods to ease the implementation effort for this rule. The
 * {@link ObjectNodeSerializable} class implements the common routines to read
 * values with field definitions. The {@link JsonSerializationUtils} provides
 * convenient methods to read fields of different types which handles type
 * incompatibility.</li>
 * </ul>
 *
 * <p>
 * A typical implementation class follows the below paradigm:
 * <pre>
 * {@code
 *   public class LongValue extends AbstractJsonSerializable {
 *
 *       public static final long DEFAULT_VALUE = 0;
 *
 *       private final long count;
 *
 *       public LongValue(long count) {
 *           this.count = count;
 *       }
 *
 *       public LongValue(JsonNode payload) {
 *           this(JsonSerializationUtils.readLong(payload, DEFAULT_VALUE));
 *       }
 *
 *       @Override
 *       public JsonNode toJson() {
 *           return JsonUtils.createJsonNode(count);
 *       }
 *
 *       @Override
 *       public boolean isEmpty() {
 *           return count == DEFAULT_VALUE;
 *       }
 *   }
 * }
 * </pre>
 *
 * <p>Note that the implementation should still be careful when serializing a
 * field of a collection or a map of arbitrary objects (or similarly json array
 * or object). We do not provide any convenience methods for arbitrary
 * collections and maps and therefore, the responsibility for compatibility
 * checks are fully on the implementation.
 *
 * @see AbstractJsonSerializable
 * @see ObjectNodeSerializable
 */
public interface JsonSerializable {

    /**
     * Returns the json serialization of this object.
     */
    JsonNode toJson();

    /**
     * Returns {@code true} if this object is of a default value. An enclosing
     * JsonSerializable object with a default field is encouraged to skip the
     * serialization of such fields. Essentially, the default flag marks that
     * the data in this object is not interesting.
     *
     * <p>
     * For each implementing class of {@code JsonSerializable}, there can be
     * many instances that are uninteresting and marked as default values. For
     * example, we may have two performance metrics having no data but only
     * different names. In this sense, these instances are interchangeable. On
     * the serialization side, these instances are all filtered out. On the
     * de-serialization side, the reader picks a "chosen" default value apriori
     * and whenever it detects a missing or incompatible value, it uses the
     * chosen default value in place.  Note that this means objects marked as
     * default of the same class may not be equal, but will all be
     * de-serialized into the same chosen default value.
     *
     * <p>
     * All the provided convenience method has the assumption that the {@code
     * null} value is a default value.  Therefore, it is strongly discouraged
     * to treat the {@code null} value otherwise.  That is, if {@code null} is
     * not interchangeable with the default values, additional care is needed.
     * For the serialization code, the {@code null} values cannot be filtered
     * out, but instead need to be put as a {@link JsonNode#jsonNodeNull}. For
     * the de-serialization code, it needs to check for {@code jsonNodeNull}
     * and de-serialize it into {@code null}.  For the field of {@code
     * JsonNode} type, this also introduces the complication of whether there
     * is any ambiguity between {@code jsonNodeNull} and {@code null}.
     *
     * <p>
     * Note that the default value of a JsonSerializable field must has
     * isDefault return {@code true}. Otherwise, it creates an ambiguity
     * between all the other values that is marked isDefault and this special
     * default value that is not. Such ambiguity indicates that special default
     * value is not interesting as well and should be marked as isDefault.
     */
    boolean isDefault();
}
