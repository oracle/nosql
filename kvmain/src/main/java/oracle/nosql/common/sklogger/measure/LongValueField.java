/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import java.util.Map;

import oracle.nosql.common.jss.ObjectNodeSerializable;
import oracle.nosql.common.jss.ObjectNodeSerializable.FieldExtractor;

/**
 * A {@link LongValue} field in an {@link ObjectNodeSerializable}
 */
public class LongValueField<T extends ObjectNodeSerializable>
    extends ObjectNodeSerializable.JsonSerializableField<LongValue, T>
{
    public static <T extends ObjectNodeSerializable>
        String create(
            Map<String, ObjectNodeSerializable.Field<?, T>> fields,
            String name,
            FieldExtractor<LongValue, T> fieldExtractor)
    {
        return new LongValueField<>(fields, name, fieldExtractor).getName();
    }

    private LongValueField(
        Map<String, ObjectNodeSerializable.Field<?, T>> fields,
        String name,
        FieldExtractor<LongValue, T> fieldExtractor)
    {
        super(fields, name, fieldExtractor,
              (p, d) -> new LongValue(p),
              LongValue.DEFAULT);
    }
}
