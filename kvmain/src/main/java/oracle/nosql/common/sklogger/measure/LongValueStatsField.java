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
 * A {@link LongValueStats} field in an {@link ObjectNodeSerializable}
 */
public class LongValueStatsField<T extends ObjectNodeSerializable>
    extends ObjectNodeSerializable.JsonSerializableField<LongValueStats, T>
{
    public static <T extends ObjectNodeSerializable>
        String create(
            Map<String, ObjectNodeSerializable.Field<?, T>> fields,
            String name,
            FieldExtractor<LongValueStats, T> fieldExtractor)
    {
        return new LongValueStatsField<>(fields, name, fieldExtractor)
            .getName();
    }

    private LongValueStatsField(
        Map<String, ObjectNodeSerializable.Field<?, T>> fields,
        String name,
        FieldExtractor<LongValueStats, T> fieldExtractor)
    {
        super(fields, name, fieldExtractor,
              (p, d) -> new LongValueStats(p),
              LongValueStats.DEFAULT);
    }
}
