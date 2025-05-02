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

import static oracle.nosql.common.jss.JsonSerializationUtils.readLong;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.jss.AbstractJsonSerializable;

/**
 * Represents a long value that is json serializable.
 */
public class LongValue extends AbstractJsonSerializable {

    /** The default value. */
    public static final long DEFAULT_VALUE = 0;
    /** The default. */
    public static final LongValue DEFAULT = new LongValue(DEFAULT_VALUE);

    private final long count;

    public LongValue(long count) {
        this.count = count;
    }

    public LongValue(JsonNode payload) {
        this(readLong(payload, DEFAULT_VALUE));
    }

    @Override
    public JsonNode toJson() {
        return JsonUtils.createJsonNode(count);
    }

    @Override
    public boolean isDefault() {
        /*
         * The count should not be negative, but do not filter out negative
         * values so that we know there is a overflow issue.
         */
        return count == DEFAULT_VALUE;
    }

    public long get() {
        return count;
    }
}
