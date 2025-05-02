/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.impl.async.perf;

import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.common.jss.ObjectNodeSerializable;

/**
 * A collection of dialog resource manager perf metrics.
 */
public class DialogResourceManagerPerf extends ObjectNodeSerializable {

    /** The field map. */
    public static final Map<String, Field<?, DialogResourceManagerPerf>>
        FIELDS = new HashMap<>();
    /** The average number of available permits. */
    public static final String AVG_AVAILABLE_PERMITS =
        DoubleField.create(FIELDS, "avgAvailablePermits",
                           (o) -> o.avgAvailablePermits);
    /** The average percentage of available permits. */
    public static final String AVG_AVAILABLE_PERCENTAGE =
        DoubleField.create(FIELDS, "avgAvailablePercentage",
                           (o) -> o.avgAvailablePercentage);
    /** The default. */
    public static final DialogResourceManagerPerf DEFAULT =
        new DialogResourceManagerPerf(-1.0, -1.0);

    private final double avgAvailablePermits;
    private final double avgAvailablePercentage;

    public DialogResourceManagerPerf(double avgAvailablePermits,
                                     double avgAvailablePercentage) {
        this.avgAvailablePermits = avgAvailablePermits;
        this.avgAvailablePercentage = avgAvailablePercentage;
    }

    public DialogResourceManagerPerf(JsonNode payload) {
        this(readField(FIELDS, payload, AVG_AVAILABLE_PERMITS),
             readField(FIELDS, payload, AVG_AVAILABLE_PERCENTAGE));
    }

    @Override
    public ObjectNode toJson() {
        return writeFields(FIELDS, this);
    }

    @Override
    public boolean isDefault() {
        /*
         * For this perf, a value is default if either of the fields is less
         * than zero or both of them are zero. In all cases when we have a
         * valid perf value, it will not be default. When things are idle, the
         * avgAvailablePercentage will be zero. However, in that case, it would
         * be still helpful to know the avgAvailablePermits.
         */
        return !((avgAvailablePermits > 0) || (avgAvailablePercentage > 0));
    }
}

