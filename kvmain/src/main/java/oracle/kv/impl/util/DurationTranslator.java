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

package oracle.kv.impl.util;

import java.util.concurrent.TimeUnit;

/**
 * In KVStore, all duration values should be expressed as milliseconds, for
 * uniformity. Durations can be expressed by the user, via the GUI and
 * XML configuration file, as a two part value and time unit. Time units are
 * defined by @{link java.util.concurrent.TimeUnit}. For example, if the
 * property sheet contains:
 *
 * &lt;property name="fooTimeoutValue" value="9"&gt;
 * &lt;property name="fooTimeoutUnit" value="SECONDS"&gt;
 *
 * Then DurationTranslator.translate(propertySheet, "fooTimeout",
 * "fooTimeoutUnit") will return 9000
 */
public class DurationTranslator {

    /**
     * Takes a property sheet with two configuration properties, where one
     * is a time value, and the other is a time unit, and
     * translates it into milliseconds
     */
    public static long translate(long duration,
                                 String timeUnitPropName) {

        TimeUnit unit = TimeUnit.valueOf(timeUnitPropName);
        return unit.toMillis(duration);
    }
}
