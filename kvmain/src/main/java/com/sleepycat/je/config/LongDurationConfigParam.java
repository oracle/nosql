/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.config;

import com.sleepycat.je.utilint.PropUtil;

/**
 * A JE configuration parameter with a duration long value in milliseconds.
 * The String format is described under Time Duration Properties in the
 * EnvironmentConfig javadoc.
 */
public class LongDurationConfigParam extends ConfigParam {

    private static final String DEBUG_NAME =
        LongDurationConfigParam.class.getName();

    private String minString;
    private long minMillis;
    private String maxString;
    private long maxMillis;

    public LongDurationConfigParam(String configName,
                                   String minVal,
                                   String maxVal,
                                   String defaultValue,
                                   boolean mutable,
                                   boolean forReplication) {
        super(configName, defaultValue, mutable, forReplication);
        if (minVal != null) {
            minString = minVal;
            minMillis = PropUtil.parseLongDuration(minVal);
        }
        if (maxVal != null) {
            maxString = maxVal;
            maxMillis = PropUtil.parseLongDuration(maxVal);
        }
    }

    @Override
    public void validateValue(String value)
        throws IllegalArgumentException {

        final long millis;
        try {
            /* Parse for validation side-effects. */
            millis = PropUtil.parseLongDuration(value);
        } catch (IllegalArgumentException e) {
            /* Identify this property in the exception message. */
            throw new IllegalArgumentException
                (DEBUG_NAME + ":" +
                    " param " + name +
                    " doesn't validate, " +
                    value +
                    " fails validation: " + e.getMessage());
        }
        /* Check min/max. */
        if (minString != null) {
            if (millis < minMillis) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ":" +
                        " param " + name +
                        " doesn't validate, " +
                        value +
                        " is less than min of " +
                        minString);
            }
        }
        if (maxString != null) {
            if (millis > maxMillis) {
                throw new IllegalArgumentException
                    (DEBUG_NAME + ":" +
                        " param " + name +
                        " doesn't validate, " +
                        value +
                        " is greater than max of " +
                        maxString);
            }
        }
    }
}

