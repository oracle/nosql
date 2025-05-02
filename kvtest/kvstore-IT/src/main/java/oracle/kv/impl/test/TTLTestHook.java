/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import java.util.logging.Logger;

import oracle.kv.impl.util.FormatUtils;

import com.sleepycat.je.dbi.TTL;
import com.sleepycat.je.utilint.TestHookAdapter;

/**
 * A JE TestHook class that can be used to set the current time for TTL to the
 * value specified by the oracle.kv.test.ttl.hook system property, if set.
 */
public class TTLTestHook extends TestHookAdapter<Long> {

    public static final String TTL_CURRENT_TIME_PROP =
        "oracle.kv.test.ttl.hook";
    private final long currentTime;

    /**
     * Enable the TTL time test hook if the system property is set.
     */
    public static void doHookIfSet(Logger logger) {
        final String ttlValue = System.getProperty(TTL_CURRENT_TIME_PROP);
        if (ttlValue != null) {
            final long time = Long.parseLong(ttlValue);
            setTTLTime(time, "system property", logger);
        }
    }

    /**
     * Set the TTL current time to the specified value.
     */
    public static void setTTLTime(long time,
                                  String fromDescription,
                                  Logger logger) {
        if (time != 0) {
            logger.info(
                String.format("TTL current time from %s set to '%s' (%d)",
                              fromDescription,
                              FormatUtils.formatDateTimeMillis(time),
                              time));
            TTL.setTimeTestHook(new TTLTestHook(time));
        } else {
            logger.info(
                String.format("TTL current time from %s cleared",
                              fromDescription));
            TTL.setTimeTestHook(null);
        }
    }

    public TTLTestHook(long time) {
        this.currentTime = time;
    }

    @Override
    public Long getHookValue() {
        return currentTime;
    }
}
