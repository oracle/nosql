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

package com.sleepycat.je.latch;

import com.sleepycat.je.util.TimeSupplier;
import com.sleepycat.je.utilint.LoggerUtils;

class OwnerInfo {

    private final Thread thread;
    private final long acquireTime;
    private final Throwable acquireStack;

    OwnerInfo(final LatchContext context) {
        thread = Thread.currentThread();
        acquireTime = TimeSupplier.currentTimeMillis();
        acquireStack =
            new Exception("Latch Acquired: " + context.getLatchName());
    }

    void toString(StringBuilder builder) {
        builder.append(" captureThread: ");
        builder.append(thread);
        builder.append(" acquireTime: ");
        builder.append(acquireTime);
        if (acquireStack != null) {
            builder.append("\n");
            builder.append(LoggerUtils.getStackTrace(acquireStack));
        } else {
            builder.append(" -no stack-");
        }
    }
}
