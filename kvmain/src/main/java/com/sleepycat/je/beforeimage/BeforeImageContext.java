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

package com.sleepycat.je.beforeimage;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.dbi.TTL;

/**
 * {@literal
 * BeforeImageContext provides info on existence of the beforeimage and
 * its TTL, this is the data which is stored with the current
 * image of primary database. so don't add any metadata which is not added
 * to current record.
 * }
 */
public class BeforeImageContext {

    private final int expTime;
    private final boolean expTimeInHrs;

    //dummy ctx for inserts
    public BeforeImageContext() {
        this.expTime = 0;
        this.expTimeInHrs = true;
    }

    public BeforeImageContext(final int expTime,
                              final boolean expTimeInHrs) {
        this.expTime = expTime;
        this.expTimeInHrs = expTimeInHrs;
    }

    //user defined
    public int getExpTime() {
        return expTime;
    }

    //logged time
    public int getLoggedExpTime() {
        return TTL.ttlToExpiration(expTime,
                                   expTimeInHrs ? TimeUnit.HOURS
                                       : TimeUnit.DAYS);
    }

    public boolean isExpTimeInHrs() {
        return expTimeInHrs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("beforeImage ExpirationTime ").append(expTime);
        sb.append(" expInHrs=").append(expTimeInHrs);
        return sb.toString();
    }
}
