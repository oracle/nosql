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

package com.sleepycat.je.cleaner;

/**
 * Used to trace the relative numbers of full INs and BIN-deltas that are
 * obsolete vs active.  May be used in the future for adjusting utilization.
 */
public class INSummary {
    public int totalINCount;
    public int totalINSize;
    public int totalBINDeltaCount;
    public int totalBINDeltaSize;
    public int obsoleteINCount;
    public int obsoleteINSize;
    public int obsoleteBINDeltaCount;
    public int obsoleteBINDeltaSize;

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("<INSummary totalINCount=\"");
        buf.append(totalINCount);
        buf.append("\" totalINSize=\"");
        buf.append(totalINSize);
        buf.append("\" totalBINDeltaCount=\"");
        buf.append(totalBINDeltaCount);
        buf.append("\" totalBINDeltaSize=\"");
        buf.append(totalBINDeltaSize);
        buf.append("\" obsoleteINCount=\"");
        buf.append(obsoleteINCount);
        buf.append("\" obsoleteINSize=\"");
        buf.append(obsoleteINSize);
        buf.append("\" obsoleteBINDeltaCount=\"");
        buf.append(obsoleteBINDeltaCount);
        buf.append("\" obsoleteBINDeltaSize=\"");
        buf.append(obsoleteBINDeltaSize);
        buf.append("\"/>");

        return buf.toString();
    }
}
