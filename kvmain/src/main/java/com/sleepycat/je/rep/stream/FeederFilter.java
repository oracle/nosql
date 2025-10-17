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

package com.sleepycat.je.rep.stream;

import static com.sleepycat.je.utilint.VLSN.INVALID_VLSN;

import java.util.logging.Logger;

import com.sleepycat.je.rep.impl.RepImpl;

/**
 * The FeederFilter is used by the Feeder to determine whether a record should
 * be sent to the Replica. The filter object is created at the replica and is
 * transmitted to the Feeder as part of the syncup process. The filter thus
 * represents replica code that is running inside the Feeder, that is, the
 * computation has been moved closer to the data and can be used to eliminate
 * unnecessary network communication overheads.
 */
public interface FeederFilter {

    /**
     * The execute method that invoked before a record is sent to the replica.
     * If the filter returns null, the feeder will not send the record to the
     * replica as part of the replication stream, since it's not of interest
     * to the replica. It can for example be used to filter out tables that
     * are not of interest to the replica.
     *
     * @param record  the record to be filtered
     * @param repImpl repImpl of the RN where the filter is executed
     *
     * @return the original input record if it is to be sent to the replica.
     * null if it's to be skipped.
     */
    OutputWireRecord execute(final OutputWireRecord record,
                             final RepImpl repImpl);


    /**
     * Gets arrays of subscribed table ids. If null or array length is 0,
     * that means the subscriber would subscribe all tables in the store.
     *
     * @return arrays of subscribed table ids
     */
    String[] getTableIds();

    /**
     * Sets the logger for classes that implement the interface
     *
     * @param logger logger
     */
    void setLogger(Logger logger);

    /**
     * Applies the filter change to the feeder filter, and returns the filter
     * change response, or default response.
     *
     * @param change    filter change
     * @param repImpl   rep env impl
     *
     * @return FeederFilterChangeResp result of applying filter change
     */
    default FeederFilterChangeResult applyChange(FeederFilterChange change,
                                                 RepImpl repImpl) {
        return new FeederFilterChangeResult(
            change.getReqId(), FeederFilterChangeResult.Status.FAIL,
            "Dynamic filter change is not supported.");
    }

    /**
     * Sets the start vlsn
     *
     * @param vlsn start vlsn
     */
    default void setStartVLSN(long vlsn) {
    }

    /**
     * Returns the last VLSN processed by filter, either blocked or passed.
     * @return the last VLSN processed by filter
     */
    default long getFilterVLSN() {
        return INVALID_VLSN;
    }

    /**
     * Returns the last VLSN passes the filter
     * @return the last VLSN passes the filter
     */
    default long getLastPassVLSN() {
        return INVALID_VLSN;
    }

    /**
     * Returns the modification time of last op processed by filter, either
     * blocked or passed, or -1 is the time is unspecified
     * @return the modification time or -1
     */
    default long getLastModTimeMs() {
        return -1;
    }

    /**
     * Returns true if the feeder should only stream entries that have been
     * durable.
     * @return true if the feeder should only stream entries that have been
     * durable, false otherwise.
     */
    default boolean durableEntriesOnly() {
        return false;
    }

    /**
     * Returns true if the feeder should include Before Images in the stream
     * entries that have them.
     * @return true to include Before Images, false by default.
     */
    default boolean includeBeforeImage() {
        return false;
    }

    /**
     * Sets the feeder filter id if implemented
     * @param filterId  feeder filter id
     */
    default void setFeederFilterId(String filterId) {
    }
}
