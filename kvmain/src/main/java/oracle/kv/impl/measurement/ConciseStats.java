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
package oracle.kv.impl.measurement;

/**
 * Interface implemented by an object which can format data to be recorded in
 * the .stat file.
 */
public interface ConciseStats {

    /**
     * Returns the start time for the collection period.
     *
     * @return the start time for the collection period
     */
    long getStart();

    /**
     * Returns the end time for the collection period.
     *
     * @return the end time for the collection period
     */
    long getEnd();

    /**
     * Returns a string formatted for the .stat log file.
     *
     * @return a formatted string
     */
    String getFormattedStats();
}
