/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

/**
 * Identifies faults whose fault domain extends to the entire process. That is,
 * the integrity of the process servicing requests is questionable after
 * encountering the associated fault.
 *
 * Note that ProcessFault is a subclass of RequestFault, to ensure that the
 * request generates a clean response before the process is terminated. That
 * is, a ProcessFault is handled by first performing actions dictated by the
 * RequestFault so that the correct request response is generated followed by
 * process level cleanup and exit.
 *
 * The sequencing of the fault response transmission and process exit is
 * achieved through the {@link ExceptionHandler#queueShutdown} method.
 */
public interface ProcessFault extends RequestFault {

    /**
     * Returns the process exit code.
     */
    ProcessExitCode getExitCode();
}
