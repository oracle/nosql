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
 * Identifies faults (Exceptions or Errors) whose fault domain can be
 * restricted to the request that was being processed at the time the fault was
 * encountered. The server state is not impacted by the fault and can continue
 * processing other requests.
 */
public interface RequestFault {

    /**
     * Returns the ErrorCode to be associated with this fault. It must not be
     * null.
     */
    ErrorCode getError();

    /**
     * Returns true if an alert needs to be logged for the fault. Implementors
     * of the interface may override this method as appropriate.
     */
    default boolean needsAlert() {
        return false;
    }
}
