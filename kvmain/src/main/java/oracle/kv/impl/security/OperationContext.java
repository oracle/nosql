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
package oracle.kv.impl.security;

import java.util.List;

/**
 * The OperationContext interface specifies the mechanism that describes
 * what action is being performed, for the purpose of being able to describe
 * this in the event of an error, and the authorization required to perform
 * the operation.
 */
public interface OperationContext {

    /**
     * Describes the context in which a Security check is taking place.
     * @return a string description of the operation
     */
    String describe();

    /**
     * Returns a list of the privileges that are required in order to proceed
     * with the operation.
     * @return an unmodifiable list of all privileges that are required to
     * perform the operation. The list is never null, but may be empty.
     */
    List<? extends KVStorePrivilege> getRequiredPrivileges();
}
