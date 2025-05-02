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

/**
 * An interface for all resource in kvstore security system which would have
 * ownership.
 */
public interface Ownable {

    /**
     * Returns the owner of a KVStore resource.  The owner is usually the
     * creator of the resource by default. This is available since R3.Q3. For
     * resources like plan and table created in an earlier version, and those
     * created in a store without security, null will be returned.
     */
    ResourceOwner getOwner();
}
