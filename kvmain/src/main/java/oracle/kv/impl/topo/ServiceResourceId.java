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

package oracle.kv.impl.topo;

import java.io.DataInput;

/**
 * Uniquely identifies a service component in the store. Service components
 * have node numbers and group IDs, where the group ID may be 0 if the
 * component is not identified by membership in a replication group.
 */
public abstract class ServiceResourceId extends ResourceId {

    private static final long serialVersionUID = 1;

    /** Create an instance of this class. */
    protected ServiceResourceId() { }

    /** FastExternalizable constructor. */
    protected ServiceResourceId(DataInput in, short serialVersion) {
        super(in, serialVersion);
    }

    /**
     * Returns the 1-based group ID of this service component, or 0 if the
     * component is not identified by membership in a group.
     *
     * @return the group ID or 0
     */
    public abstract int getGroupId();

    /**
     * Returns the 1-based node ID of this service component relative to the
     * component's group, or relative to the store as a whole if the component
     * is not identified by membership in a group. Some services may return 0
     * to represent a bootstrap instance, which may not be unique across the
     * store.
     */
    public abstract int getNodeNum();
}
