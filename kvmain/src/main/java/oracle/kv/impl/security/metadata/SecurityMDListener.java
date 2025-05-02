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

package oracle.kv.impl.security.metadata;

/**
 * This is a simple interface that is implemented by services or portions of
 * services that want to be notified of security metadata change at runtime.
 */
public interface SecurityMDListener {

    /**
     * Notify listener of security metadata change.
     * 
     * @param mdChange Security metadata change
     */
    public void notifyMetadataChange(SecurityMDChange mdChange);
}
