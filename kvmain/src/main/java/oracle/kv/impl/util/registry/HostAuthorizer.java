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

package oracle.kv.impl.util.registry;

import java.net.InetAddress;

/**
 * Provide a mechanism for deciding whether to allow incoming connections from
 * a host.
 */
public interface HostAuthorizer {
    /**
     * Check whether an incoming connection from a client host should be
     * allowed.
     * @param clientAddress the address of a host from which a connection
     *  was received.
     * @return true if a connection should be allowed, and false otherwise.
     */
    boolean allowConnection(InetAddress clientAddress);
}

