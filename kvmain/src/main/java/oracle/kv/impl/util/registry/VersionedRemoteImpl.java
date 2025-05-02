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

import oracle.kv.impl.security.annotations.PublicAPI;
import oracle.kv.impl.util.SerialVersion;

/**
 * Base class for all service implementations.
 *
 * @see VersionedRemote
 */
@PublicAPI
public class VersionedRemoteImpl implements VersionedRemote {

    private short serialVersion = SerialVersion.CURRENT;

    @Override
    public short getSerialVersion() {
        return serialVersion;
    }

    /**
     * Overrides the value returned by getSerialVersion for testing.
     */
    public void setTestSerialVersion(short useSerialVersion) {
        serialVersion = useSerialVersion;
    }

    /**
     * Check whether the client meets the specified minimum required version.
     *
     * @param clientSerialVersion the serial version of the client
     * @param requiredSerialVersion the minimum required version
     * @throws UnsupportedOperationException if the requirement is not met
     */
    protected void checkClientSupported(short clientSerialVersion,
                                        short requiredSerialVersion) {
        if (clientSerialVersion < requiredSerialVersion) {
            throw SerialVersion.clientUnsupportedException(
                clientSerialVersion, requiredSerialVersion);
        }
    }
}
