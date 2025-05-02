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

import java.rmi.RemoteException;

import oracle.kv.impl.util.SerialVersion;

/**
 * Base class for API classes that wrap remote interfaces to provide an API
 * called by clients of the remote service.
 *
 * @see VersionedRemote
 */
public abstract class RemoteAPI {

    private final short serialVersion;
    private final VersionedRemote remote;

    /**
     * Retrieves the server version and caches the effective version. This
     * constructor should be called only by a private constructor in the API
     * class, which is called only by the API's wrap() method.
     */
    protected RemoteAPI(VersionedRemote remote)
        throws RemoteException {

        this(remote, remote.getSerialVersion());
    }

    /**
     * Caches the effective version. This constructor should be called only by
     * a private constructor in the API class, which is called only by the
     * API's wrap() method.
     */
    protected RemoteAPI(VersionedRemote remote, short serverVersion) {
        if (serverVersion < SerialVersion.MINIMUM) {
            throw SerialVersion.serverUnsupportedException(
                serverVersion, SerialVersion.MINIMUM);
        }
        serialVersion = (short) Math.min(SerialVersion.CURRENT, serverVersion);

        this.remote = remote;
    }

    /**
     * Returns the effective version, which is the minimum of the current
     * service and client version, and should be passed as the last argument of
     * each remote method.
     */
    public short getSerialVersion() {
        return serialVersion;
    }

    @Override
    public int hashCode() {
        return remote.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if ((other == null) || !getClass().equals(other.getClass())) {
            return false;
        }
        final RemoteAPI o = (RemoteAPI) other;
        return remote.equals(o.remote);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]",
                             getClass().getSimpleName(),
                             remote);
    }
}
