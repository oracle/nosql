/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.test;

import java.rmi.RemoteException;

/**
 * A simple interface to test extension of RemoteTestInterface.  Used by
 * RemoteTestInterfaceTest.
 */
public interface RemoteTestExtension extends RemoteTestInterface {

    public abstract boolean testExtension()
        throws RemoteException;
}
