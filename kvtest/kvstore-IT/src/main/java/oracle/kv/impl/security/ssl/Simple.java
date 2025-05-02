/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.ssl;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A simple interface for testing SSL RMI communications.
 */
public interface Simple extends Remote {
    /* add one to the input value */
    int addOne(int input) throws RemoteException;
}
