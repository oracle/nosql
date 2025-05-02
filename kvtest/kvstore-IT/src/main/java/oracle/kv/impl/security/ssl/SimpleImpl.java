/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.ssl;

import java.io.Serializable;
import java.rmi.RemoteException;

/**
 * A simple interface for testing SSL RMI communications.
 */
public class SimpleImpl implements Simple, Serializable {
    private static final long serialVersionUID = 1L;

    /* add one to the input value */
    @Override
    public int addOne(int input) throws RemoteException {
        return input + 1;
    }
}
