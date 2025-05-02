/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.util.TestUtils.safeUnexport;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import oracle.kv.TestBase;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;

public class LoginTestBase extends TestBase {

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        super.tearDown();
    }

    protected <T extends Remote> T export(T object)
        throws RemoteException {

        tearDowns.add(() -> safeUnexport(object));
        @SuppressWarnings("unchecked")
        final T result = (T) UnicastRemoteObject.exportObject(object, 0);
        return result;
    }

    void tearDownListenHandle(ListenHandle listenHandle) {
        if (listenHandle != null) {
            tearDowns.add(() -> listenHandle.shutdown(true));
        }
    }
}
