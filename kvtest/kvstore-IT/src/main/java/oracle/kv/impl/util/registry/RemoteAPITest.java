/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.util.registry;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.RemoteException;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SerialVersion;

import org.junit.Test;

/** Tests for {@link RemoteAPI}. */
public class RemoteAPITest extends TestBase {

    /**
     * Test that the client of a RemoteAPI subclass detects that the server is
     * running at below the minimum supported serial version.
     */
    @Test
    public void testServerVersionBelowMinimum() throws Exception {
        class FooRemoteImpl implements FooRemote {
            @Override
            public short getSerialVersion() {
                return (short) (SerialVersion.MINIMUM - 1);
            }
        }
        class FooRemoteAPI extends RemoteAPI {
            FooRemoteAPI(FooRemote foo) throws RemoteException {
                super(foo);
            }
        }
        FooRemoteImpl fooImpl = new FooRemoteImpl();
        try {
            @SuppressWarnings("unused")
            FooRemoteAPI api = new FooRemoteAPI(fooImpl);
            fail("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage(),
                       e.getMessage().contains("server is incompatible"));
        }
    }

    interface FooRemote extends VersionedRemote { }
}
