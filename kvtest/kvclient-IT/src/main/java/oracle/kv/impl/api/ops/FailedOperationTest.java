/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.async.AbstractCreatorEndpoint;
import oracle.kv.impl.async.dialog.nio.NioEndpointGroup;
import oracle.kv.impl.util.registry.AsyncControl;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;

import org.junit.Test;

/**
 * Test that failures are handled and reported correctly through to the client.
 */
public class FailedOperationTest extends ClientTestBase {

    @Override
    public void setUp()
        throws Exception {

        startServices = false;
        super.setUp();
    }

    @Override
    public void tearDown()
        throws Exception {

        AbstractCreatorEndpoint.startDialogHook = null;
        AbstractCreatorEndpoint.connectHook = null;
        close();
        super.tearDown();
    }

    /**
     * Test that an exception unexpectedly thrown by
     * AsyncRegistryUtils.getRequestHandler gets reflected back through the API
     * and does not cause problems for the next API call. There was a bug where
     * AbstractCreatorEndpoint.startDialog was throwing an exception directly
     * rather than passing it to the dialog handler, which led to this problem.
     * [KVSTORE-523]
     */
    @Test
    public void testGetReqHandlerRefUnexpectedException()
        throws Exception {

        assumeTrue("Requires async", AsyncControl.serverUseAsync);

        open();

        /*
         * Reset all of the request handlers so the next call will attempt to
         * get the request handler, since that is the path where the problem
         * occurs
         */
        ((KVStoreImpl) store).getDispatcher()
            .getRepGroupStateTable()
            .getRepNodeStates()
            .forEach(s -> s.resetReqHandlerRef());

        /* Inject a runtime exception when starting a dialog */
        final String msg = "Injected exception";
        AbstractCreatorEndpoint.startDialogHook = dialogType -> {
            throw new RuntimeException(msg);
        };

        final Key key = Key.createKey("one", "two");
        try {
            store.get(key);
            fail("Expected exception");
        } catch (RuntimeException e) {
            assertEquals(msg, e.getMessage());
        }

        AbstractCreatorEndpoint.startDialogHook = null;
        assertEquals(null, store.get(key));
    }

    /**
     * Test that an unexpected exception thrown when creating the socket
     * channel gets reflected back through the API and does not cause problems
     * for the next API call.
     * [KVSTORE-523]
     */
    @Test
    public void testConnectUnexpectedException()
        throws Exception {

        assumeTrue("Requires async", AsyncControl.serverUseAsync);

        open();

        /* Inject a runtime exception when starting a dialog */
        final String msg = "Injected exception";
        AbstractCreatorEndpoint.connectHook = (v) -> {
            throw new RuntimeException(msg);
        };

        /* Shuts down the all endpoint handlers */
        ((NioEndpointGroup) AsyncRegistryUtils.getEndpointGroup())
            .shutdownCreatorEndpointHandlers(
                "test for unexpected connect exception", true);        

        /* Sleep a while so that endpoint handlers are shut down fully */
        Thread.sleep(1000);

        /*
         * Reset all of the request handlers so the next call will attempt to
         * get the request handler, since that is the path where the problem
         * occurs
         */
        ((KVStoreImpl) store).getDispatcher()
            .getRepGroupStateTable()
            .getRepNodeStates()
            .forEach(s -> s.resetReqHandlerRef());

        final Key key = Key.createKey("one", "two");
        try {
            store.get(key);
            fail("Expected exception");
        } catch (RequestTimeoutException e) {
            assertEquals(msg, e.getCause().getMessage());
        }

        AbstractCreatorEndpoint.connectHook = null;
        assertEquals(null, store.get(key));
    }
}
