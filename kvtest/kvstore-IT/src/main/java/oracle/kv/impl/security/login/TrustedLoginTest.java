/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.security.Principal;
import java.util.Set;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.security.KVStoreRolePrincipal;
import oracle.kv.impl.security.login.SessionId.IdScope;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Test the TrustedLogin interface and surrounding classes.
 */
public class TrustedLoginTest extends LoginTestBase {

    private static final String TRUSTED_LOGIN = "TrustedLogin";
    private int registryPort = 0;
    private Registry registry;
    private ListenHandle registryHandle;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        registryPort =
            new FreePortLocator("localHost", 5050, 5100).next();

        registry = TestUtils.createRegistry(registryPort);

        if (AsyncRegistryUtils.serverUseAsync) {
            registryHandle = TestUtils.createServiceRegistry(registryPort);
        }
    }

    @Override
    public void tearDown()
        throws Exception {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        if (registryHandle != null) {
            registryHandle.shutdown(true);
        }
        super.tearDown();
    }

    @Test
    public void testLoginInternal()
        throws RemoteException, NotBoundException {

        final TLFH tlfh = new TLFH(logger);
        final AdminId rid = new AdminId(1);
        final TrustedLoginHandler tlh = new TrustedLoginHandler(rid, true);
        final TrustedLoginImpl tli = new TrustedLoginImpl(tlfh, tlh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, TRUSTED_LOGIN,
                                 rid, export(tli), DEFAULT_CSF, DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () -> new TrustedLoginResponder(
                                     tli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final TrustedLoginAPI tliAPI =
            RegistryUtils.getTrustedLogin("localhost", registryPort,
                                          TRUSTED_LOGIN, logger);

        final LoginResult lr = tliAPI.loginInternal();
        assertNotNull(lr);
        assertNotNull(lr.getLoginToken());
    }

    @Test
    public void testValidateLoginToken()
        throws RemoteException, NotBoundException {

        final TLFH tlfh = new TLFH(logger);
        final AdminId rid = new AdminId(1);
        final TrustedLoginHandler tlh = new TrustedLoginHandler(rid, true);
        final TrustedLoginImpl tli = new TrustedLoginImpl(tlfh, tlh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, TRUSTED_LOGIN,
                                 rid, export(tli), DEFAULT_CSF, DEFAULT_SSF,
                                 TRUSTED_LOGIN_TYPE_FAMILY,
                                 () -> new TrustedLoginResponder(
                                     tli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final TrustedLoginAPI tliAPI =
            RegistryUtils.getTrustedLogin("localhost", registryPort,
                                          TRUSTED_LOGIN, logger);

        final LoginResult lr = tliAPI.loginInternal();

        /* Validate a bogus token */
        final byte[] sid = new byte[20];
        for (int i = 0; i < sid.length; i++) {
            sid[i] = (byte) i;
        }
        final long et = 1234567890123456L;
        final ResourceId snrid = new StorageNodeId(17);
        final LoginToken lt =
            new LoginToken(new SessionId(sid, IdScope.LOCAL, snrid), et);

        Subject subject = tliAPI.validateLoginToken(lt);
        assertNull(subject);

        /* Validate a good token */
        subject = tliAPI.validateLoginToken(lr.getLoginToken());
        assertNotNull(subject);
        final Set<Principal> principals = subject.getPrincipals();
        assertNotNull(principals);
        assertTrue(principals.size() == 1);

        /*
         * Since role principal is written as R3 object for backward
         * compatibility, we only compare role name and ignore case here.
         */
        for (Principal princ : principals) {
            assertTrue(((KVStoreRolePrincipal)princ).getName().
                equalsIgnoreCase(KVStoreRolePrincipal.INTERNAL.getName()));
        }

        /* Now, log out the session and try again */
        tliAPI.logout(lr.getLoginToken());

        /* Validate the logged-out token */
        subject = tliAPI.validateLoginToken(lr.getLoginToken());
        assertNull(subject);
    }

    static class TLFH extends TestProcessFaultHandler {
        TLFH(Logger logger) {
            super(logger, ProcessExitCode.RESTART);
        }

        @Override
        public void queueShutdownInternal(Throwable th, ProcessExitCode pec) {
            fail("queueShutdownInternal called");
        }
    }
}
