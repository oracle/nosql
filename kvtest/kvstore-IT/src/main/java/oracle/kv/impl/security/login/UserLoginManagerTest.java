/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.AuthenticationFailureException;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.api.TopologyManager;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.security.ScaffoldUserVerifier;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId.ResourceType;
import oracle.kv.impl.topo.ServiceResourceId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.ClearClientSocketFactory;
import oracle.kv.impl.util.registry.ClearServerSocketFactory;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.ServerSocketFactory;

import org.junit.Test;

/**
 * Test the UserLoginManager family of classes.
 */
public class UserLoginManagerTest extends LoginTestBase {

    private static final int N_ID_BYTES = 16;

    public static final String ADMIN_USER_NAME = "admin";
    public static final char[] ADMIN_USER_PASSWORD =
        "NoSql00__hello".toCharArray();

    private static final String ADMIN1_LOGIN = "admin:LOGIN";
    private static final String RN1_LOGIN = "test_store:rg1-rn1:LOGIN";
    private static final String STORE_NAME = "test_store";

    private int registryPort = 0;
    private ClientSocketFactory csf;
    private ServerSocketFactory ssf;
    private Registry registry;
    private boolean createdAsyncRegistry;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        registryPort =
            new FreePortLocator("localHost", 5050, 5100).next();

        /* Create socket factories to permit sharing between RMI and async */
        csf = new ClearClientSocketFactory("csf", 0, 0, null);
        RegistryUtils.setRegistryCSF(csf, STORE_NAME, null /* clientId */);
        ssf = new ClearServerSocketFactory(0 /* backlog */, registryPort,
                                           registryPort, 0 /* maxConnect */);
        registry = LocateRegistry.createRegistry(registryPort, csf, ssf);
    }

    @Override
    public void tearDown()
        throws Exception {

        if (registry != null) {
            TestUtils.destroyRegistry(registry);
        }
        super.tearDown();
    }

    @Test
    public void testAdminManager()
        throws Exception {

        /* Setup */
        final LFH lfh = new LFH(logger);
        final AdminId admin1 = new AdminId(1);

        final HostPort target = new HostPort("localhost", registryPort);

        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        /* Build the admin1 login */
        final UserLoginHandler a1ulh =
            new UserLoginHandler(admin1, true, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig().
                                 setAllowExtension(true).
                                 setSessionLifetime(3600),
                                 logger);
        createUserLogin(ADMIN1_LOGIN, admin1, lfh, a1ulh);

        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final LoginResult a1lr = a1ulh.login(creds, "localhost");
        assertNotNull(a1lr);
        assertNotNull(a1lr.getLoginToken());

        /* Construct an admin login manager */
        final AdminLoginManager alm =
            new AdminLoginManager(creds.getUsername(),
                                  false /* no auto-renew */, logger);
        alm.initialize(a1lr.getLoginToken(), "localhost", registryPort);

        LoginToken currLt = null;
        LoginHandle currLh = null;

        /* Test getHandle()*/
        final LoginHandle lh = alm.getHandle(target, ResourceType.ADMIN);
        assertNotNull(lh);
        currLh = lh;
        final LoginToken lt1 = lh.getLoginToken();
        assertNotNull(lt1);
        currLt = lt1;

        final Subject subject = a1ulh.validateLoginToken(lt1);
        assertNotNull(subject);

        final LoginHandle lh2 = alm.getHandle(target, ResourceType.ADMIN);
        assertNotNull(lh2);
        final LoginToken lt2 = lh2.getLoginToken();
        assertNotNull(lt2);
        assertEquals(lt1, lt2);

        /* Test renewToken()*/
        final LoginToken lt3 = currLh.renewToken(currLt);
        assertNotNull(lt3);
        assertFalse(currLt.equals(lt3));
        currLt = lt3;

        /* Test logout()*/
        alm.logout();

        /* make sure logout() took */
        Subject loSubj = a1ulh.validateLoginToken(currLt);
        assertNull(loSubj);
    }

    @Test
    public void testRepNodeManager()
        throws Exception {

        /* Setup */
        final LFH lfh = new LFH(logger);

        final Topology topo = makeTestTopology();
        final TopologyManager topoManager =
            new TopologyManager(STORE_NAME, 100, logger);
        topoManager.update(topo);

        final RepNodeId rn1 = new RepNodeId(1, 1);

        final HostPort target = new HostPort("localhost", registryPort);

        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        /* Build the rn1 login */
        final UserLoginHandler rn1ulh =
            new UserLoginHandler(rn1, false, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig().
                                 setAllowExtension(true).
                                 setSessionLifetime(3600),
                                 logger);
        createUserLogin(RN1_LOGIN, rn1, lfh, rn1ulh);

        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        final RepNodeLoginManager rnlm =
            new RepNodeLoginManager(creds.getUsername(),
                                    false /* no auto-renew */);

        final String[] helperHosts = { "localhost:" + registryPort };
        rnlm.bootstrap(helperHosts, creds, STORE_NAME);

        /*
         * Here is where the client would use the bootstrap login manager
         * to acquire topology.
         */
        rnlm.setTopology(topoManager);

        LoginHandle currLh = null;
        LoginToken currLt = null;

        /* Test getHandle()*/
        final LoginHandle lh = rnlm.getHandle(target,
                                              ResourceType.REP_NODE);
        assertNotNull(lh);
        currLh = lh;
        final LoginToken lt1 = lh.getLoginToken();
        assertNotNull(lt1);
        currLt = lt1;

        final Subject subject = rn1ulh.validateLoginToken(lt1);
        assertNotNull(subject);

        final LoginToken lt2 = lh.getLoginToken();
        assertNotNull(lt2);
        assertEquals(lt1, lt2);

        /* Test renewToken()*/
        final LoginToken lt3 = currLh.renewToken(currLt);
        assertNotNull(lt3);
        assertFalse(currLt.equals(lt3));
        currLt = lt3;

        /* Test logout()*/
        rnlm.logout();

        /* make sure logout() took */
        Subject loSubj = rn1ulh.validateLoginToken(currLt);
        assertNull(loSubj);
    }

    /**
     * This is a test of the ability to not initialize an admin login manager
     * but to have a delayed initialize, which occurs at the call to
     * getHandle().
     */
    @Test
    public void testDelayedAdminManager()
        throws Exception {

        /* Setup */
        final LFH lfh = new LFH(logger);
        final AdminId admin1 = new AdminId(1);

        final HostPort target = new HostPort("localhost", registryPort);

        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        /* Build the admin1 login */
        final UserLoginHandler a1ulh =
            new UserLoginHandler(admin1, true, /* localId */
                                 verifier, null, /* pwdRenewer */
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig().
                                 setAllowExtension(true).
                                 setSessionLifetime(3600),
                                 logger);
        createUserLogin(ADMIN1_LOGIN, admin1, lfh, a1ulh);

        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);
        final LoginCredentials badCreds =
            new PasswordCredentials(ADMIN_USER_NAME + "X", ADMIN_USER_PASSWORD);

        final String[] hostPorts =
            new String[] { "localhost:" + registryPort };

        /* Construct a delayed admin login manager */
        final DelayedLoginManager dlm =
            new DelayedLoginManager(hostPorts, creds);

        /* Test getHandle() */
        final LoginHandle lh = dlm.getHandle(target, ResourceType.ADMIN);
        assertNotNull(lh);
        final LoginToken lt1 = lh.getLoginToken();
        assertNotNull(lt1);

        /* Test logout()*/
        dlm.logout();

        /*
         * Construct a delayed admin login manager with bad creds to verify
         * that the login is delayed.
         */
        final DelayedLoginManager badDlm =
            new DelayedLoginManager(hostPorts, badCreds);

        /* Test getHandle() */
        try {
            badDlm.getHandle(target, ResourceType.ADMIN);
            fail("expected exception");
        } catch (AuthenticationFailureException afe) {
            assertTrue(true); /* expected */
        }

        /* Test logout() while not logged in */
        try {
            badDlm.logout();
        } catch (Exception e) {
            fail("unexpected exception: " + e);
        }

    }

    private Topology makeTestTopology() {
        final Topology topo = new Topology(STORE_NAME);
        final Datacenter dc =
            Datacenter.newInstance("myDc", 1, DatacenterType.PRIMARY, false,
                                   false);

        topo.add(dc);
        final StorageNode sn = new StorageNode(dc, "localhost", registryPort);
        topo.add(sn);
        final RepNode rn = new RepNode(sn.getStorageNodeId());
        final RepGroup rg = new RepGroup();
        topo.add(rg);
        rg.add(rn);
        return topo;
    }

    static class LFH extends TestProcessFaultHandler {
        LFH(Logger logger) {
            super(logger, ProcessExitCode.RESTART);
        }

        @Override
        public void queueShutdownInternal(Throwable th, ProcessExitCode pec) {
            fail("queueShutdownInternal called");
        }
    }

    private final class DelayedLoginManager extends AdminLoginManager {
        private final String[] hostPorts;
        private final LoginCredentials creds;

        private DelayedLoginManager(String[] hostPorts,
                                    LoginCredentials creds) {
            super(creds.getUsername(), true, logger);
            this.hostPorts = hostPorts;
            this.creds = creds;
        }

        @Override
        protected void initializeLoginHandle() {
            bootstrap(hostPorts, creds);
        }

    }

    /** Create a UserLogin object, and export with both RMI and async. */
    private void createUserLogin(String bindingName,
                                 ServiceResourceId serviceId,
                                 ProcessFaultHandler faultHandler,
                                 UserLoginHandler loginHandler)
        throws IOException
    {
        if (!createdAsyncRegistry && AsyncRegistryUtils.serverUseAsync) {
            final ListenHandle asyncRegistryHandle =
                AsyncRegistryUtils.createRegistry("localhost", registryPort,
                                                  ssf, logger);
            tearDowns.add(() -> asyncRegistryHandle.shutdown(true));
            createdAsyncRegistry = true;
        }

        final UserLoginImpl userLoginImpl =
            new UserLoginImpl(faultHandler, loginHandler, logger);
        final ListenHandle userLoginHandle = RegistryUtils.rebind(
            "localhost", registryPort, bindingName, serviceId,
            export(userLoginImpl), DEFAULT_CSF, DEFAULT_SSF,
            USER_LOGIN_TYPE_FAMILY,
            () -> new AsyncUserLoginResponder(
                userLoginImpl, DEFAULT_THREAD_POOL, logger),
            logger);
        if (userLoginHandle != null) {
            tearDowns.add(() -> userLoginHandle.shutdown(true));
        }
    }
}
