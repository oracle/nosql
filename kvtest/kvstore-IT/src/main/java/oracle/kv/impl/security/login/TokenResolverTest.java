/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.login;

import static oracle.kv.impl.async.StandardDialogTypeFamily.USER_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.async.StandardDialogTypeFamily.TRUSTED_LOGIN_TYPE_FAMILY;
import static oracle.kv.impl.util.TestUtils.DEFAULT_CSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_SSF;
import static oracle.kv.impl.util.TestUtils.DEFAULT_THREAD_POOL;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.logging.Logger;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.Parameters;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.async.EndpointGroup.ListenHandle;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.fault.ProcessFaultHandler;
import oracle.kv.impl.fault.TestProcessFaultHandler;
import oracle.kv.impl.security.ScaffoldUserVerifier;
import oracle.kv.impl.security.UserVerifier;
import oracle.kv.impl.security.login.ParamTopoResolver.ParamsHandleImpl;
import oracle.kv.impl.security.login.TopoTopoResolver.TopoTopoHandle;
import oracle.kv.impl.security.login.UserLoginHandler.LoginConfig;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.Datacenter;
import oracle.kv.impl.topo.DatacenterType;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepNode;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNode;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.AsyncRegistryUtils;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;

import org.junit.Test;

/**
 * Test the TokenResolver interface and surrounding classes.
 */
public class TokenResolverTest extends LoginTestBase {

    private static final int N_ID_BYTES = 16;

    public static final String ADMIN_USER_NAME = "admin";
    public static final char[] ADMIN_USER_PASSWORD =
        "NoSql00__hello".toCharArray();

    private static final String ADMIN1_LOGIN = "admin:LOGIN";
    private static final String SN1_BS_LOGIN = "SNA:TRUSTED_LOGIN";
    private static final String SN1_LOGIN = "SNA:TRUSTED_LOGIN";
    private static final String RN1_LOGIN = "test_store:rg1-rn1:LOGIN";
    private static final String STORE_NAME = "test_store";
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
    public void testResolve()
        throws IOException, NotBoundException {

        /* Setup */
        final LFH lfh = new LFH(logger);

        final Topology topo = makeTestTopology();
        final Parameters params = makeTestParameters();

        /*
         * TODO: derive these from params/topo rather then just hoping that
         * they align.
         */
        final AdminId admin1 = new AdminId(1);
        final RepNodeId rn1 = new RepNodeId(1, 1);
        final StorageNodeId sn1 = new StorageNodeId(1);

        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        /* Build the admin1 login */
        /* TODO: add local login resolve testing */
        final UserLoginHandler a1ulh =
            new UserLoginHandler(admin1, false /* localId */, verifier, null,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl a1uli = new UserLoginImpl(lfh, a1ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, ADMIN1_LOGIN,
                                 admin1, export(a1uli),
                                 DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     a1uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI a1ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, ADMIN1_LOGIN,
                                       null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */, logger);

        /* Build the rn1 login */
        final UserLoginHandler rn1ulh =
            new UserLoginHandler(rn1, false, verifier, null /* pwdRenewer */,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        createUserLogin(RN1_LOGIN, rn1, lfh, rn1ulh);

        final UserLoginAPI rn1ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, RN1_LOGIN,
                                       null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */, logger);

        /* Build the sn1 login */
        final TrustedLoginHandler sn1tlh = new TrustedLoginHandler(sn1, false);
        final TrustedLoginImpl sn1tli =
            new TrustedLoginImpl(lfh, sn1tlh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(
                "localhost", registryPort, SN1_LOGIN, sn1, export(sn1tli),
                DEFAULT_CSF, DEFAULT_SSF, TRUSTED_LOGIN_TYPE_FAMILY,
                () -> new TrustedLoginResponder(sn1tli, DEFAULT_THREAD_POOL,
                                                logger),
                logger));

        final TrustedLoginAPI sn1tlAPI =
            RegistryUtils.getTrustedLogin("localhost", registryPort, SN1_LOGIN,
                                          logger);

        /* Get logins */

        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        final LoginResult a1lr = a1ulAPI.login(creds);
        assertNotNull(a1lr);
        assertNotNull(a1lr.getLoginToken());

        final LoginResult rn1lr = rn1ulAPI.login(creds);
        assertNotNull(rn1lr);
        assertNotNull(rn1lr.getLoginToken());

        final LoginResult sn1lr = sn1tlAPI.loginInternal();
        assertNotNull(sn1lr);
        assertNotNull(sn1lr.getLoginToken());

        final TokenResolverImpl adminResolverImpl =
            new TokenResolverImpl(
                "localhost", registryPort, STORE_NAME,
                new ParamTopoResolver(new ParamsHandleImpl(params), logger),
                (LoginManager) null, logger);
        final TokenResolver adminResolver = adminResolverImpl;

        final TopoTopoResolver ttResolver =
            new TopoTopoResolver(new TopoTopoHandle(topo), null, logger);
        final TokenResolverImpl rnResolverImpl =
            new TokenResolverImpl("localhost", registryPort, STORE_NAME,
                                  ttResolver, (LoginManager) null, logger);
        final TokenResolver rnResolver = rnResolverImpl;

        /* Test admin resolution */
        final Subject subject1 = adminResolver.resolve(a1lr.getLoginToken());
        assertNotNull(subject1);

        /* Test repnode resolution */
        final Subject subject2 = rnResolver.resolve(rn1lr.getLoginToken());
        assertNotNull(subject2);

        /* Test sna resolution */
        final Subject subject3 = adminResolver.resolve(sn1lr.getLoginToken());
        assertNotNull(subject3);
    }

    @Test
    public void testBootstrapResolve()
        throws RemoteException, NotBoundException {

        /* Setup */
        final LFH lfh = new LFH(logger);

        final AdminId admin1 = new AdminId(1);
        final StorageNodeId sn1 = new StorageNodeId(1);

        final UserVerifier verifier =
            new ScaffoldUserVerifier(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        /* Build the admin1 login */
        /* TODO: add local login resolve testing */
        final UserLoginHandler a1ulh =
            new UserLoginHandler(admin1, true /* localId */, verifier, null,
                                 new LoginTable(100, new byte[0], N_ID_BYTES),
                                 new LoginConfig(), logger);
        final UserLoginImpl a1uli = new UserLoginImpl(lfh, a1ulh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind("localhost", registryPort, ADMIN1_LOGIN,
                                 admin1, export(a1uli),
                                 DEFAULT_CSF, DEFAULT_SSF,
                                 USER_LOGIN_TYPE_FAMILY,
                                 () -> new AsyncUserLoginResponder(
                                     a1uli, DEFAULT_THREAD_POOL, logger),
                                 logger));

        final UserLoginAPI a1ulAPI =
            RegistryUtils.getUserLogin(null /* storeName */, "localhost",
                                       registryPort, ADMIN1_LOGIN,
                                       null /* loginMgr */,
                                       null /* clientId */,
                                       Protocols.getDefault(),
                                       false /* ignoreWrongType */, logger);

        /* Build the sn1 bootstrap login */
        final TrustedLoginHandler sn1bstlh = new TrustedLoginHandler(sn1, true);
        final TrustedLoginImpl sn1bstli =
            new TrustedLoginImpl(lfh, sn1bstlh, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(
                "localhost", registryPort, SN1_BS_LOGIN, sn1, export(sn1bstli),
                DEFAULT_CSF, DEFAULT_SSF, TRUSTED_LOGIN_TYPE_FAMILY,
                () -> new TrustedLoginResponder(sn1bstli, DEFAULT_THREAD_POOL,
                                                logger),
                logger));

        final TrustedLoginAPI sn1bstlAPI =
            RegistryUtils.getTrustedLogin("localhost", registryPort,
                                          SN1_BS_LOGIN, logger);

        /* Get logins */

        final LoginCredentials creds =
            new PasswordCredentials(ADMIN_USER_NAME, ADMIN_USER_PASSWORD);

        final LoginResult a1lr = a1ulAPI.login(creds);
        assertNotNull(a1lr);
        assertNotNull(a1lr.getLoginToken());

        final LoginResult sn1bslr = sn1bstlAPI.loginInternal();
        assertNotNull(sn1bslr);
        assertNotNull(sn1bslr.getLoginToken());

        final TokenResolverImpl adminResolverImpl =
            new TokenResolverImpl(
                "localhost", registryPort, null, null,
                (LoginManager) null, logger);
        final TokenResolver adminResolver = adminResolverImpl;

        /* Test admin resolution */
        final Subject subject1 = adminResolver.resolve(a1lr.getLoginToken());
        assertNotNull(subject1);

        /* Test sna bootstrap resolution */
        final Subject subject2 = adminResolver.resolve(sn1bslr.getLoginToken());
        assertNotNull(subject2);
    }

    private Parameters makeTestParameters() {
        final Parameters params = new Parameters(STORE_NAME);

        final StorageNodeId snid = new StorageNodeId(1);
        final StorageNodeParams snp =
            new StorageNodeParams(snid, "localhost", registryPort, "An SN");
        params.add(snp);

        final AdminParams ap =
            new AdminParams(params.getNextAdminId(), snid, 
                            null, snp.getRootDirPath());
        params.add(ap);
        return params;
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

    /** Create a UserLogin object, and export with both RMI and async. */
    private void createUserLogin(String bindingName,
                                 RepNodeId rnId,
                                 ProcessFaultHandler faultHandler,
                                 UserLoginHandler loginHandler)
        throws IOException
    {
        final UserLoginImpl userLoginImpl =
            new UserLoginImpl(faultHandler, loginHandler, logger);
        tearDownListenHandle(
            RegistryUtils.rebind(
                "localhost", registryPort, bindingName, rnId,
                export(userLoginImpl),
                DEFAULT_CSF, DEFAULT_SSF, USER_LOGIN_TYPE_FAMILY,
                () -> new AsyncUserLoginResponder(userLoginImpl,
                                                  DEFAULT_THREAD_POOL, logger),
                logger));
    }
}
