/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.ssl;

import static oracle.kv.impl.util.TestUtils.safeUnexport;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.rmi.ConnectIOException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.topo.util.FreePortLocator;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.registry.ClientSocketFactory;
import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryArgs;
import oracle.kv.impl.util.registry.RMISocketPolicy.SocketFactoryPair;
import oracle.kv.impl.util.registry.ServerSocketFactory;

import org.junit.Test;

/**
 * Test the interaction of SSL classes in the system.
 */
public class SSLSystemTest extends TestBase {

    private static final String SIMPLE = "Simple";
    private static final String TRANSPORT = "trans";
    private static final String TRUSTED = "trusted";
    private int registryPort = 0;
    private Registry registry;
    private Remote exportedObj;

    @Override
    public void setUp()
        throws Exception {

        super.setUp();
        ClientSocketFactory.newGeneration();

        registryPort =
            new FreePortLocator("localHost", 5050, 5100).next();
        SSLTestUtils.setRMIRegistryFilter();
    }

    @Override
    public void tearDown()
        throws Exception {

        if (registry != null) {
            if (exportedObj != null) {

                /* Stop accepting new calls for this object. */
                UnicastRemoteObject.unexportObject(exportedObj, true);
            }
            TestUtils.destroyRegistry(registry);
        }
        SSLTestUtils.clearRMIRegistryFilter();
        super.tearDown();
    }

    @Test
    public void testBasic()
        throws Exception {

        /* Configure via SecurityParams */
        final SecurityParams sp = new SecurityParams();
        final File sslDir = SSLTestUtils.getTestSSLDir();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_KS_NAME).getPath());
        sp.setTruststoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_TS_NAME).getPath());
        sp.setPasswordFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_PW_NAME).getPath());
        sp.addTransportMap(TRANSPORT);
        sp.setTransServerKeyAlias(TRANSPORT, SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        final ParameterMap transMap = sp.getTransportMap(TRANSPORT);

        /* Create SSLControl objects from the SecurityParams config */
        final SSLTransport builder = new SSLTransport();
        final RMISocketPolicy rmiPolicy =
            builder.makeSocketPolicy(sp, transMap, logger);

        /* Make socket factories */
        final SocketFactoryArgs serverArgs =
            new SocketFactoryArgs().setSsfName("ssf").
            setCsfName("csf");

        /* Get Server Registry factory */
        final ServerSocketFactory regSsf =
            rmiPolicy.getRegistrySSF(serverArgs);

        /* Get Server Bind factories */
        final SocketFactoryPair serverBindPair =
            rmiPolicy.getBindPair(serverArgs);
        final ClientSocketFactory bindCsf = serverBindPair.getClientFactory();
        final ServerSocketFactory bindSsf = serverBindPair.getServerFactory();

        /* Get Client Registry factories */
        final SocketFactoryArgs clientArgs =
            new SocketFactoryArgs().setCsfName("regCsf");
        final ClientSocketFactory csf = rmiPolicy.getRegistryCSF(clientArgs);

        /* set up the CSF SSLControl */
        rmiPolicy.prepareClient(null, null);

        /* Create the registry and look it up */
        registry = LocateRegistry.createRegistry(registryPort,
                                                 null /* CSF */,
                                                 regSsf);
        final Registry reg1 =
            LocateRegistry.getRegistry("localhost", registryPort, csf);

        final SimpleImpl si = new SimpleImpl();
        reg1.rebind(SIMPLE, export(si, bindCsf, bindSsf));

        final Simple siStub = (Simple) reg1.lookup(SIMPLE);
        assertNotNull(siStub);

        final int result = siStub.addOne(9);
        assertEquals(10, result);
    }

    @Test
    public void testTrusted()
        throws Exception {

        /*
         * ss1 is a system setup that uses trust, and uses the normal
         * configuration pattern.
         */
        final SystemSetup ss1 = makeTrustedSetup(makeTrustedConfig());

        /* set up the CSF SSLControl */
        ss1.rmiPolicy.prepareClient(null, null);
        ss1.trustedPolicy.prepareClient(null, null);

        /* Create the registry and look it up */
        registry =
            LocateRegistry.createRegistry(registryPort, ss1.regCsf, ss1.regSsf);
        final Registry reg1 =
            LocateRegistry.getRegistry("localhost", registryPort, ss1.csf);

        final SimpleImpl si = new SimpleImpl();
        reg1.rebind(SIMPLE, export(si, ss1.bindCsf, ss1.bindSsf));

        final Simple siStub = (Simple) reg1.lookup(SIMPLE);
        assertNotNull(siStub);

        final int result = siStub.addOne(9);
        assertEquals(10, result);

        /*
         * ss2 is a system setup that uses trust, and uses the normal
         * configuration pattern, but with a different set of trust and keys
         * than ss1.
         */
        final SystemSetup ss2 = makeTrustedSetup(makeOtherTrustedConfig());

        /* set up the CSF SSLControl */
        ss2.rmiPolicy.prepareClient(null, null);
        ss2.trustedPolicy.prepareClient(null, null);

        /* Look up the registry with ss2 */
        final Registry reg2 =
            LocateRegistry.getRegistry("localhost", registryPort, ss2.csf);

        try {
            reg2.lookup(SIMPLE);
            fail("expected exception");
        } catch (ConnectIOException e) {
            if (TestUtils.testDebugEnabled()) {
                System.out.println("testTrusted: ss2 caught: " + e);
            }
        }

        /*
         * ss3 is a system setup that uses trust, and uses the normal
         * configuration pattern, but with a the same trust that is used by
         * different set of trust and keys
         * than ss1.
         */
        final SystemSetup ss3 = makeTrustedSetup(makeMixedTrustedConfig());

        /* set up the CSF SSLControl */
        ss3.rmiPolicy.prepareClient(null, null);
        ss3.trustedPolicy.prepareClient(null, null);

        /* Look up the registry with ss3 */
        final Registry reg3 =
            LocateRegistry.getRegistry("localhost", registryPort, ss3.csf);

        final Simple siStub3 = (Simple) reg3.lookup(SIMPLE);
        try {
            siStub3.addOne(9);
            fail("expected exception");
        } catch (ConnectIOException e) {
            if (TestUtils.testDebugEnabled()) {
                System.out.println("testTrusted: ss3 caught: " + e);
            }
        }

        /*
         * ss4 is a system setup that uses trust, and uses the normal
         * configuration pattern, but with an alters server validation string
         * that will cause it to fail.
         */
        final SystemSetup ss4 = makeTrustedSetup(makeInvalidTrustedConfig());

        /* set up the CSF SSLControl */
        ss4.rmiPolicy.prepareClient(null, null);
        ss4.trustedPolicy.prepareClient(null, null);

        /* Look up the registry with ss4 */
        final Registry reg4 =
            LocateRegistry.getRegistry("localhost", registryPort, ss4.csf);

        final Simple siStub4 = (Simple) reg4.lookup(SIMPLE);
        try {
            siStub4.addOne(9);
            fail("expected exception");
        } catch (ConnectIOException e) {
            if (TestUtils.testDebugEnabled()) {
                System.out.println("testTrusted: ss4 caught: " + e);
            }
        }
    }

    class SystemSetup {
        SecurityParams sp;

        RMISocketPolicy rmiPolicy;
        RMISocketPolicy trustedPolicy;

        ClientSocketFactory regCsf;
        ServerSocketFactory regSsf;

        ClientSocketFactory bindCsf;
        ServerSocketFactory bindSsf;

        ClientSocketFactory csf;
    }

    SystemSetup makeTrustedSetup(SecurityParams sp)
        throws Exception {

        final SystemSetup ss = new SystemSetup();

        ss.sp = sp;
        final ParameterMap transMap = sp.getTransportMap(TRANSPORT);
        final ParameterMap trustedMap = sp.getTransportMap(TRUSTED);

        final SSLTransport builder = new SSLTransport();
        ss.rmiPolicy = builder.makeSocketPolicy(sp, transMap, logger);
        ss.trustedPolicy = builder.makeSocketPolicy(sp, trustedMap, logger);

        final SocketFactoryArgs serverArgs =
            new SocketFactoryArgs().setSsfName("ssf").
            setCsfName("csf");

        /* Get Server Registry factory */
        ss.regSsf = ss.rmiPolicy.getRegistrySSF(serverArgs);

        final SocketFactoryPair serverBindPair =
            ss.trustedPolicy.getBindPair(serverArgs);
        ss.bindCsf = serverBindPair.getClientFactory();
        ss.bindSsf = serverBindPair.getServerFactory();

        final SocketFactoryArgs clientArgs =
            new SocketFactoryArgs().setCsfName("regCsf");
        ss.csf = ss.rmiPolicy.getRegistryCSF(clientArgs);

        return ss;
    }

    private SecurityParams makeTrustedConfig() {
        final SecurityParams sp = new SecurityParams();
        final File sslDir = SSLTestUtils.getTestSSLDir();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_KS_NAME).getPath());
        sp.setTruststoreFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_TS_NAME).getPath());
        sp.setPasswordFile(new File(sslDir.getPath(),
                                    SSLTestUtils.SSL_PW_NAME).getPath());
        sp.addTransportMap(TRANSPORT);
        sp.setTransServerKeyAlias(TRANSPORT, SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);

        sp.addTransportMap(TRUSTED);
        sp.setTransServerKeyAlias(TRUSTED, SSLTestUtils.SSL_KS_ALIAS_DEF);
        sp.setTransFactory(TRUSTED, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransClientIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        sp.setTransServerIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        return sp;
    }

    /**
     * Do the same thing as makeTrustedConfig, but alter it so that the server
     * validation fails.
     */
    private SecurityParams makeInvalidTrustedConfig() {
        final SecurityParams sp = makeTrustedConfig();
        sp.setTransServerIdentityAllowed(TRUSTED, "dnmatch(foo)");
        return sp;
    }

    /**
     * Creates a configuration using an alternate set of keys/trust
     */
    private SecurityParams makeOtherTrustedConfig() {
        final SecurityParams sp = new SecurityParams();
        final File sslDir = SSLTestUtils.getTestSSLDir();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_OTHER_KS_NAME).getPath());
        sp.setTruststoreFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_OTHER_TS_NAME).getPath());
        sp.setPasswordFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_OTHER_PW_NAME).getPath());
        sp.addTransportMap(TRANSPORT);
        sp.setTransServerKeyAlias(TRANSPORT,
                                  SSLTestUtils.SSL_OTHER_KS_ALIAS_DEF);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);

        sp.addTransportMap(TRUSTED);
        sp.setTransServerKeyAlias(TRUSTED, SSLTestUtils.SSL_OTHER_KS_ALIAS_DEF);
        sp.setTransFactory(TRUSTED, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransClientIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        sp.setTransServerIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        return sp;
    }

    /**
     * Creates a configuration using an alternate set of keys, but matching
     * trust
     */
    private SecurityParams makeMixedTrustedConfig() {
        final SecurityParams sp = new SecurityParams();
        final File sslDir = SSLTestUtils.getTestSSLDir();
        sp.setSecurityEnabled(true);
        sp.setKeystoreFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_OTHER_KS_NAME).getPath());
        sp.setTruststoreFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_TS_OTHER_NAME).getPath());
        sp.setPasswordFile(
            new File(sslDir.getPath(),
                     SSLTestUtils.SSL_OTHER_PW_NAME).getPath());
        sp.addTransportMap(TRANSPORT);
        sp.setTransServerKeyAlias(TRANSPORT,
                                  SSLTestUtils.SSL_OTHER_KS_ALIAS_DEF);
        sp.setTransFactory(TRANSPORT, SSLTestUtils.SSL_TRANSPORT_FACTORY);

        sp.addTransportMap(TRUSTED);
        sp.setTransServerKeyAlias(TRUSTED, SSLTestUtils.SSL_OTHER_KS_ALIAS_DEF);
        sp.setTransFactory(TRUSTED, SSLTestUtils.SSL_TRANSPORT_FACTORY);
        sp.setTransClientIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        sp.setTransServerIdentityAllowed(TRUSTED,
                                         SSLTestUtils.SSL_CERT_IDENT);
        return sp;
    }

    private Remote export(SimpleImpl object,
                          RMIClientSocketFactory csf,
                          RMIServerSocketFactory ssf)
        throws RemoteException {

        tearDowns.add(() -> safeUnexport(object));
        final Remote result =
            UnicastRemoteObject.exportObject(object, 0, csf, ssf);
        exportedObj = object;
        return result;
    }
}
