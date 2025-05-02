/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import oracle.kv.AuthenticationRequiredException;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.LoginCredentials;
import oracle.kv.PasswordCredentials;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.admin.ClientAdminServiceImpl;
import oracle.kv.impl.admin.CommandServiceImpl;
import oracle.kv.impl.admin.CommandServiceInitiator;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.client.admin.AsyncClientAdminServiceInitiator;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataKey;
import oracle.kv.impl.metadata.MetadataKeyTypeFinders;
import oracle.kv.impl.monitor.MonitorAgentInitiator;
import oracle.kv.impl.rep.admin.RepNodeAdminAPI;
import oracle.kv.impl.rep.admin.RepNodeAdminImpl;
import oracle.kv.impl.rep.admin.RepNodeAdminInitiator;
import oracle.kv.impl.security.annotations.PublicAPI;
import oracle.kv.impl.security.annotations.PublicMethod;
import oracle.kv.impl.security.annotations.SecureAPI;
import oracle.kv.impl.security.annotations.SecureAutoMethod;
import oracle.kv.impl.security.annotations.SecureInternalMethod;
import oracle.kv.impl.security.login.AsyncUserLoginInitiator;
import oracle.kv.impl.security.login.UserLoginImpl;
import oracle.kv.impl.sna.StorageNodeAgentImpl;
import oracle.kv.impl.sna.StorageNodeAgentInterfaceInitiator;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.util.SerialVersion;
import oracle.kv.impl.util.registry.AsyncRegistryUtils.InitiatorProxyFactory;
import oracle.kv.impl.util.registry.Protocols;
import oracle.kv.impl.util.registry.RegistryUtils;
import oracle.kv.impl.util.registry.VersionedRemote;
import oracle.kv.util.CreateStore.SecureUser;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test active use of the SecureProxy API.
 */
public class SecureProxyUseTest extends SecureTestBase {

    private static final String USER_NAME = "admin";
    private static final String USER_PW = "NoSql00__7654321";

    @BeforeClass
    public static void staticSetUp()
        throws Exception {

        users.add(new SecureUser(USER_NAME, USER_PW, true /* admin */));
        startup();
    }

    @AfterClass
    public static void staticTearDown()
        throws Exception {

        shutdown();
    }

    /**
     * Notes: This test did not call super setUp method to clean test directory.
     */
    @Override
    public void setUp()
        throws Exception {

    }

    @Override
    public void tearDown()
        throws Exception {

    }

    /**
     * Run through all interfaces bound in the registry and check that they
     * are functioning as expected, where it can be done generically.  For each
     * method that requires security, we call the method without providing
     * authentication and check that we get an ARE exception.
     */
    @Test
    public void testSecureProxyUse() throws Exception {
        final Set<Class<?>> implClassesTested = new HashSet<Class<?>>();

        /*
         * Look at each of the services in the registry and decide how
         * to handle each of them.
         */
        final List<String> serviceNames = RegistryUtils.getServiceNames(
            createStore.getStoreName(), HOST, registryPorts[0],
            Protocols.getDefault(), null /* client Id */, logger);
        for (final String serviceName : serviceNames) {

            Class<?> implClass = null;
            InitiatorProxyFactory<Remote> proxyFactory = null;
            if ("commandService".equals(serviceName)) {
                /* admin command service */
                implClass = CommandServiceImpl.class;
                proxyFactory = CommandServiceInitiator::createProxy;
            } else if (serviceName.equals
                        (GlobalParams.CLIENT_ADMIN_SERVICE_NAME)) {
                implClass = ClientAdminServiceImpl.class;
                proxyFactory =
                    AsyncClientAdminServiceInitiator::createSyncProxy;
            } else if (serviceName.endsWith(":LOGIN")) {
                /* Some login interface (rep or admin) */
                implClass = UserLoginImpl.class;
                proxyFactory = AsyncUserLoginInitiator::createSyncProxy;
            } else if (serviceName.startsWith(
                           createStore.getStoreName() + ":rg") &&
                       serviceName.endsWith(":MAIN")) {
                /*
                 * RequestHandler - we know that it doesn't use the
                 * SecureProxy mechanism
                 */
                assertTrue(true); /* ignore */
            } else if (serviceName.startsWith(
                           createStore.getStoreName() + ":rg") &&
                       serviceName.endsWith(":MONITOR")) {
                /* rep monitor */
                implClass = oracle.kv.impl.rep.monitor.MonitorAgentImpl.class;
                proxyFactory = MonitorAgentInitiator::createProxy;
            } else if (serviceName.startsWith(
                           createStore.getStoreName() + ":rg") &&
                       serviceName.endsWith(":ADMIN")) {
                /* RepNodeAdmin */
                implClass = RepNodeAdminImpl.class;
                proxyFactory = RepNodeAdminInitiator::createProxy;
            } else if (serviceName.startsWith(
                           createStore.getStoreName() + ":sn") &&
                       serviceName.endsWith(":MAIN")) {
                /* SNA */
                implClass = StorageNodeAgentImpl.class;
                proxyFactory =
                    StorageNodeAgentInterfaceInitiator::createProxy;
            } else if (serviceName.startsWith(
                           createStore.getStoreName() + ":sn") &&
                       serviceName.endsWith(":MONITOR")) {
                /* SNA monitor */
                implClass = oracle.kv.impl.sna.MonitorAgentImpl.class;
                proxyFactory = MonitorAgentInitiator::createProxy;
            } else if (serviceName.endsWith(":TEST") ||
                       "commandServiceTest".equals(serviceName)) {
                /* we ignore test interfaces */
                assertTrue(true); /* ignore */
            } else if ("SNA:TRUSTED_LOGIN".equals(serviceName)) {
                /* We ignore the trusted login interface */
                assertTrue(true); /* ignore */
            } else  {
                fail("Encountered unrecognized service name: " + serviceName);
            }

            if (implClass != null) {
                assertNotNull(proxyFactory);
                try {
                    final Remote stub = RegistryUtils.getRemoteService(
                        createStore.getStoreName(), HOST, registryPorts[0],
                        serviceName, proxyFactory, logger);
                    verifySecureAPI(serviceName, stub, implClass);
                    implClassesTested.add(implClass);
                } catch (NotBoundException nbe) {
                    fail("encountered exception during registry lookup: " +
                         nbe);
                }
            }
        }

        /*
         * The list of SecureProxy implementation classes that we expect to
         * encounter.
         */
        final Set<Class<?>> expectImpls = new HashSet<Class<?>>();
        expectImpls.add(CommandServiceImpl.class);
        expectImpls.add(StorageNodeAgentImpl.class);
        expectImpls.add(RepNodeAdminImpl.class);
        expectImpls.add(oracle.kv.impl.sna.MonitorAgentImpl.class);
        expectImpls.add(oracle.kv.impl.rep.monitor.MonitorAgentImpl.class);
        expectImpls.add(UserLoginImpl.class);
        expectImpls.add(ClientAdminServiceImpl.class);

        /*
         * Check that we saw all that we expected to see and that our list of
         * expectations isn't missing anything
         */
        assertEquals("Impl classes tested", expectImpls, implClassesTested);
    }

    /**
     * Test that Security Metadata cannot be accessed by ordinary users.
     */
    @Test
    public void testSecurityMetadataAccess() throws Exception {
        final LoginCredentials creds =
            new PasswordCredentials(USER_NAME, USER_PW.toCharArray());
        final KVStoreConfig kvConfig =
            new KVStoreConfig(createStore.getStoreName(),
                              HOST + ":" + registryPorts[0]);
        final Properties props = new Properties();
        addTransportProps(props);
        kvConfig.setSecurityProperties(props);

        final KVStore store = KVStoreFactory.getStore(kvConfig, creds, null);

        final RegistryUtils ruAuth =
            new RegistryUtils(createStore.getAdmin().getTopology(),
                              KVStoreImpl.getLoginManager(store),
                              logger);

        final RegistryUtils ruNoAuth =
            new RegistryUtils(createStore.getAdmin().getTopology(), null,
                              logger);

        final List<RepNodeId> rnIds =
            createStore.getRNs(createStore.getStorageNodeIds()[0]);

        final RepNodeAdminAPI rnaAuth = ruAuth.getRepNodeAdmin(rnIds.get(0));

        try {
            rnaAuth.getMetadataSeqNum(MetadataType.SECURITY);
        } catch (UnauthorizedException ue) {
            assertTrue(true); /* OK */
        }

        try {
            rnaAuth.getMetadata(MetadataType.SECURITY, 0);
        } catch (UnauthorizedException ue) {
            assertTrue(true); /* OK */
        }

        try {
            rnaAuth.getMetadata(MetadataType.SECURITY,
                                  new DummyMetadataKey(), 0);
        } catch (UnauthorizedException ue) {
            assertTrue(true); /* OK */
        }

        final RepNodeAdminAPI rnaNoAuth =
            ruNoAuth.getRepNodeAdmin(rnIds.get(0));

        try {
            rnaNoAuth.getMetadataSeqNum(MetadataType.SECURITY);
        } catch (AuthenticationRequiredException are) {
            assertTrue(true); /* OK */
        }

        try {
            rnaNoAuth.getMetadata(MetadataType.SECURITY, 0);
        } catch (AuthenticationRequiredException are) {
            assertTrue(true); /* OK */
        }

        try {
            rnaNoAuth.getMetadata(MetadataType.SECURITY,
                                  new DummyMetadataKey(), 0);
        } catch (AuthenticationRequiredException are) {
            assertTrue(true); /* OK */
        }

        store.close();
    }

    /**
     * Check that the stub and expected implementation class have compatible
     * interfaces, then test that the methods defined in the interfaces
     * are reasonably defined with secure annotations and perform some testing
     * to check that the method annotations work as expected.
     */
    private void verifySecureAPI(String serviceName,
                                 Remote stub,
                                 Class<?> implClass) {
        final Class<?> stubClass = stub.getClass();

        final Class<?>[] stubIFs = stubClass.getInterfaces();
        final Class<?>[] implIFs = implClass.getInterfaces();

        /*
         * Make sure that the stub provides the interfaces that we think it
         * should.
         */
        final Set<Class<?>> remoteIFs = new HashSet<Class<?>>();
        for (Class<?> implIF : implIFs) {
            if (VersionedRemote.class.isAssignableFrom(implIF)) {
                boolean foundIF = false;
                for (Class<?> stubIF : stubIFs) {
                    if (stubIF == implIF) {
                        foundIF = true;
                        addRemoteInterfaces(remoteIFs, stubIF);
                    }
                }
                assertTrue(serviceName, foundIF);
            }
        }

        /* Check the methods in the remote interfaces */
        verifySecureAPIClass(serviceName, stub, implClass, remoteIFs);
    }

    /**
     * The arg stubIF is an interface class that extends VersionedRemote.
     * Add stubIF and any of its direct or indirect super interfaces to
     * remoteIFs.
     */
    private void addRemoteInterfaces(Set<Class<?>> remoteIFs, Class<?> stubIF) {

        if (stubIF == VersionedRemote.class) {
            return;
        }
        remoteIFs.add(stubIF);
        for (Class<?> superIF : stubIF.getInterfaces()) {
            if (VersionedRemote.class.isAssignableFrom(superIF)) {
                addRemoteInterfaces(remoteIFs, superIF);
            }
        }
    }

    /**
     * Given an RMI stub, a list of VersionedRemote interfaces implementd
     * by the stub and a class that we expect is providing the implementation
     * for the stub, check the method annotations on the implementations
     * of the interface methods.
     */
    private void verifySecureAPIClass(String serviceName,
                                      Remote stub,
                                      Class<?> implClass,
                                      Set<Class<?>> remoteIFs) {

        for (Class<?> remoteIF : remoteIFs) {
            for (Method ifMeth : remoteIF.getDeclaredMethods()) {
                verifySecureAPIMethod(serviceName, stub, ifMeth, implClass);
            }
        }
    }

    /**
     * Given an RMI stub, a method defined by a VersionedRemote interface
     * declared by the stub, and a class that provides the implementation
     * for the stub, check the method annotations and, if the method is
     * annotated as an R2 compatibility method where the R3 version requires
     * authentication, or if it is an R3 method that requires authentication,
     * try calling the method to be sure that the annotations are working as
     * expected.
     */
    private void verifySecureAPIMethod(String serviceName,
                                       Remote stub,
                                       Method ifMethod,
                                       Class<?> implClass) {

        final String methodName = serviceName + "." + ifMethod.getName();

        /* find the method in the implementation class */
        final Method implMethod = findImplMethod(implClass, ifMethod);
        assertNotNull(methodName, implMethod);

        /*
         * The method may be implemented on a superclass of implClass, so
         * don't assume implClass here.
         */
        final Class<?> methodImplClass = implMethod.getDeclaringClass();

        /* get the method and class-level secure annotations */
        final Annotation methAnn = getSecureMethodAnnotation(implMethod);
        final Annotation classAnn = getSecureClassAnnotation(methodImplClass);

        assertNotNull(serviceName, classAnn);
        if (classAnn instanceof SecureAPI) {
            assertNotNull(methodName, methAnn);
        }

        /*
         * Do testing of R3 methods, where possible.
         */
        if (methAnn instanceof SecureAutoMethod) {
            checkSecureAutoMethod(methodName, stub, ifMethod, implMethod);
        }
    }

    /**
     * Given an interface method that is a SecureAuto interface, attempt
     * invocation if possible, to test that authentication is checked.
     */
    private void checkSecureAutoMethod(String methodName,
                                       Remote stub,
                                       Method ifMethod,
                                       Method implMethod) {

        final Annotation methAnn = getSecureMethodAnnotation(implMethod);

        assertTrue(methAnn instanceof SecureAutoMethod);

        final SecureAutoMethod sam = (SecureAutoMethod) methAnn;
        final KVStorePrivilegeLabel[] privis = sam.privileges();

        if (privis != null && privis.length > 0) {
            /*
             * This is a SecureAuto method that requires some level of
             * authentication.  Try calling without authentication to make
             * sure it complains.
             */
            runSecureAutoMethod(methodName, stub, ifMethod);
        }
    }

    /*
     * ifMethod is a SecureAuto that requires some level of authentication.
     * Run it and expect ARE.
     */
    private void runSecureAutoMethod(String methodName,
                                     Remote stub,
                                     Method ifMethod) {

        final Class<?>[] argTypes = ifMethod.getParameterTypes();
        final Object[] args = new Object[argTypes.length];
        assertTrue(methodName, argTypes.length > 1);
        assertSame(methodName, short.class, argTypes[args.length - 1]);
        assertSame(methodName, AuthContext.class, argTypes[args.length - 2]);
        args[args.length - 1] = SerialVersion.CURRENT;

        /* fill in primitive types with 0's */
        for (int i = 0; i < argTypes.length - 1; i++) {
            args[i] = makeNull(argTypes[i]);
        }

        try {
            ifMethod.invoke(stub, args);
            fail(methodName + ": Should have had an exception");
        } catch (InvocationTargetException ite) {
            try {
                throw ite.getCause();
            } catch (AuthenticationRequiredException are) {
                /* This is what we want */
                assertTrue(true); /* ignore */
            } catch (Throwable t) {
                fail(methodName + ": wrong exception type: " + t);
            }
        } catch (Exception e) {
            fail(methodName + ": wrong exception type: " + e);
        }
    }

    /**
     * Make a "null" representation of the specified object type.  For most
     * types, null is null, but for primitive types, we need to supply a
     * non-null value.
     */
    private Object makeNull(Class<?> objType) {
        if (objType == int.class) {
            return 0;
        } else if (objType == short.class) {
            return (short) 0;
        } else if (objType == long.class) {
            return 0L;
        } else if (objType == byte.class) {
            return (byte) 0;
        } else if (objType == boolean.class) {
            return false;
        } else if (objType == char.class) {
            return ' ';
        }
        /* Ignore float types - we don't care */
        return null;
    }

    /**
     * Find the class-level security annotation for a class.  If more than
     * one is found, fail the test.
     */
    private Annotation getSecureClassAnnotation(Class<?> clazz) {
        Annotation classAnn = null;
        for (Annotation a : clazz.getDeclaredAnnotations()) {
            if (a instanceof SecureAPI ||
                a instanceof PublicAPI) {

                assertNull(classAnn);
                classAnn = a;
            }
        }

        return classAnn;
    }

    /**
     * Find the method-level security annotation for a method.  If more than
     * one is found, fail the test.
     */
    private Annotation getSecureMethodAnnotation(Method meth) {

        Annotation methAnn = null;
        for (Annotation a : meth.getAnnotations()) {
            if (a instanceof SecureAutoMethod ||
                a instanceof SecureInternalMethod ||
                a instanceof PublicMethod) {

                if (methAnn != null) {
                    fail("multiple secure method annotations for " + meth);
                }

                methAnn = a;
            }
        }

        return methAnn;
    }

    /**
     * Given an interface method, find the method on implClass that implements
     * the interface method.
     */
    private Method findImplMethod(Class<?> implClass, Method ifMethod) {
        try {
            final Method implMethod =
                implClass.getDeclaredMethod(ifMethod.getName(),
                                            ifMethod.getParameterTypes());
            assertNotNull(implMethod);
            return implMethod;
        } catch (NoSuchMethodException nsme) {
            if (implClass == Object.class) {
                fail("method not found");
            }
            return findImplMethod(implClass.getSuperclass(), ifMethod);
        }
    }

    public static class DummyMetadataKey
        implements MetadataKey, Serializable {

        private static final long serialVersionUID = 1L;

        private static final int KEY_TYPE_VALUE = 100;
        private static MetadataKeyType KEY_TYPE = new MetadataKeyType() {
            @Override
            public int getIntValue() { return KEY_TYPE_VALUE; }
            @Override
            public DummyMetadataKey readMetadataKey(DataInput in, short sv)
                throws IOException
            {
                return new DummyMetadataKey();
            }
        };
        static {
            MetadataKeyTypeFinders.addFinder(
                intValue -> (intValue == KEY_TYPE_VALUE) ? KEY_TYPE : null);
        }

        DummyMetadataKey() {
        }

        @Override
        public void writeFastExternal(DataOutput out, short sv) { }

        @Override
        public MetadataType getType() {
            return MetadataType.SECURITY;
        }

        @Override
        public MetadataKeyType getMetadataKeyType() {
            return KEY_TYPE;
        }
    }
}
