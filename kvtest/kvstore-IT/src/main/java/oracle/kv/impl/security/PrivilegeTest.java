/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security;

import static java.util.Collections.emptySet;
import static oracle.kv.impl.util.SerialTestUtils.serialVersionChecker;
import static oracle.kv.impl.util.SerialTestUtils.versionHashes;
import static oracle.kv.impl.util.TestUtils.checkFastSerialize;
import static oracle.kv.impl.util.TestUtils.checkSerialize;
import static oracle.kv.util.TestUtils.checkAll;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import oracle.kv.AuthenticationRequiredException;
import oracle.kv.TestBase;
import oracle.kv.UnauthorizedException;
import oracle.kv.impl.security.ExecutionContext.PrivilegeCollection;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.security.login.SessionId;
import oracle.kv.impl.util.RateLimitingLogger;
import oracle.kv.table.TableAPI;

import org.junit.Test;

public class PrivilegeTest extends TestBase {
    private final static int DUMMY_TABLE_ID = 0;
    private final static String DUMMY_TABLE_NAME = "foo";
    private final static String DUMMY_TABLE_NAMESPACE = "ns";

    @Test
    public void testPrivilegeTypeCheck() {

        for (KVStorePrivilegeLabel label : KVStorePrivilegeLabel.values()) {
            switch (label.getType()) {
            case OBJECT:
                /* Test getting a system privilege using object label */
                try {
                    SystemPrivilege.get(label);
                    fail("expected exception");
                } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */{
                } /* CHECKSTYLE:ON */

                /* Test getting a table privilege using object label */
                try {
                    TablePrivilege.get(label, DUMMY_TABLE_ID,
                        DUMMY_TABLE_NAMESPACE, DUMMY_TABLE_NAME);
                    fail("expected exception");
                } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */{
                } /* CHECKSTYLE:ON */

                break;

            case SYSTEM:
                /* Should go well */
                try {
                    SystemPrivilege.get(label);
                } catch (Exception e) {
                    fail("Unexpected exception");
                }

                /* Test getting a table privilege using system label */
                try {
                    TablePrivilege.get(label, 0, "ns", "foo");
                    fail("expected exception");
                } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */{
                } /* CHECKSTYLE:ON */

                break;

            case TABLE:
                /* Test getting a system privilege using table label */
                try {
                    SystemPrivilege.get(label);
                    fail("expected exception");
                } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */{
                } /* CHECKSTYLE:ON */

                /* Should go well */
                try {
                    TablePrivilege.get(label, 0, "ns", "foo");
                } catch (Exception e) {
                    fail("Unexpected exception");
                }
                break;

            case NAMESPACE:
                /* Test getting a system privilege using namespace label */
                try {
                    SystemPrivilege.get(label);
                    fail("expected exception");
                } catch (IllegalArgumentException iae) /* CHECKSTYLE:OFF */{
                } /* CHECKSTYLE:ON */

                if ( label.equals(KVStorePrivilegeLabel.MODIFY_IN_NAMESPACE)) {
                    /* Skip MODIFY_IN_NAMESPACE since it's just convenience */
                    break;
                }
                /* Should go well */
                try {
                    NamespacePrivilege.get(label, "ns");
                } catch (Exception e) {
                    fail("Unexpected exception");
                }
                break;
            }
        }
    }

    @Test
    public void testImplication() {

        /* Test SYSDBA implication */
        final PrivilegeCollection dbaPriv =
            buildPrivsCollection(SystemPrivilege.SYSDBA);

        final List<KVStorePrivilege> privsImpliedBySysdba =
             Arrays.asList(
                 SystemPrivilege.SYSDBA,
                 SystemPrivilege.CREATE_ANY_TABLE,
                 SystemPrivilege.DROP_ANY_TABLE,
                 SystemPrivilege.EVOLVE_ANY_TABLE,
                 SystemPrivilege.CREATE_ANY_INDEX,
                 SystemPrivilege.DROP_ANY_INDEX,
                 SystemPrivilege.READ_ANY_SCHEMA,
                 SystemPrivilege.WRITE_ANY_SCHEMA,
                 SystemPrivilege.CREATE_ANY_NAMESPACE,
                 SystemPrivilege.DROP_ANY_NAMESPACE,
                 SystemPrivilege.CREATE_ANY_REGION,
                 SystemPrivilege.DROP_ANY_REGION,
                 SystemPrivilege.SET_LOCAL_REGION,
                 new TablePrivilege.CreateIndex(DUMMY_TABLE_ID,
                     DUMMY_TABLE_NAMESPACE),
                 new TablePrivilege.EvolveTable(DUMMY_TABLE_ID,
                     DUMMY_TABLE_NAMESPACE),
                 new TablePrivilege.DropIndex(DUMMY_TABLE_ID,
                     DUMMY_TABLE_NAMESPACE)
             );

        checkPrivImplication("SYSDBA", dbaPriv, privsImpliedBySysdba);

        /* Test READ_ANY implication */
        final PrivilegeCollection readAnyPriv =
            buildPrivsCollection(SystemPrivilege.READ_ANY);

        final List<? extends KVStorePrivilege> privsImpliedByReadany =
            Arrays.asList(
                  SystemPrivilege.READ_ANY,
                  SystemPrivilege.READ_ANY_TABLE,
                  new TablePrivilege.ReadTable(DUMMY_TABLE_ID)
            );

        checkPrivImplication("READ_ANY", readAnyPriv, privsImpliedByReadany);

        /* Test READ_ANY_TABLE implication */
        final PrivilegeCollection readAnyTablePriv =
            buildPrivsCollection(SystemPrivilege.READ_ANY_TABLE);
        final List<? extends KVStorePrivilege> privsImpliedByReadanyTable =
            Arrays.asList(
                  SystemPrivilege.READ_ANY_TABLE,
                  new TablePrivilege.ReadTable(DUMMY_TABLE_ID)
            );

        checkPrivImplication("READ_ANY_TABLE", readAnyTablePriv,
                             privsImpliedByReadanyTable);

        /* Test WRITE_ANY implication */
        final PrivilegeCollection writeAnyPriv =
            buildPrivsCollection(SystemPrivilege.WRITE_ANY);

        final List<? extends KVStorePrivilege> privsImpliedByWriteAny =
           Arrays.asList(
                SystemPrivilege.WRITE_ANY,
                SystemPrivilege.INSERT_ANY_TABLE,
                SystemPrivilege.DELETE_ANY_TABLE,
                new TablePrivilege.InsertTable(DUMMY_TABLE_ID),
                new TablePrivilege.DeleteTable(DUMMY_TABLE_ID)
           );

        checkPrivImplication("WRITE_ANY", writeAnyPriv,
                             privsImpliedByWriteAny);

        /* Test INSERT_ANY_TABLE implication */
        final PrivilegeCollection insertAnyTablePriv =
            buildPrivsCollection(SystemPrivilege.INSERT_ANY_TABLE);

        final List<? extends KVStorePrivilege> privsImpliedByInsertAnyTable =
            Arrays.asList(
                 SystemPrivilege.INSERT_ANY_TABLE,
                 new TablePrivilege.InsertTable(DUMMY_TABLE_ID)
            );

        checkPrivImplication("INSERT_ANY_TABLE", insertAnyTablePriv,
                             privsImpliedByInsertAnyTable);

        /* Test DELETE_ANY_TABLE implication */
        final PrivilegeCollection deleteAnyTablePriv =
            buildPrivsCollection(SystemPrivilege.DELETE_ANY_TABLE);

        final List<? extends KVStorePrivilege> privsImpliedByDeleteAnyTable =
            Arrays.asList(
                 SystemPrivilege.DELETE_ANY_TABLE,
                 new TablePrivilege.DeleteTable(DUMMY_TABLE_ID)
            );

        checkPrivImplication("DELETE_ANY_TABLE", deleteAnyTablePriv,
                             privsImpliedByDeleteAnyTable);
    }

    /*
     * Tests that all table privilege label has an implying namespace privilege
     * label defined and the implying privileges of corresponding table
     * privilege have all implying privileges of the namespace privilege.
     */
    @Test
    public void testTableNamespacePrivsImplication() {
        for (KVStorePrivilegeLabel label : KVStorePrivilegeLabel.values()) {
            if (label.getType() == KVStorePrivilege.PrivilegeType.TABLE) {
                KVStorePrivilegeLabel nsLabel =
                    TablePrivilege.implyingNamespacePrivLabel(label);
                assertNotNull(nsLabel);
                TablePrivilege tbPriv = TablePrivilege.get(
                    label, 0, TableAPI.SYSDEFAULT_NAMESPACE_NAME, "foo");
                NamespacePrivilege nsPriv = NamespacePrivilege.get(
                    nsLabel, TableAPI.SYSDEFAULT_NAMESPACE_NAME);

                assertTrue(Arrays.asList(tbPriv.implyingPrivileges())
                                 .containsAll(Arrays.asList(
                                     nsPriv.implyingPrivileges())));
            }
        }
    }

    /*
     * Tests that an ExecutionContext has all privileges defined by the roles
     * of its user subject.
     */
    @Test
    public void testExecutionContextPrivileges() {

        final AuthContext authCtx = new AuthContext(
            new LoginToken(new SessionId(new byte[4]), 0L));
        final KVBuiltInRoleResolver roleResolver =
            new KVBuiltInRoleResolver();
        final RoleAccessChecker roleAccessChecker = new RoleAccessChecker(
            roleResolver, Logger.getLogger("PrivilegeTest"));

        final Collection<RoleInstance> roles = roleResolver.getAllRoles();
        try {
            for (final RoleInstance role : roles) {
                roleAccessChecker.updateRole(role);
                final OperationContext opCtx = new RoleCheckContext(role);
                /*
                 * Checks subjects have all privileges defined by its role in
                 * their execution context.
                 */
                ExecutionContext.create(roleAccessChecker, authCtx, opCtx);
            }
        } catch (UnauthorizedException uae) {
            fail("Should not see UnauthorizedException");
        } catch (Exception e) {
            fail("Unexpected exception");
        }
    }

    @Test
    public void testRateLimitingLogging()
        throws Exception {

        final AuthContext authCtx = new AuthContext(
            new LoginToken(new SessionId(new byte[4]), 0L));
        final KVBuiltInRoleResolver roleResolver =
            new KVBuiltInRoleResolver();
        final int logSamplePeriodMs = 5 * 1000;
        final int faultsLimit = 10;
        long startMs = System.currentTimeMillis();

        /* Specify logSamplePeriod and maximum fault limits for testing */
        final RoleAccessChecker roleAccessChecker = new RoleAccessChecker(
            roleResolver, Logger.getLogger("RateLimitingLoggingTest"),
            logSamplePeriodMs, faultsLimit);
        final RateLimitingLogger<String> rateLimitingLogger =
            roleAccessChecker.getRateLimitingLogger();

        /*
         * Pick a operation the requires role not in the execution context
         * to simulate the unauthorized accesses.
         */
        final OperationContext opCtx =
            new RoleCheckContext(RoleInstance.READWRITE);
        try {
            ExecutionContext.create(roleAccessChecker, authCtx, opCtx);
            fail("expected unauthorized exception");
        } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */{
        } /* CHECKSTYLE:ON */

        /* One unauthorized error should be logged */
        assertEquals(1, rateLimitingLogger.getLimitedMessageCount());

        /* Simulate the same unauthorized operations hundreds times */
        for (int i = 0; i < 100; i++) {
            try {
                ExecutionContext.create(roleAccessChecker, authCtx, opCtx);
                fail("expected unauthorized exception");
            } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */{
            } /* CHECKSTYLE:ON */
        }
        assertTrue(System.currentTimeMillis() < (startMs + logSamplePeriodMs));

        /* Just one unauthorized error should be logged */
        assertEquals(1, rateLimitingLogger.getLimitedMessageCount());

        /*
         * Sleep a little longer than log period time in order to make
         * rate limiting logger to be able to log the previous operation again.
         */
        Thread.sleep(logSamplePeriodMs + 100);

        /* Simulate the same unauthorized operations hundreds times */
        for (int i = 0; i < 100; i++) {
            try {
                ExecutionContext.create(roleAccessChecker, authCtx, opCtx);
                fail("expected unauthorized exception");
            } catch (UnauthorizedException ue) /* CHECKSTYLE:OFF */{
            } /* CHECKSTYLE:ON */
        }

        /* Now should be two records logged */
        assertEquals(2, rateLimitingLogger.getLimitedMessageCount());
    }

    @Test
    public void testRolePrivileges() {
        assertEquals(set(SystemPrivilege.READ_ANY),
                     RoleInstance.READONLY.getPrivileges());
        assertEquals(set(SystemPrivilege.WRITE_ANY),
                     RoleInstance.WRITEONLY.getPrivileges());
        assertEquals(set(SystemPrivilege.READ_ANY, SystemPrivilege.WRITE_ANY),
                     RoleInstance.READWRITE.getPrivileges());
        assertEquals(set(SystemPrivilege.SYSDBA, SystemPrivilege.DBVIEW),
                     RoleInstance.DBADMIN.getPrivileges());
        assertEquals(set(SystemPrivilege.SYSDBA, SystemPrivilege.SYSOPER,
                         SystemPrivilege.SYSVIEW),
                     RoleInstance.SYSADMIN.getPrivileges());
        assertEquals(set(SystemPrivilege.USRVIEW, SystemPrivilege.DBVIEW),
                     RoleInstance.PUBLIC.getPrivileges());
        assertEquals(set(SystemPrivilege.WRITE_SYSTEM_TABLE),
                     RoleInstance.WRITESYSTABLE.getPrivileges());
        assertEquals(set(SystemPrivilege.INTLOPER),
                     RoleInstance.INTERNAL.getPrivileges());
        assertEquals(emptySet(), RoleInstance.ADMIN.getPrivileges());
        assertEquals(emptySet(), RoleInstance.AUTHENTICATED.getPrivileges());
    }

    @Test
    public void testRoleGrantedRoles() {
        assertEquals(emptySet(), RoleInstance.READONLY.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.WRITEONLY.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.READWRITE.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.DBADMIN.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.SYSADMIN.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.PUBLIC.getGrantedRoles());
        assertEquals(emptySet(), RoleInstance.WRITESYSTABLE.getGrantedRoles());
        assertEquals(set(RoleInstance.PUBLIC.name(),
                         RoleInstance.SYSADMIN.name(),
                         RoleInstance.READWRITE.name(),
                         RoleInstance.WRITESYSTABLE.name()),
                     RoleInstance.INTERNAL.getGrantedRoles());
        assertEquals(set(RoleInstance.PUBLIC.name(),
                         RoleInstance.SYSADMIN.name(),
                         RoleInstance.READWRITE.name()),
                     RoleInstance.ADMIN.getGrantedRoles());
        assertEquals(set(RoleInstance.PUBLIC.name(),
                         RoleInstance.READWRITE.name()),
                     RoleInstance.AUTHENTICATED.getGrantedRoles());
    }

    @Test
    public void testSerialization() throws Exception {

        /* Serialization hashes for all system privileges */
        final Map<Object, NavigableMap<Short, Long>> systemHashes = hashesMap(
            SystemPrivilege.READ_ANY, 0x5ba93c9db0cff93fL,
            SystemPrivilege.WRITE_ANY, 0xbf8b4530d8d246ddL,
            SystemPrivilege.SYSVIEW, 0x9842926af7ca0a8cL,
            SystemPrivilege.USRVIEW, 0x8dc00598417d4eb7L,
            SystemPrivilege.SYSOPER, 0x2d0134ed3b9de132L,
            SystemPrivilege.INTLOPER, 0x5d1be7e9dda1ee88L,
            SystemPrivilege.DBVIEW, 0xa42c6cf1de3abfdeL,
            SystemPrivilege.SYSDBA, 0xc4ea21bb365bbeeaL,
            SystemPrivilege.WRITE_SYSTEM_TABLE, 0x4345cb1fa27885a8L,
            SystemPrivilege.READ_ANY_SCHEMA, 0x8d883f1577ca8c33L,
            SystemPrivilege.WRITE_ANY_SCHEMA, 0xac9231da4082430aL,
            SystemPrivilege.READ_ANY_TABLE, 0xc7255dc48b42d44fL,
            SystemPrivilege.DELETE_ANY_TABLE, 0x6e14a407faae9399L,
            SystemPrivilege.INSERT_ANY_TABLE, 0xa8abd012eb59b862L,
            SystemPrivilege.CREATE_ANY_TABLE, 0xadc83b19e793491bL,
            SystemPrivilege.DROP_ANY_TABLE, 0x67d5096f219c64bL,
            SystemPrivilege.EVOLVE_ANY_TABLE, 0x1e32e3c360501a0eL,
            SystemPrivilege.CREATE_ANY_INDEX, 0x11f4de6b8b45cf80L,
            SystemPrivilege.DROP_ANY_INDEX, 0x320355ced694aa69L,
            SystemPrivilege.CREATE_ANY_NAMESPACE, 0xc2143b1a0db17957L,
            SystemPrivilege.DROP_ANY_NAMESPACE, 0xe9c5d7db93a1c17dL,
            SystemPrivilege.CREATE_ANY_REGION, 0xd08f88df745fa795L,
            SystemPrivilege.DROP_ANY_REGION, 0x3cdf2936da2fc556L,
            SystemPrivilege.SET_LOCAL_REGION, 0x7c4d33785daa5c23L);
        checkAll(SystemPrivilege.getAllSystemPrivileges().stream()
                 .map(priv -> {
                         assertTrue("No hash for " + priv,
                                    systemHashes.containsKey(priv));
                         return (Runnable) () ->
                             testSerialization(priv, systemHashes.get(priv));
                     }));

        /* Hashes for table privileges */
        final Map<Object, NavigableMap<Short, Long>> tableHashes = hashesMap(
            KVStorePrivilegeLabel.DELETE_TABLE, 0x39b3c5822e1b38f0L,
            KVStorePrivilegeLabel.READ_TABLE, 0xf37f3b8d604514eeL,
            KVStorePrivilegeLabel.INSERT_TABLE, 0x22dd94261583c57L,
            KVStorePrivilegeLabel.EVOLVE_TABLE, 0xd18b1127f1366378L,
            KVStorePrivilegeLabel.CREATE_INDEX, 0xcc209c69697cd163L,
            KVStorePrivilegeLabel.DROP_INDEX, 0xa210c689687cd851L);
        checkAll(TablePrivilege.getAllTablePrivileges(
                     DUMMY_TABLE_NAMESPACE, DUMMY_TABLE_ID, DUMMY_TABLE_NAME)
                 .stream()
                 .map(priv -> {
                         assertTrue("No hash for " + priv.getLabel(),
                                    tableHashes.containsKey(priv.getLabel()));
                         return (Runnable) () -> testSerialization(
                             priv, tableHashes.get(priv.getLabel()));
                     }));

        /* Hashes for namespace privileges */
        final Map<Object, NavigableMap<Short, Long>> namespaceHashes =
            hashesMap(KVStorePrivilegeLabel.READ_IN_NAMESPACE,
                      0xf472602afdd19c2bL,
                      KVStorePrivilegeLabel.INSERT_IN_NAMESPACE,
                      0x32094d10e6efa4fbL,
                      KVStorePrivilegeLabel.DELETE_IN_NAMESPACE,
                      0xef2cdf51821240d1L,
                      KVStorePrivilegeLabel.CREATE_TABLE_IN_NAMESPACE,
                      0xa7bdfbdc69bd8ef5L,
                      KVStorePrivilegeLabel.DROP_TABLE_IN_NAMESPACE,
                      0x36186f06efb61425L,
                      KVStorePrivilegeLabel.EVOLVE_TABLE_IN_NAMESPACE,
                      0x5a5e9831950cd1fbL,
                      KVStorePrivilegeLabel.CREATE_INDEX_IN_NAMESPACE,
                      0xdefa2d31ad1d0d2fL,
                      KVStorePrivilegeLabel.DROP_INDEX_IN_NAMESPACE,
                      0x975ce28d4c5aa413L);
        checkAll(NamespacePrivilege.getAllNamespacePrivileges(
                     DUMMY_TABLE_NAMESPACE)
                 .stream()
                 .map(priv -> {
                         assertTrue("No hash for " + priv.getLabel(),
                                    namespaceHashes.containsKey(
                                        priv.getLabel()));
                         return (Runnable) () -> testSerialization(
                             priv, namespaceHashes.get(priv.getLabel()));
                     }));
    }

    /* Allow runtime check for maps and longs */
    @SuppressWarnings("unchecked")
    private Map<Object, NavigableMap<Short, Long>>
        hashesMap(Object... objectsAndHashes)
    {
        assertTrue(objectsAndHashes.length % 2 == 0);
        final Map<Object, NavigableMap<Short, Long>> map = new HashMap<>();
        for (int i = 0; i < objectsAndHashes.length; i+=2) {
            final Object object = objectsAndHashes[i];
            final Object hash = objectsAndHashes[i+1];
            map.put(object,
                    (hash instanceof Long) ?
                    versionHashes((Long) hash) :
                    (NavigableMap<Short, Long>) hash);


        }
        return map;
    }

    private void testSerialization(KVStorePrivilege priv,
                                   NavigableMap<Short, Long> hashes) {
        checkSerialize(priv);
        checkFastSerialize(priv, KVStorePrivilege::readKVStorePrivilege,
                           KVStorePrivilege::writeKVStorePrivilege);
        serialVersionChecker(priv, hashes)
            .reader(KVStorePrivilege::readKVStorePrivilege)
            .writer(KVStorePrivilege::writeKVStorePrivilege)
            .check();
    }

    private static PrivilegeCollection
        buildPrivsCollection(KVStorePrivilege... privs) {
        final Set<KVStorePrivilege> privsSet =
            new HashSet<KVStorePrivilege>(Arrays.asList(privs));

        return new ExecutionContext.PrivilegeCollection(privsSet);
    }

    private static void checkPrivImplication(
        String privName,
        PrivilegeCollection priv,
        Collection<? extends KVStorePrivilege> impliedPrivs) {

        for (KVStorePrivilege implied : impliedPrivs) {
            assertTrue(privName + " does not imply " + implied,
                       priv.implies(implied));
        }

        /* Should not imply other system privileges */
        final Set<? extends KVStorePrivilege> otherSysPrivs =
            complementarySet(SystemPrivilege.getAllSystemPrivileges(),
                             impliedPrivs);

        for (KVStorePrivilege notImplied : otherSysPrivs) {
            assertFalse(privName + " implies " + notImplied,
                        priv.implies(notImplied));
        }

        /* Should not imply other table privileges */
        final Set<? extends KVStorePrivilege> otherTablePrivs =
            complementarySet(TablePrivilege.getAllTablePrivileges(
                DUMMY_TABLE_NAMESPACE, DUMMY_TABLE_ID, DUMMY_TABLE_NAME),
                impliedPrivs);

        for (KVStorePrivilege notImplied : otherTablePrivs) {
            assertFalse(privName + " implies " + notImplied,
                        priv.implies(notImplied));
        }
    }

    /**
     * Returns the complementary set of collection B regarding to set A
     */
    private static Set<? extends KVStorePrivilege>
        complementarySet(Set<? extends KVStorePrivilege> setA,
                         Collection<? extends KVStorePrivilege> colB) {
        final Set<KVStorePrivilege> copyOfSetA =
            new HashSet<KVStorePrivilege>(setA);
        copyOfSetA.removeAll(colB);
        return copyOfSetA;
    }

    /**
     * An access checker which builds a user subject with only the specified
     * role.
     */
    class RoleAccessChecker extends AccessCheckerImpl {

        public RoleAccessChecker(RoleResolver resolver, Logger logger) {
            super(null, resolver, null, logger);
        }

        public RoleAccessChecker(RoleResolver resolver,
                                 Logger logger,
                                 int logPeriod,
                                 int faultsLimit) {
            super(null, resolver, null, logger, logPeriod, faultsLimit);
        }

        private volatile RoleInstance userRole = RoleInstance.PUBLIC;

        @Override
        public Subject identifyRequestor(AuthContext context) {
            if (context == null || context.getLoginToken() == null) {
                throw new AuthenticationRequiredException(
                    "Not authenticated", false /* isReturnSignal */);
            }

            return makeAuthenticatedSubject();
        }

        void updateRole(RoleInstance newRole) {
            this.userRole = newRole;
        }

        private Subject makeAuthenticatedSubject() {
            final Set<Principal> subjPrincs = new HashSet<Principal>();
            subjPrincs.add(KVStoreRolePrincipal.get(userRole.name()));
            final Set<Object> publicCreds = new HashSet<Object>();
            final Set<Object> privateCreds = new HashSet<Object>();
            return new Subject(true, subjPrincs, publicCreds, privateCreds);
        }
    }

    class RoleCheckContext implements OperationContext {
        private final RoleInstance role;

        private RoleCheckContext(final RoleInstance role) {
            this.role = role;
        }

        @Override
        public String describe() {
            return "Check privileges for role " + role.name();
        }

        @Override
        public List<? extends KVStorePrivilege> getRequiredPrivileges() {
            return new ArrayList<KVStorePrivilege>(role.getPrivileges());
        }
    }

    @SafeVarargs
    private static <E> Set<E> set(E... elements) {
        final Set<E> result = new HashSet<>();
        for (final E e : elements) {
            result.add(e);
        }
        return result;
    }
}
