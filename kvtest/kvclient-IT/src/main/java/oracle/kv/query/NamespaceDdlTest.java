/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Set;

import oracle.kv.StaticClientTestBase;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.kv.table.Index;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.util.DDLTestUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class NamespaceDdlTest extends StaticClientTestBase {

    @BeforeClass
    public static void mySetUp() throws Exception {
        staticSetUp("NamespaceDdlTestStore", SECURITY_ENABLE);
    }

    @AfterClass
    public static void myTearDown() throws Exception {
        staticTearDown();
    }

    @Override
    public void tearDown() throws Exception {
        final Set<String> namespaces = store.getTableAPI().listNamespaces();
        for (String name : namespaces) {
            if (name != null && !name.toLowerCase().startsWith("sys")) {
                try {
                    execStmt("DROP NAMESPACE " + name + " CASCADE");
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
    }

    /* Run the test using the old RN APIs to get namespaces. */
    @Test
    public void testCreateNsLegacy() throws Exception {
        runTestCreateNs(false);
    }

    /* Run the test using the table MD system table to get namespaces. */
    @Test
    public void testCreateNs() throws Exception {
        runTestCreateNs(true);
    }

    private void runTestCreateNs(boolean useMDSysTable) throws Exception {

        /* syntax test: positive tests */
        final int MAX_NAMESPACE_LENGTH = 128;
        final StringBuilder buf = new StringBuilder("Ns_");
        for (int i = 0; i < MAX_NAMESPACE_LENGTH - 3; i++) {
            buf.append("a");
        }
        String longNsName = buf.toString();
        assertTrue(longNsName.length() == MAX_NAMESPACE_LENGTH);
        final String[] validNames = {
            "Ns001", "Ns_002", "Ns.N003", longNsName,
        };
        for (String name : validNames) {
            execStmt("CREATE NAMESPACE " + name);
        }
        final TableAPIImpl tableAPI = (TableAPIImpl)store.getTableAPI();
        tableAPI.setEnableTableMDSysTable(useMDSysTable);
        if (useMDSysTable) {
            waitForTableMDSysTable(tableAPI);
        }
        Set<String> namespaces = store.getTableAPI().listNamespaces();
        assertNotNull(namespaces);
        for (String name : validNames) {
            assertTrue(namespaces.contains(name));
        }

        /* syntax test: negative tests */
        final String[] invalidNames = {
            "", " ", "9Ns", "Ns.003", "Ns$name", "Ns:name", "Ns@name",
            "Ns-name", "_NsName", "sysname",
            TableAPI.SYSDEFAULT_NAMESPACE_NAME,
        };
        for (String name : invalidNames) {
            execStmtErr("CREATE NAMESPACE " + name,
                IllegalArgumentException.class);
        }

        /* name exceeds MAX_NAMESPACE_LENGTH */
        buf.append("Z");
        longNsName = buf.toString();
        assertTrue(longNsName.length() > MAX_NAMESPACE_LENGTH);
        execStmtErr("CREATE NAMESPACE " + longNsName,
            IllegalArgumentException.class,
            "Namespaces must be less than or equal to " +
            MAX_NAMESPACE_LENGTH + " characters");

        namespaces = store.getTableAPI().listNamespaces();
        assertNotNull(namespaces);
        for (String name : invalidNames) {
            if (name.equals(TableAPI.SYSDEFAULT_NAMESPACE_NAME))
                assertTrue(namespaces.contains(name));
            else
                assertFalse(namespaces.contains(name));
        }
        assertFalse(namespaces.contains(longNsName));

        /* "CREATE NAMESPACE <IF NOT EXISTS>" statement */
        execStmt("CREATE NAMESPACE Ns003");
        execStmtErr("CREATE NAMESPACE nS003", IllegalArgumentException.class);
        execStmt("CREATE NAMESPACE IF NOT EXISTS Ns003");
        execStmt("CREATE NAMESPACE IF NOT EXISTS NS003");
    }

    @Test
    public void testDropNs() throws Exception {

        /* DROP NAMESPACE on an empty namespace */
        execStmt("CREATE NAMESPACE Ns001");
        execStmt("DROP NAMESPACE Ns001");

        /* "DROP NAMESPACE <IF EXISTS>" statement */
        execStmtErr("DROP NAMESPACE nS001", IllegalArgumentException.class);
        execStmt("DROP NAMESPACE IF EXISTS Ns001");
        execStmt("DROP NAMESPACE IF EXISTS NS001");

        /* DROP NAMESPACE on an non-empty namespace */
        execStmt("CREATE NAMESPACE Ns002");
        execStmt("CREATE TABLE Ns002:T1 " +
            "(id1 string, c2 integer, primary key(id1))");
        execStmtErr("DROP NAMESPACE Ns002", IllegalArgumentException.class);
        execStmt("DROP TABLE Ns002:T1");
        execStmt("DROP NAMESPACE Ns002");

        /**
         * DROP NAMESPACE ... CASCADE on an non-empty namespace that contains
         * parent-child tables and indexes
         */
        execStmt("CREATE NAMESPACE Ns003");
        execStmt("CREATE TABLE Ns003:T1 " +
            "(id1 string, c2 integer, primary key(id1))");
        execStmt("CREATE TABLE Ns003:P1 " +
            "(pid1 string, c2 integer, primary key(pid1))");
        execStmt("CREATE TABLE Ns003:P1.C1 " +
            "(cid1 string, c3 integer, primary key(cid1))");
        execStmt("CREATE TABLE Ns003:P1.C1.D1 " +
            "(did1 string, c4 integer, primary key(did1))");
        execStmt("CREATE INDEX Idx1 ON Ns003:T1(c2)");
        execStmt("CREATE INDEX Idx2 ON Ns003:P1(c2)");
        execStmt("CREATE INDEX Idx3 ON Ns003:P1.C1(c3)");
        execStmt("CREATE INDEX Idx4 ON Ns003:P1.C1.D1(c4)");
        execStmtErr("DROP NAMESPACE Ns003", IllegalArgumentException.class);
        execStmt("DROP NAMESPACE Ns003 CASCADE");

        /* "sysXXX" namespaces cannot be dropped */
        final String[] sysNsNames = {"sysdefault", "SYSdefault", "sys001"};
        for (String name : sysNsNames) {
            execStmtErr("DROP NAMESPACE " + name,
                IllegalArgumentException.class);
        }

        /* drop a namespace that exceeds MAX_NAMESPACE_LENGTH characters */
        final int MAX_NAMESPACE_LENGTH = 128;
        final StringBuilder buf = new StringBuilder("Ns_");
        for (int i = 0; i < MAX_NAMESPACE_LENGTH - 3; i++) {
            buf.append("b");
        }
        buf.append("Z");
        final String longNsName = buf.toString();
        assertTrue(longNsName.length() > MAX_NAMESPACE_LENGTH);
        execStmtErr("DROP NAMESPACE " + longNsName,
            IllegalArgumentException.class,
            "Namespaces must be less than or equal to " +
            MAX_NAMESPACE_LENGTH + " characters");
    }

    /**
     * Test if multiple tables and indexes with the same name could be created
     * in different namespaces.
     */
    @Test
    public void testTableIndexWithSameName() throws Exception {

        execStmt("CREATE NAMESPACE Ns001");
        execStmt("CREATE NAMESPACE Ns002");
        final Set<String> namespaces = store.getTableAPI().listNamespaces();
        assertNotNull(namespaces);
        assertTrue(namespaces.contains("Ns001"));
        assertTrue(namespaces.contains("Ns002"));

        /* create tables with the same name on different namespaces */
        /* create table T1 and T2 in namespace Ns001 */
        execStmt("CREATE TABLE Ns001:T1 " +
            "(id1 string, c2 integer, c3 float, primary key(id1))");
        execStmt("CREATE TABLE ns001:T2 " +
            "(id1 string, c2 integer, c3 float, primary key(id1))");
        /* create table T1 in namespace Ns002 */
        execStmt("CREATE TABLE Ns002:T1 " +
            "(id1 string, c2 integer, c3 float, primary key(id1))");
        /* create table T1 in namespace sysdefault */
        execStmt("CREATE TABLE T1 " +
            "(id1 string, c2 integer, c3 float, primary key(id1))");

        /* create indexes with the same name on different namespaces */
        execStmt("CREATE INDEX T1 on Ns001:T1(c2)");
        /* create indexes with the same name on different tables */
        execStmt("CREATE INDEX Idx1 on Ns001:T1(c3)");
        execStmt("CREATE INDEX Idx1 on ns001:T2(c2)");
        execStmt("CREATE INDEX Idx1 on Ns002:T1(c2)");
        execStmt("CREATE INDEX Idx1 on T1(c2)");
        execStmt("CREATE INDEX T1 on T1(id1, c2)");

        final Table ns1T1 = store.getTableAPI().getTable("Ns001", "T1");
        assertNotNull(ns1T1);
        final Table ns1T2 = store.getTableAPI().getTable("Ns001", "T2");
        assertNotNull(ns1T2);
        final Table ns2T1 = store.getTableAPI().getTable("Ns002", "T1");
        assertNotNull(ns2T1);
        final Table T1 = store.getTableAPI().getTable("T1");
        assertNotNull(T1);

        /* check indexes on table Ns001:T1 */
        final Index ns1T1T1 = ns1T1.getIndex("T1");
        assertNotNull(ns1T1T1);
        final Index ns1T1Idx1 = ns1T1.getIndex("Idx1");
        assertNotNull(ns1T1Idx1);

        /* check indexes on table Ns001:T2 */
        final Index ns1T2Idx1 = ns1T2.getIndex("Idx1");
        assertNotNull(ns1T2Idx1);
        /* check indexes on table Ns002:T1 */
        final Index ns2T1Idx1 = ns2T1.getIndex("Idx1");
        assertNotNull(ns2T1Idx1);
        /* check indexes on table T1 */
        final Index t1Idx1 = T1.getIndex("Idx1");
        assertNotNull(t1Idx1);
        final Index t1T1 = T1.getIndex("T1");
        assertNotNull(t1T1);
    }

    private static void execStmt(String statement) throws Exception {
        DDLTestUtils.execStatement(store, statement);
    }

    private static void execStmtErr(String statement,
                                    Class<? extends Exception> exception)
        throws Exception {
        execStmtErr(statement, exception, null);
    }

    private static void execStmtErr(String statement,
                                    Class<? extends Exception> exception,
                                    String expectedErrMsg)
        throws Exception {
            try {
                DDLTestUtils.execStatement(store, statement);
                fail("Statement should have failed to compile or execute");
            } catch (Exception e) {
                if (!exception.isInstance(e)) {
                    e.printStackTrace();
                    fail("Unexpectd exception: " + e);
                }
                if (expectedErrMsg != null &&
                    (e.getMessage() == null ||
                    !e.getMessage().contains(expectedErrMsg))) {
                    fail("Unexpected error message: " + e);
                }
            }
    }

}
