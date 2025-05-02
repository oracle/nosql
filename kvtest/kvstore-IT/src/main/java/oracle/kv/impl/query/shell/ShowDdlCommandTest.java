/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.query.shell;

import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.query.shell.OnqlShell.ShowCommand;
import oracle.kv.impl.xregion.XRegionTestBase;
import oracle.kv.table.TableAPI;
import oracle.kv.util.shell.CommonShell.NamespaceCommand;
import oracle.kv.util.shell.Shell;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class ShowDdlCommandTest extends ShellTestBase {

    private final static String SHOW = "show";
    private final static String DDL = "ddl";

    private final static String TABLE_DDL =
        "create table %s(id integer, " +
                        "name string, " +
                        "info json, " +
                        "primary key(id))";

    private final static String IDX1_FIELD = "name";
    private final static String IDX1_DDL =
        "create index idx1 on %s(" + IDX1_FIELD +")";

    private final static String IDX2_FIELDS =
        "info.age as Integer,info.address[].zipcode as String";
    private final static String IDX2_DDL =
        "create index idx2 on %s(" + IDX2_FIELDS + ")";

    private final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS ";
    private final static String CREATE_IDX1 =
        "CREATE INDEX IF NOT EXISTS idx1 ON ";
    private final static String CREATE_IDX2 =
        "CREATE INDEX IF NOT EXISTS idx2 ON ";

    private final Shell sqlsh = getTestShell();
    private final ShowCommand showCmd = new ShowCommand();

    @Rule
    public TestName name = new TestName();

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        String test = name.getMethodName();
        if (test.equals("testNamespace")) {
            executeDDL("drop namespace if exists n1");
        } else if (test.equals("testMRTable")) {
            executeDDL("drop region phx");
        }
    }

    @Test
    public void testBasic() {
        String tname = "users";

        /* create table */
        String ddl = String.format(TABLE_DDL, tname);
        executeDDL(ddl);

        /* show ddl users */
        String[] args = {SHOW, DDL, tname};
        runCommand(sqlsh, showCmd, args, CREATE_TABLE + tname);

        /* create 2 indexes */
        ddl = String.format(IDX1_DDL, tname);
        executeDDL(ddl);
        ddl = String.format(IDX2_DDL, tname);
        executeDDL(ddl);

        /* show ddl users */
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + tname,
                   CREATE_IDX1 + tname, IDX1_FIELD,
                   CREATE_IDX2 + tname, IDX2_FIELDS);

        /* create child table */
        String ctable = tname + ".logins";
        ddl = "create table " + ctable + "(from timestamp(3), " +
                                          "to timestamp(3), " +
                                          "primary key(from))";
        executeDDL(ddl);

        /* show ddl users.logins */
        args = new String[]{SHOW, DDL, ctable};
        runCommand(sqlsh, showCmd, args, CREATE_TABLE + ctable);

        /* show ddl */
        args = new String[]{SHOW, DDL};
        runWithIncorrectNumberOfArguments(sqlsh, showCmd, args);

        /* show ddl table_not_existed */
        args = new String[]{SHOW, DDL, "table_not_existed"};
        runCommand(sqlsh, showCmd, args, false,
                   "Table 'table_not_existed' not found");
    }

    @Test
    public void testNamespace() {
        String namespace = "n1";
        String tname = "users";
        String qualName = NameUtils.makeQualifiedName(namespace, tname);

        NamespaceCommand nscmd = new NamespaceCommand();
        String ddl = "create namespace if not exists " + namespace;
        executeDDL(ddl);

        /* create table n1:users and 2 indexes */
        ddl = String.format(TABLE_DDL, qualName);
        executeDDL(ddl);
        ddl = String.format(IDX1_DDL, qualName);
        executeDDL(ddl);
        ddl = String.format(IDX2_DDL, qualName);
        executeDDL(ddl);

        /* show ddl n1:users */
        String[] args = new String[]{SHOW, DDL, qualName};
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + qualName,
                   CREATE_IDX1 + qualName, IDX1_FIELD,
                   CREATE_IDX2 + qualName, IDX2_FIELDS);

        /* namespace n1: set current namespace to n1 */
        args = new String[]{"namespace", namespace};
        runCommand(sqlsh, nscmd, args);

        /* show ddl users */
        args = new String[]{SHOW, DDL, tname};
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + qualName,
                   CREATE_IDX1 + qualName, IDX1_FIELD,
                   CREATE_IDX2 + qualName, IDX2_FIELDS);

        /* show ddl n1:users */
        args = new String[]{SHOW, DDL, qualName};
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + qualName,
                   CREATE_IDX1 + qualName, IDX1_FIELD,
                   CREATE_IDX2 + qualName, IDX2_FIELDS);

        /* namespace sysdefault: set namespace to default */
        args = new String[] {"namespace", TableAPI.SYSDEFAULT_NAMESPACE_NAME};
        runCommand(sqlsh, nscmd, args);

        /* show ddl n1:users */
        args = new String[]{SHOW, DDL, qualName};
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + qualName,
                   CREATE_IDX1 + qualName, IDX1_FIELD,
                   CREATE_IDX2 + qualName, IDX2_FIELDS);
    }

    @Test
    public void testMRTable() {
        /* This test requires the MRT system tables */
        XRegionTestBase.waitForMRSystemTables(tableAPI);

        String tname = "users";

        String ddl = "set local region iad";
        executeDDL(ddl);

        ddl = "create region phx";
        executeDDL(ddl);

        /* create table users (...) in regions iad, phx*/
        ddl = String.format(TABLE_DDL, tname) + " in regions iad, phx";
        executeDDL(ddl);
        ddl = String.format(IDX1_DDL, tname);
        executeDDL(ddl);
        ddl = String.format(IDX2_DDL, tname);
        executeDDL(ddl);

        /* show ddl users */
        String[] args = new String[]{SHOW, DDL, tname};
        runCommand(sqlsh, showCmd, args,
                   CREATE_TABLE + tname, "IN REGIONS iad, phx",
                   CREATE_IDX1 + tname, IDX1_FIELD,
                   CREATE_IDX2 + tname, IDX2_FIELDS);
    }
}
