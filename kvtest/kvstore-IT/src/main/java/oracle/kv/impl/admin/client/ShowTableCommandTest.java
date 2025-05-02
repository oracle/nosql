/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.impl.api.table.TableTestBase;
import oracle.nosql.common.json.JsonUtils;
import oracle.kv.util.shell.ShellCommandResult;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.ShellHelpException;

import org.junit.BeforeClass;
import org.junit.Test;

/* Test show table(s) command in admin CLI */
public class ShowTableCommandTest extends TableTestBase {

    private final CommandShell shell = new CommandShell(System.in, System.out);

    private final ShowCommand cmd = new ShowCommand();
    private final String showIndexes = "show indexes";
    private final String showIndexesNamespace = "show indexes -namespace %s";
    private final String showTableIndexes = "show indexes -table %s";
    private final String showTableIndex = "show index -name %s -table %s";

    @BeforeClass
    public static void staticSetUp() throws Exception {
        TableTestBase.staticSetUp();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());
    }

    @Test
    public void testShowTableIndexes() throws Exception {

        executeDdl("CREATE TABLE airport(id integer, name string, " +
                   "primary key(id))");
        executeDdl("CREATE INDEX idx_ap_name on airport(name)");
        executeDdl("CREATE TABLE airport.user(uid integer, primary key(uid))", true, true);
        executeDdl("CREATE NAMESPACE IF NOT EXISTS ns");
        executeDdl("CREATE TABLE user(id integer, name string, primary key(id))",
                   "ns");
        executeDdl("CREATE INDEX idx_user_name on user(name)", "ns");
        executeDdl("CREATE TABLE user.email(eid integer, primary key(eid))",
                   "ns", true, true);

        /* show tables */
        String ret = cmd.execute(new String[]{"show", "tables"}, shell);
        checkShowTableResult(ret, "airport", "airport.user",
                             "ns:user", "ns:user.email");

        /* show tables -namespace sysdefault */
        ret = cmd.execute(new String[]{"show",
                                       "tables",
                                       "-namespace",
                                       "sysdefault"},
                          shell);
        checkShowTableResult(ret, "airport", "airport.user",
                            "ns:user", "ns:user.email");

        /* show tables -namespace ns */
        ret = cmd.execute(new String[]{"show",
                                       "tables",
                                       "-namespace",
                                       "ns"},
                          shell);
        checkShowTableResult(ret, "user", "user.email");

        /*
         * namespace sysdefault
         * show tables
         * show table -name user
         * show table -name ns:user
         */
        shell.setNamespace("sysdefault");
        ret = cmd.execute(new String[]{"show", "tables"}, shell);
        checkShowTableResult(ret, "airport", "airport.user",
                             "ns:user", "ns:user.email");

        try {
            ret = cmd.execute(new String[]{"show", "table", "-name", "user"},
                              shell);
            fail("Table user should not exist");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Table user does not exist."));
        }

        ret = cmd.execute(new String[]{"show", "table", "-name", "ns:user"},
                          shell);
        assertTrue(ret.contains("\"name\" : \"user\""));

        ret = cmd.execute(new String[]{"show", "indexes"}, shell);
        assertTrue(ret.contains("Indexes on table airport"));
        assertTrue(ret.contains("idx_ap_name"));
        assertTrue(ret.contains("Indexes on table ns:user"));
        assertTrue(ret.contains("idx_user_name"));

        /*
         * namespace
         * show tables
         * show table -name user
         * show table -name ns:user
         * show indexes
         */
        shell.setNamespace(null);
        ret = cmd.execute(new String[]{"show", "tables"}, shell);
        checkShowTableResult(ret, "airport", "airport.user",
                             "ns:user", "ns:user.email");

        try {
            ret = cmd.execute(new String[]{"show", "table", "-name", "user"},
                              shell);
            fail("Table user should not exist");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Table user does not exist."));
        }

        ret = cmd.execute(new String[]{"show",
                                       "table",
                                       "-name",
                                       "ns:user"},
                          shell);
        assertTrue(ret.contains("\"name\" : \"user\""));

        ret = cmd.execute(new String[]{"show", "indexes"}, shell);
        assertTrue(ret.contains("Indexes on table airport"));
        assertTrue(ret.contains("idx_ap_name"));
        assertTrue(ret.contains("Indexes on table ns:user"));
        assertTrue(ret.contains("idx_user_name"));

        /*
         * namespace ns
         * show tables
         * show table -name user.email
         * show table -name airport
         * shod indexes
         */
        shell.setNamespace("ns");
        ret = cmd.execute(new String[]{"show", "tables"}, shell);
        checkShowTableResult(ret, "user", "user.email");

        ret = cmd.execute(new String[]{"show",
                                       "table",
                                       "-name",
                                       "user.email"},
                          shell);
        assertTrue(ret.contains("\"name\" : \"email\""));

        try {
            ret = cmd.execute(new String[]{"show", "table", "-name", "airport"},
                              shell);
            fail("Table ns:airport should not exist");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                        "Table ns:airport does not exist."));
        }

        ret = cmd.execute(new String[]{"show", "indexes"}, shell);
        assertFalse(ret.contains("idx_ap_name"));
        assertTrue(ret.contains("Indexes on table ns:user"));
        assertTrue(ret.contains("idx_user_name"));
    }

    /* Test show indexes of table in specific namespace */
    @Test
    public void testShowIndexesInNamespace() throws ShellException {

        final String[] tableDdls = new String[] {
            "create table if not exists t0(id integer, name string, " +
                                         "primary key(id))",
            "create namespace if not exists ns",
            "create table if not exists ns:t1(id integer, name string, " +
                                            "primary key(id))",
        };

        final String[] indexDdls = new String[] {
            "create index if not exists idx1t0 on t0(name)",
            "create index if not exists idx1nst1 on ns:t1(name)"
        };

        String ret;

        for (String ddl : tableDdls) {
            executeDdl(ddl);
        }

        for (String ddl : indexDdls) {
            executeDdl(ddl);
        }

        ret = runCommand(cmd, showIndexes, false);
        checkResultContains(ret, "t0", "idx1t0 (name)",
                             "ns:t1", "idx1nst1 (name)");

        ret = runCommand(cmd, showIndexes, true);

        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t0\"",
                            "\"name\":\"idx1t0\"",
                            "\"namespace\":\"ns\"",
                            "\"tableName\":\"t1\"",
                            "\"name\":\"idx1nst1\"");

        ret = runCommand(cmd, String.format(showIndexesNamespace, "ns"), false);
        checkResultContains(ret, "ns:t1", "idx1nst1 (name)");

        ret = runCommand(cmd, String.format(showIndexesNamespace, "ns"), true);
        checkResultContains(deBeautify(ret),
                            "\"namespace\":\"ns\"",
                            "\"tableName\":\"t1\"",
                            "\"name\":\"idx1nst1\"");
    }

    /* Test show indexes of child tables */
    @Test
    public void testShowIndexesChildTable() throws ShellException {

        final String tableDdl =
                "create table if not exists t1(id integer, name string, " +
                "primary key(id))";

        final String[] childDdls = new String[] {
            "create table if not exists t1.c1(cid integer, name string, " +
                                             "primary key(cid))",
            "create table if not exists t1.c1.g1(gid integer, name string, " +
                                                "primary key(gid))",
        };

        final String[] t1IndexDdls = new String[] {
            "create index if not exists idx1t1 on t1(name)",
            "create index if not exists idx2t1 on t1(name, id)"
        };

        final String[] c1IndexDdls = new String[] {
           "create index if not exists idx1c1 on t1.c1(name)",
           "create index if not exists idx2c1 on t1.c1(name, cid)"
        };

        final String[] g1IndexDdls = new String[] {
            "create index if not exists idx1g1 on t1.c1.g1(name)"
        };

        String ret;

        executeDdl(tableDdl);
        for (String ddl : childDdls) {
            executeDdl(ddl, true, true);
        }

        /* Create indexes on t1.c1.g1 */
        for (String ddl : g1IndexDdls) {
            executeDdl(ddl);
        }

        /*
         * Show all indexes: show indexes [-json]
         *
         * return index idx1g1.
         */
        ret = runCommand(cmd, showIndexes, false);
        checkResultContains(ret, "t1.c1.g1", "idx1g1 (name)");

        ret = runCommand(cmd, showIndexes, true);
        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t1.c1.g1\"",
                            "\"name\":\"idx1g1\"");

        /*
         * Show all indexes of the table: show indexes -table t1.c1.g1 [-json]
         *
         * return index idx1g1.
         */
        String line = String.format(showTableIndexes, "t1.c1.g1");
        ret = runCommand(cmd, line, false);
        checkResultContains(ret, "t1.c1.g1", "idx1g1 (name)");

        ret = runCommand(cmd, line, true);
        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t1.c1.g1\"",
                            "\"name\":\"idx1g1\"");

        /*
         * Show single index: show index -table t1.c1.g1 -name idx1g1 [-json]
         */
        line = String.format(showTableIndex, "idx1g1", "t1.c1.g1");
        ret = runCommand(cmd, line, false);
        checkResultContains(ret, "idx1g1 (name)");

        ret = runCommand(cmd, line, true);
        checkResultContains(deBeautify(ret),
                            "\"name\":\"idx1g1\"");

        /* Create 2 indexes on t1.c1 */
        for (String ddl : c1IndexDdls) {
            executeDdl(ddl);
        }

        /*
         * Show all indexes: show indexes [-json]
         *
         * return 3 indexes: idx1c1, idx2c1, idx1g1.
         */
        ret = runCommand(cmd, showIndexes, false);
        checkResultContains(ret, "t1.c1", "idx1c1 (name)", "idx2c1 (name, cid)",
                            "t1.c1.g1", "idx1g1 (name)");

        ret = runCommand(cmd, showIndexes, true);
        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t1.c1\"",
                            "\"name\":\"idx1c1\"",
                            "\"name\":\"idx2c1\"",
                            "\"tableName\":\"t1.c1.g1\"",
                            "\"name\":\"idx1g1\"");

        /*
         * Show all indexes of the table: show indexes -table t1.c1 [-json]
         *
         * return 2 indexes: idx1c1, idx2c1
         */
        line = String.format(showTableIndexes, "t1.c1");
        ret = runCommand(cmd, line, false);
        checkResultContains(ret, "t1.c1", "idx1c1 (name)", "idx2c1 (name, cid)");

        ret = runCommand(cmd, line, true);
        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t1.c1\"",
                            "\"name\":\"idx1c1\"",
                            "\"name\":\"idx2c1\"");

        /* Create 2 indexes on t1 */
        for (String ddl : t1IndexDdls) {
            executeDdl(ddl);
        }

        /*
         * Show all indexes: show indexes [-json]
         *
         * return 5 indexes: idx1t1, idx2t1, idx1c1, idx2c1, idx1g1.
         */
        ret = runCommand(cmd, showIndexes, false);
        checkResultContains(ret, "t1", "idx1t1 (name)", "idx2t1 (name, id)",
                           "t1.c1", "idx1c1 (name)", "idx2c1 (name, cid)",
                           "t1.c1.g1", "idx1g1 (name)");

        ret = runCommand(cmd, showIndexes, true);
        checkResultContains(deBeautify(ret),
                            "\"tableName\":\"t1\",",
                            "\"name\":\"idx1t1\"",
                            "\"tableName\":\"t1.c1\"",
                            "\"name\":\"idx1c1\"",
                            "\"name\":\"idx2c1\"",
                            "\"tableName\":\"t1.c1.g1\"",
                            "\"name\":\"idx1g1\"");
    }

    @Test
    public void testShowTableWithChildTables() throws Exception {

        executeDdl("CREATE TABLE airport(id integer, name string, " +
                "primary key(id))");
        executeDdl("CREATE INDEX idx_ap_name on airport(name)");
        executeDdl("CREATE TABLE airport.user(uid integer, primary key(uid))",
                true, true);
        executeDdl("CREATE NAMESPACE IF NOT EXISTS ns");
        executeDdl("CREATE TABLE user(id integer, name string, primary key(id))",
                "ns");

        /* show tables */
        String ret = cmd.execute(new String[] { "show", "tables" }, shell);
        checkShowTableResult(ret, "airport", "airport.user",
                "ns:user");

        /* show tables -namespace sysdefault */
        ret = cmd.execute(new String[] { "show",
                "tables",
                "-namespace",
                "sysdefault" },
                shell);
        checkShowTableResult(ret, "airport", "airport.user",
                "ns:user");

        /* show tables expecting a ShellHelpException */
        try {
            ret = cmd.execute(new String[] { "show",
                    "tables", "help" },
                    shell);
            fail("A ShellHelpException was expected");
        } catch (ShellHelpException e) {
            assertTrue(e.getMessage().contains("Usage: show tables"));
        }

        ShellCommandResult shelResultJsonO = cmd.executeJsonOutput(new String[] { "show",
                "tables",
                "-parent",
                "airport"
        },
                shell);
        checkResultContains(deBeautify(shelResultJsonO.convertToJson()),
                "\"tables\":[{\"name\":\"airport.user\"");

        /* block for verbose mode */
        shell.setVerbose(true);
        shelResultJsonO = cmd.executeJsonOutput(new String[] { "show",
                "tables",
                "-parent",
                "airport"
        },
                shell);
        checkResultContains(deBeautify(shelResultJsonO.convertToJson()),
                "\"tables\":[{\"name\":\"airport.user\"");

        ret = cmd.execute(new String[] { "show",
                "tables",
                "-parent",
                "airport",
                "-namespace",
                "sysdefault" },
                shell);
        checkResultContains(ret, "airport.user");
    }

    private String runCommand(ShowCommand showCmd,
                              String cmdLine,
                              boolean isJsonOut)
        throws ShellException {

        String[] args = cmdLine.split(" ");
        if (isJsonOut) {
            ShellCommandResult out = showCmd.executeJsonOutput(args, shell);
            return out.convertToJson();
        }
        return showCmd.execute(args, shell);
    }

    private void checkShowTableResult(String result, String... expTables) {
        String[] lines = result.split("\n");
        int i = 0;
        for (String line : lines) {
            line = line.trim();
            if (line.length() == 0 ||
                line.startsWith("Tables in") ||
                line.startsWith("SYS$")) {
                continue;
            }
            assertEquals(expTables[i++], line);
        }
    }

    private void checkResultContains(String result, String... strs) {
        for (String s : strs) {
            assertTrue(result.contains(s));
        }
    }

    /*
     * turns pretty-printed JSON into not pretty-printed
     */
    private static String deBeautify(String json) {
        return JsonUtils.toJsonString(JsonUtils.parseJsonObject(json), false);
    }
}
