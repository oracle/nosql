/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin.client;

import static oracle.kv.impl.util.FileUtils.computeSha256Hash;
import static oracle.kv.impl.util.FileUtils.getFormattedFileModTime;
import static oracle.kv.util.TestUtils.checkException;
import static oracle.nosql.common.json.JsonUtils.createObjectNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.kv.TestBase;
import oracle.kv.util.CreateStore;
import oracle.kv.util.shell.Shell.HelpCommand;
import oracle.kv.util.shell.ShellUsageException;

import oracle.nosql.common.json.JsonNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import org.junit.Test;

/* Test show tls-credentials command in admin CLI */
public class ShowTlsCommandTest extends TestBase {

    private final CommandShell shell = new CommandShell(System.in, System.out);
    private final ShowCommand cmd = new ShowCommand();
    private CreateStore createStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        if (createStore != null) {
            createStore.shutdown();
        }
        super.tearDown();
    }

    @Test
    public void testUnexpectedArgs() {
        checkException(() -> execute("show tls-credentials extra-arg"),
                       ShellUsageException.class,
                       "Incorrect number of arguments for command:" +
                       " tls-credentials");
        checkException(() -> executeJson("show tls-credentials extra-arg"),
                       ShellUsageException.class,
                       "Incorrect number of arguments for command:" +
                       " tls-credentials");
    }

    @Test
    public void testHelp() throws Exception {
        final String helpMessage = new HelpCommand().execute(
            new String[] { "help", "show", "tls-credentials" }, shell);
        assertEquals("Usage: show tls-credentials [-json|-json-v1]\n" +
                     "\tShows information about the TLS credentials" +
                     " installed, and updates\n" +
                     "waiting to be installed, on all SNAs.",
                     helpMessage);
    }

    @Test
    public void testNonSecure() throws Exception {
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      1, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      1, /* numPartitions */
                                      1 /* capacity */);
        createStore.start();
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        checkException(() -> execute("show tls-credentials"),
                       ShellUsageException.class,
                       "Cannot get TLS credentials for a non-secure store");
        checkException(() -> executeJson("show tls-credentials"),
                       ShellUsageException.class,
                       "Cannot get TLS credentials for a non-secure store");

    }

    @Test
    public void testSecure() throws Exception {

        /* Create a secure store with RF=1 and three shards */
        createStore = new CreateStore(kvstoreName,
                                      5000, /* startPort */
                                      3, /* numStorageNodes */
                                      1, /* replicationFactor */
                                      3, /* numPartitions */
                                      1 /* capacity */);
        createStore.setSecure(true);
        createStore.start();
        shell.connectAdmin(createStore.getHostname(),
                           createStore.getRegistryPort(),
                           createStore.getDefaultUserName(),
                           createStore.getDefaultUserLoginPath());

        final File ks = createStore.getKeyStore();
        final String ksModTime = getFormattedFileModTime(ks);
        final String ksHash = computeSha256Hash(ks);

        /*
         * Don't use CreateStore.getTrustStore: that returns the client
         * truststore, not the store one
         */
        final File ts =
            new File(createStore.getRootDir(), "security/store.trust");
        final String tsModTime = getFormattedFileModTime(ts);
        final String tsHash = computeSha256Hash(ts);

        /* Test with all SNs up */
        String expected =
            "Installed credentials status: consistent\n" +
            "Pending updates status: none\n" +
            "SN sn1:\n" +
            "  installed:\n" +
            "    keystore: file=store.keys" +
            " modTime=" + ksModTime + " hash=" + ksHash + "\n" +
            "    truststore: file=store.trust" +
            " modTime=" + tsModTime + " hash=" + tsHash + "\n" +
            "SN sn2:\n" +
            "  installed:\n" +
            "    keystore: file=store.keys" +
            " modTime=" + ksModTime + " hash=" + ksHash + "\n" +
            "    truststore: file=store.trust" +
            " modTime=" + tsModTime + " hash=" + tsHash + "\n" +
            "SN sn3:\n" +
            "  installed:\n" +
            "    keystore: file=store.keys" +
            " modTime=" + ksModTime + " hash=" + ksHash + "\n" +
            "    truststore: file=store.trust" +
            " modTime=" + tsModTime + " hash=" + tsHash + "\n";
        String ret = execute("show tls-credentials");
        assertEquals(expected, ret);

        ret = executeJson("show tls-credentials");
        ObjectNode on = JsonUtils.parseJsonObject(ret);
        ObjectNode expectedOn = obj(
            "operation", "show tls-credentials",
            "returnCode", 5000,
            "description", "Operation ends successfully",
            "returnValue",
            obj("installedCredentialsStatus", "consistent",
                "pendingUpdatesStatus", "none",
                "sns",
                obj("sn1",
                    obj("installedCredentials",
                        obj("keystore",
                            fileNode("store.keys", ksModTime, ksHash),
                            "truststore",
                            fileNode("store.trust", tsModTime, tsHash)),
                        "pendingUpdates",
                        obj()),
                     "sn2",
                    obj("installedCredentials",
                        obj("keystore",
                            fileNode("store.keys", ksModTime, ksHash),
                            "truststore",
                            fileNode("store.trust", tsModTime, tsHash)),
                        "pendingUpdates",
                        obj()),
                    "sn3",
                    obj("installedCredentials",
                        obj("keystore",
                            fileNode("store.keys", ksModTime, ksHash),
                            "truststore",
                            fileNode("store.trust", tsModTime, tsHash)),
                        "pendingUpdates",
                        obj()))));

        assertEquals(expectedOn.toPrettyString(),
                     on.toPrettyString());

        /* Test with sn2 offline and with updates */
        createStore.shutdownSNA(1, false /* force */);

        final File updatesDir =
            new File(createStore.getRootDir(), "security/updates");
        updatesDir.mkdirs();
        final File ksUpdate = new File(updatesDir, "store.keys");
        ksUpdate.createNewFile();
        try (final FileOutputStream out = new FileOutputStream(ksUpdate)) {
            out.write(1);
        }
        final String ksUpdateModTime = getFormattedFileModTime(ksUpdate);
        final String ksUpdateHash = computeSha256Hash(ksUpdate);

        final File tsUpdate = new File(updatesDir, "store.trust");
        tsUpdate.createNewFile();
        try (final FileOutputStream out = new FileOutputStream(tsUpdate)) {
            out.write(1);
        }
        final String tsUpdateModTime = getFormattedFileModTime(tsUpdate);
        final String tsUpdateHash = computeSha256Hash(tsUpdate);

        expected =
            "Installed credentials status: maybe-consistent\n" +
            "Pending updates status: maybe-consistent\n" +
            "SN sn1:\n" +
            "  installed:\n" +
            "    keystore: file=store.keys" +
            " modTime=" + ksModTime + " hash=" + ksHash + "\n" +
            "    truststore: file=store.trust" +
            " modTime=" + tsModTime + " hash=" + tsHash + "\n" +
            "  updates:\n" +
            "    keystore: file=updates/store.keys" +
            " modTime=" + ksUpdateModTime + " hash=" + ksUpdateHash + "\n" +
            "    truststore: file=updates/store.trust" +
            " modTime=" + tsUpdateModTime + " hash=" + tsUpdateHash + "\n" +
            "SN sn2:\n" +
            "  exception: (?<e2>[^\"]*)\n" +
            "SN sn3:\n" +
            "  installed:\n" +
            "    keystore: file=store.keys" +
            " modTime=" + ksModTime + " hash=" + ksHash + "\n" +
            "    truststore: file=store.trust" +
            " modTime=" + tsModTime + " hash=" + tsHash + "\n" +
            "  updates:\n" +
            "    keystore: file=updates/store.keys" +
            " modTime=" + ksUpdateModTime + " hash=" + ksUpdateHash + "\n" +
            "    truststore: file=updates/store.trust" +
            " modTime=" + tsUpdateModTime + " hash=" + tsUpdateHash + "\n";
        ret = execute("show tls-credentials");
        Matcher matcher = assertMatches(expected, ret);

        String e2 = matcher.group("e2");
        String exceptionPattern =
            "(?s)java[.]rmi[.]" +
            "(ConnectException: Unable to connect.*" +
            "|NoSuchObjectException: no such object.*)";
        assertMatches(exceptionPattern, e2);

        ret = executeJson("show tls-credentials");
        on = JsonUtils.parseJsonObject(ret);
        String exception = on.getObject("returnValue")
            .getObject("sns")
            .getObject("sn2")
            .get("exception")
            .asText();
        assertMatches(exceptionPattern, exception);
        expectedOn = obj(
            "operation", "show tls-credentials",
            "returnCode", 5000,
            "description", "Operation ends successfully",
            "returnValue",
            obj("installedCredentialsStatus", "maybe-consistent",
                "pendingUpdatesStatus", "maybe-consistent",
                "sns",
                obj("sn1",
                    obj("installedCredentials",
                        obj("keystore",
                            fileNode("store.keys", ksModTime, ksHash),
                            "truststore",
                            fileNode("store.trust", tsModTime, tsHash)),
                        "pendingUpdates",
                        obj("keystore",
                            fileNode("updates/store.keys",
                                     ksUpdateModTime, ksUpdateHash),
                            "truststore",
                            fileNode("updates/store.trust",
                                     tsUpdateModTime, tsUpdateHash))),
                     "sn2",
                    obj("exception", exception),
                    "sn3",
                    obj("installedCredentials",
                        obj("keystore",
                            fileNode("store.keys", ksModTime, ksHash),
                            "truststore",
                            fileNode("store.trust", tsModTime, tsHash)),
                        "pendingUpdates",
                        obj("keystore",
                            fileNode("updates/store.keys",
                                     ksUpdateModTime, ksUpdateHash),
                            "truststore",
                            fileNode("updates/store.trust",
                                     tsUpdateModTime, tsUpdateHash))))));
        assertEquals(expectedOn.toPrettyString(),
                     on.toPrettyString());
    }

    private String execute(String command) throws Exception {
        return cmd.execute(command.split(" "), shell);
    }

    private String executeJson(String command) throws Exception {
        return cmd.executeJsonOutput(command.split(" "), shell)
            .convertToJson();
    }

    private static Matcher assertMatches(String expected, String string) {
        final Pattern pattern = Pattern.compile(expected);
        final Matcher matcher = pattern.matcher(string);
        assertTrue("Expected: " + expected + "\nFound: " + string,
                   matcher.matches());
        return matcher;
    }

    private static ObjectNode fileNode(String file,
                                       String modTime,
                                       String hash) {
        return createObjectNode()
            .put("file", file)
            .put("modTime", modTime)
            .put("hash", hash);
    }

    private static ObjectNode obj(Object... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Odd number of args");
        }
        final ObjectNode on = createObjectNode();
        for (int i = 0; i < args.length; i += 2) {
            putMisc(on, (String) args[i], args[i+1]);
        }
        return on;
    }

    private static void putMisc(ObjectNode on, String key, Object val) {
        if (val instanceof String) {
            on.put(key, (String) val);
        } else if (val instanceof Integer) {
            on.put(key, (Integer) val);
        } else if (val instanceof Boolean) {
            on.put(key, (Boolean) val);
        } else if (val instanceof JsonNode) {
            on.put(key, (JsonNode) val);
        } else if (val == null) {
            on.putNull(key);
        } else {
            throw new IllegalArgumentException(
                "Bad value for key " + key + ": " + val);
        }
    }
}
