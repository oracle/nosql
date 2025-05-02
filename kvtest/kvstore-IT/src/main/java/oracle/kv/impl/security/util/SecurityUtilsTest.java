/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.util;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static oracle.kv.impl.security.util.SecurityUtils.InstallMatchingResult.INSTALLED_UPDATE;
import static oracle.kv.impl.security.util.SecurityUtils.InstallMatchingResult.NO_UPDATE_FOUND;
import static oracle.kv.impl.security.util.SecurityUtils.InstallMatchingResult.UPDATE_ALREADY_INSTALLED;
import static oracle.kv.impl.security.util.SecurityUtils.addTruststoreUpdates;
import static oracle.kv.impl.security.util.SecurityUtils.deleteMatching;
import static oracle.kv.impl.security.util.SecurityUtils.getUpdateAlias;
import static oracle.kv.impl.security.util.SecurityUtils.getUpdateNumber;
import static oracle.kv.impl.security.util.SecurityUtils.installMatching;
import static oracle.kv.impl.security.util.SecurityUtils.listKeystore;
import static oracle.kv.impl.security.util.SecurityUtils.loadKeyStore;
import static oracle.kv.impl.security.util.SecurityUtils.verifyTls;
import static oracle.kv.impl.security.util.SecurityUtils.verifyTlsCredentialUpdates;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.FileUtils.computeSha256Hash;
import static oracle.kv.impl.util.SSLTestUtils.SSL_CTS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_JKS_KS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_JKS_TS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_ALIAS_DEF;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_KS_PWD_DEF;
import static oracle.kv.impl.util.SSLTestUtils.SSL_MERGED_TS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_NEW_KS_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_PW_NAME;
import static oracle.kv.impl.util.SSLTestUtils.SSL_TS_NAME;
import static oracle.kv.util.TestUtils.checkCause;
import static oracle.kv.util.TestUtils.checkException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.security.auth.x500.X500Principal;

import oracle.kv.TestBase;
import oracle.kv.impl.security.filestore.FileStoreManager;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource.FilePasswordSource;
import oracle.kv.impl.security.util.SecurityUtils.AbstractInstallTruststoreUpdates;
import oracle.kv.impl.security.util.SecurityUtils.KadminSetting;
import oracle.kv.impl.security.util.SecurityUtils.Krb5Config;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.SSLTestUtils;
import oracle.kv.impl.util.TestUtils;

import com.sleepycat.je.rep.net.PasswordSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Tests of SecurityUtils functionality.
 */
public class SecurityUtilsTest extends TestBase {
    private static String WILD_CARD_DN =
        "EMAILADDRESS=test@example, CN=*.host.name, OU=Hosting, " +
        "O=Test Corporation, L=Bozeman, ST=Montana, C=US";
    private static String VALID_REGEX =
        "dnmatch(\\S*=test@example, CN=\\*.host.name, OU=Hosting, " +
        "O=Test Corporation, L=Bozeman, ST=Montana, C=US)";
    private static String OID_REGEX =
       "dnmatch(OID.1.2.840.113549.1.9.1=test@example, CN=\\*.host.name, " +
       "OU=Hosting, O=Test Corporation, L=Bozeman, ST=Montana, C=US)";

    private static final File testDir = TestUtils.getTestDir();
    private static final File testSSLDir = SSLTestUtils.getTestSSLDir();
    private static final File configDir = new File(testDir, "test-security");
    private static final File updatesDir =
        new File(configDir, SECURITY_UPDATES_DIR);

    private static final Properties DEFAULT_SECURITY_PROPS =
        new Properties();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteDirectory(configDir);
        configDir.mkdir();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(configDir);
        super.tearDown();
    }

    /**
     * Test the listKeystore() method.
     */
    @Test
    public void testListKeystore() {
        final File keystoreFile = new File(testSSLDir, SSL_KS_NAME);
        final File jksKeystoreFile = new File(testSSLDir, SSL_JKS_KS_NAME);
        testListKeystore(jksKeystoreFile);
        testListKeystore(keystoreFile);
    }

    private void testListKeystore(File keystoreFile) {
        final Enumeration<String> ksEntries =
            SecurityUtils.listKeystore(keystoreFile, SSL_KS_PWD_DEF);

        assertNotNull(ksEntries);
        ksEntries.asIterator().forEachRemaining(e -> {
            assertEquals(SSL_KS_ALIAS_DEF, e);
        });
    }

    /*
     * Test the listTruststore method.
     */
    @Test
    public void testListTruststore() {
        final File truststoreFile = new File(testSSLDir, SSL_TS_NAME);
        final File jksTruststoreFile = new File(testSSLDir, SSL_JKS_TS_NAME);
        testListTrustStore(truststoreFile);
        testListTrustStore(jksTruststoreFile);
    }

    private void testListTrustStore(File truststoreFile) {
        final Enumeration<String> tsEntries =
            SecurityUtils.listKeystore(truststoreFile, SSL_KS_PWD_DEF);

        assertNotNull(tsEntries);
        tsEntries.asIterator().forEachRemaining(e -> {
            assertEquals(SSL_KS_ALIAS_DEF, e);
        });
    }

    @Test
    public void testLoadKeyStore() throws Exception {
        final File tmpDir = new File(testDir, "tmp");
        FileUtils.deleteDirectory(tmpDir);
        tmpDir.mkdir();
        tearDowns.add(() -> FileUtils.deleteDirectory(tmpDir));
        final File keystoreFile = new File(tmpDir, SSL_KS_NAME);

        /* File not found */
        checkCause(
            checkException(
                () -> loadKeyStore(
                    keystoreFile.toString(), null, "keystore", null),
                IllegalArgumentException.class),
            FileNotFoundException.class);

        /* Junk file */
        Files.writeString(keystoreFile.toPath(), "abc");
        checkCause(
            checkException(
                () -> loadKeyStore(
                    keystoreFile.toString(), null, "keystore", null),
                IllegalArgumentException.class,
                "Error reading from keystore file"),
            IOException.class);

        /* File not readable */
        keystoreFile.setReadable(false);
        checkCause(
            checkException(
                () -> loadKeyStore(
                    keystoreFile.toString(), null, "keystore", null),
                IllegalArgumentException.class),
            FileNotFoundException.class);

        /* Get hash */
        Files.copy(testSSLDir.toPath().resolve(SSL_KS_NAME),
                   keystoreFile.toPath(), REPLACE_EXISTING);
        final String hash = computeSha256Hash(keystoreFile);
        assertNotNull(hash);
        final AtomicReference<String> getHash = new AtomicReference<>();
        loadKeyStore(keystoreFile.toString(), null, "keystore", null, getHash);
        assertEquals(hash, getHash.get());
    }

    @Test
    public void testParseBadKrb5ConfigFile() throws Exception {
        Krb5Config krb5Config = new Krb5Config(new File("non-existent"));
        try {
            krb5Config.parseConfigFile();
            fail("Config file does not exist");
        } catch (IOException ioe) {
            assertThat("non-existent in the exception message",
                       ioe.getMessage(), containsString("non-existent"));
        }
        final File testFile = File.createTempFile("bad-krb", ".conf");
        testFile.deleteOnExit();

        final String missingSectionName =
           "{kdc = localhost:8080}" +
           "[libdefaults]\n" +
           " default_realm = US.ORACLE.COM\n";

        try {
            krb5Config = new Krb5Config(
                writeConfigToFile(testFile, missingSectionName));
            krb5Config.parseConfigFile();
            fail("Config file in bad format");
        } catch (IOException ioe) /* CHECKSTYLE:OFF */ {
        } /* CHECKSTYLE:ON */

        final String noDefaultRealm =
            "[libdefaults]\n" +
            "dns_lookup_realm = false" +
            "[realms]\n" +
            "US.ORACLE.COM = {" +
            "kdc = localhost:8080 }";
        krb5Config = new Krb5Config(
            writeConfigToFile(testFile, noDefaultRealm));
        krb5Config.parseConfigFile();
        assertNull(krb5Config.getDefaultRealm());
        assertNull(krb5Config.getKdc());

        final String noDefaultKdc =
            "[libdefaults]\n" +
            "default_realm = 'US.ORACLE.COM'\n" +
            "dns_lookup_realm = false\n" +
            "[realms]\n" +
            "US.ORACLE.COM = {\n" +
            "admin_server = localhost\n}";
        krb5Config = new Krb5Config(
            writeConfigToFile(testFile, noDefaultKdc));
        krb5Config.parseConfigFile();
        assertEquals(krb5Config.getDefaultRealm(), "US.ORACLE.COM");
        assertNull(krb5Config.getKdc());

        final String strangeFormat =
            "[libdefaults]\n" +
            "default_realm = 'US.ORACLE.COM'\n" +
            "dns_lookup_realm = false\n" +
            "[realms]\n" +
            "US.ORACLE.COM = \n {\n" +
            "kdc = localhost:8080\n}";
        krb5Config = new Krb5Config(
            writeConfigToFile(testFile, strangeFormat));
        krb5Config.parseConfigFile();
        assertEquals(krb5Config.getDefaultRealm(), "US.ORACLE.COM");
        assertEquals(krb5Config.getKdc(), "localhost:8080");

        final String multiRealms =
            "[libdefaults]\n" +
            "default_realm = 'US.ORACLE.COM'\n" +
            "dns_lookup_realm = false\n" +
            "[realms]\n" +
            "US.ORACLE.COM = {\n" +
            "kdc = localhost:8080\n" +
            "admin_server = slc00bsf.us.oracle.com}" +
            "EXAMPLE.COM = {\n" +
            "kdc = example.com:8080\n}";
        krb5Config = new Krb5Config(
            writeConfigToFile(testFile, multiRealms));
        krb5Config.parseConfigFile();
        assertEquals(krb5Config.getDefaultRealm(), "US.ORACLE.COM");
        assertEquals(krb5Config.getKdc(), "localhost:8080");
    }

    @Test
    public void testKadminSetting() {
        KadminSetting setting = new KadminSetting();
        assertEquals(setting.getKrbAdminPath(), SecurityUtils.KADMIN_DEFAULT);

        try {
            setting.validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            assertThat("IllegalArgumentException", iae.getMessage(),
                       containsString("must specify principal name"));
        }
        setting.setKrbAdminCcache("ccache");
        assertEquals(setting.getKrbAdminCcache(), "ccache");
        try {
            setting.validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            assertThat("IllegalArgumentException", iae.getMessage(),
                       containsString("does not exist"));
        }
        assertTrue(setting.useCcache());
        setting.setKrbAdminKeytab("keytab");
        assertEquals(setting.getKrbAdminKeytab(), "keytab");
        try {
            setting.validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            assertThat("IllegalArgumentException", iae.getMessage(),
                       containsString("cannot use admin ketyab and " +
                                      "credential cache together"));
        }
        setting.setKrbAdminCcache(null);
        assertNull(setting.getKrbAdminCcache());
        try {
            setting.validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            assertThat("IllegalArgumentException", iae.getMessage(),
                       containsString("must specify admin principal"));
        }
        setting.setKrbAdminPrinc("admin");
        assertEquals(setting.getKrbAdminPrinc(), "admin");
        try {
            setting.validateKadminSetting();
        } catch (IllegalArgumentException iae) {
            assertThat("IllegalArgumentException", iae.getMessage(),
                       containsString("does not exist"));
        }
        assertTrue(setting.useKeytab());
    }

    @Test
    public void testWildCardCertDNMatch() {
        X500Principal princ = new X500Principal(WILD_CARD_DN);
        String invalid = "dnmatch(" + WILD_CARD_DN + ")";
        String result = SecurityUtils.verifyCertIdentityAllowed(princ, invalid);
        assertThat("not match", result, containsString("does not match"));
        result = SecurityUtils.verifyCertIdentityAllowed(princ, VALID_REGEX);
        assertNull(result);
        result = SecurityUtils.verifyCertIdentityAllowed(princ, OID_REGEX);
        assertNull(result);
    }

    /**
     * Just write the text to the file, replacing an existing contents.
     */
    private File writeConfigToFile(File file, String configText)
        throws Exception {

        final FileWriter writer = new FileWriter(file);
        try {
            writer.write(configText);
            writer.flush();
        } finally {
            writer.close();
        }
        return file;
    }

    @Test
    public void testAddTruststoreUpdates() throws IOException {
        final File installedTruststore = new File(configDir, SSL_TS_NAME);
        final File updateTruststore = new File(updatesDir, SSL_TS_NAME);
        final File tmpTruststore = new File(configDir, SSL_TS_NAME + ".tmp");
        final FilePasswordSource storePwd = new FilePasswordSource(
            new File(testSSLDir, "store.passwd").toString(),
            FileStoreManager.class.getName(), "keystore");
        final String storeTrustHash =
            "eac4aaf7f95ee557f033239aaf6a57ff87568fd12c7749676c63b4f90c783549";

        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   installedTruststore.toPath());

        /* No password source */
        checkException(() -> addTruststoreUpdates(
                           null, configDir, SSL_TS_NAME, "pkcs12",
                           DEFAULT_SECURITY_PROPS, logger),
                       IllegalStateException.class,
                       "Unable to create keystore password source");
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* No updates directory */
        assertEquals(NO_UPDATE_FOUND.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* No updates */
        updatesDir.mkdir();
        assertEquals(NO_UPDATE_FOUND.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* Update not readable */
        Files.copy(installedTruststore.toPath(), updateTruststore.toPath());
        updateTruststore.setReadable(false);
        /*
         * loadKeyStore uses java.io file operations which don't distinguish
         * failures, so accessing an unreadable file throws
         * FileNotFoundException
         */
        assertEquals(NO_UPDATE_FOUND.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* Update already installed */
        updateTruststore.setReadable(true);
        assertEquals(UPDATE_ALREADY_INSTALLED.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* No password */
        checkException(() -> addTruststoreUpdates(
                           () -> null, configDir, SSL_TS_NAME, "pkcs12",
                           DEFAULT_SECURITY_PROPS, logger),
                       IllegalStateException.class,
                       "Truststore password was not found");
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* Wrong password */
        final PasswordSource badPwd = "nope"::toCharArray;
        checkCause(checkException(() -> addTruststoreUpdates(
                                      badPwd, configDir, SSL_TS_NAME,
                                      "pkcs12", DEFAULT_SECURITY_PROPS,
                                      logger),
                                  IllegalArgumentException.class),
                   IOException.class,
                   "keystore password was incorrect");
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* Merge entries from merge.trust, save fails */
        tearDowns.add(() -> SecurityUtils.addTrustUpdatesStoreHook = null);
        SecurityUtils.addTrustUpdatesStoreHook = ignore -> {
            throw new IOException("Injected failure saving truststore");
        };
        Files.copy(testSSLDir.toPath().resolve("merge.trust"),
                   updateTruststore.toPath(), REPLACE_EXISTING);
        checkCause(
            checkException(() -> addTruststoreUpdates(
                               storePwd, configDir, SSL_TS_NAME, "pkcs12",
                               DEFAULT_SECURITY_PROPS, logger),
                           IllegalStateException.class),
            IOException.class,
            "Injected failure saving truststore");
        assertEquals(storeTrustHash, computeSha256Hash(installedTruststore));
        assertFalse(tmpTruststore.exists());

        /* Merge entries from merge trust succeeds */
        SecurityUtils.addTrustUpdatesStoreHook = null;
        assertEquals(INSTALLED_UPDATE.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        final String storePwdString = String.valueOf(storePwd.getPassword());
        assertEquals(Set.of("shared", "__update#1__"),
                     toSet(listKeystore(installedTruststore, storePwdString)));
        assertFalse(tmpTruststore.exists());

        /* Merge again */
        assertEquals(UPDATE_ALREADY_INSTALLED.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(Set.of("shared", "__update#1__"),
                     toSet(listKeystore(installedTruststore, storePwdString)));
        assertFalse(tmpTruststore.exists());

        /* Revert update, updates should get removed */
        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   updateTruststore.toPath(),
                   REPLACE_EXISTING);

        assertEquals(INSTALLED_UPDATE.msg,
                     addTruststoreUpdates(storePwd, configDir, SSL_TS_NAME,
                                          "pkcs12", DEFAULT_SECURITY_PROPS,
                                          logger));
        assertEquals(Set.of("shared"),
                     toSet(listKeystore(installedTruststore, storePwdString)));
    }

    private static <T> Set<T> toSet(Enumeration<T> enumeration) {
        final Set<T> set = new HashSet<>();
        while (enumeration.hasMoreElements()) {
            set.add(enumeration.nextElement());
        }
        return set;
    }

    @Test
    public void testInstallTruststoreUpdates() throws Exception {
        final TestInstallTruststoreUpdates installTruststoreUpdates =
            new TestInstallTruststoreUpdates();

        final Map<String, String> installed = new HashMap<>();
        installed.put("orig-alias1", "orig-cert1");
        assertFalse(installTruststoreUpdates.addUpdates(Map.of(), installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1"), installed);

        assertTrue(
            installTruststoreUpdates.addUpdates(
                Map.of("update-alias1", "update-cert1"), installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1",
                            "__update#1__", "update-cert1"),
                     installed);

        assertTrue(
            installTruststoreUpdates.addUpdates(
                Map.of("update-alias1", "update-cert1",
                       "update-alias2", "update-cert2"),
                installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1",
                            "__update#1__", "update-cert1",
                            "__update#2__", "update-cert2"),
                     installed);

        assertTrue(
            installTruststoreUpdates.addUpdates(
                Map.of("update-alias2", "update-cert2"), installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1",
                            "__update#2__", "update-cert2"),
                     installed);

        assertTrue(
            installTruststoreUpdates.addUpdates(
                Map.of("orig-alias1", "orig-cert1"), installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1"), installed);

        assertFalse(
            installTruststoreUpdates.addUpdates(
                Map.of("update-alias1", "orig-cert1"),
                installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1"), installed);

        assertFalse(
            installTruststoreUpdates.addUpdates(
                Map.of("orig-alias1", "orig-cert1"), installed));
        assertEquals(Map.of("orig-alias1", "orig-cert1"), installed);
    }

    static class TestInstallTruststoreUpdates
            extends AbstractInstallTruststoreUpdates<
                        @NonNull Map<String, String>, @NonNull String>
    {
        @Override
        Enumeration<String> aliases(Map<String, String> ks) {
            return Collections.enumeration(ks.keySet());
        }
        @Override
        String getCert(Map<String, String> ks, String alias) {
            return ks.get(alias);
        }
        @Override
        boolean containsCert(Map<String, String> ks, String cert) {
            return ks.containsValue(cert);
        }
        @Override
        void deleteEntry(Map<String, String> ks, String alias) {
            ks.remove(alias);
        }
        @Override
        void setCert(Map<String, String> ks, String alias, String cert) {
            ks.put(alias, cert);
        }
    }

    @Test
    public void testGetUpdateNumber() {
        assertEquals(-1, getUpdateNumber("shared"));
        assertEquals(-1, getUpdateNumber("__update#__"));
        assertEquals(-1, getUpdateNumber("__update#12"));
        assertEquals(-1, getUpdateNumber("__update#a1a__"));
        assertEquals(1, getUpdateNumber("__update#1__"));
        assertEquals(70, getUpdateNumber("__update#70__"));
    }

    @Test
    public void testGetUpdateAlias() {
        assertEquals("__update#1__", getUpdateAlias(1));
        assertEquals("__update#70__", getUpdateAlias(70));
    }

    @Test
    public void testInstallMatching() throws IOException {
        final String abcHash =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        final Path installed = testDir.toPath().resolve("abc");
        Files.deleteIfExists(installed);
        final Path tmp = testDir.toPath().resolve("abc.tmp");
        Files.deleteIfExists(tmp);
        FileUtils.deleteDirectory(updatesDir);
        final Path update = updatesDir.toPath().resolve("abc");

        /* No update */
        Files.writeString(installed, "abc");
        assertEquals(NO_UPDATE_FOUND,
                     installMatching(update, abcHash, installed));
        assertFalse(Files.exists(update));
        assertFalse(Files.exists(tmp));

        /* Update directory not found */
        Files.writeString(installed, "def");
        checkException(() -> installMatching(update, abcHash, installed),
                       IllegalStateException.class,
                       "Needed update not found: " + update.getFileName());
        assertFalse(Files.exists(update));
        assertFalse(Files.exists(tmp));

        /* Update file not found */
        Files.createDirectory(updatesDir.toPath());
        checkException(() -> installMatching(update, abcHash, installed),
                       IllegalStateException.class,
                       "Needed update not found: " + update.getFileName());
        assertFalse(Files.exists(update));
        assertFalse(Files.exists(tmp));

        /* Update not needed */
        Files.writeString(installed, "abc");
        Files.writeString(update, "abc");
        assertEquals(UPDATE_ALREADY_INSTALLED,
                     installMatching(update, abcHash, installed));
        assertFalse(Files.exists(tmp));

        /* Update needed */
        Files.writeString(installed, "def");
        final String defHash = computeSha256Hash(installed.toFile());
        assertNotNull(defHash);
        assertEquals(INSTALLED_UPDATE,
                     installMatching(update, abcHash, installed));
        assertFalse(Files.exists(tmp));
        assertEquals(abcHash, computeSha256Hash(installed.toFile()));

        /* Update has wrong hash */
        Files.writeString(installed, "abc");
        Files.writeString(update, "xyz");
        checkException(
            () -> installMatching(update, defHash, installed),
            IllegalStateException.class,
            "Update has wrong hash");

        /* Update not readable */
        Files.writeString(update, "abc");
        update.toFile().setReadable(false);
        Files.writeString(installed, "def");
        checkCause(checkException(
                       () -> installMatching(update, abcHash, installed),
                       IllegalStateException.class),
                   AccessDeniedException.class,
                   update.toString());
        assertFalse(Files.exists(tmp));
    }

    @Test
    public void testDeleteMatching() throws IOException {
        final String abcHash =
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        final Path dir = testDir.toPath().resolve("testDir");
        FileUtils.deleteDirectory(dir.toFile());
        final Path path = dir.resolve("abc");
        Files.deleteIfExists(path);
        tearDowns.add(() -> {
                dir.toFile().setWritable(true);
                path.toFile().delete();
                dir.toFile().delete();
            });

        /* No directory */
        deleteMatching(path, abcHash, logger);

        /* No file */
        Files.createDirectory(dir);
        deleteMatching(path, abcHash, logger);
        assertFalse(Files.exists(path));

        /* Wrong hash */
        Files.writeString(path, "def");
        deleteMatching(path, abcHash, logger);
        assertTrue(Files.exists(path));

        /* Right hash */
        Files.writeString(path, "abc");
        deleteMatching(path, abcHash, logger);
        assertFalse(Files.exists(path));

        /* File not readable */
        Files.writeString(path, "abc");
        path.toFile().setReadable(false);
        checkCause(checkException(() -> deleteMatching(path, abcHash, logger),
                                  IllegalStateException.class),
                   AccessDeniedException.class,
                   path.toString());

        /* Directory not writable */
        path.toFile().setReadable(true);
        dir.toFile().setWritable(false);
        checkCause(checkException(() -> deleteMatching(path, abcHash, logger),
                                  IllegalStateException.class),
                   AccessDeniedException.class,
                   path.toString());
    }

    @Test
    public void testVerifyTlsCredentialUpdates() throws Exception {
        updatesDir.mkdir();
        final File installedKeystore = new File(configDir, SSL_KS_NAME);
        final File updateKeystore = new File(updatesDir, SSL_KS_NAME);
        final File installedTruststore = new File(configDir, SSL_TS_NAME);
        final File updateTruststore = new File(updatesDir, SSL_TS_NAME);
        final FilePasswordSource storePwd = new FilePasswordSource(
            new File(testSSLDir, SSL_PW_NAME).toString(),
            FileStoreManager.class.getName(), "keystore");

        Files.writeString(updateKeystore.toPath(), "abc");
        Files.writeString(updateTruststore.toPath(), "def");
        checkAllForce(
            force -> checkException(
                () -> verifyTlsCredentialUpdates(
                    storePwd, configDir, SSL_KS_NAME, "PKCS12",
                    SSL_TS_NAME, "PKCS12",
                    singletonMap("shared", singleton("dnMatch(.*)")),
                    "shared", "shared", force),
                IllegalStateException.class,
                "Problems with credential updates:\n" +
                "Error reading from keystore file .*\n" +
                "Error reading from truststore file .*\n"));

        Files.copy(testSSLDir.toPath().resolve(SSL_KS_NAME),
                   updateKeystore.toPath(), REPLACE_EXISTING);
        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   updateTruststore.toPath(), REPLACE_EXISTING);
        checkAllForce(force -> verifyTlsCredentialUpdates(
                          storePwd, configDir, SSL_KS_NAME, "PKCS12",
                          SSL_TS_NAME, "PKCS12",
                          singletonMap("shared", singleton("dnMatch(.*)")),
                          "shared", "shared", force));

        updateKeystore.delete();
        updateTruststore.delete();
        checkAllForce(force -> checkException(
                          () -> verifyTlsCredentialUpdates(
                              storePwd, configDir, SSL_KS_NAME, "PKCS12",
                              SSL_TS_NAME, "PKCS12",
                              singletonMap("shared", singleton("dnMatch(.*)")),
                              "shared", "shared", force),
                          IllegalStateException.class,
                          "Unable to locate specified keystore"));

        Files.writeString(installedKeystore.toPath(), "abc");
        Files.writeString(installedTruststore.toPath(), "def");
        checkAllForce(force -> checkException(
                          () -> verifyTlsCredentialUpdates(
                              storePwd, configDir, SSL_KS_NAME, "PKCS12",
                              SSL_TS_NAME, "PKCS12",
                              singletonMap("shared", singleton("dnMatch(.*)")),
                              "shared", "shared", force),
                          IllegalStateException.class,
                          "Problems with installed credentials:\n" +
                          "Error reading from keystore file .*\n" +
                          "Error reading from truststore file .*\n"));

        Files.copy(testSSLDir.toPath().resolve(SSL_KS_NAME),
                   installedKeystore.toPath(), REPLACE_EXISTING);
        checkAllForce(force -> checkException(
                          () -> verifyTlsCredentialUpdates(
                              storePwd, configDir, SSL_KS_NAME, "PKCS12",
                              SSL_TS_NAME, "PKCS12",
                              singletonMap("shared", singleton("dnMatch(.*)")),
                              "shared", "shared", force),
                          IllegalStateException.class,
                          "Problems with installed credentials:\n" +
                          "Error reading from truststore file .*\n"));

        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   installedTruststore.toPath(), REPLACE_EXISTING);
        checkAllForce(force -> verifyTlsCredentialUpdates(
                          storePwd, configDir, SSL_KS_NAME, "PKCS12",
                          SSL_TS_NAME, "PKCS12",
                          singletonMap("shared", singleton("dnMatch(.*)")),
                          "shared", "shared", force));

        checkAllForce(
            force -> checkException(
                () -> verifyTlsCredentialUpdates(
                    storePwd, configDir, SSL_KS_NAME, "unknownKT",
                    SSL_TS_NAME, "PKCS12",
                    singletonMap("shared", singleton("dnMatch(.*)")),
                    "shared", "shared", force),
                IllegalStateException.class,
                "Unable to find a keystore instance of type unknownKT"));

        checkAllForce(
            force -> checkException(
                () -> verifyTlsCredentialUpdates(
                    storePwd, configDir, SSL_KS_NAME, "PKCS12",
                    SSL_TS_NAME, "unknownTT",
                    singletonMap("shared", singleton("dnMatch(.*)")),
                    "shared", "shared", force),
                IllegalStateException.class,
                "Unable to find a truststore instance of type unknownTT"));
    }

    static <E extends Throwable>
        void checkAllForce(Consumer<Boolean> consumer)
    {
        consumer.accept(false);
        consumer.accept(true);
    }

    /** Test checkKeystoreEntry via calls to verifyTlsCredentialUpdates */
    @Test
    public void testCheckKeystoreEntry() throws Exception {
        updatesDir.mkdir();
        final File installedKeystore = new File(configDir, SSL_KS_NAME);
        final File installedTruststore = new File(configDir, SSL_TS_NAME);
        final FilePasswordSource storePwd = new FilePasswordSource(
            new File(testSSLDir, SSL_PW_NAME).toString(),
            FileStoreManager.class.getName(), "keystore");

        Files.copy(testSSLDir.toPath().resolve(SSL_KS_NAME),
                   installedKeystore.toPath(), REPLACE_EXISTING);
        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   installedTruststore.toPath(), REPLACE_EXISTING);

        checkAllForce(force -> verifyTlsCredentialUpdates(
                          storePwd, configDir, SSL_KS_NAME, "PKCS12",
                          SSL_TS_NAME, "PKCS12",
                          singletonMap("shared", singleton("dnMatch(.*)")),
                          "none", "none", force));

        checkExceptionNotForce(
            force -> verifyTlsCredentialUpdates(
                storePwd, configDir, SSL_KS_NAME, "PKCS12",
                SSL_TS_NAME, "PKCS12",
                singletonMap("key-not-found", singleton("dnMatch(.*)")),
                "shared", "shared", force),
            IllegalStateException.class,
            "Private key key-not-found does not exist in keystore");

        checkExceptionNotForce(
            force -> verifyTlsCredentialUpdates(
                storePwd, configDir, SSL_KS_NAME, "PKCS12",
                SSL_TS_NAME, "PKCS12",
                singletonMap("shared", singleton("dnMatch(cn-pattern)")),
                "shared", "shared", force),
            IllegalStateException.class,
            "The subject name .* does not match" +
            " 'dnMatch\\(cn-pattern\\)' .*");

        checkExceptionNotForce(
            force -> verifyTlsCredentialUpdates(
                storePwd, configDir, SSL_KS_NAME, "PKCS12",
                SSL_TS_NAME, "PKCS12",
                singletonMap("shared", singleton("dnMatch(.*)")),
                "shared", "sig-public", force),
            IllegalStateException.class,
            "Certificate not found for alias sig-public in" +
            " truststore");

        Files.copy(testSSLDir.toPath().resolve(SSL_MERGED_TS_NAME),
                   installedTruststore.toPath(), REPLACE_EXISTING);
        checkExceptionNotForce(
            force -> verifyTlsCredentialUpdates(
                storePwd, configDir, SSL_KS_NAME, "PKCS12",
                SSL_TS_NAME, "PKCS12",
                singletonMap("shared", singleton("dnMatch(.*)")),
                "shared", "mykey", force),
            IllegalStateException.class,
            "Certificate for alias mykey in truststore .*" +
            " does not match the certificate associated with" +
            " alias shared in keystore");

        Files.copy(testSSLDir.toPath().resolve(SSL_TS_NAME),
                   installedTruststore.toPath(), REPLACE_EXISTING);
        Files.copy(testSSLDir.toPath().resolve(SSL_NEW_KS_NAME),
                   installedKeystore.toPath(), REPLACE_EXISTING);
        checkExceptionNotForce(
            force ->
            verifyTlsCredentialUpdates(
                storePwd, configDir, SSL_KS_NAME, "PKCS12",
                SSL_TS_NAME, "PKCS12",
                singletonMap("shared", singleton("dnMatch(.*)")),
                "none", "none", force),
            IllegalStateException.class,
            "Self-signed certificate was not found in truststore");
    }

    interface OperationWithForce {
        void call(boolean force) throws Exception;
    }

    private static <E extends Throwable>
        void checkExceptionNotForce(OperationWithForce op,
                                    Class<E> exceptionClass,
                                    String messagePattern)
        throws Exception
    {
        checkException(() -> op.call(false /* force */),
                       exceptionClass, messagePattern);
        op.call(true /* force */);
    }

    @Test
    public void testVerifyTls() {
        final FilePasswordSource storePwd = new FilePasswordSource(
            new File(testSSLDir, "store.passwd").toString(),
            FileStoreManager.class.getName(), "keystore");

        verifyTls(storePwd,
                  "dnmatch(CN=Unit Test)",
                  "dnmatch(CN=Unit Test)",
                  new File(testSSLDir, SSL_KS_NAME).toString(),
                  "PKCS12",
                  new File(testSSLDir, SSL_TS_NAME).toString(),
                  "PKCS12",
                  "shared",
                  logger);

        /* PKCS12 and JKS are interchangeable */
        verifyTls(storePwd,
                  "dnmatch(CN=Unit Test)",
                  "dnmatch(CN=Unit Test)",
                  new File(testSSLDir, SSL_KS_NAME).toString(),
                  "JKS",
                  new File(testSSLDir, SSL_TS_NAME).toString(),
                  "JKS",
                  "shared",
                  logger);

        /* Truststore without password works */
        verifyTls(storePwd,
                  "dnmatch(CN=Unit Test)",
                  "dnmatch(CN=Unit Test)",
                  new File(testSSLDir, SSL_KS_NAME).toString(),
                  "PKCS12",
                  new File(testSSLDir, SSL_CTS_NAME).toString(),
                  "PKCS12",
                  "shared",
                  logger);

        checkException(() -> verifyTls(
                           storePwd,
                           "dnmatch(CN=Unit Test)",
                           "dnmatch(CN=Server Not Recognized)",
                           new File(testSSLDir, SSL_KS_NAME).toString(),
                           "PKCS12",
                           new File(testSSLDir, SSL_TS_NAME).toString(),
                           "PKCS12",
                           "shared",
                           logger),
                       IllegalStateException.class,
                       "Server identity could not be verified");

        /*
         * This test fails because async does not enforce client identity
         * checks. [KVSTORE-2344]
         */
        checkException(() -> verifyTls(
                           storePwd,
                           "dnmatch(CN=Client Not Recognized)",
                           "dnmatch(CN=Unit Test)",
                           new File(testSSLDir, SSL_KS_NAME).toString(),
                           "PKCS12",
                           new File(testSSLDir, SSL_TS_NAME).toString(),
                           "PKCS12",
                           "shared",
                           logger),
                       IllegalStateException.class,
                       "Client identity could not be verified");

        /* Keystore key doesn't match truststore cert */
        checkException(() -> verifyTls(
                           storePwd,
                           "dnmatch(CN=Unit Test)",
                           "dnmatch(CN=Unit Test)",
                           new File(testSSLDir, SSL_NEW_KS_NAME).toString(),
                           "PKCS12",
                           new File(testSSLDir, SSL_TS_NAME).toString(),
                           "PKCS12",
                           "shared",
                           logger),
                       IllegalStateException.class,
                       "Certificate signature validation failed");
    }
}
