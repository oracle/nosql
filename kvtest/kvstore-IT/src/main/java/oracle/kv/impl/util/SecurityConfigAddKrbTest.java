/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.logging.Logger;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KeyTab;

import oracle.kv.impl.admin.param.SecurityParams;

import org.junit.Test;

/**
 * Test the securityconfig add-kerberos command-line interface.
 *
 * This test relies on the store.keytab file under test/kerberos that should
 * not be modified.
 */
public class SecurityConfigAddKrbTest extends SecurityConfigTestBase {

    public final static String KEYTAB_NAME = "store.keytab";
    public final static String KRB_CONF_FILE = "krb5.conf";
    public final static String SERVICE_PRINCIPAL = "oraclenosql@US.ORACLE.COM";

    private File testRoot = null;
    private File securityDir = null;
    private File testKeytab = null;
    private final String emptySecDir = "emptySecDir";

    @Override
    public void setUp() throws Exception {

        super.setUp();
        testRoot = new File(TestUtils.getTestDir(), "testroot");
        removeDirectory(testRoot, 20);
        testRoot.mkdir();
        securityDir = new File(testRoot, emptySecDir);
        securityDir.mkdir();
    }

    @Override
    public void tearDown() throws Exception {

        super.tearDown();
        removeDirectory(testRoot, 20);
    }

    @Test
    public void testAddWithErrors()
        throws Exception {

        String s;
        /**
         * add-kerberos in a non-existent root directory
         */
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        "/nonexistent/kvroot"});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * add-kerberos in a non-existent security directory
         */
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        testRoot.getPath() +
                                        "/nonexistent/security"});

        assertTrue(s.indexOf("does not exist") != -1);

        /**
         * add-kerberos in a security directory without security.xml file
         */
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        testRoot.getPath(),
                                        "-secdir",
                                        emptySecDir});

        assertTrue(s.indexOf("security.xml file does not exist") != -1);

        final File secXmlFile =
            new File(securityDir.getPath(), FileNames.SECURITY_CONFIG_FILE);
        final SecurityParams sp =  new SecurityParams();

        /* Populate Kerberos parameters in security.xml file */
        sp.setKerberosConfFile(securityDir.getPath() + KRB_CONF_FILE);
        sp.setKerberosRealmName("US.ORACLE.COM");

        /* Overwrite the security.xml file */
        ConfigUtils.createSecurityConfig(sp, secXmlFile);

        /* Copy prepared keytab file to security directory */
        generateKeytabFile();

        /**
         * add-kerberos in a security directory with non-existent kerberos
         * configuration file.
         */
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        testRoot.getPath(),
                                        "-secdir",
                                        emptySecDir,
                                        "-krb-conf",
                                        "non-existent"});

        assertTrue(s.indexOf("configuration file does not exist") != -1);

        /**
         * add-kerberos in a security directory having an existing keytab file
         */
        final File krbConf = new File(securityDir, KRB_CONF_FILE);
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        testRoot.getPath(),
                                        "-secdir",
                                        emptySecDir,
                                        "-krb-conf",
                                        krbConf.getPath(),
                                        "-admin-principal",
                                        "admin/admin"});

        assertTrue(s.indexOf("not adding this Kerberos service") != -1);

        /**
         * add-kerberos in a security directory having an existing keytab file
         * with different name
         */
        s = runCLICommand(null,
                          new String[] {"config", "add-kerberos",
                                        "-root",
                                        testRoot.getPath(),
                                        "-secdir",
                                        emptySecDir,
                                        "-krb-conf",
                                        krbConf.getPath(),
                                        "-admin-principal",
                                        "admin/admin",
                                        "-param",
                                        "krbServiceKeytab=test.keytab"});
        assertTrue(s.indexOf("rename keytab file from") != -1);
    }

    @Test
    public void testAddWithoutKadmin()
        throws Exception {

        String s;

        final File secXmlFile =
                new File(securityDir.getPath(), FileNames.SECURITY_CONFIG_FILE);
        final SecurityParams sp =  new SecurityParams();
        ConfigUtils.createSecurityConfig(sp, secXmlFile);
        final File testkrbDir = TestUtils.getKrbTestDir();
        TestUtils.copyFile(KRB_CONF_FILE, testkrbDir, securityDir);
        final File krbConf = new File(securityDir, KRB_CONF_FILE);

        /**
         * add-kerberos without performing kadmin
         */
        s = runCLICommand(
            null,
            "config",
            "add-kerberos",
            "-root",
            testRoot.getPath(),
            "-secdir",
            emptySecDir,
            "-kadmin-path",
            "none",
            "-krb-conf",
            krbConf.getPath(),
            "-instance-name",
            "test.instance",
            "-param",
            "krbServiceName=test.service");

        assertTrue(s.indexOf("not creating a keytab") != -1);
        assertTrue(s.indexOf("Updated Kerberos configuration") != -1);

        SecurityParams secParams = ConfigUtils.getSecurityParams(
            new File(new File(testRoot, emptySecDir), SEC_XML),
            Logger.getLogger("test"));
        assertEquals(secParams.getKerberosInstanceName(), "test.instance");
        assertEquals(secParams.getKerberosServiceName(), "test.service");
    }

    private void generateKeytabFile() throws Exception {
        final File testkrbDir = TestUtils.getKrbTestDir();
        TestUtils.copyFile(KEYTAB_NAME, testkrbDir, securityDir);
        TestUtils.copyFile(KRB_CONF_FILE, testkrbDir, securityDir);
        testKeytab = new File(securityDir, KEYTAB_NAME);
        final KeyTab keytab = KeyTab.getInstance(testKeytab);
        final KerberosPrincipal princ =new KerberosPrincipal(SERVICE_PRINCIPAL);

        /* Error if the original keytab is corrupted */
        assertTrue(keytab.getKeys(princ).length != 0);
    }
}
