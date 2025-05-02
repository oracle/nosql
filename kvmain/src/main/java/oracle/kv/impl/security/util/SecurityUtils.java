/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * This file was distributed by Oracle as part of a version of Oracle NoSQL
 * Database made available at:
 *
 * http://www.oracle.com/technetwork/database/database-technologies/nosqldb/downloads/index.html
 *
 * Please see the LICENSE file included in the top-level directory of the
 * appropriate version of Oracle NoSQL Database for a copy of the license and
 * additional information.
 */
package oracle.kv.impl.security.util;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static oracle.kv.impl.async.FutureUtils.checked;
import static oracle.kv.impl.async.FutureUtils.checkedVoid;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_CLIENT;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_INTERNAL;
import static oracle.kv.impl.param.ParameterState.SECURITY_TRANSPORT_JE_HA;
import static oracle.kv.impl.security.TopoSignatureHelper.SIG_PRIVATE_KEY_ALIAS_DEFAULT;
import static oracle.kv.impl.security.TopoSignatureHelper.SIG_PUBLIC_KEY_ALIAS_DEFAULT;
import static oracle.kv.impl.util.FileNames.SECURITY_UPDATES_DIR;
import static oracle.kv.impl.util.FileUtils.computeSha256Hash;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStore.PrivateKeyEntry;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertPathBuilder;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.PKIXBuilderParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509CertSelector;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import javax.security.auth.x500.X500Principal;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.async.EndpointConfig;
import oracle.kv.impl.async.EndpointConfigBuilder;
import oracle.kv.impl.async.ListenerConfig;
import oracle.kv.impl.async.ListenerConfigBuilder;
import oracle.kv.impl.async.NetworkAddress;
import oracle.kv.impl.async.dialog.nio.NioUtil;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterState.Info;
import oracle.kv.impl.security.KVStoreUserPrincipal;
import oracle.kv.impl.security.ResourceOwner;
import oracle.kv.impl.security.ssl.KeyStorePasswordSource;
import oracle.kv.impl.security.ssl.SSLConfig;
import oracle.kv.impl.sna.StorageNodeAgentAPI.CredentialHashes;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.test.TestIOHook;
import oracle.kv.impl.util.ConfigUtils;
import oracle.kv.impl.util.EmbeddedMode;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.impl.util.NonNullByDefault;
import oracle.kv.impl.util.SecurityConfigCreator.IOHelper;
import oracle.kv.impl.util.SecurityConfigCreator.ParsedConfig.ParamSetting;

import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.net.PasswordSource;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A collection of security-related utilities.
 */
@NonNullByDefault
public final class SecurityUtils {

    public static final String KEY_CERT_FILE = "certFileName";
    private static final String CERT_FILE_DEFAULT = "store.cert";

    public static final String KEY_KEY_ALGORITHM = "keyAlgorithm";
    private static final String KEY_ALGORITHM_DEFAULT = "RSA";

    public static final String KEY_KEY_SIZE = "keySize";
    private static final String KEY_SIZE_DEFAULT = "2048";

    public static final String KEY_DISTINGUISHED_NAME = "distinguishedName";
    private static final String DISTINGUISHED_NAME_DEFAULT = "cn=NoSQL";

    /** A regex pattern that matches any identity. */
    private static final String ALLOW_ANY_IDENTITY = "dnmatch(.*)";

    public static final String KEY_KEY_ALIAS = "keyAlias";
    public static final String KEY_ALIAS_DEFAULT = "shared";

    public static final String KEY_VALIDITY = "validity";
    private static final String VALIDITY_DEFAULT = "365";

    public static final String KEYSTORE_TYPE = "ksType";

    public enum InstallMatchingResult {
        NO_UPDATE_FOUND("No update found"),
        INSTALLED_UPDATE("Installed update"),
        UPDATE_ALREADY_INSTALLED("Update already installed");
        final String msg;
        InstallMatchingResult(String msg) {
            this.msg = msg;
        }
    }

    /* Java standard name for PKCS12 and JKS KeyStore type */
    private static final String PKCS12_TYPE = "PKCS12";
    private static final String JKS_TYPE = "JKS";

    /* Default type of Java KeyStore to create */
    public static final String KS_TYPE_DEFAULT = PKCS12_TYPE;

    /*
     * Java standard file name suffix of backup JKS Java KeyStore,
     * used in the Java KeyStore type update.
     */
    private static final String BACKUP_FILE_SUFFIX = ".old";

    /*
     * This is a Java-specified standard requirement for KeyStore
     * implementations is 6 character minimum, though some implementations
     * might add additional requirements.
     */
    public static final int MIN_STORE_PASSPHRASE_LEN = 6;

    /*
     * The list of preferred protocols.  Both KV and JE SSL implementations
     * will filter out any that are not supported.  If none are supported,
     * an exception will be thrown.
     */
    public static final String PREFERRED_PROTOCOLS_DEFAULT = "TLSv1.3,TLSv1.2";
    private static final String TEMP_CERT_FILE = "temp.cert";

    /*
     * The strings used by the Kerberos utility.
     */
    public static final String KADMIN_DEFAULT = "/usr/kerberos/sbin/kadmin";
    public static final String KRB_CONF_FILE = "/etc/krb5.conf";

    private static final String PRINCIPAL_VALIDITY = "krbPrincValidity";
    private static final String PRINC_VALIDITY_DEFAULT = "365days";

    private static final String KEYSALT_LIST = "krbKeysalt";
    private static final String PRINCIPAL_PWD_EXPIRE = "krbPrincPwdExpire";

    private static final String PRINC_PWD_EXPIRE_DEFAULT = "365days";
    private static final String KEYSALT_LIST_DEFAULT = "des3-cbc-sha1:normal," +
        "aes128-cts-hmac-sha1-96:normal,arcfour-hmac:normal";
    public static final String KERBEROS_AUTH_NAME = "KERBEROS";
    public static final String KRB_NAME_COMPONENT_SEPARATOR_STR = "/";
    public static final String KRB_NAME_REALM_SEPARATOR_STR = "@";

    public static final Properties princDefaultProps = new Properties();

    /* The strings used by the IDCS OAuth */
    public static final String OAUTH_AUTH_NAME = "IDCSOAUTH";

    private static final String digitSet = "0123456789";
    private static final String upperSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String lowerSet = "abcdefghijklmnopqrstuvwxyz";
    private static final String specialSet = "!#$%&'()*+,-./:;<>?@[]^_`{|}~";
    private static final String allCharSet = digitSet + upperSet +
        lowerSet + specialSet;

    private static final SecureRandom random = new SecureRandom();

    /* User id prefix used for creating user principal */
    public static final String IDCS_OAUTH_USER_ID_PREFIX = "idcs";

    /**
     * Test hook called by addTruststoreUpdates before it stores the updated
     * truststore to the specified path.
     */
    public static volatile @Nullable TestIOHook<Path> addTrustUpdatesStoreHook;

    /**
     * Test hook called by addTruststoreUpdates before moving the updated
     * truststore to the specified path.
     */
    public static volatile @Nullable TestIOHook<Path> addTrustUpdatesMoveHook;

    /**
     * Test hook called by addTruststoreUpdates before deleting the temporary
     * truststore with the specified path.
     */
    public static volatile @Nullable TestIOHook<Path>
        addTrustUpdatesDeleteHook;

    /**
     * Test hook called by installMatching before copying the file to the
     * specified path.
     */
    public static volatile @Nullable TestIOHook<Path> installMatchingCopyHook;

    /**
     * Test hook called by installMatching before moving the file to the
     * specified path.
     */
    public static volatile @Nullable TestIOHook<Path> installMatchingMoveHook;

    /**
     * Test hook called by installMatching before deleting the file with the
     * specified path. Note that IOExceptions thrown by this hook will be
     * discarded because the deletion ignores failures.
     */
    public static volatile @Nullable TestIOHook<Path>
        installMatchingDeleteHook;

    /**
     * Test hook called by deleteMatching before deleting the file with the
     * specified path.
     */
    public static volatile @Nullable TestIOHook<Path> deleteMatchingHook;

    /* System properties used to create password-less PKCS12 trust store */
    private static final String PKCS12_CERT_PROTECTION_ALG =
        "keystore.pkcs12.certProtectionAlgorithm";
    private static final String PKCS12_MAC_ALG = "keystore.pkcs12.macAlgorithm";

    static {
        princDefaultProps.put(PRINCIPAL_VALIDITY, PRINC_VALIDITY_DEFAULT);
        princDefaultProps.put(KEYSALT_LIST, KEYSALT_LIST_DEFAULT);
        princDefaultProps.put(PRINCIPAL_PWD_EXPIRE, PRINC_PWD_EXPIRE_DEFAULT);
    }

    private static final Set<String> preferredProtocols = new HashSet<>();

    static {
        preferredProtocols.add("TLSv1.3");
        preferredProtocols.add("TLSv1.2");
    }

    /* not instantiable */
    private SecurityUtils() {
    }

    /**
     * Given an abstract file, attempt to change permissions so that it is
     * readable only by the owner of the file.
     * @param f a File referencing a file or directory on which permissions are
     * to be changed.
     * @return true if the permissions were successfully changed
     */
    public static boolean makeOwnerAccessOnly(File f)
        throws IOException {

        if (!f.exists()) {
            return false;
        }

        final FileSysUtils.Operations osOps = FileSysUtils.selectOsOperations();
        return osOps.makeOwnerAccessOnly(f);
    }

    /**
     * Given an abstract file, attempt to change permissions so that it is
     * writable only by the owner of the file.
     * @param f a File referencing a file or directory on which permissions are
     * to be changed.
     * @return true if the permissions were successfully changed
     */
    public static boolean makeOwnerOnlyWriteAccess(File f)
        throws IOException {

        if (!f.exists()) {
            return false;
        }

        final FileSysUtils.Operations osOps = FileSysUtils.selectOsOperations();
        return osOps.makeOwnerAccessOnly(f);
    }

    public static boolean passwordsMatch(char @Nullable[] pwd1,
                                         char @Nullable[] pwd2) {
        if (pwd1 == pwd2) {
            return true;
        }

        if (pwd1 == null || pwd2 == null) {
            return false;
        }

        return Arrays.equals(pwd1, pwd2);
    }

    public static void clearPassword(char @Nullable[] pwd) {
        if (pwd != null) {
            for (int i = 0; i < pwd.length; i++) {
                pwd[i] = ' ';
            }
        }
    }

    /**
     * Make a java keystore and an associated trustStore.
     * @param securityDir the directory in which the keystore and truststore
     *    will be created.
     * @param sp a SecurityParams instance containing information regarding
     * the keystore and truststore file names
     * @param keyStorePassword the password with which the keystore and
     * truststore will be secured
     * @param ctsPwd password for client.trust or null
     * @param props a set of optional settings that can alter the
     *    keystore creation.
     * @return true if the creation process was successful and false
     *    if an error occurred.
     */
    public static boolean initKeyStore(File securityDir,
                                       SecurityParams sp,
                                       char[] keyStorePassword,
                                       char @Nullable[] ctsPwd,
                                       @Nullable Properties props) {
        if (props == null) {
            props = new Properties();
        }

        final String certFileName = props.getProperty(KEY_CERT_FILE,
                                                      CERT_FILE_DEFAULT);

        final String keyStoreFile =
            new File(securityDir.getPath(), sp.getKeystoreFile()).getPath();
        final String trustStoreFile =
            new File(securityDir.getPath(), sp.getTruststoreFile()).getPath();
        final String certFile =
            new File(securityDir.getPath(), certFileName).getPath();
        final String ctsFile =
            new File(securityDir.getPath(), FileNames.CLIENT_TRUSTSTORE_FILE)
            .getPath();

        try {
            final String keyAlg = props.getProperty(KEY_KEY_ALGORITHM,
                                                    KEY_ALGORITHM_DEFAULT);
            final String keySize = props.getProperty(KEY_KEY_SIZE,
                                                     KEY_SIZE_DEFAULT);
            final String dname = props.getProperty(KEY_DISTINGUISHED_NAME,
                                                   DISTINGUISHED_NAME_DEFAULT);
            final String keyAlias = props.getProperty(KEY_KEY_ALIAS,
                                                      KEY_ALIAS_DEFAULT);
            final String keyStoreType = props.getProperty(KEYSTORE_TYPE,
                                                          KS_TYPE_DEFAULT);
            final String trustStoreType = props.getProperty(KEYSTORE_TYPE,
                                                            KS_TYPE_DEFAULT);

            /*
             * TODO: converting to String here introduces some security risk.
             * Consider changing the keytool invocation to respond directly to
             * the password prompt rather an converting to String and sticking
             * on the command line.  In the meantime, this is a relatively low
             * security risk since it is only used in one-shot setup commands.
             */
            final String keyStorePasswordStr = new String(keyStorePassword);
            final String validityDays = props.getProperty(KEY_VALIDITY,
                                                          VALIDITY_DEFAULT);

            final String[] keyStoreCmds = new String[] {
                "keytool",
                "-genkeypair",
                "-keystore", keyStoreFile,
                "-storetype", keyStoreType,
                "-storepass", keyStorePasswordStr,
                "-keypass", keyStorePasswordStr,
                "-alias", keyAlias,
                "-dname", dname,
                "-keyAlg", keyAlg,
                "-keysize", keySize,
                "-validity", validityDays };
            int result = runCmd(keyStoreCmds);
            if (result != 0) {
                System.err.println(
                    "Error creating keyStore: return code " + result);
                return false;
            }

            final String[] exportCertCmds = new String[] {
                "keytool",
                "-export",
                "-file", certFile,
                "-keystore", keyStoreFile,
                "-storetype", keyStoreType,
                "-storepass", keyStorePasswordStr,
                "-alias", keyAlias };
            result = runCmd(exportCertCmds);

            if (result != 0) {
                System.err.println(
                    "Error exporting certificate: return code " + result);
                return false;
            }

            @Nullable String ctsType;
            try {
                /*
                 * We will re-use the keystore password for the truststore
                 */
                final String[] importCertCmds = new String[] {
                    "keytool",
                    "-import",
                    "-file", certFile,
                    "-keystore", trustStoreFile,
                    "-storetype", trustStoreType,
                    "-storepass", keyStorePasswordStr,
                    "-noprompt" };
                result = runCmd(importCertCmds);

                if (result != 0) {
                    if (!EmbeddedMode.isEmbedded()) {
                        System.err.println(
                            "Error importing certificate to trustStore: " +
                            "return code " + result);
                    }
                    return false;
                }

                /*
                 * use default mykey as certificate alias in client.trust
                 * as the previous releases
                 */
                ctsType = initClientTrust(
                    "mykey", ctsFile, certFile, keyStoreType, ctsPwd,
                    keyStorePasswordStr);
                if (ctsType == null) {
                    return false;
                }
            } finally {
                /* Delete the cert file when done - we no longer need it */
                new File(certFile).delete();
            }

            initClientSecurity(sp, securityDir, ctsType);
            makeOwnerOnlyWriteAccess(new File(keyStoreFile));
            makeOwnerOnlyWriteAccess(new File(trustStoreFile));
            makeOwnerOnlyWriteAccess(new File(ctsFile));
        } catch (IOException ioe) {
            if (!EmbeddedMode.isEmbedded()) {
                System.err.println("IO error encountered: " +
                    ioe.getMessage());
            }
            return false;
        }

        return true;
    }

    /**
     * Build a security file that captures the salient bits that
     * the customer needs in order to connect to the KVStore.
     *
     * @param sp security parameters
     * @param secDir security configuration directory
     * @param ctsType store type of client.trust or null if not specified
     * @throws IOException if writing to file failed
     */
    public static void initClientSecurity(SecurityParams sp,
                                          File secDir,
                                          @Nullable String ctsType)
        throws IOException {
        final Properties securityProps = sp.getClientAccessProps();
        initClientSecurity(securityProps, secDir, ctsType);
    }

    private static void initClientSecurity(Properties securityProps,
                                           File secDir,
                                           @Nullable String ctsType)
        throws IOException {

        final File securityFile =
            new File(secDir.getPath(), FileNames.CLIENT_SECURITY_FILE);

        /*
         * The client access properties have a trustStore setting that
         * references the store.trust file.  Update it to reference the
         * client.trust file.
         */
        final @Nullable String trustStoreRef =
            securityProps.getProperty(SSLConfig.TRUSTSTORE_FILE);
        if (trustStoreRef != null) {
            securityProps.put(SSLConfig.TRUSTSTORE_FILE,
                              FileNames.CLIENT_TRUSTSTORE_FILE);
        }
        if (ctsType != null) {
            securityProps.put(SSLConfig.TRUSTSTORE_TYPE, ctsType);
        }

        /* remove server-only oracle.kv.ssl.keyStoreType */
        securityProps.remove(SSLConfig.KEYSTORE_TYPE);

        final String securityComment =
            "Security property settings for communication with " +
            "KVStore servers";
        ConfigUtils.storeProperties(securityProps, securityComment,
                                    securityFile);
    }

    /*
     * Initialize a Java TrustStore client.trust to be used by client
     * applications. A PKCS12 password-less store is created by default
     * if no password of is specified. If the Java running this doesn't
     * support PKCS12 password-less store, fall back to create a JKS
     * TrustStore instead and print a warning.
     *
     * Return the store type of the TrustStore has been created, return
     * null if there is problem creating the TrustStore.
     */
    private static
        @Nullable String initClientTrust(String certAlias,
                                         String ctsFile,
                                         String certFile,
                                         String ctsType,
                                         char @Nullable[] ctsPassword,
                                         @Nullable String ksPassword) {
        try (FileInputStream fis = new FileInputStream(certFile);
             FileOutputStream fos = new FileOutputStream(ctsFile)) {

            if (isPasswordLessPKCS12(ctsType, ctsPassword)) {
                /*
                 * System properties required to create
                 * password-less PKCS12 TrustStore
                 *
                 * TODO: There is an enhancement in Java 18 but not
                 * backported to Java 11 and 17 yet, which eliminate
                 * the use of security property. Update after that
                 * is available. JDK-8231107
                 */
                System.setProperty(PKCS12_CERT_PROTECTION_ALG, "NONE");
                System.setProperty(PKCS12_MAC_ALG, "NONE");
            }

            if (ctsPassword == null && !isPKCS12(ctsType)) {
                /*
                 * As previous releases, JKS client.trust uses the same
                 * password as store.trust if not specified. It's not
                 * required to provide a password to load JKS client.trust
                 * by client applications anyways.
                 */
                ctsPassword = stringToChars(ksPassword);
            }

            KeyStore cts = KeyStore.getInstance(ctsType);
            cts.load(null, null);

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Certificate cert = cf.generateCertificate(fis);
            cts.setCertificateEntry(certAlias, cert);
            cts.store(fos, ctsPassword);
        } catch (Exception e) {
            if (!EmbeddedMode.isEmbedded()) {
                StringBuilder sb = new StringBuilder();
                sb.append("Error creating " + ctsType + " client.trust: ")
                .append(e.getMessage());

                if (e instanceof IllegalArgumentException &&
                    isPasswordLessPKCS12(ctsType, ctsPassword)) {

                    /*
                     * Java releases that don't support password-less
                     * PKCS12 store would throw a generic IAE, adding a note
                     * in exception message to warn user.
                     */
                    sb.append(".\nWARNING: Failed to create a PKCS12 ")
                    .append("client.trust without password, falling back ")
                    .append("to create it as a JKS KeyStore. To create ")
                    .append("as a password-less PKCS12 KeyStore, ")
                    .append("upgrade Java to versions that support ")
                    .append("security properties '")
                    .append(PKCS12_CERT_PROTECTION_ALG)
                    .append("' and '").append(PKCS12_MAC_ALG)
                    .append("' or specify a password via -ctspwd")
                    .append(" to create client.trust as a ")
                    .append("password-protected PKCS12 KeyStore.");

                    System.err.println(sb.toString());
                    return initClientTrust(certAlias, ctsFile, certFile,
                                           JKS_TYPE, ctsPassword, ksPassword);
                }
                System.err.println(sb.toString());
            }
            return null;
        } finally {
            if (isPasswordLessPKCS12(ctsType, ctsPassword)) {
                System.clearProperty(PKCS12_CERT_PROTECTION_ALG);
                System.clearProperty(PKCS12_MAC_ALG);
            }
        }
        return ctsType;
    }

    private static char @Nullable[] stringToChars(@Nullable String string) {
        return (string != null) ? string.toCharArray() : null;
    }

    public static boolean isJKS(String storeType) {
        return JKS_TYPE.equalsIgnoreCase(storeType);
    }

    public static boolean isPKCS12(String storeType) {
        return PKCS12_TYPE.equalsIgnoreCase(storeType);
    }

    public static boolean isPasswordLessPKCS12(String storeType,
                                               char @Nullable[] password) {
        return isPKCS12(storeType) &&
               (password == null || password.length == 0);
    }

    /**
     * Update type of Java KeyStore in a security configuration directory.
     *
     * Server key and trust store are updated using 'keytool -importkeystore'
     * command directly. The command convert the KeyStores to given type and
     * make the a backup of original stores named with '.old' suffix. The
     * updated store will use the same password of original store. The type
     * of server key and trust store are also updated to PKCS12 in security.xml.
     *
     * Client trust store (client.trust), will be re-created as a PKCS12
     * password-less store by default if no password of client.trust is
     * specified. If the Java running this this tool doesn't support PKCS12
     * password-less store, fall back to create a JKS client.trust instead.
     *
     * After update, client trust store, server key and trust store in given
     * security configuration directory will be updated to PKCS12. The original
     * KeyStores are also kept with suffix '.old' as backups, but the doc should
     * also warn users to make a backup of the whole directory before update.
     *
     * @param secDir security configuration directory
     * @param ksType store type to update, now only support PKCS12
     * @param ctsPwd password for client.trust or null
     * @return true if update succeed, false otherwise
     */
    public static boolean updateKeyStoreType(File secDir,
                                             String ksType,
                                             char @Nullable[] ctsPwd) {
        if (!isPKCS12(ksType)) {
            System.err.println("Only support updating store type to PKCS12");
            return false;
        }
        final SecurityParams sp = loadSecurityParams(secDir);
        if (!isJKS(sp.getKeystoreType()) || !isJKS(sp.getTruststoreType())) {
            System.err.println("Only support updating JKS KeyStores to PKCS12");
            return false;
        }

        /*
         * TODO: converting to String here introduces some security risk.
         * Consider changing the keytool invocation to respond directly to
         * the password prompt rather an converting to String and sticking
         * on the command line.  In the meantime, this is a relatively low
         * security risk since it is only used in one-shot setup commands.
         */
        final @Nullable String ksPwd =
            charsToString(retrieveKeystorePassword(sp));

        return createClientTrustFromServerTrust(sp, secDir, ksPwd, ctsPwd) &&
               updateTrustStorePKCS12(sp, secDir, ksPwd) &&
               updateKeyStorePKCS12(sp, secDir, ksPwd) &&
               updateParamsPKCS12(sp, secDir);
    }

    private static @Nullable String charsToString(char @Nullable[] chars) {
        return (chars != null) ? new String(chars) : null;
    }

    /*
     * Update type of server key store to PKCS12
     */
    private static boolean updateKeyStorePKCS12(SecurityParams sp,
                                                File secDir,
                                                @Nullable String ksPwd) {
        final String keyStoreFile = new File(
            secDir.getPath(), sp.getKeystoreFile()).getPath();
        final File backupKeyStoreFile = new File(
            secDir.getPath(), sp.getKeystoreFile() + BACKUP_FILE_SUFFIX);
        if (backupKeyStoreFile.exists()) {
            System.err.println(
                "Unable to update the type of the Java KeyStore because " +
                "a backup copy of the server KeyStore already exists in " +
                secDir + ". Retry after removing or renaming file: " +
                backupKeyStoreFile);
            return false;
        }

        return migrateToPKCS12(keyStoreFile, ksPwd);
    }

    /*
     * Update type of server trust store to PKCS12
     */
    private static boolean updateTrustStorePKCS12(SecurityParams sp,
                                                  File secDir,
                                                  @Nullable String ksPwd) {
        final String trustStoreFile = new File(
            secDir.getPath(), sp.getTruststoreFile()).getPath();
        final File backupTrustStoreFile = new File(
            secDir.getPath(), sp.getTruststoreFile() + BACKUP_FILE_SUFFIX);

        if (backupTrustStoreFile.exists()) {
            System.err.println(
                "Unable to update the type of Java TrustStore because " +
                "a backup copy of the server TrustStore already exists in " +
                secDir + ". Retry after removing or renaming file: " +
                backupTrustStoreFile);
            return false;
        }

        return migrateToPKCS12(trustStoreFile, ksPwd);
    }

    private static boolean migrateToPKCS12(String ksFile,
                                           @Nullable String ksPwd) {
        try {
            /*
             * This keytool command migrate the keystore to PKCS12. Name
             * of the migrated keystore remains the same. The old keystore
             * will be backed up as "<ksFile>.old".
             */
            String[] migrateCmds = new String[] {
                "keytool",
                "-importkeystore",
                "-srckeystore", ksFile,
                "-destkeystore", ksFile,
                "-deststoretype", PKCS12_TYPE,
                "-srcstorepass", ksPwd,
                "-deststorepass", ksPwd};
            int result = runCmd(migrateCmds);
            if (result != 0) {
                System.err.println(
                    "Error migrating " + ksFile +
                    ": return code " + result);
                return false;
            }
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        }
        return true;
    }

    /*
     * Create client.trust from server trust store in security
     * configuration directory, also generate a new security
     * properties file based on the store type of client.trust.
     */
    private static
        boolean createClientTrustFromServerTrust(SecurityParams sp,
                                                 File secDir,
                                                 @Nullable String ksPwd,
                                                 char @Nullable[] ctsPwd) {
        try {
            createClientTrustFromServerTrust(
                sp.getTruststoreFile(), sp.getTruststoreType(),
                sp.getClientAccessProps(), secDir, ksPwd, ctsPwd);
            return true;
        } catch (IllegalStateException e) {
            System.err.println(e.getMessage());
            return false;
        }
    }

    private static
        void createClientTrustFromServerTrust(String trustStoreFile,
                                              String trustStoreType,
                                              Properties securityProps,
                                              File secDir,
                                              @Nullable String ksPwd,
                                              char @Nullable[] ctsPwd) {
        trustStoreFile = new File(secDir.getPath(), trustStoreFile).getPath();
        final File clientTrustFile = new File(
            secDir.getPath(), FileNames.CLIENT_TRUSTSTORE_FILE);

        final @Nullable Enumeration<String> certs = listKeystore(
            new File(trustStoreFile), ksPwd);
        if (certs == null) {
            throw new IllegalStateException(
                "Failed to create client.trust, unable to load " +
                trustStoreFile);
        }

        /* The file to hold the temporary cert */
        final String certFileName = TEMP_CERT_FILE;
        final String certFile = new File(
            secDir.getPath(), certFileName).getPath();

        @Nullable File newClientTrustFile = null;
        try {
            /* The file to hold the backup client.trust */
            final File backupClientTrustFile = new File(
                secDir.getPath(),
                FileNames.CLIENT_TRUSTSTORE_FILE + BACKUP_FILE_SUFFIX);
            if (!backupClientTrustFile.createNewFile()) {
                throw new IllegalStateException(
                    "Unable to update type of Java KeyStore, " +
                    "the backup files of existing KeyStores exists in " +
                    secDir + ", remove or rename existing " +
                    backupClientTrustFile);
            }

            /*
             * The file client.trust.new to hold the updated client.trust
             * temporarily, will be copied to client.trust and deleted.
             */
            newClientTrustFile = new File(
                secDir.getPath(),
                FileNames.CLIENT_TRUSTSTORE_FILE + ".new");
            if (!newClientTrustFile.createNewFile()) {
                throw new IllegalStateException(
                    "Unable to update type of " +
                    FileNames.CLIENT_TRUSTSTORE_FILE +
                    "in, remove or rename existing " + newClientTrustFile);
            }

            /*
             * Keytool command doesn't support creating password-less PKCS12
             * store without updating java.security file. It's also possible
             * that client.trust doesn't exist in security configuration
             * directory. Do not update the store type of client.trust via
             * keytool, but make a standard backup and create a new PKCS12
             * client.trust from store.trust.
             */
            if (clientTrustFile.exists()) {
                copyOwnerWriteFile(clientTrustFile, backupClientTrustFile);
            }

            boolean init = false;
            @Nullable String ctsType = PKCS12_TYPE;
            while (certs.hasMoreElements()) {
                final String certAlias = certs.nextElement();
                final String[] exportCertCmds = new String[] {
                    "keytool",
                    "-export",
                    "-file", certFile,
                    "-keystore", trustStoreFile,
                    "-storetype", trustStoreType,
                    "-storepass", ksPwd,
                    "-alias", certAlias };
                int result = runCmd(exportCertCmds);

                if (result != 0) {
                    throw new IllegalStateException(
                        "Error exporting certificate from " + trustStoreFile +
                        ": return code " + result);
                }

                if (!init) {
                    ctsType = initClientTrust(certAlias,
                        newClientTrustFile.getPath(), certFile, ctsType,
                        ctsPwd, ksPwd);

                    if (ctsType == null) {
                        throw new IllegalStateException(
                            "Failed to create client.trust");
                    }

                    /*
                     * client.trust fall back to JKS store use the
                     * password of the server trust as before
                     */
                    if (isJKS(ctsType)) {
                        ctsPwd = stringToChars(ksPwd);
                    }
                    init = true;
                } else {
                    List<String> importCertCmds = new ArrayList<>();
                    importCertCmds.add("keytool");
                    importCertCmds.add("-import");
                    importCertCmds.add("-file");
                    importCertCmds.add(certFile);
                    importCertCmds.add("-alias");
                    importCertCmds.add(certAlias);
                    importCertCmds.add("-keystore");
                    importCertCmds.add(newClientTrustFile.getPath());
                    importCertCmds.add("-storetype");
                    importCertCmds.add(ctsType);
                    importCertCmds.add("-noprompt");

                    if (ctsPwd != null) {
                        importCertCmds.add("-storepass");
                        importCertCmds.add(charsToString(ctsPwd));
                    }

                    result = runCmd(importCertCmds.toArray(new String[0]));
                    if (result != 0) {
                        throw new IllegalStateException(
                            "Error importing certificate to " +
                            newClientTrustFile + ": return code " + result);
                    }
                }
            }

            initClientSecurity(securityProps, secDir, ctsType);
            copyOwnerWriteFile(newClientTrustFile, clientTrustFile);
        } catch (IOException ioe) {
            throw new IllegalStateException(
                "I/O error encountered: " + ioe.getMessage(), ioe);
        } finally {
            /* Remove temporary files but keep the backups */
            new File(certFile).delete();

            if (newClientTrustFile != null) {
                newClientTrustFile.delete();
            }
        }
    }

    private static boolean updateParamsPKCS12(SecurityParams sp, File secDir) {
        final File secXmlFile = new File(
            secDir, FileNames.SECURITY_CONFIG_FILE);
        final ParameterMap pmap = sp.getMap();
        pmap.setParameter(ParameterState.SEC_KEYSTORE_TYPE, PKCS12_TYPE);
        pmap.setParameter(ParameterState.SEC_TRUSTSTORE_TYPE, PKCS12_TYPE);
        ConfigUtils.createSecurityConfig(sp, secXmlFile);
        return true;
    }

    /**
     * Update the security parameters in the given security directory.
     * @param secDir security configuration directory
     * @param params list of security parameters to update
     * @throws IllegalStateException if an error occurs in the update process
     */
    public static void updateSecurityParams(File secDir,
                                            List<ParamSetting> params) {
        final File secXmlFile =
            new File(secDir, FileNames.SECURITY_CONFIG_FILE);
        if (!secXmlFile.exists()) {
            throw new IllegalStateException(
                "security.xml file does not exist, " +
                "cannot update the security parameters");
        }

        /* Get original security parameters */
        final SecurityParams sp = loadSecurityParams(secDir);
        applyParamsChanges(sp, params);
        ConfigUtils.createSecurityConfig(sp,secXmlFile);
    }

    public static void applyParamsChanges(SecurityParams sp,
                                          List<ParamSetting> paramSettings) {
        final ParameterMap pmap = sp.getMap();
        for (ParamSetting setting : paramSettings) {
            final ParameterState pstate = setting.getParameterState();
            if (pstate.appliesTo(Info.TRANSPORT)) {
                if (setting.getTransportName() == null) {
                    for (ParameterMap tmap : sp.getTransportMaps()) {
                        tmap.setParameter(setting.getParamName(),
                                          setting.getParamValue());
                    }
                } else {
                    final ParameterMap tmap =
                        sp.getTransportMap(setting.getTransportName());
                    tmap.setParameter(setting.getParamName(),
                                      setting.getParamValue());
                }
            } else {
                pmap.setParameter(setting.getParamName(),
                                  setting.getParamValue());
            }
        }
    }

    /**
     * Merges the trust information from srcSecDir into updateSecDir.
     * @param srcSecDir a File reference to the security directory from which
     *   trust information will be extracted
     * @param updateSecDir a File reference to the security directory into
     *   which trust information will be merged
     * @param ctsPwd password of client.trust after update or null
     * @return true if the merge was successful and false otherwise
     */
    public static boolean mergeTrust(File srcSecDir,
                                     File updateSecDir,
                                     char @Nullable[] ctsPwd) {

        /* Get sour and dest security parameters */
        final SecurityParams srcSp = loadSecurityParams(srcSecDir);
        final SecurityParams updateSp = loadSecurityParams(updateSecDir);

        final String srcTruststoreType = srcSp.getTruststoreType();
        final String updateTruststoreType = updateSp.getTruststoreType();

        /*
         * The existing configuration is already created with PKCS12 stores,
         * but users are attempting to merge and update with a configuration
         * with JKS stores. Since we're making PKCS12 as default, there is
         * no reason to revert via merging trust.
         */
        if (isPKCS12(updateTruststoreType) && isJKS(srcTruststoreType)) {
            System.err.println(
                "The new security configuration in " + srcSecDir +
                " was created with JKS KeyStores, but the existing security" +
                " configuration in " + updateSecDir + " has PKCS12" +
                " KeyStores. Changing the type of an existing KeyStore from" +
                " PKCS12 to JKS is not supported. Convert the new" +
                " configuration to PKCS12 before retrying the operation.");
            return false;
        }

        /* Get source truststore info */
        final String srcTrustFile =
            new File(srcSecDir, srcSp.getTruststoreFile()).getPath();
        /*
         * TODO: converting to String here introduces some security risk.
         * Consider changing the keytool invocation to respond directly to
         * the password prompt rather an converting to String and sticking
         * on the command line.  In the meantime, this is a relatively low
         * security risk since it is only used in one-shot setup commands.
         */
        final @Nullable String srcTruststorePwd =
            charsToString(retrieveKeystorePassword(srcSp));

        final @Nullable Enumeration<String> stEntries =
            listKeystore(new File(srcTrustFile), srcTruststorePwd);
        if (stEntries == null) {
            System.err.println("Failed to merge trust, unable to load " +
                               srcTrustFile);
            return false;
        }

        /* Get dest truststore info */
        final String updateTrustFile =
            new File(updateSecDir, updateSp.getTruststoreFile()).getPath();

        /*
         * TODO: converting to String here introduces some security risk.
         * Consider changing the keytool invocation to respond directly to
         * the password prompt rather an converting to String and sticking
         * on the command line.  In the meantime, this is a relatively low
         * security risk since it is only used in one-shot setup commands.
         */
        final @Nullable String updateTruststorePwd =
            charsToString(retrieveKeystorePassword(updateSp));
        final @Nullable Enumeration<String> utEntries =
            listKeystore(new File(updateTrustFile), updateTruststorePwd);
        if (utEntries == null) {
            System.err.println("Failed to merge trust, unable to load " +
                               updateTrustFile);
            return false;
        }

        /*
         * Convert the to-be-updated list to a set of alias names for
         * ease of later access.
         */
        final Set<String> utAliasSet = new HashSet<String>();
        utEntries.asIterator().forEachRemaining(e -> {utAliasSet.add(e);});

        /* The file to hold the temporary cert */
        final String certFileName = TEMP_CERT_FILE;
        final String certFile =
            new File(srcSecDir.getPath(), certFileName).getPath();

        try {
            while (stEntries.hasMoreElements()) {
                String alias = stEntries.nextElement();
                final String[] exportCertCmds = new String[] {
                    "keytool",
                    "-export",
                    "-file", certFile,
                    "-keystore", srcTrustFile,
                    "-storetype", srcTruststoreType,
                    "-storepass", srcTruststorePwd,
                    "-alias", alias };
                int result = runCmd(exportCertCmds);

                if (result != 0) {
                    System.err.println(
                        "Error exporting certificate: return code " + result);
                    return false;
                }

                /*
                 * Determine an available alias
                 */
                if (utAliasSet.contains(alias)) {
                    int i = 2;
                    while (true) {
                        final String tryAlias = alias + "_" + i;
                        if (!utAliasSet.contains(tryAlias)) {
                            alias = tryAlias;
                            break;
                        }
                        i++;
                    }
                }
                utAliasSet.add(alias);

                final String[] importCertCmds = new String[] {
                    "keytool",
                    "-import",
                    "-file", certFile,
                    "-alias", alias,
                    "-keystore", updateTrustFile,
                    "-storetype", updateTruststoreType,
                    "-storepass", updateTruststorePwd,
                    "-noprompt" };
                result = runCmd(importCertCmds);

                if (result != 0) {
                    System.err.println(
                        "Error importing certificate to trustStore: " +
                        "return code " + result);
                    return false;
                }
            }
        } catch (IOException ioe) {
            System.err.println(
                "Exception " + ioe + " while merging truststore files");
            return false;
        }

        /*
         * Existing trust store to be updated is PKCS12 already, create
         * a client.trust from the updated server trust store.
         */
        if (isPKCS12(updateTruststoreType)) {
            return createClientTrustFromServerTrust(
                updateSp, updateSecDir, updateTruststorePwd, ctsPwd);
        }

        if (isJKS(srcTruststoreType)) {

            /*
             * Both source and update trust store is JKS, just copy the new
             * trust store file to a client.trust file so that the two are
             * consistent.
             */
            final File srcFile = new File(updateTrustFile);
            final File dstFile =
                new File(updateSecDir, FileNames.CLIENT_TRUSTSTORE_FILE);
            try {
                SecurityUtils.copyOwnerWriteFile(srcFile, dstFile);
            } catch (IOException ioe) {
                System.err.println(
                    "Exception " + ioe + " while copying " + srcFile +
                    " to " + dstFile);
                return false;
            }

            return true;
        }

        /*
         * Existing trust store to be updated is JKS but the new security
         * configuration is created with PKCS12 KeyStores, the next step of
         * updating SSL key would ask users to copy the new PKCS12 server key
         * store directly. Convert the trust store and update the security
         * parameters here to make them consistent.
         *
         * In addition, convert the key store to PKCS12 format. Before copy the
         * new server key store, user needs to restart every storage node in
         * store to load all certificates from merged trust store. Because
         * Java 8 has an issue to load JKS key store with store type as PKCS12,
         * the storage node won't be able to restart with old key store. Java 9
         * and later versions of Java have added DualFormat KeyStore, which
         * don't have this problem. [KVSTORE-1594]
         */
        return createClientTrustFromServerTrust(updateSp, updateSecDir,
                                                updateTruststorePwd, ctsPwd) &&
               updateKeyStorePKCS12(updateSp, updateSecDir,
                                    updateTruststorePwd) &&
               updateTrustStorePKCS12(updateSp, updateSecDir,
                                      updateTruststorePwd) &&
               updateParamsPKCS12(updateSp, updateSecDir);
    }

    /**
     * Print out entries in keystores of given security configuration
     * directory.
     * @param secConfigDir security configuration directory.
     * @return a string of entries information in keystores if successful,
     *         or a string describing problems reading the keystores
     */
    public static String printKeystores(File secConfigDir) {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final String keystoreFile =
            new File(secConfigDir, sp.getKeystoreFile()).getPath();
        final String truststoreFile =
            new File(secConfigDir, sp.getTruststoreFile()).getPath();

        /*
         * TODO: converting to String here introduces some security risk.
         * Consider changing the keytool invocation to respond directly to
         * the password prompt rather an converting to String and sticking
         * on the command line.  In the meantime, this is a relatively low
         * security risk since it is only used in one-shot setup commands.
         */
        final @Nullable String keystorePwd =
            charsToString(retrieveKeystorePassword(sp));
        final String keystoreType = sp.getKeystoreType();
        final String truststoreType = sp.getTruststoreType();

        return printKeystore(keystoreFile, keystoreType, keystorePwd) + "\n" +
            printKeystore(truststoreFile, truststoreType, keystorePwd);
    }

    static String printKeystore(String keystoreFile,
                                String keystoreType,
                                @Nullable String keystorePwd) {
        final StringBuilder sb = new StringBuilder();
        try {
            final String[] keyStoreCmds = new String[] {
                    "keytool",
                    "-list",
                    "-keystore", keystoreFile,
                    "-storetype", keystoreType,
                    "-storepass", keystorePwd };

            final List<String> output = new ArrayList<String>();
            final int result = runCmd(keyStoreCmds, output);
            if (result != 0) {
                sb.append("Error listing keyStore: ").append(output);
                return sb.toString();
            }
            sb.append("Keystore: " + keystoreFile + "\n");

            for (String s : output) {
                sb.append(s + "\n");
            }
            return sb.toString();
        } catch (IOException ioe) {
            sb.append("IO error encountered: ").append(ioe.getMessage());
            return sb.toString();
        }
    }

    /**
     * List the entries in a keystore (or truststore) file.
     * @param keystoreFile the keystore file
     * @param storePassword the password for the store
     * @return a list of the keystore alias if successful, or null otherwise
     */
    public static @Nullable Enumeration<String>
        listKeystore(File keystoreFile,
                     @Nullable String storePassword) {
        try {
            final KeyStore keystore = KeyStore.getInstance(
                keystoreFile, stringToChars(storePassword));
            return keystore.aliases();
        } catch (IOException ioe) {
            System.err.println("IO error encountered while listing " +
                               keystoreFile + ": " + ioe.getMessage());
        } catch (CertificateException ce) {
            System.err.println("Unable to load certificates from " +
                               keystoreFile + ": " + ce.getMessage());
        } catch (KeyStoreException kse) {
            System.err.println("Problem loading keystore " +
                               keystoreFile + ": " + kse.getMessage());
        } catch (NoSuchAlgorithmException nsae) {
            System.err.println("Unable to find the algorithm used to " +
                               "check integrity of " + keystoreFile +
                               ": " + nsae.getMessage());
        }
        return null;
    }

    /**
     * Make a copy of a file where the resulting copy should be writable only
     * by the owner with read privilege determined by system policy
     * (i.e. umask).
     * @param srcFile a file to be copied
     * @param destFile the destination file
     * @throws IOException if an error occurs in the copy process
     */
    public static void copyOwnerWriteFile(File srcFile, File destFile)
        throws IOException {

        FileUtils.copyFile(srcFile, destFile);
        SecurityUtils.makeOwnerOnlyWriteAccess(destFile);
    }

    /**
     * Create store service principal and extract keytab file.
     *
     * @param securityDir the directory in which the keytab will be created.
     * @param sp a SecurityParams instance containing information regarding
     *        the store service principal and keytab file names
     * @param kadminSetting kadmin settings
     * @param props a set of optional settings that can alter the
     *        principal creation, or null
     * @param ioHelper I/O helper class used to read kadmin password
     * @return true if the generation process was successful and false
     *         if an error occurred.
     */
    public static boolean generateKeyTabFile(File securityDir,
                                             SecurityParams sp,
                                             KadminSetting kadminSetting,
                                             @Nullable Properties props,
                                             IOHelper ioHelper) {
        if (props == null) {
            props = new Properties();
        }

        final String keytabFile = new File(
            securityDir.getPath(), sp.getKerberosKeytabFile()).getPath();

        try {
            final String princName = sp.getKerberosServiceName();
            final String validityDays = props.getProperty(PRINCIPAL_VALIDITY);
            final String keysaltList = props.getProperty(KEYSALT_LIST);
            final String pwdExpire = props.getProperty(PRINCIPAL_PWD_EXPIRE);
            final String instance = sp.getKerberosInstanceName();
            final String realm = sp.getKerberosRealmName();
            final String principal = (instance != null) ?
                                     princName + "/" + instance :
                                     princName;

            final List<String> kadminCmdsList =
                generateKadminCmds(kadminSetting, realm);

            /* Add store service principal */
            final String addPrincCmds = "add_principal" +
                " -expire " + validityDays +
                " -pwexpire " + pwdExpire +
                " -randkey " + "\"" + principal + "\"";
            kadminCmdsList.add(addPrincCmds);

            System.out.println("Adding principal " + principal);
            int result = runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error adding service principal: return code " + result);
                return false;
            }
            kadminCmdsList.remove(kadminCmdsList.size() - 1);

            /* Extract keytab of service principal */
            System.out.println("Extracting keytab " + keytabFile);
            final String extractKeytabCmds = "ktadd" +
                " -k " + keytabFile +
                " -e " + keysaltList +
                " " + "\"" + principal + "\"";
            kadminCmdsList.add(extractKeytabCmds);
            result = runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error extracting keytab file: return code " + result);
                return false;
            }

            makeOwnerOnlyWriteAccess(new File(keytabFile));
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Renew keytab file in given security directory.
     *
     * @param secDir security directory
     * @param keysaltList keysalt list used to generate new keytab file or null
     *        if not specified
     * @param kadminSetting settings for connecting kadmin
     * @param ioHelper I/O helper class to read kadmin password
     * @return true if the renew process was successful and false
     *         if an error occurred.
     */
    public static boolean renewKeytab(File secDir,
                                      @Nullable String keysaltList,
                                      KadminSetting kadminSetting,
                                      IOHelper ioHelper) {
        final SecurityParams sp = loadSecurityParams(secDir);
        final File keytabFile = new File(secDir, sp.getKerberosKeytabFile());
        if (!keytabFile.exists()) {
            System.err.println("keytab " + keytabFile + " does not exist");
            return false;
        }
        final String principal = SecurityUtils.getCanonicalPrincName(sp);
        @Nullable File tmpKeytab = null;
        try {
            final List<String> kadminCmdsList =
                generateKadminCmds(kadminSetting, sp.getKerberosRealmName());
            if (keysaltList == null) {
                keysaltList = KEYSALT_LIST_DEFAULT;
            }

            /*
             * Create a temporary file to store the new keys of principal,
             * so that old key can be reserved in case of extracting keytab
             * error.
             *
             * Using createTempFile method to create temporary file then
             * remove it and only keep the file name, since ktadd command
             * cannot store keys to a file generated by Java.
             */
            final File keytab = File.createTempFile("tmp", ".keytab");
            tmpKeytab = keytab;
            if (!keytab.delete()) {
                System.err.println("Error generating a temporary keytab file");
                return false;
            }
            final String extractKeytabCmds = "ktadd" +
                " -k " + keytab.getAbsolutePath() +
                " -e " + keysaltList +
                " " + principal;
            kadminCmdsList.add(extractKeytabCmds);
            final int result =
                runKadminCmd(kadminSetting, ioHelper, kadminCmdsList);
            if (result != 0) {
                System.err.println(
                    "Error extracting keytab file: return code " + result);
                return false;
            }
            if (!keytabFile.delete()) {
                System.err.println("Old keytab " + keytabFile +
                                   " cannot be deleted");
                return false;
            }
            if (!keytab.renameTo(keytabFile)) {
                System.err.println("keytab " + keytab +
                                   " cannot be renamed as " + keytabFile);
                return false;
            }
            makeOwnerOnlyWriteAccess(keytabFile);
        } catch (IOException ioe) {
            System.err.println("IO error encountered: " + ioe.getMessage());
            return false;
        } finally {
            if (tmpKeytab != null &&
                tmpKeytab.exists() &&
                !tmpKeytab.delete()) {
                System.err.println("Temporary keytab " + tmpKeytab +
                                   " cannot be deleted");
                return false;
            }
        }
        return true;
    }

    /**
     * Check given authentication method name is Kerberos.
     */
    public static boolean isKerberos(@Nullable String authMethod) {
        if (authMethod == null) {
            return false;
        }
        return authMethod.equalsIgnoreCase(KERBEROS_AUTH_NAME);
    }

    /**
     * Check given authentication methods in format "authMethod1,authMethod2"
     * contains Kerberos.
     */
    public static boolean hasKerberos(@Nullable String authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods.split(",")) {
            if (isKerberos(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication method name is IDCS OAuth.
     */
    private static boolean isIDCSOAuth(@Nullable String authMethod) {
        if (authMethod == null) {
            return false;
        }
        return authMethod.equalsIgnoreCase(OAUTH_AUTH_NAME);
    }

    /**
     * Check given authentication methods in format "authMethod1,authMethod2"
     * contains IDCS OAuth.
     */
    public static boolean hasIDCSOAuth(@Nullable String authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods.split(",")) {
            if (isIDCSOAuth(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication methods array contains element of IDCS OAuth.
     */
    public static boolean hasIDCSOAuth(String @Nullable[] authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods) {
            if (isIDCSOAuth(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check given authentication methods array contains element of Kerberos.
     */
    public static boolean hasKerberos(String @Nullable[] authMethods) {
        if (authMethods == null) {
            return false;
        }

        for (String method : authMethods) {
            if (isKerberos(method)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return canonical Kerberos service principal.
     */
    public static String getCanonicalPrincName(SecurityParams secParams) {
        final StringBuilder sb = new StringBuilder();
        sb.append(secParams.getKerberosServiceName());

        @Nullable String instanceName = secParams.getKerberosInstanceName();
        if (instanceName != null && !instanceName.equals("")) {
            sb.append(KRB_NAME_COMPONENT_SEPARATOR_STR);
            sb.append(secParams.getKerberosInstanceName());
        }

        if (!secParams.getKerberosRealmName().equals("")) {
            sb.append(KRB_NAME_REALM_SEPARATOR_STR);
            sb.append(secParams.getKerberosRealmName());
        }
        return sb.toString();
    }

    private static List<String> generateKadminCmds(KadminSetting kadminSetting,
                                                   String defaultRealm) {
        final List<String> kadminCmdsList = new ArrayList<String>();
        kadminCmdsList.add(kadminSetting.getKrbAdminPath());
        kadminCmdsList.add("-r");
        kadminCmdsList.add(defaultRealm);

        if (kadminSetting.useKeytab()) {
            kadminCmdsList.add("-k");
            kadminCmdsList.add("-t");
            kadminCmdsList.add(kadminSetting.getKrbAdminKeytab());
            System.out.println(String.format(
                "Login Kerberos admin via keytab %s with %s",
                kadminSetting.getKrbAdminKeytab(),
                kadminSetting.getKrbAdminPrinc()));
        } else if (kadminSetting.useCcache()) {
            kadminCmdsList.add("-c");
            kadminCmdsList.add(kadminSetting.getKrbAdminCcache());
            System.out.println(String.format(
                "Login Kerberos admin via credential cache %s with %s",
                kadminSetting.getKrbAdminCcache(),
                kadminSetting.getKrbAdminPrinc()));
        }

        if (kadminSetting.getKrbAdminPrinc() != null) {
            kadminCmdsList.add("-p");
            kadminCmdsList.add(kadminSetting.getKrbAdminPrinc());
        }
        kadminCmdsList.add("-q");
        return kadminCmdsList;
    }

    /**
     * The class maintain Kerberos V5 configuration information that retrieved
     * from user specified configuration files. Copy the approach used by the
     * Java internal Kerberos parsing code to retrieve and validate default
     * realm and kdc, which is required for NoSQL Kerberos authentication.
     * The rest of configuration parameters are validated by Java Kerberos
     * login module when performing the actual authentication.
     */
    public static class Krb5Config {
        private File configFile;
        private @Nullable String defaultRealm;
        private @Nullable String realmKdc;

        public Krb5Config(File krb5ConfFile) {
            configFile = krb5ConfFile;
        }

        /**
         * Parse krb5 configuration file and identify default realm and
         * corresponding kdc.
         */
        public void parseConfigFile() throws IOException {
            final List<String> lines = loadConfigFile();
            final Map<String, String> kdcs = new HashMap<>();

            for (int i = 0; i < lines.size(); i++) {
                String line = lines.get(i).trim();

                /* Find default realm from libdefaults */
                if (line.equalsIgnoreCase("[libdefaults]")) {
                    for (int count = i + 1; count < lines.size(); count++) {
                        line = lines.get(count).trim();

                        final int equalsPos = line.indexOf('=');
                        if (equalsPos > 0) {
                            final String key =
                                line.substring(0, equalsPos).trim();

                            if (key.equalsIgnoreCase("default_realm")) {
                                defaultRealm =
                                    trimmed(line.substring(equalsPos + 1));
                            }
                        }
                        if (lines.get(count).startsWith("[")) {
                            i = count - 1;
                            break;
                        }
                    }
                } else if (line.equalsIgnoreCase("[realms]")) {
                    /* Parse all realms and cache their corresponding kdc */
                    String realm = "";

                    for (int count = i + 1; count < lines.size(); count++) {
                        line = lines.get(count).trim();
                        if (line.endsWith("{")) {
                            final int equalsPos = line.indexOf('=');
                            if (equalsPos > 0) {
                                realm = line.substring(0, equalsPos).trim();
                            }
                        } else if (!line.startsWith("}")) {
                            final int equalsPos = line.indexOf('=');
                            if (equalsPos > 0) {
                                final String key =
                                    line.substring(0, equalsPos).trim();

                                if (key.equalsIgnoreCase("kdc") &&
                                    !realm.equals("")) {
                                    /*
                                     * User can specify multiple realms in
                                     * the configuration file, cache them
                                     * firstly and find the default kdc
                                     * later.
                                     */
                                    kdcs.put(realm, trimmed(
                                        line.substring(equalsPos + 1)));
                                }
                            }
                        }

                        if (lines.get(count).startsWith("[")) {
                            i = count - 1;
                            break;
                        }
                    }
                }
            }

            if (defaultRealm != null) {
                realmKdc = kdcs.get(defaultRealm);
            }
        }

        public @Nullable String getDefaultRealm() {
            return defaultRealm;
        }

        public @Nullable String getKdc() {
            return realmKdc;
        }

        public String getConfigFilePath() {
            return configFile.getAbsolutePath();
        }

        private String trimmed(String s) {
            s = s.trim();
            if (s.charAt(0) == '"' && s.charAt(s.length()-1) == '"' ||
                s.charAt(0) == '\'' && s.charAt(s.length()-1) == '\'') {
                s = s.substring(1, s.length()-1).trim();
            }
            return s;
        }

        private List<String> loadConfigFile() throws IOException {
            final List<String> lines = new ArrayList<String>();

            try (final BufferedReader br = new BufferedReader(
                     new InputStreamReader(new FileInputStream(configFile)))) {
                String line;

                /*
                 * Cache previous line, used to resolve the case that Kerberos
                 * configuration file accepts and convert to standard format.
                 *  EXAMPLE.COM =
                 *  {
                 *      kdc = kerberos.example.com
                 *  }
                 */
                @Nullable String previous = null;

                while ((line = br.readLine()) != null) {
                    /* Ignore comments and blank lines */
                    if (!(line.startsWith("#") || line.trim().isEmpty())) {
                        String current = line.trim();

                        if (current.startsWith("{")) {
                            if (previous == null) {
                                throw new IOException(
                                     "Config file should not start with \"{\"");
                            }
                            previous += " " + current;
                        } else {
                            if (previous != null) {
                                lines.add(previous);
                            }
                            previous = current;
                        }
                    }
                }

                if (previous != null) {
                    lines.add(previous);
                }
                return lines;
            }
        }
    }

    /**
     * The class maintains the setting used to connecting kadmin utility.
     */
    public static class KadminSetting {
        private static final String NO_KADMIN = "NONE";
        private String krbAdminPath = KADMIN_DEFAULT;
        private @Nullable String krbAdminPrinc;
        private @Nullable String krbAdminKeytab;
        private @Nullable String krbAdminCcache;

        public KadminSetting setKrbAdminPath(String kadminPath) {
            this.krbAdminPath = kadminPath;
            return this;
        }

        public String getKrbAdminPath() {
            return krbAdminPath;
        }

        public KadminSetting setKrbAdminPrinc(@Nullable String kadminPrinc) {
            this.krbAdminPrinc = kadminPrinc;
            return this;
        }

        public @Nullable String getKrbAdminPrinc() {
            return krbAdminPrinc;
        }

        public KadminSetting setKrbAdminKeytab(@Nullable String adminKeytab) {
            this.krbAdminKeytab = adminKeytab;
            return this;
        }

        public @Nullable String getKrbAdminKeytab() {
            return krbAdminKeytab;
        }

        public KadminSetting setKrbAdminCcache(@Nullable String adminCcache) {
            this.krbAdminCcache = adminCcache;
            return this;
        }

        public @Nullable String getKrbAdminCcache() {
            return krbAdminCcache;
        }

        public boolean doNotPerformKadmin() {
            return krbAdminPath.equalsIgnoreCase(NO_KADMIN);
        }

        /**
         * Validate if given kadmin settings are appropriate.
         *
         * @throws IllegalArgumentException
         */
        public void validateKadminSetting()
            throws IllegalArgumentException {

            if (doNotPerformKadmin()) {
                return;
            }

            /*
             * Check if user specified admin keytab and credential cache at the
             * same time
             */
            if (krbAdminKeytab != null) {
                if (krbAdminCcache != null) {
                    throw new IllegalArgumentException(
                        "cannot use admin ketyab and credential cache together");
                }

                if (krbAdminPrinc == null) {
                    throw new IllegalArgumentException(
                        "must specify admin principal when using keytab file");
                }

                if (!new File(krbAdminKeytab).exists()) {
                    throw new IllegalArgumentException(
                        "keytab file " + krbAdminKeytab + " does not exist");
                }
            }

            /* check if kadmin ccache exists */
            if (krbAdminCcache != null && !new File(krbAdminCcache).exists()) {
                throw new IllegalArgumentException(
                    "credential cache " + krbAdminCcache + " does not exist");
            }

            /* Must specify principal if use password */
            if (krbAdminKeytab == null &&
                krbAdminCcache == null &&
                krbAdminPrinc == null) {
                throw new IllegalArgumentException("use kadmin with password " +
                    "must specify principal name");
            }
        }

        /**
         * Whether use keytab to connect kadmin utility.
         */
        public boolean useKeytab() {
            return krbAdminKeytab != null &&
                   krbAdminPrinc != null &&
                   krbAdminCcache == null;
        }

        /**
         * Whether use credential cache to connect kadmin utility.
         */
        public boolean useCcache() {
            return krbAdminCcache != null &&
                   krbAdminKeytab == null;
        }

        /**
         * Whether prompt password to connect kadmin utility.
         */
        public boolean promptPwd() {
            return krbAdminCcache == null && krbAdminKeytab == null;
        }
    }

    /**
     * Run a command in a subshell.
     * @param args an array of command-line arguments, in the format expected
     * by Runtime.exec(String[]).
     * @return the process exit code
     * @throws IOException if an IO error occurs during the exec process
     */
    static int runCmd(String[] args)
        throws IOException {
        final Process proc = Runtime.getRuntime().exec(args);

        boolean done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }
        return returnCode;
    }

    /**
     * Run kadmin-related commands. If users do not specify keytab or credential
     * cache for kadmin user, prompt for admin user password.
     *
     * @param setting settings for connecting kadmin utility
     * @param ioHelper I/O helper may be used for password prompt
     * @param args an list of command line arguments.
     * @return the process exit code
     * @throws IOException if an IO error occurs during the process execution.
     */
    private static int runKadminCmd(KadminSetting setting,
                                    IOHelper ioHelper,
                                    List<String> args)
        throws IOException {

        final List<String> output = new ArrayList<>();
        final ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        char @Nullable[] pwd = null;

        final Process proc = pb.start();
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));

        if (setting.promptPwd()) {
            final BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(proc.getOutputStream()));

            pwd = ioHelper.readPassword(
                "Password for " + setting.getKrbAdminPrinc() + ": ");
            if (pwd == null) {
                System.err.println("Failed to acquire kadmin password");
            }
            writer.write(pwd);
            SecurityUtils.clearPassword(pwd);
            writer.write("\n");
            writer.flush();
        }

        /* Read lines of input */
        boolean done = false;
        while (!done) {
            final @Nullable String s = br.readLine();
            if (s == null) {
                done = true;
            } else {
                output.add(s);
            }
        }

        /* Then get exit code */
        done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        /* Output kadmin error and std output for easier debugging */
        for (String s : output) {
            System.err.println(s);
        }
        return returnCode;
    }

    public static int runCmd(String[] args, List<String> output)
        throws IOException {

        final Process proc = Runtime.getRuntime().exec(args);
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(proc.getInputStream()));

        /* Read lines of input */
        boolean done = false;
        while (!done) {
            final @Nullable String s = br.readLine();
            if (s == null) {
                done = true;
            } else {
                output.add(s);
            }
        }

        /* Then get exit code */
        done = false;
        int returnCode = 0;
        while (!done) {
            try {
                returnCode = proc.waitFor();
                done = true;
            } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        return returnCode;
    }

    /**
     * Report whether the host parameter is an address that is local to this
     * machine. If the host is a name rather than a literal address, all
     * resolutions of the name must be local in order for the host to be
     * considered local.
     *
     * @param host either an IP address literal or host name
     * @return true it the host represents a local address
     * @throws SocketException if an IO exception occurs
     */
    public static boolean isLocalHost(String host)
        throws SocketException {

        try {
            boolean anyLocal = false;
            for (InetAddress hostAddr : InetAddress.getAllByName(host)) {
                if (isLocalAddress(hostAddr)) {
                    anyLocal = true;
                } else {
                    return false;
                }
            }
            return anyLocal;
        } catch (UnknownHostException uhe) {
            return false;
        }
    }

    /**
     * Determine whether the address portion of the InetAddress (host name is
     * ignored) is an address that is local to this machine.
     */
    private static boolean isLocalAddress(InetAddress address)
        throws SocketException {

        final Enumeration<NetworkInterface> netIfs =
            NetworkInterface.getNetworkInterfaces();
        while (netIfs.hasMoreElements()) {
            final NetworkInterface netIf = netIfs.nextElement();
            if (isLocalAddress(netIf, address)) {
                return true;
            }

            final Enumeration<NetworkInterface> subIfs =
                netIf.getSubInterfaces();
            while (subIfs.hasMoreElements()) {
                if (isLocalAddress(subIfs.nextElement(), address)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Determine whether the address portion of the InetAddress (host name is
     * ignored) is an address that is local to a network interface.
     */
    private static boolean isLocalAddress(NetworkInterface netIf,
                                          InetAddress address) {

        final Enumeration<InetAddress> addrs = netIf.getInetAddresses();
        while (addrs.hasMoreElements()) {
            final InetAddress addr = addrs.nextElement();
            if (addr.equals(address)) {
                return true;
            }
        }
        return false;
    }

    public static SecurityParams loadSecurityParams(File secDir) {
        final File secFile = new File(secDir, FileNames.SECURITY_CONFIG_FILE);
        return ConfigUtils.getSecurityParams(secFile);
    }

    private static
        char @Nullable[] retrieveKeystorePassword(SecurityParams sp)
    {
        final @Nullable KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        return (pwdSrc == null) ? null : pwdSrc.getPassword();
    }

    /**
     * Constructs a resource owner from KVStore user in current context.  Null
     * will be return if we could not detect a user principal in current
     * execution context, or the current execution context is null.
     */
    public static @Nullable ResourceOwner currentUserAsOwner() {
        final @Nullable KVStoreUserPrincipal currentUserPrinc =
            KVStoreUserPrincipal.getCurrentUser();
        if (currentUserPrinc == null) {
            return null;
        }
        return new ResourceOwner(currentUserPrinc.getUserId(),
                                 currentUserPrinc.getName());
    }

    /**
     * Return the default Kerberos principal configuration properties. These
     * properties are used whiling adding principal and extracting keytab.
     */
    public static Properties getDefaultKrbPrincipalProperties() {
        return (Properties) princDefaultProps.clone();
    }

    /**
     * Run various verifications against given security configuration directory.
     *
     * @param secConfigDir security configuration directory
     */
    public static String verifyConfiguration(File secConfigDir) {

        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final StringBuilder errors = new StringBuilder();
        /* Verify security parameters */

        /*
         * Check that JE HA, internal, and client transports are using
         * preferred protocols.
         */

        /*
         * JE HA transport does not have notion of client and server: the
         * allowed protocols applies to both of them.
         */
        try {
            final String specifiedProtocols =
                sp.getTransAllowProtocols(SECURITY_TRANSPORT_JE_HA);
            if (!checkIfProtocolsAllowed(specifiedProtocols)) {
                errors.append(
                    "Transport JE HA is not using preferred protocols.")
                    .append(" Found: ")
                    .append(specifiedProtocols)
                    .append(" Preferred protocols: ")
                    .append(PREFERRED_PROTOCOLS_DEFAULT);
            }
        } catch (IllegalArgumentException iae) {
            errors.append(
                "Problem with protocols specified for transport JE HA: ")
                .append(iae.getMessage());
        }

        /* Internal and client transport only have client allowed protocols */
        @Nullable String verifyResult = checkClientAllowedProtocols(
            SECURITY_TRANSPORT_INTERNAL, sp);
        if (verifyResult != null) {
            errors.append(verifyResult);
        }
        verifyResult = checkClientAllowedProtocols(
            SECURITY_TRANSPORT_CLIENT, sp);
        if (verifyResult != null) {
            errors.append(verifyResult);
        }

        /* Check if all transport are using the same key alias */
        final String serverInternalKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_INTERNAL);
        final String clientInternalKeyAlias =
            sp.getTransClientKeyAlias(SECURITY_TRANSPORT_INTERNAL);
        if (!serverInternalKeyAlias.equals(clientInternalKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as client ")
                .append(clientInternalKeyAlias)
                .append(".\n");
        }
        final String jeHaKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_JE_HA);
        if (!serverInternalKeyAlias.equals(jeHaKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as JE HA transport ")
                .append(jeHaKeyAlias)
                .append(".\n");
        }
        final String clientServerKeyAlias =
            sp.getTransServerKeyAlias(SECURITY_TRANSPORT_CLIENT);
        if (!serverInternalKeyAlias.equals(clientServerKeyAlias)) {
            errors.append("Key alias of internal transport server ")
                .append(serverInternalKeyAlias)
                .append(" is not the same as server for client transport ")
                .append(clientServerKeyAlias)
                .append(".\n");
        }

        /* Check if all transport are configured to allow the same identity */
        final String clientInternalAllowedIdentity =
            sp.getTransClientIdentityAllowed(SECURITY_TRANSPORT_INTERNAL);
        final String serverAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_INTERNAL);
        if (!clientInternalAllowedIdentity.equals(serverAllowedIdentity)) {
            errors.append("Identities allowed by server side of internal")
                .append(" transport ")
                .append(serverAllowedIdentity)
                .append(" are not the same as client side of internal")
                .append(" transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }
        final String jeHaAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_JE_HA);
        if (!clientInternalAllowedIdentity.equals(jeHaAllowedIdentity)) {
            errors.append("Identities allowed by JE HA transport ")
                .append(jeHaAllowedIdentity)
                .append(" are not the same as internal transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }
        final String clientAllowedIdentity =
            sp.getTransServerIdentityAllowed(SECURITY_TRANSPORT_CLIENT);
        if (!clientInternalAllowedIdentity.equals(clientAllowedIdentity)) {
            errors.append("Identities allowed by client transport ")
                .append(clientAllowedIdentity)
                .append(" are not the same as internal transport ")
                .append(clientInternalAllowedIdentity)
                .append(".\n");
        }

        /* Verify keystore and truststore installation */
        final @Nullable String result = checkKeystoreInstallation(sp);
        if (result != null) {
            errors.append(result);
        }
        return errors.toString();
    }

    /**
     * Load KeyStore from given key or trust store file.
     * @param storeName KeyStore file name
     * @param storePassword KeyStore password or null
     * @param storeFlavor the flavor of KeyStore, like keystore that contains
     * private key entries and truststore that contains certificates
     * @param storeType KeyStore type, like jks, or null if not specified
     * @return Loaded KeyStore object
     * @throws IllegalArgumentException if any errors occurs during loading
     */
    public static KeyStore loadKeyStore(String storeName,
                                        char @Nullable[] storePassword,
                                        String storeFlavor,
                                        @Nullable String storeType)
        throws IllegalArgumentException {

        return loadKeyStore(storeName, storePassword, storeFlavor, storeType,
                            null /* getHash */);
    }

    static KeyStore loadKeyStore(String storeName,
                                 char @Nullable[] storePassword,
                                 String storeFlavor,
                                 @Nullable String storeType,
                                 @Nullable AtomicReference<String> getHash) {
        final String explicitStoreType =
            (storeType == null || storeType.isEmpty()) ?
            KeyStore.getDefaultType() :
            storeType;

        final KeyStore ks;
        try {
            ks = KeyStore.getInstance(explicitStoreType);
        } catch (KeyStoreException kse) {
            throw new IllegalArgumentException(
                "Unable to find a " + storeFlavor + " instance of type " +
                    storeType, kse);
        }

        try (final FileInputStream fis = new FileInputStream(storeName)) {
            if (getHash != null) {
                final String digest =
                    computeSha256Hash(fis, is -> ks.load(is, storePassword));
                getHash.set(digest);
            } else {
                ks.load(fis, storePassword);
            }
        } catch (FileNotFoundException fnfe) {
            throw new IllegalArgumentException(
                "Unable to locate specified " + storeFlavor + " " + storeName +
                ": " + fnfe,
                fnfe);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Error reading from " + storeFlavor + " file " + storeName +
                ": " + ioe,
                ioe);
        } catch (NoSuchAlgorithmException nsae) {
            throw new IllegalArgumentException(
                "Unable to check " + storeFlavor + " integrity: " + storeName +
                ": " + nsae,
                nsae);
        } catch (CertificateException ce) {
            throw new IllegalArgumentException(
                "Not all certificates could be loaded: " + storeName +
                ": " + ce,
                ce);
        } catch (GeneralSecurityException gse) {
            throw new IllegalArgumentException(
                "Problem loading keystore: " + storeName + ": " + gse,
                gse);
        }
        return ks;
    }

    /*
     * Check if given transport specify the preferred protocol for client.
     */
    private static
        @Nullable String checkClientAllowedProtocols(String trans,
                                                     SecurityParams sp) {
        final String specifiedProtocol = sp.getTransClientAllowProtocols(trans);

        try {
            if (!checkIfProtocolsAllowed(specifiedProtocol)) {
                return "Transport " + trans +
                    " is not using preferred protocols " + specifiedProtocol +
                    " , the prefered protocols are " +
                    PREFERRED_PROTOCOLS_DEFAULT;
            }
        } catch (IllegalArgumentException iae) {
            return "Problem with protocols specified for transport " + trans +
                ": " + iae.getMessage();
        }
        return null;
    }

    /*
     * Check if given protocols are allowed. Given protocols string must be in
     * the format of "x,y,z" using commas as delimiters, and at least one of
     * the specified protocols must be in the preferred protocols list.
     */
    private static boolean checkIfProtocolsAllowed(String protocols) {
        final String[] protocolList = protocols.split(",");

        if (protocolList.length == 0) {
            throw new IllegalArgumentException(
                "'" + protocols + "' does not have the correct format," +
                " must be specified in the format 'x,y,z', using commas as" +
                " delimiters");
        }

        for (String protocol : protocolList) {
            if (preferredProtocols.contains(protocol.trim())) {
                return true;
            }
        }
        return false;
    }

    /** Check the installed keystore and truststore for consistency. */
    private static
        @Nullable String checkKeystoreInstallation(SecurityParams sp)
    {
        final @Nullable KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);

        if (pwdSrc == null) {
            /*
             * Return directly, cannot perform following verification
             * without password
             */
            return "Unable to create keystore password source.\n";
        }
        final String keystoreName =
            sp.getConfigDir() + File.separator + sp.getKeystoreFile();
        final String truststoreName =
            sp.getConfigDir() + File.separator + sp.getTruststoreFile();

        final StringBuilder warnings = new StringBuilder();
        char @Nullable[] ksPwd = null;
        try {
            ksPwd = pwdSrc.getPassword();
            @Nullable KeyStore keystore = null;
            @Nullable KeyStore truststore = null;
            try {
                keystore = loadKeyStore(
                    keystoreName, ksPwd, "keystore", sp.getKeystoreType());
            } catch (IllegalArgumentException e) {
                warnings.append(e.getMessage()).append('\n');
            }
            try {
                truststore = loadKeyStore(
                    truststoreName, ksPwd, "truststore",
                    sp.getTruststoreType());
            } catch (IllegalArgumentException e) {
                warnings.append(e.getMessage()).append('\n');
            }
            if ((keystore == null) || (truststore == null)) {
                return warnings.toString();
            }
            final Map<String, Set<String>> aliasInfo = new HashMap<>();
            addAliasInfo(sp.getTransServerKeyAlias(SECURITY_TRANSPORT_JE_HA),
                         sp.getTransClientIdentityAllowed(
                             SECURITY_TRANSPORT_JE_HA),
                         aliasInfo);
            String sigPrivateAlias = sp.getKeystoreSigPrivateKeyAlias();
            if (sigPrivateAlias == null) {
                sigPrivateAlias = SIG_PRIVATE_KEY_ALIAS_DEFAULT;
            }
            if (sigPrivateAlias != null) {
                addAliasInfo(sigPrivateAlias, ALLOW_ANY_IDENTITY, aliasInfo);
            }
            String sigPublicAlias = sp.getTruststoreSigPublicKeyAlias();
            if (sigPublicAlias == null) {
                sigPublicAlias = SIG_PUBLIC_KEY_ALIAS_DEFAULT;
            }
            return checkKeystores(keystoreName, keystore, truststoreName,
                                  truststore, ksPwd, aliasInfo,
                                  sigPrivateAlias, sigPublicAlias);
        } finally {
            SecurityUtils.clearPassword(ksPwd);
        }
    }

    /**
     * Verify that any TLS credential updates are consistent either with each
     * other or with installed credentials. Throws IllegalStateException if a
     * problem is detected, else returns the hashes of the keystore and
     * truststore that will be installed after updates are performed. If force
     * is true, ignore credential problems within the keystore.
     *
     * @param secConfigDir the directory containing security configuration
     * files
     * @param force whether the force flag was specified
     * @param logger for debug logging
     * @return the hashes of the keystore and truststore that will be installed
     * after updates are performed
     * @throws IllegalStateException if a problem is detected
     */
    public static
        CredentialHashes verifyTlsCredentialUpdates(File secConfigDir,
                                                    boolean force,
                                                    Logger logger)
    {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final @Nullable KeyStorePasswordSource pwdSrc =
            KeyStorePasswordSource.create(sp);
        if (pwdSrc == null) {
            throw new IllegalStateException(
                "Unable to access keystore passwords");
        }
        final Map<String, Set<String>> aliasInfo = new HashMap<>();
        addAliasInfo(sp.getTransClientKeyAlias(SECURITY_TRANSPORT_INTERNAL),
                     sp.getTransServerIdentityAllowed(
                         SECURITY_TRANSPORT_INTERNAL),
                     aliasInfo);
        addAliasInfo(sp.getTransServerKeyAlias(SECURITY_TRANSPORT_INTERNAL),
                     sp.getTransClientIdentityAllowed(
                         SECURITY_TRANSPORT_INTERNAL),
                     aliasInfo);
        addAliasInfo(sp.getTransServerKeyAlias(SECURITY_TRANSPORT_CLIENT),
                     sp.getTransClientIdentityAllowed(
                         SECURITY_TRANSPORT_CLIENT),
                     aliasInfo);
        addAliasInfo(sp.getTransServerKeyAlias(SECURITY_TRANSPORT_JE_HA),
                     sp.getTransClientIdentityAllowed(
                         SECURITY_TRANSPORT_JE_HA),
                     aliasInfo);
        String sigPrivateAlias = sp.getKeystoreSigPrivateKeyAlias();
        if (sigPrivateAlias == null) {
            sigPrivateAlias = SIG_PRIVATE_KEY_ALIAS_DEFAULT;
        }
        final boolean addedSigAlias =
            !aliasInfo.containsKey(sigPrivateAlias);
        addAliasInfo(sigPrivateAlias, ALLOW_ANY_IDENTITY, aliasInfo);
        String sigPublicAlias = sp.getTruststoreSigPublicKeyAlias();
        if (sigPublicAlias == null) {
            sigPublicAlias = SIG_PUBLIC_KEY_ALIAS_DEFAULT;
        }
        final CredentialHashes hashes =
            verifyTlsCredentialUpdates(pwdSrc, secConfigDir,
                                       sp.getKeystoreFile(),
                                       sp.getKeystoreType(),
                                       sp.getTruststoreFile(),
                                       sp.getTruststoreType(),
                                       aliasInfo,
                                       sigPrivateAlias,
                                       sigPublicAlias,
                                       force);

        if (!force) {
            Map<String, Set<String>> verifyTlsAliasInfo = aliasInfo;
            if (addedSigAlias) {
                /* Don't verify TLS for credentials only used for signing */
                verifyTlsAliasInfo = new HashMap<>(aliasInfo);
                verifyTlsAliasInfo.remove(sigPrivateAlias);
            }
            verifyTls(pwdSrc,
                      new File(secConfigDir, sp.getKeystoreFile()).toString(),
                      sp.getKeystoreType(),
                      new File(secConfigDir, sp.getTruststoreFile()).toString(),
                      sp.getTruststoreType(),
                      verifyTlsAliasInfo,
                      logger);
        }
        return hashes;
    }

    /**
     * Internal method that doesn't depend on SecurityParams, for testing.
     */
    static CredentialHashes
        verifyTlsCredentialUpdates(PasswordSource pwdSrc,
                                   File secConfigDir,
                                   String keystoreFile,
                                   String keystoreType,
                                   String truststoreFile,
                                   String truststoreType,
                                   Map<String, Set<String>> aliasInfo,
                                   String sigPrivateAlias,
                                   String sigPublicAlias,
                                   boolean force) {
        String keystoreName =
            secConfigDir + File.separator + SECURITY_UPDATES_DIR +
            File.separator + keystoreFile;
        String truststoreName =
            secConfigDir + File.separator + SECURITY_UPDATES_DIR +
            File.separator + truststoreFile;

        char @Nullable[] ksPwd = null;
        final AtomicReference<String> keystoreHash = new AtomicReference<>();
        final AtomicReference<String> truststoreHash = new AtomicReference<>();
        try {
            ksPwd = pwdSrc.getPassword();
            final StringBuilder errors = new StringBuilder();
            @Nullable KeyStore keystore = null;
            try {
                keystore = loadKeyStore(
                    keystoreName, ksPwd, "keystore", keystoreType,
                    keystoreHash);
            } catch (IllegalArgumentException e) {
                if (!(e.getCause() instanceof FileNotFoundException)) {
                    errors.append(e.getMessage()).append("\n");
                }
            }
            @Nullable KeyStore truststore = null;
            try {
                truststore = loadKeyStore(
                    truststoreName, ksPwd, "truststore", truststoreType,
                    truststoreHash);
            } catch (IllegalArgumentException e) {
                if (!(e.getCause() instanceof FileNotFoundException)) {
                    errors.append(e.getMessage()).append("\n");
                }
            }

            /* Give up if there were problems reading existing updates */
            if (errors.length() > 0) {
                throw new IllegalStateException(
                    "Problems with credential updates:\n" + errors);
            }
            boolean keystoreUpdate = true;
            try {
                if (keystore == null) {
                    keystoreName =
                        secConfigDir + File.separator + keystoreFile;
                    keystore = loadKeyStore(
                        keystoreName, ksPwd, "keystore", keystoreType,
                        keystoreHash);
                    keystoreUpdate = false;
                }
            } catch (IllegalArgumentException e) {
                errors.append(e.getMessage()).append("\n");
            }
            boolean truststoreUpdate = true;
            try {
                if (truststore == null) {
                    truststoreName =
                        secConfigDir + File.separator + truststoreFile;
                    truststore = loadKeyStore(
                        truststoreName, ksPwd, "truststore",
                        truststoreType, truststoreHash);
                    truststoreUpdate = false;
                }
            } catch (IllegalArgumentException e) {
                errors.append(e.getMessage()).append("\n");
            }
            if ((keystore == null) || (truststore == null)) {
                throw new IllegalStateException(
                    "Problems with installed credentials:\n" + errors);
            }
            if (!force) {
                final String problems = checkKeystores(
                    keystoreName, keystore, truststoreName, truststore, ksPwd,
                    aliasInfo, sigPrivateAlias, sigPublicAlias);
                if (problems != null) {
                    throw new IllegalStateException(problems);
                }
            }
            return new CredentialHashes(keystoreHash.get(),
                                        keystoreUpdate,
                                        truststoreHash.get(),
                                        truststoreUpdate);
        } finally {
            clearPassword(ksPwd);
        }
    }


    /**
     * Add an allowed identity pattern to the set of patterns stored in the map
     * for the alias.
     */
    private static void addAliasInfo(String alias,
                                     String allowedIdentity,
                                     Map<String, Set<String>> aliasInfo) {
        if (alias != null) {
            final Set<String> allowedIdentities =
                aliasInfo.computeIfAbsent(alias, k -> new HashSet<>());
            if (allowedIdentity != null) {
                allowedIdentities.add(allowedIdentity);
            }
        }
    }

    /**
     * Check the specified keystore and truststore for consistency, returning
     * a String describing any problems found, or null if no problems.
     */
    private static
        @Nullable String checkKeystores(String keystoreName,
                                        KeyStore keystore,
                                        String truststoreName,
                                        KeyStore truststore,
                                        char @Nullable[] ksPwd,
                                        Map<String, Set<String>> aliasInfo,
                                        String sigPrivateAlias,
                                        String sigPublicAlias) {
        final StringBuilder errors = new StringBuilder();
        for (final Entry<String, Set<String>> e : aliasInfo.entrySet()) {
            final String alias = e.getKey();
            final Set<String> allowedIdentities = e.getValue();
            final String sigTruststoreAlias =
                alias.equals(sigPrivateAlias) ?
                sigPublicAlias :
                null;
            final @Nullable String err = checkKeystoreEntry(
                keystoreName, keystore, alias, sigTruststoreAlias,
                allowedIdentities, truststoreName, truststore, ksPwd);
            if (err != null) {
                errors.append(err);
            }
        }
        return (errors.length() > 0) ? errors.toString() : null;
    }

    /** Check a single keystore alias for consistency */
    private static @Nullable
        String checkKeystoreEntry(String keystoreName,
                                  KeyStore keystore,
                                  String keystoreAlias,
                                  @Nullable String sigTruststoreAlias,
                                  Set<String> allowedIdentities,
                                  String truststoreName,
                                  KeyStore truststore,
                                  char @Nullable[] ksPwd) {

        /* Check if private key entry exists */
        final @Nullable PrivateKeyEntry pkEntry;
        try {
            pkEntry = (PrivateKeyEntry) keystore.getEntry(
                keystoreAlias, new PasswordProtection(ksPwd));
        } catch (GeneralSecurityException gse) {
            return "Problem accessing private key " + keystoreAlias +
                " in keystore " + keystoreName + ": " + gse + "\n";
        }
        if (pkEntry == null) {

            /*
             * Return directly, cannot perform following verification without a
             * private key in key store.
             */
            return "Private key " + keystoreAlias +
                " does not exist in keystore " + keystoreName + ".\n";
        }

        final Certificate cert = pkEntry.getCertificate();
        if (!(cert instanceof X509Certificate)) {
            /*
             * Return directly, cannot perform following verification if
             * private key in key store does not have valid x509 certificate.
             */
            return "Certificate of " + keystoreAlias +
                " in keystore " + keystoreName +
                " is not a valid X509 certificate.\n";
        }

        /*
         * Check if the subject of key is one of the identities allowed. Using
         * regular expression matching to check this as the internal SSL
         * verifier does.
         */
        final X509Certificate x509Cert = (X509Certificate) cert;
        final X500Principal subject = x509Cert.getSubjectX500Principal();
        for (final String allowedIdentity : allowedIdentities) {
            final @Nullable String verifyResult = verifyCertIdentityAllowed(
                subject, allowedIdentity,
                String.format("The subject name '%s' of the certificate" +
                              " for alias %s" +
                              " in keystore %s",
                              subject,
                              keystoreAlias,
                              keystoreName));
            if (verifyResult != null) {
                return verifyResult;
            }
        }

        /* Check validity of certificate */
        try {
            x509Cert.checkValidity();
        } catch (CertificateExpiredException |
                 CertificateNotYetValidException e) {
            return "Certificate of " + keystoreAlias + " is not valid: " +
                e.getMessage() + ".\n";
        }
        final String issuer = x509Cert.getIssuerX500Principal().getName();

        /*
         * Check that the key certificate matches the sigPublicAlias
         * certificate in the truststore
         */
        if (sigTruststoreAlias != null) {
            final Certificate sigCert;
            try {
                sigCert = truststore.getCertificate(sigTruststoreAlias);
            } catch (KeyStoreException kse) {
                return "Problem accessing truststore " + truststoreName +
                    ": " + kse + ".\n";
            }
            if (sigCert == null) {
                return "Certificate not found for alias " +
                    sigTruststoreAlias + " in truststore " +
                    truststoreName + ".\n";
            }
            if (!pkEntry.getCertificate().equals(sigCert)) {
                return "Certificate for alias " + sigTruststoreAlias +
                    " in truststore " + truststoreName +
                    " does not match the certificate associated with alias " +
                    keystoreAlias + " in keystore " + keystoreName + ".\n";
            }
        }

        /*
         * Self-signed certificate, just check that truststore contains the
         * certificate for the private key
         */
        if (subject.getName().equals(issuer)) {
            boolean found = false;
            if (sigTruststoreAlias != null) {
                /* Already checked */
                found = true;
            } else {
                try {
                    for (final String alias : iterate(truststore.aliases())) {
                        final Certificate c = truststore.getCertificate(alias);
                        if (c instanceof X509Certificate) {
                            if (x509Cert.equals(c)) {
                                found = true;
                                break;
                            }
                        }
                    }
                } catch (KeyStoreException kse) {
                    return "Problem accessing truststore " + truststoreName +
                        ": " + kse + ".\n";
                }
            }
            return found ?
                null :
                "Self-signed certificate was not found in truststore" +
                " for alias " + keystoreAlias + " in keystore.\n";
        }

        final X509CertSelector target = new X509CertSelector();
        final Certificate[] chain = pkEntry.getCertificateChain();
        target.setCertificate((X509Certificate) chain[0]);

        final Set<TrustAnchor> trustAnchors = new HashSet<>();
        for (Certificate certificate : chain) {
            if (!(certificate instanceof X509Certificate)) {
                return "Certificate chain contains invalid " +
                    "X509 certificate " + certificate + ".\n";
            }
            if (!certificate.equals(pkEntry.getCertificate())) {
                trustAnchors.add(
                    new TrustAnchor((X509Certificate) certificate, null));
            }
        }

        try {
            /*
             * Attempting to build a CertPath with the certificates in the
             * private key entry to verify if the keystore is correctly
             * installed. The root and intermediate certificates in the chain
             * will be specified as trust anchors in PKIXBuilderParameters to
             * build the CertPath for validation.
             *
             * PKIXBuilderParameters can also be built with keystore, but the
             * validation would fail if the keystore doesn't have separate root
             * and intermediate certificate entries, which is not necessary and
             * usually not true for the keystore converted with openssl
             * command.
             */
            final PKIXBuilderParameters params =
                new PKIXBuilderParameters(trustAnchors, target);
            final CertPathBuilder builder =
                CertPathBuilder.getInstance("PKIX");

            /*
             * Disable revocation check for now since we don't have support to
             * get CRL information, but should be enabled once we can provide
             * CRL list.
             */
            params.setRevocationEnabled(false);

            /*
             * Attempts to build certificate path. If it fails, it will throw a
             * CertPathBuilderException
             */
            builder.build(params);
        } catch (Exception e) {
            /*
             * Hide the original exception since the failure of build is
             * difficult to track down from CertPathBuilderException. The error
             * message of this exception is not very descriptive, so here we
             * only warns users the installation is incorrect.
             */
            return "Problem with verifying certificate chain for " +
                keystoreAlias + " in " + keystoreName +
                ", current certificate chain:\n" +
                Arrays.toString(chain) + ".\n";
        }

        /* Check truststore contains necessary certificates */
        boolean foundRequiredTrust = false;
        try {
            final Enumeration<String> aliases = truststore.aliases();
            while (aliases.hasMoreElements()) {
                final Certificate trust =
                    truststore.getCertificate(aliases.nextElement());
                if (trust instanceof X509Certificate) {
                    if (x509Cert.equals(trust)) {
                        foundRequiredTrust = true;
                        break;
                    }
                }
            }
        } catch (KeyStoreException kse) {
            return "Problem accessing truststore " + truststoreName + ": " +
                kse + ".\n";
        }
        if (!foundRequiredTrust) {
            return truststoreName +
                " must contain the certificate " +
                x509Cert.getSubjectX500Principal().getName() + ".\n";
        }
        return null;
    }

    /**
     * Verify name of X500 principal in RFC1779 format match the regular
     * expression specified in allowedIdentities.
     */
    static
        @Nullable String verifyCertIdentityAllowed(X500Principal principal,
                                                   String allowedIdentities) {
        return verifyCertIdentityAllowed(
            principal, allowedIdentities,
            String.format("The certificate's subject name '%s'", principal));
    }

    private static
        @Nullable String verifyCertIdentityAllowed(X500Principal principal,
                                                   String allowedIdentities,
                                                   String subjectDesc) {

        final String rfc1779Name = principal.getName(X500Principal.RFC1779);
        if (checkIdentityAllowed(allowedIdentities, rfc1779Name)) {
            return null;
        }

        return String.format(
            "%s" +
            " when displayed in RFC 1779 format as '%s'" +
            " does not match '%s' specified in allowedIdentities.\n",
            subjectDesc,
            rfc1779Name,
            allowedIdentities);
    }

    /*
     * Using regular expression matching for now to check if given identity
     * is allowed.
     */
    private static boolean checkIdentityAllowed(String allowedIdentities,
                                                String identity) {
        final String regex = allowedIdentities.substring(
            "dnmatch(".length(), allowedIdentities.length() - 1);
        return identity.matches(regex);
    }

    private static void verifyTls(PasswordSource pwdSrc,
                                  String keystoreFile,
                                  String keystoreType,
                                  String truststoreFile,
                                  String truststoreType,
                                  Map<String, Set<String>> aliasInfo,
                                  Logger logger) {
        for (final Entry<String, Set<String>> e : aliasInfo.entrySet()) {
            final String alias = e.getKey();
            final Set<String> allowedIdentities = e.getValue();
            for (final String allowedIdentity : allowedIdentities) {
                verifyTls(pwdSrc, allowedIdentity, allowedIdentity,
                          keystoreFile, keystoreType,
                          truststoreFile, truststoreType,
                          alias, logger);
            }
        }
    }

    /**
     * Opens a TLS connection on localhost using the specified security
     * parameters and attempts to communicate, to verify that the parameters
     * are working properly. Throws IllegalStateException if the check fails.
     */
    static void verifyTls(PasswordSource pwdSrc,
                          String clientAuthenticator,
                          String serverHostVerifier,
                          String keystoreFile,
                          String keystoreType,
                          String truststoreFile,
                          String truststoreType,
                          String keyAlias,
                          Logger logger) {

        /* Convert arguments to SSLConfig */
        final Properties secProps = new Properties();
        secProps.setProperty(SSLConfig.CLIENT_AUTHENTICATOR,
                             clientAuthenticator);
        secProps.setProperty(SSLConfig.SERVER_HOST_VERIFIER,
                             serverHostVerifier);
        secProps.setProperty(SSLConfig.KEYSTORE_FILE, keystoreFile);
        secProps.setProperty(SSLConfig.KEYSTORE_TYPE, keystoreType);
        secProps.setProperty(SSLConfig.TRUSTSTORE_FILE, truststoreFile);
        secProps.setProperty(SSLConfig.TRUSTSTORE_TYPE, truststoreType);
        secProps.setProperty(SSLConfig.KEYSTORE_ALIAS, keyAlias);
        final SSLConfig sslConfig = new SSLConfig(secProps);
        sslConfig.setKeystorePassword(pwdSrc.getPassword());
            sslConfig.setTrustStorePassword(pwdSrc.getPassword());

        ServerSocketChannel listener = null;
        SocketChannel client = null;
        DataChannel clientChannel = null;
        SocketChannel server = null;
        DataChannel serverChannel = null;
        try {

            /* Create listener and start listening */
            final EndpointConfigBuilder serverEndpointConfigBuilder =
                new EndpointConfigBuilder()
                .sslControl(sslConfig.makeSSLControl(true, logger));
            final ListenerConfig listenerConfig = new ListenerConfigBuilder()
                /*
                 * For now, update a socket listener on localhost with an
                 * anonymous port. It would be better to use Unix domain
                 * sockets, but we don't currently support using them with SSL.
                 */
                .hostName("localhost")
                .endpointConfigBuilder(serverEndpointConfigBuilder)
                .build();
            listener = NioUtil.listen(listenerConfig);
            final NetworkAddress listenerAddress =
                NetworkAddress.convertNow(listener.getLocalAddress());
            final CompletableFuture<SocketChannel> serverFuture =
                CompletableFuture.supplyAsync(checked(listener::accept));

            /* Create and connect client */
            final EndpointConfig clientEndpointConfig =
                new EndpointConfigBuilder()
                .sslControl(sslConfig.makeSSLControl(false, logger))
                .build();
            client = NioUtil.getSocketChannel(listenerAddress, null,
                                              clientEndpointConfig)
                .get(10, SECONDS);
            client.configureBlocking(true);
            client.finishConnect();

            /* Wait for server connection */
            server = serverFuture.get(10, SECONDS);
            server.configureBlocking(true);

            /* Create data channels */
            clientChannel = NioUtil.getDataChannel(
                client, clientEndpointConfig, true /* isClient */,
                listenerAddress, logger);
            clientChannel.configureBlocking(true);
            final NetworkAddress clientAddress =
                NetworkAddress.convertNow(client.getLocalAddress());
            serverChannel = NioUtil.getDataChannel(
                server, serverEndpointConfigBuilder.build(),
                false /* isClient */, clientAddress, logger);
            serverChannel.configureBlocking(true);

            /* Try communicating over the channels */
            verifyTlsChannels(clientChannel, serverChannel);

        } catch (IllegalStateException e) {
            throw e;
        } catch (IOException e) {
            throw new IllegalStateException(
                "Unexpected I/O error while verifying TLS credentials: " + e,
                e);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            safeClose(clientChannel);
            safeClose(client);
            safeClose(serverChannel);
            safeClose(server);
            safeClose(listener);
        }
    }

    private static void verifyTlsChannels(DataChannel clientChannel,
                                          DataChannel serverChannel)
        throws Exception {

        /* Read data on server */
        final ByteBuffer serverBuffer = ByteBuffer.allocate(1);
        final CompletableFuture<Void> serverRead =
            CompletableFuture.runAsync(
                checkedVoid(
                    () -> {
                        final int count = serverChannel.read(serverBuffer);
                        if (count != 1) {
                            throw new IllegalStateException(
                                "Expected 1 byte, got " + count);
                        }
                        final int b = serverBuffer.rewind().get();
                        if (b != 1) {
                            throw new IllegalStateException(
                                "Expected 1, got " + b);
                        }
                    }));

        /* Write data on client */
        final ByteBuffer clientBuffer = ByteBuffer.allocate(1);
        clientBuffer.put((byte) 1).rewind();
        clientChannel.write(clientBuffer);

        /* Wait for server */
        serverRead.get(10, SECONDS);

        /* Write data on server */
        final CompletableFuture<Void> serverWrite =
            CompletableFuture.runAsync(
                checkedVoid(
                    () -> {
                        serverBuffer.rewind().put((byte) 2).rewind();
                        serverChannel.write(serverBuffer);
                    }));

        /* Read data on client */
        clientBuffer.rewind();
        final int count = clientChannel.read(clientBuffer);
        if (count != 1) {
            throw new IllegalStateException("Expected 1 bytes, got " + count);
        }
        final int b = clientBuffer.rewind().get();
        if (b != 2) {
            throw new IllegalStateException("Expected 2, got " + b);
        }

        /* Wait for server */
        serverWrite.get(10, SECONDS);
    }

    private static void safeClose(@Nullable Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                /* ignore */
            }
        }
    }

    /*
     * Generate random password in the specified length.
     */
    public static char[] generateKeyStorePassword(int length) {
        final char[] pwd = new char[length];
        for (int i = 0; i < length; i++) {
            pwd[i] = allCharSet.charAt(random.nextInt(allCharSet.length()));
        }
        return pwd;
    }

    /*
     * Generate fixed length of password used for creating a default
     * user of secured KVLite. The fixed length is 16.
     */
    public static char[] generateUserPassword() {
        char[] pwd = new char[16];
        for (int i = 0; i < pwd.length; i += 4) {
            pwd[i] = upperSet.charAt(random.nextInt(upperSet.length()));
            pwd[i+1] = lowerSet.charAt(random.nextInt(lowerSet.length()));
            pwd[i+2] = specialSet.charAt(random.nextInt(specialSet.length()));
            pwd[i+3] = digitSet.charAt(random.nextInt(digitSet.length()));
        }
        return permuteCharArray(pwd);
    }

    private static char[] permuteCharArray(char[] chars) {
        final List<Character> list = new ArrayList<Character>();
        for (char c : chars) {
            list.add(c);
        }
        Collections.shuffle(list);
        char[] result = new char[list.size()];
        for (int i = 0; i < list.size(); i++) {
            Character c = list.get(i);
            result[i] = c.charValue();
        }
        list.clear();
        return result;
    }

    /**
     * Make sure that the installed truststore contains the credentials in the
     * update truststore, if present, storing them in entries with names
     * "__update#N__" for N=1 and higher, and removing any update entries that
     * no longer appear in the update truststore. Saves the updated truststore
     * temporarily to a file with the ".tmp" suffix so that it can be updated
     * atomically. This method throws a RuntimeException if an unexpected
     * problem occurs
     */
    public static String addTruststoreUpdates(File secConfigDir,
                                              Logger logger) {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        return addTruststoreUpdates(KeyStorePasswordSource.create(sp),
                                    secConfigDir, sp.getTruststoreFile(),
                                    sp.getTruststoreType(),
                                    sp.getClientAccessProps(), logger);
    }

    /**
     * Internal method that doesn't depend on SecurityParams, for testing.
     */
    static String addTruststoreUpdates(@Nullable PasswordSource pwdSrc,
                                       File configDir,
                                       String truststoreFile,
                                       String truststoreType,
                                       Properties securityProps,
                                       Logger logger) {
        if (pwdSrc == null) {
            throw new IllegalStateException(
                "Unable to create keystore password source");
        }
        final File updatesDir = new File(configDir, SECURITY_UPDATES_DIR);
        final File update = new File(updatesDir, truststoreFile);
        if (!update.exists()) {
            return InstallMatchingResult.NO_UPDATE_FOUND.msg;
        }
        final File installed = new File(configDir, truststoreFile);
        char @Nullable[] pwd = null;
        try {
            pwd = pwdSrc.getPassword();
            if (pwd == null) {
                throw new IllegalStateException(
                    "Truststore password was not found");
            }
            final KeyStore updateTruststore;
            try {
                updateTruststore = loadKeyStore(
                    update.toString(), pwd, "truststore", truststoreType);
            } catch (IllegalArgumentException e) {
                if (e.getCause() instanceof FileNotFoundException) {
                    return InstallMatchingResult.NO_UPDATE_FOUND.msg;
                }
                throw e;
            }
            final KeyStore installedTruststore = loadKeyStore(
                installed.toString(), pwd, "truststore", truststoreType);
            final boolean modified =
                InstallTruststoreUpdates.INSTANCE.addUpdates(
                    updateTruststore, installedTruststore);
            if (modified) {
                /* Save the modified truststore */
                final File tmp = new File(configDir, truststoreFile + ".tmp");
                try (final FileOutputStream fos = new FileOutputStream(tmp)) {
                    assert TestHookExecute.doIOHookIfSet(
                        addTrustUpdatesStoreHook, tmp.toPath());
                    installedTruststore.store(fos, pwdSrc.getPassword());
                    assert TestHookExecute.doIOHookIfSet(
                        addTrustUpdatesMoveHook, installed.toPath());
                    Files.move(tmp.toPath(), installed.toPath(),
                               REPLACE_EXISTING, ATOMIC_MOVE);
                    logger.fine(() -> "Stored modifications: " +
                                installedTruststore);
                } finally {
                    try {
                        assert TestHookExecute.doIOHookIfSet(
                            addTrustUpdatesDeleteHook, tmp.toPath());
                        Files.delete(tmp.toPath());
                    } catch (IOException e) {
                    }
                }
                /*
                 * Update client trust as well, deleting any earlier backup if
                 * present
                 */
                final File backupTruststore = new File(
                    configDir,
                    FileNames.CLIENT_TRUSTSTORE_FILE + BACKUP_FILE_SUFFIX);
                backupTruststore.delete();
                createClientTrustFromServerTrust(
                    truststoreFile, truststoreType, securityProps,
                    configDir, charsToString(pwd), null /* ctsPwd */);

                /* Make sure truststore updates are noticed */
                waitForNextSecond();

                return InstallMatchingResult.INSTALLED_UPDATE.msg;
            }
            return InstallMatchingResult.UPDATE_ALREADY_INSTALLED.msg;
        } catch (GeneralSecurityException | IOException e) {
            throw new IllegalStateException(
                "Unexpected problem installing truststore updates: " +
                e.getMessage(),
                e);
        } finally {
            SecurityUtils.clearPassword(pwd);
        }
    }

    /**
     * Wait until the start of the next second so that we can be sure that
     * KeyStoreCache will notice a newly changed file. Throws
     * IllegalStateException if interrupted.
     */
    private static void waitForNextSecond() {
        final long now = System.currentTimeMillis();
        final long until = ((now / 1000) * 1000) + 1000;
        try {
            Thread.sleep(until - now);
        } catch (InterruptedException e) {
            throw new IllegalStateException(
                "Unexpected interrupt: " + e, e);
        }
    }

    /**
     * A base class that abstracts adding truststore updates to allow simpler
     * testing without KeyStores or Certificates.
     */
    static abstract
        class AbstractInstallTruststoreUpdates<@NonNull K, @NonNull C>
    {
        abstract Enumeration<String> aliases(K ks) throws KeyStoreException;
        abstract C getCert(K ks, String alias) throws KeyStoreException;
        abstract boolean containsCert(K ks, C cert) throws KeyStoreException;
        abstract void deleteEntry(K ks, String alias) throws KeyStoreException;
        abstract void setCert(K ks, String alias, C cert)
            throws KeyStoreException;
        boolean addUpdates(K update, K installed)
            throws GeneralSecurityException
        {
            /*
             * Remove updates that are no longer present in the update
             * truststore, and compute the number of the highest remaining
             * update alias
             */
            int maxUpdate = 0;
            boolean modified = false;
            /* Copy the aliases so we can delete entries while iterating */
            for (final String alias : Collections.list(aliases(installed))) {
                final C cert = getCert(installed, alias);
                if (cert != null) {
                    final int updateNumber = getUpdateNumber(alias);
                    if (updateNumber >= 0) {
                        if (!containsCert(update, cert)) {
                            deleteEntry(installed, alias);
                            modified = true;
                        } else {
                            maxUpdate = Math.max(maxUpdate, updateNumber);
                        }
                    }
                }
            }
            /* Add missing updates */
            for (final String alias : Collections.list(aliases(update))) {
                final C cert = getCert(update, alias);
                if ((cert != null) && !containsCert(installed, cert)) {
                    modified = true;
                    setCert(installed, getUpdateAlias(++maxUpdate), cert);
                }
            }
            return modified;
        }
    }

    /** Implementation for KeyStores and Certificates */
    private static class InstallTruststoreUpdates
        extends AbstractInstallTruststoreUpdates<@NonNull KeyStore,
                                                 @NonNull Certificate>
    {
        private static final InstallTruststoreUpdates INSTANCE =
            new InstallTruststoreUpdates();

        @Override
        Enumeration<String> aliases(KeyStore ks) throws KeyStoreException {
            return ks.aliases();
        }
        @Override
        Certificate getCert(KeyStore ks, String alias) throws KeyStoreException
        {
            return ks.getCertificate(alias);
        }
        @Override
        boolean containsCert(KeyStore ks, Certificate cert)
            throws KeyStoreException
        {
            return ks.getCertificateAlias(cert) != null;
        }
        @Override
        void deleteEntry(KeyStore ks, String alias) throws KeyStoreException {
            ks.deleteEntry(alias);
        }
        @Override
        void setCert(KeyStore ks, String alias, Certificate cert)
            throws KeyStoreException
        {
            ks.setCertificateEntry(alias, cert);
        }
    }

    /** Returns the update number for an update alias or else -1. */
    static int getUpdateNumber(String alias) {
        if (alias.startsWith("__update#") && alias.endsWith("__")) {
            try {
                return Integer.parseInt(alias.substring(9, alias.length() - 2));
            } catch (NumberFormatException e) {
            }
        }
        return -1;
    }

    /** Returns the alias for an update with the specified number. */
    static String getUpdateAlias(int updateNumber) {
        return "__update#" + updateNumber + "__";
    }

    /** Convert an Enumeration to an Iterable for use in a for statement. */
    private static <T> Iterable<T> iterate(Enumeration<T> enumeration) {
        return enumeration::asIterator;
    }

    /** Install the keystore update. */
    public static String installKeystoreUpdate(String keystoreHash,
                                               File secConfigDir) {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final Path configDir = sp.getConfigDir().toPath();
        final Path updatesDir = configDir.resolve(SECURITY_UPDATES_DIR);

        final String keystore = sp.getKeystoreFile();
        final Path update = updatesDir.resolve(keystore);
        final Path installed = configDir.resolve(keystore);
        final InstallMatchingResult result =
            installMatching(update, keystoreHash, installed);
        if (result == InstallMatchingResult.INSTALLED_UPDATE) {
            /* Make sure keystore update is noticed */
            waitForNextSecond();
        }
        return result.msg;
    }

    /** Install the truststore update */
    public static String installTruststoreUpdate(String truststoreHash,
                                                 File secConfigDir) {
        final SecurityParams sp = loadSecurityParams(secConfigDir);
        final Path configDir = sp.getConfigDir().toPath();
        final Path updatesDir = configDir.resolve(SECURITY_UPDATES_DIR);

        final String truststore = sp.getTruststoreFile();
        final Path truststoreUpdate = updatesDir.resolve(truststore);
        final Path installedTruststore = configDir.resolve(truststore);
        final InstallMatchingResult updateResult = installMatching(
            truststoreUpdate, truststoreHash, installedTruststore);

        if (updateResult == InstallMatchingResult.INSTALLED_UPDATE) {

            /* Delete the backup client.trust file if present */
            final File backupTruststore = new File(
                secConfigDir,
                FileNames.CLIENT_TRUSTSTORE_FILE + BACKUP_FILE_SUFFIX);
            backupTruststore.delete();

            final @Nullable String ksPwd =
                charsToString(retrieveKeystorePassword(sp));
            createClientTrustFromServerTrust(
                truststore, sp.getTruststoreType(),
                sp.getClientAccessProps(), secConfigDir, ksPwd,
                null /* ctsPwd */);
            waitForNextSecond();
        }

        return updateResult.msg;
    }

    /**
     * Copy the update to the installed path if needed, removing any existing
     * temporary files. The copy is only performed if the installed file does
     * not have the specified hash, the update file exists, and the update has
     * the specified hash. Throws a runtime exception if the update is needed
     * and is not found, if the update has the wrong hash, or if an unexpected
     * problem occurs when attempting to perform file system operations.
     */
    static InstallMatchingResult installMatching(Path update,
                                                 String hash,
                                                 Path installed) {
        final Path tmp = addSuffix(installed, ".tmp");
        try {
            if (hash.equals(computeSha256Hash(installed.toFile()))) {
                return update.toFile().exists() ?
                    InstallMatchingResult.UPDATE_ALREADY_INSTALLED :
                    InstallMatchingResult.NO_UPDATE_FOUND;
            }
            try {
                assert TestHookExecute.doIOHookIfSet(installMatchingCopyHook,
                                                     tmp);
                Files.copy(update, tmp, REPLACE_EXISTING);
            } catch (NoSuchFileException e) {
                throw new IllegalStateException(
                    "Needed update not found: " + update.toFile().getName());
            }
            final String fileHash = computeSha256Hash(tmp.toFile());
            if (!hash.equals(fileHash)) {
                throw new IllegalStateException(
                    "Update has wrong hash: " + update.toFile().getName() +
                    " expected: " + hash + " found: " + fileHash);
            }
            assert TestHookExecute.doIOHookIfSet(installMatchingMoveHook,
                                                 installed);
            Files.move(tmp, installed, REPLACE_EXISTING, ATOMIC_MOVE);
            return InstallMatchingResult.INSTALLED_UPDATE;
        } catch (IOException e) {
            throw new IllegalStateException(
                "Unexpected I/O exception: " + e.getMessage(), e);
        } finally {
            try {
                assert TestHookExecute.doIOHookIfSet(installMatchingDeleteHook,
                                                     tmp);
                Files.delete(tmp);
            } catch (IOException e) {
            }
        }
    }

    private static Path addSuffix(Path path, String suffix) {
        return path.resolveSibling(path.getFileName() + suffix);
    }

    /**
     * Delete a file if it matches a hash. Throws a runtime exception if an
     * unexpected problem occurs when attempting to perform file system
     * operations.
     */
    static void deleteMatching(Path path, String hash, Logger logger) {
        try {
            final String fileHash = computeSha256Hash(path.toFile());
            if (fileHash == null) {
                logger.fine("File not found: " + path);
            } else if (!hash.equals(fileHash)) {
                logger.fine(
                    () -> "File has wrong hash, not deleted: " + path +
                    " expected: " + hash + " found: " + fileHash);
            } else {
                assert TestHookExecute.doIOHookIfSet(deleteMatchingHook, path);
                Files.delete(path);
                logger.fine(() -> "Deleted file: " + path);
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "Unexpected I/O exception: " + e.getMessage(), e);
        }
    }
}
