/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_DELIMITER;
import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_NAME;
import static oracle.kv.impl.sna.StorageNodeAgent.RMI_REGISTRY_FILTER_REQUIRED;

import java.io.File;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * A collection of SSL-centric utilities for kvstore unit tests.
 */
public final class SSLTestUtils {

    public static final String SSL_DIR_PROP = "testssldir";
    public static final String SSL_KS_PWD_PROP = "testsslpwd";

    /* constants for the standard security configuration */
    public static final String SSL_KS_PWD_DEF = "unittest";
    public static final String SSL_KS_ALIAS_DEF = "shared";
    public static final String SSL_KS_NAME = "store.keys";
    public static final String SSL_TS_NAME = "store.trust";

    /* a trust store using SSL_OTHER_PW_NAME as password */
    public static final String SSL_TS_OTHER_NAME = "store.trust.other";

    /* Default PKCS12 password-less client.trust */
    public static final String SSL_CTS_NAME = "client.trust";

    /* PKCS12 password-protected client.trust */
    public static final String SSL_PASS_CTS_NAME = "client.trust.pass";

    /* JKS client.trust */
    public static final String SSL_JKS_CTS_NAME = "client.trust.jks";

    /* JKS server key and trust stores */
    public static final String SSL_JKS_KS_NAME = "store.keys.jks";
    public static final String SSL_JKS_TS_NAME = "store.trust.jks";

    public static final String SSL_TS_ALIAS_DEF = "shared";
    public static final String SSL_PW_NAME = "store.passwd";
    public static final String SSL_CERT_IDENT = "dnmatch(CN=Unit Test)";

    /* constants for an alternate security configuration */
    public static final String SSL_OTHER_KS_PWD_DEF = "othertest";
    public static final String SSL_OTHER_KS_ALIAS_DEF = "shared";
    public static final String SSL_OTHER_KS_NAME = "other.keys";
    public static final String SSL_OTHER_TS_NAME = "other.trust";

    /* a PKCS12 password-less client.trust */
    public static final String SSL_OTHER_CTS_NAME = "other.client.trust";
    public static final String SSL_OTHER_TS_ALIAS_DEF = "shared";
    public static final String SSL_OTHER_PW_NAME = "other.passwd";

    /*
     * constants for an merged truststore, which contains the certificates
     * of store.trust and other.trust in test/ssl directory.
     */
    public static final String SSL_MERGED_TS_NAME = "merge.trust";

    /* a PKCS12 password-less client.trust */
    public static final String SSL_MERGED_CTS_NAME = "merge.client.trust";

    /*
     * To simplify the SSL certificate updating test, make a copy of
     * other.keys using the password of store.keys, so don't need to
     * run extra keytool command to change password.
     */
    public static final String SSL_NEW_KS_NAME = "new.keys";

    /*
     * A security configuration directory created using CA signed certificate.
     */
    public static final String CA_SIGNED_CONFIG = "ca-signed-config";

    /*
     * Key store in above configuration directory, which contains a
     * valid private key and its certificate chain imported via openssl.
     */
    public static final String OPENSSL_KS_NAME = "openssl-store.keys";

    /*
     * Incorrect key store in above configuration directory, which does not
     * contains root and intermediate certificate.
     */
    public static final String ERROR_KS_NAME = "error-store.keys";

    /*
     * Incorrect trust store in above configuration directory, which does not
     * contains CA signed certificate used for NoSQL store.
     */
    public static final String ERROR_TS_NAME = "error-store.trust";

    public static final String SSL_TRANSPORT_FACTORY =
        "oracle.kv.impl.security.ssl.SSLTransport";
    public static final String CLEAR_TRANSPORT_FACTORY =
         "oracle.kv.impl.security.ClearTransport";

    /* not instantiable */
    private SSLTestUtils() {
    }

    public static File getTestSSLDir() {
        final String dir = System.getProperty(SSL_DIR_PROP);
        if (dir == null || dir.length() == 0) {
            throw new IllegalArgumentException
                ("System property must be set to test ssl directory: " +
                 SSL_DIR_PROP);
        }

        return new File(dir).getAbsoluteFile();
    }

    public static void setSSLProperties() {
        System.setProperty("javax.net.ssl.trustStore",
                           getTestSSLDir().getPath() + File.separator +
                           SSL_TS_NAME);
        System.setProperty("javax.net.ssl.keyStore",
                           getTestSSLDir().getPath() + File.separator +
                           SSL_KS_NAME);
        String ksPwd = System.getProperty(SSL_KS_PWD_PROP);
        if (ksPwd == null) {
            ksPwd = SSL_KS_PWD_DEF;
        }
        System.setProperty("javax.net.ssl.keyStorePassword", ksPwd);

        /* PKCS12 trust store also requires a password */
        System.setProperty("javax.net.ssl.trustStorePassword", ksPwd);

        for (String storeProp : new String[] { "keyStore", "trustStore" }) {
            final String prop = "javax.net.ssl." + storeProp;
            final String propVal = System.getProperty(prop);
            if (propVal == null) {
                throw new IllegalArgumentException(
                    "property " + prop + " did not get set.");
            }
            final File storeFile = new File(propVal);
            if (!storeFile.exists()) {
                throw new IllegalArgumentException(
                    "The file " + storeFile + " does not exist.");
            }
        }
    }

    /**
     * Since Java 8u121, it is required to set RMI registry filter white list,
     * when create a registry. Set the required filter pattern for unit tests
     * that initialize registry themselves instead of creating though SNA.
     */
    public static void setRMIRegistryFilter() {
        System.setProperty(
            RMI_REGISTRY_FILTER_NAME,
            Arrays.stream(RMI_REGISTRY_FILTER_REQUIRED).
                collect(Collectors.joining(RMI_REGISTRY_FILTER_DELIMITER)));

    }

    public static void clearRMIRegistryFilter() {
       System.clearProperty(RMI_REGISTRY_FILTER_NAME);
    }

    /**
     * Get absolute path of SSL test file.
     */
    public static String getSSLFilePath(String file) {
        return new File(getTestSSLDir(), file).getAbsolutePath();
    }
}
