/*-
 * Copyright (C) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.util.ssl;

import java.io.File;

/**
 * Configuration related to SSL
 */
public class SSLConfig {

    /*
     * Environment variable to enable SSL on http server in internal components
     * including SC, PH, TM and AS.
     */
    private static final String INTERNAL_SSL_ENABLE_PROP = "INTERNAL_SSL_ENABLE";

    /* Environment variable for SSL certificate file for internal components */
    public static final String INTERNAL_SSL_CERT_PROP = "INTERNAL_SSL_CERTIFICATE";

    /* Environment variable for SSL private key file for internal components */
    public static final String INTERNAL_SSL_KEY_PROP = "INTERNAL_SSL_PRIVATE_KEY";

    private static boolean isInternalSSLEnabled =
        Boolean.valueOf(System.getenv(INTERNAL_SSL_ENABLE_PROP));

    public static boolean isInternalSSLEnabled() {
        return isInternalSSLEnabled;
    }

    public static File getInternalSSLCert() {
        return getSSLFileFromEnvProp("Internal SSL certificate",
                                     INTERNAL_SSL_CERT_PROP);
    }

    public static File getInternalSSLKey() {
        return getSSLFileFromEnvProp("Internal SSL private key",
                                     INTERNAL_SSL_KEY_PROP);
    }

    private static File getSSLFileFromEnvProp(String name, String envProp) {
        String path = System.getenv(envProp);
        if (path == null) {
            throw new IllegalArgumentException(
               name + " file does not exist: " + path +
               ", please specify valid file using environment variable " +
               envProp);
        }
        File file = new File(path);
        if (!file.exists() || !file.isFile()) {
            throw new IllegalArgumentException("Invalid " + name +
                " file, it does not exist or is not a file " + path +
                ", please specify valid file using environment variable " +
                envProp);
        }
        return file;
    }
}
