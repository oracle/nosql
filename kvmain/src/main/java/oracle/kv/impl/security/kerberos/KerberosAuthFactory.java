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

package oracle.kv.impl.security.kerberos;

import java.io.File;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.Authenticator;
import oracle.kv.impl.security.AuthenticatorFactory;

/**
 * Factory for Kerberos implementation of authenticator.
 */
public class KerberosAuthFactory implements AuthenticatorFactory {

    @Override
    public Authenticator getAuthenticator(SecurityParams secParams,
                                          GlobalParams globalParams)
        throws IllegalArgumentException {

        final String krbConfFile = secParams.getKerberosConfFile();
        if (krbConfFile == null) {
            throw new IllegalArgumentException("Kerberos configuration file " +
               "is not specified");
        }
        final File krb5Conf = new File(secParams.getKerberosConfFile());
        if (!krb5Conf.exists()) {
            throw new IllegalArgumentException("Kerberos configuration file " +
                "does not exist");
        }

        final String realm = secParams.getKerberosRealmName();
        if (realm == null || realm.equals("")) {
            throw new IllegalArgumentException("Default realm name " +
                "is not specified");
        }

        final String keytabFile = secParams.getKerberosKeytabFile();
        if (keytabFile == null) {
            throw new IllegalArgumentException("Server keytab file " +
                "is not specified");
        }
        final File keytab = new File(secParams.getConfigDir(),
                                     secParams.getKerberosKeytabFile());
        if (!keytab.exists()) {
            throw new IllegalArgumentException("Server keytab file " +
                "does not exist");
        }
        return new KerberosAuthenticator(secParams);
    }
}
