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
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.security.util.SecurityUtils;

import org.ietf.jgss.GSSException;
import org.ietf.jgss.Oid;

/**
 * Kerberos V5 login module related JAAS login configuration.
 */
public class KerberosConfig {

    private static final String KRB_V5_PRINCIPAL_OID = "1.2.840.113554.1.2.2.1";

    private static final String KRB_V5_MECH_OID = "1.2.840.113554.1.2.2";

    private static final String SUN_KRB_LOGIN_MODULE =
        "com.sun.security.auth.module.Krb5LoginModule";

    private static final String KRB_CONFIG_NAME =
        "java.security.krb5.conf";

    public static Oid getKrbPrincNameType() throws GSSException {
        return new Oid(KRB_V5_PRINCIPAL_OID);
    }

    public static Oid getKerberosMethOid() throws GSSException {
        return new Oid(KRB_V5_MECH_OID);
    }

    public static String getKrb5LoginModuleName() {
        return SUN_KRB_LOGIN_MODULE; 
    }

    public static String getKrb5ConfigName() {
        return KRB_CONFIG_NAME;
    }

    /**
     * JAAS login configuration used while building JAAS context on server.
     */
    public static class StoreKrbConfiguration extends Configuration {

        /* Whether enable Kerberos login module debug mode */
        private static final String DEBUG = "false";

        /* Name of server nodes Kerberos configuration */
        public static final String STORE_KERBEROS_CONFIG = "store-krb-config";

        /* Kerberos user-defined configuration options */
        private static final Map<String, String> STORE_KERBEROS_OPTIONS = 
            new HashMap<String, String>();

        static {
            STORE_KERBEROS_OPTIONS.put("doNotPrompt", "true");
            STORE_KERBEROS_OPTIONS.put("useKeyTab", "true");
            STORE_KERBEROS_OPTIONS.put("storeKey", "true");
            STORE_KERBEROS_OPTIONS.put("debug", DEBUG);
        }

        private static final AppConfigurationEntry STORE_KERBEROS_LOGIN =
            new AppConfigurationEntry(getKrb5LoginModuleName(),
                                      LoginModuleControlFlag.REQUIRED,
                                      STORE_KERBEROS_OPTIONS);

        private final SecurityParams secParams;

        public StoreKrbConfiguration(SecurityParams secParameters) {
            this.secParams = secParameters;
        }

        public String getPrincipal() {
            final String serviceName = secParams.getKerberosServiceName();
            final String instanceName = secParams.getKerberosInstanceName();

            if (instanceName != null && !instanceName.equals("")) {
                return serviceName +
                       SecurityUtils.KRB_NAME_COMPONENT_SEPARATOR_STR +
                       instanceName +
                       SecurityUtils.KRB_NAME_REALM_SEPARATOR_STR +
                       secParams.getKerberosRealmName();
            }
            return serviceName +
                   SecurityUtils.KRB_NAME_REALM_SEPARATOR_STR +
                   secParams.getKerberosRealmName();
        }

        public String getKrbConfig() {
            return secParams.getKerberosConfFile();
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if (STORE_KERBEROS_CONFIG.equals(name)) {
                final String keytab = secParams.getConfigDir() +
                    File.separator + secParams.getKerberosKeytabFile();
                STORE_KERBEROS_OPTIONS.put("keyTab", keytab);
                STORE_KERBEROS_OPTIONS.put("principal", getPrincipal());
                return new AppConfigurationEntry[] { STORE_KERBEROS_LOGIN };
            }
            return null;
        }
    }

    /**
     * JAAS login configuration used while building client JAAS context.
     */
    public static class ClientKrbConfiguration extends Configuration {

        private static final String DEBUG = "false";

        /* Name of client Kerberos configuration */
        public static final String CLIENT_KERBEROS_CONFIG = "client-krb-config";

        private static final Map<String, String> CLIENT_KERBEROS_OPTIONS =
            new HashMap<String, String>();

        static {
            CLIENT_KERBEROS_OPTIONS.put("isInitiator", "true");
            CLIENT_KERBEROS_OPTIONS.put("useTicketCache", "true");
            CLIENT_KERBEROS_OPTIONS.put("useKeyTab", "true");
            CLIENT_KERBEROS_OPTIONS.put("debug", DEBUG);
        }

        private static final AppConfigurationEntry CLIENT_KERBEROS_LOGIN =
            new AppConfigurationEntry(getKrb5LoginModuleName(),
                                      LoginModuleControlFlag.REQUIRED,
                                      CLIENT_KERBEROS_OPTIONS);
        private String userName;
        private String ccache;
        private String keytab;
        private boolean doNotPrompt;

        public ClientKrbConfiguration(String userName,
                                      String ccache,
                                      String keytab,
                                      boolean doNotPrompt) {
            this.userName = userName;
            this.ccache = ccache;
            this.keytab = keytab;
            this.doNotPrompt = doNotPrompt;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if (CLIENT_KERBEROS_CONFIG.equals(name)) {
                CLIENT_KERBEROS_OPTIONS.put("principal", userName);

                if (ccache != null){
                    CLIENT_KERBEROS_OPTIONS.put("ticketCache", ccache);
                } else {
                    /*
                     * In case users want to switch back to default ticket cache
                     * in the run time
                     */
                    CLIENT_KERBEROS_OPTIONS.remove("ticketCache");
                }

                if (keytab != null) {
                    CLIENT_KERBEROS_OPTIONS.put("keyTab", keytab);
                } else {
                    /*
                     * In case users want to switch back to default keytab
                     * in the run time
                     */
                    CLIENT_KERBEROS_OPTIONS.remove("keyTab");
                }
                CLIENT_KERBEROS_OPTIONS.put("doNotPrompt",
                                            String.valueOf(doNotPrompt));
                return new AppConfigurationEntry[] { CLIENT_KERBEROS_LOGIN };
            }
            return null;
        }
    }
}
