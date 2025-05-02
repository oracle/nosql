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

package oracle.kv.impl.security.login;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import javax.security.auth.Subject;

import oracle.kv.LoginCredentials;
import oracle.kv.impl.security.util.SecurityUtils;

/**
 * The class contains Kerberos client credentials retrieved from KDC of the
 * specified user and resolved configuration information used for NoSQL Kerberos 
 * authentication..<p>
 *
 * The following information need to fill in for authentication:
 * <ul>
 * <li>The principal name of the client user.
 * <li>The client login subject that contains Kerberos credentials.
 * <li>The host-service mapping in the pattern host:principal
 *     [,host:principal]*.
 * <li>Whether to enable mutual authentication.
 * </ul>
 *
 *@since 3.5
 */
public class KerberosClientCreds implements LoginCredentials, Serializable {

    private static final long serialVersionUID = 1L;

    private static final String HOST_PRINCIPAL_PAIR_PATTERN =
            "^([^:,]+):([^:,]+)(,([^:,]+):([^:,]+))*";

    /* User principal name */
    private final String username;

    /* Credentials obtained from login subject */
    private Subject loginSubj;

    /* Service principal and host name mapping information */
    private final KrbServicePrincipals servicePrincipalInfo;

    /* Whether enable mutual authentication */
    private final boolean requireMutualAuth;

    public KerberosClientCreds(String username,
                               Subject subject,
                               String hostPrincPairs,
                               boolean requireMutualAuth)
        throws IllegalArgumentException {

        if (username == null) {
            throw new IllegalArgumentException(
                "The username argument must not be null");
        }
        if (subject == null) {
            throw new IllegalArgumentException("The subject must not be null");
        }

        this.username = username;
        this.loginSubj = subject;

        if (hostPrincPairs == null) {
            /*
             * If user does not specify helper host service principal pairs,
             * assume they choose to use the default service name for server
             * principals.
             */
            this.servicePrincipalInfo = new KrbServicePrincipals();
        } else {
            this.servicePrincipalInfo = new KrbServicePrincipals(
                makeServicePrincipalMap(hostPrincPairs));
        }
        this.requireMutualAuth = requireMutualAuth;
    }

    /** Instances of this class should not be serialized. */
    @SuppressWarnings("unused")
    public KerberosClientCreds(DataInput in, short sv) {
        throw new IllegalStateException(
            "Unexpected attempt to deserialize an instance of" +
            " KerberosClientCreds");
    }

    /** Instances of this class should not be serialized. */
    @Override
    public void writeFastExternal(DataOutput out, short sv) {
        throw new IllegalStateException(
            "Unexpected attempt to serialize an instance of" +
            " KerberosClientCreds");
    }

    @Override
    public LoginCredentialsType getLoginCredentialsType() {
        return StdLoginCredentialsType.KERBEROS_CLIENT;
    }

    public Subject getLoginSubject() {
        return loginSubj;
    }

    public KrbServicePrincipals getKrbServicePrincipals() {
        return servicePrincipalInfo;
    }

    public boolean getRequireMutualAuth() {
        return requireMutualAuth;
    }

    public void addServicePrincipal(String hostName, String instanceName) {
        servicePrincipalInfo.addPrincipal(hostName, instanceName);
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String toString() {
        return "user name: " + username + "\n" +
               "login subject: " + loginSubj.toString();
    }

    /*
     * Make the map of host:principal entry according to the string in the
     * configuration.
     */
    private Map<String, String> makeServicePrincipalMap(String hostPrincPairs) {

        /* Check if given string of host principal pairs is in valid format */
        if (!Pattern.matches(HOST_PRINCIPAL_PAIR_PATTERN, hostPrincPairs)) {
             throw new IllegalArgumentException("Service principal input " +
                 hostPrincPairs + " does not match the pattern " +
                "host:principal [,host:principal]*");
        }

        final Map<String, String> map = new HashMap<String, String>();
        for (String comp: hostPrincPairs.trim().split(",")) {
            final String[] pair = comp.split(":");
            if (pair.length != 2) {
                throw new IllegalArgumentException("Invalid pair of " +
                    "service principal: " + comp);
            }
            String host = pair[0].trim();
            String principal = pair[1].trim();
            map.put(host, principal);
        }
        return map;
    }

    public static class KrbServicePrincipals {
        private static final char KRB_COMPONENT_SEPARATOR = '/';
        private static final char KRB_REALM_SEPARATOR = '@';
        private static final String KRB_SERVICE_NAME_DEFAULT = "oraclenosql";
        private final Map<String, String> principals;

        /* Value resolved from specified host-service mapping */
        private String defaultRealm;
        private String serviceName;

        KrbServicePrincipals() {
            this.serviceName = KRB_SERVICE_NAME_DEFAULT;
            this.principals  = new HashMap<String, String>();
        }

        KrbServicePrincipals(Map<String, String> helperHostPrincs)
            throws IllegalArgumentException {

            this.principals = helperHostPrincs;

            /* Discover common service name and realm name */
            discoverPrincipalInfo();
        }

        public Map<String, String> getHelperhostPrincipals() {
            return principals;
        }

        /**
         * Return principal name of given host. If there is no any entry can
         * be found, return default service name for Kerberos login if it has
         * been specified.
         */
        public String getPrincipal(String hostName) {
            if (principals.isEmpty() && serviceName != null) {
                return serviceName;
            }
            return principals.get(hostName);
        }

        /**
         * Build a new principal based on given instance name for specified
         * host name. The principal are generated with default realm, service
         * and given instance name.
         */
        public void addPrincipal(String hostName, String instanceName) {
            final StringBuilder sb = new StringBuilder();
            sb.append(serviceName);

            if (instanceName != null && !instanceName.equals("")) {
                sb.append(SecurityUtils.KRB_NAME_COMPONENT_SEPARATOR_STR);
                sb.append(instanceName);
            }

            if (defaultRealm != null) {
                sb.append(SecurityUtils.KRB_NAME_REALM_SEPARATOR_STR);
                sb.append(defaultRealm);
            }
            principals.put(hostName, sb.toString());
        }

        /* For testing */
        public String getDefaultRealm() {
            return defaultRealm;
        }

        public String getServiceName() {
            return serviceName;
        }

        /*
         * Discover service principal service and realm name from given
         * principal information of helper hosts. NoSQL database service use
         * the same service and realm name in principal, this method resolve
         * all principals user specified, if any principal use the different
         * service or realm name, throw IllegalArgumentException.
         *
         * Note that Java login module can read realm from Kerberos
         * configuration file and build the canonical principal name itself,
         * so realm name of helper host principal is allowed to be empty, but
         * service name must be found in this method, otherwise throw
         * IllegalArgumentException.
         */
        private void discoverPrincipalInfo()
            throws IllegalArgumentException {

            /*
             * NoSQL database service principals are in format:
             * 'service/instance@realm', find the last '/' as the component
             * separator, the characters before this separator are considered
             * as service name, the rest characters before the '@' separator
             * are instance name. Note that Kerberos principal does not allow
             * more '@' exist in principal name, if encounter more than one '@'
             * throw IllegalArgumentException.
             */
            boolean first = true;
            for (String principal : principals.values()) {
                int compSeparator = -1;
                int realmSeparator = -1;
                for (int i = 0; i < principal.length(); i++) {
                    char c = principal.charAt(i);
                    if (c == KRB_COMPONENT_SEPARATOR) {
                        /* Find the last '/' separator before '@' separator */
                        if ((realmSeparator == -1) || (c < realmSeparator)) {
                            compSeparator = i;
                        }
                    }
                    if (c == KRB_REALM_SEPARATOR) {
                        /* Principal name does not allow two '@' */
                        if (realmSeparator != -1) {
                            throw new IllegalArgumentException(
                                "Invalid principal name " + principal);
                        }
                        realmSeparator = i;
                    }
                }
                String service = principal;
                String realm = null;

                /* Principal have multiple components */
                if (compSeparator != -1) {
                    service = principal.substring(0, compSeparator);
                }

                /* Principal has realm */
                if (realmSeparator != -1) {
                    realm = principal.substring(realmSeparator + 1,
                                                principal.length());
                }

                if (serviceName == null) {
                    serviceName = service;
                } else if (!serviceName.equals(service)) {
                    throw new IllegalArgumentException(
                        "Principal service name must be the same in a store");
                }

                if (first) {
                    defaultRealm = realm;
                    first = false;
                } else {
                    if (defaultRealm == null) {
                        if (realm != null) {
                            throw new IllegalArgumentException(
                                "Principal " + principal + " cannot specify" +
                                " a realm when the principal for the first" +
                                " host mapping uses the default realm");
                        }
                    } else if (!defaultRealm.equals(realm)) {
                        throw new IllegalArgumentException(
                            "Principal " + principal + " must specify the" +
                            " same realm as the first principal: " +
                            defaultRealm);
                    }
                }
            }
        }
    }
}
