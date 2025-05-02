/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.security;

import static oracle.nosql.proxy.protocol.BinaryProtocol.INVALID_AUTHORIZATION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.SECURITY_INFO_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.HttpConstants.AUTHORIZATION;
import static oracle.nosql.proxy.protocol.HttpConstants.TOKEN_PREFIX;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.filter.FilterHandler.Filter;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.TenantManager;

import org.checkerframework.checker.nullness.qual.Nullable;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;

public class AccessCheckerFactory {
    private static final String IAM_ACCESS_CHECKER_CLASS =
        "oracle.nosql.proxy.security.iam.IAMAccessChecker";

    /**
     * Create an insecure access checker. Should only be used by CloudSim.
     */
    public static AccessChecker createInsecureAccessChecker() {
        return new InsecureAccessChecker();
    }

    /**
     * Create access checkers.
     *
     * It creates IAM access checkers if configuration is available,
     * will do the security checks at request basis.
     *
     * @param tm only used by IAM access checker
     * @param configFile IAM configuration
     * @return an IAM access checker
     */
    public static AccessChecker createIAMAccessChecker(TenantManager tm,
                                                       String configFile) {
        Class<?> iamClass;
        try {
            iamClass = Class.forName(IAM_ACCESS_CHECKER_CLASS);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException(
                "Unable to find IAM access checker class", cnfe);
        }

        try {
            Method method = iamClass.getMethod("createAccessChecker",
                TenantManager.class, String.class);
             return (AccessChecker) method.invoke(null, tm, configFile);
        } catch (Throwable t) {
            throw new IllegalArgumentException(
                "Unable to create IAM access checker", t);
        }
    }

    /**
     * Insecure access checker - should used by CloudSim only.
     */
    private static class InsecureAccessChecker implements AccessChecker {

        private final AccessContext nullAccessContext =
            new AccessContext() {
                /*
                 * Note well: this returns a default invalid namespace string
                 * To satisfy the logic in TableUtils namespaceName(). After
                 * much deliberations including JC and GF, we decided to do
                 * this instead of more complex refactoring. JC 8/24/20
                 */
                @Override
                public String getNamespace() {
                    return "in.valid.iac.name.space";
                }

                @Override
                public void setNamespace(String namespace) {
                }

                @Override
                public String getPrincipalId() {
                    return null;
                }

                @Override
                public String getTenantId() {
                    return null;
                }

                @Override
                public Type getType() {
                    return Type.INSECURE;
                }

                @Override
                public String getCompartmentId() {
                    return null;
                }
            };

        private final Map<String, Long> simulatedIAMCache;

        public InsecureAccessChecker() {
            /* allow simulating IAM auth cache behavior */
            if (Boolean.getBoolean("test.simulateiam")) {
                simulatedIAMCache = new HashMap<String, Long>();
            } else {
                simulatedIAMCache = null;
            }
        }

        private boolean validateLegacyAuth(String authHeader) {
            /*
             * legacy auth header (bearer token) format:
             * Bearer <tenantId>
             */
            if (authHeader.indexOf(TOKEN_PREFIX) != 0) {
                return false;
            }
            final String[] splitAuthHeader =
                authHeader.trim().split(TOKEN_PREFIX);
               if (splitAuthHeader.length != 2) {
                return false;
            }
            return true;
        }

        private boolean minimallyValidateIAMAuth(String authHeader) {
            /*
             * IAM header format:
             *
             * Signature version="%s",headers="%s",keyId="%s",
             *           algorithm="rsa-sha256",signature="%s"
             *
             * example real header:
             * Signature headers="(request-target) host date",keyId="ocid1.tenancy.oc1..aaaaaaaaba3pv6wuzr4h25vqstifsfdsq/ocid1.user.oc1..aaaaaaaa65vwl75tewwm32rgqvm6i34unq/9b:39:03:07:c6:fa:5c:58:7d:60:85:d8:3e:5c:be:7e",algorithm="rsa-sha256",signature="LLszR7k+iORqsLNOVXdPVjRupFDnV99PhByYqWGxsJi6/04xWD0jVA4hnawCG5ciyXA4O2eUH+Ggh/glEnbLht3yowdLelPDnI6nQ9fC7tsQjIM5YsFka0k9AzPPRkpX6l2Ic3/CWvonf9zjeR6KM1ICcakCrYj6Xjmla5tapbJJ5AOv1r5jzCiIAq6avZSS+rRHrFjFVbgKkGekFJKJjh4CPA1beO1YYBF+ZcIGwxL7ItvWkV2AFTEv/0L15W4hEkEbDjQq5eeCvJdLUD8VfLYt1ELLmMZdnUvPXVfYrCHM1qQWLKS6KSerIjdaSKvzYD71idCDDQ+FGFYxcOPA8Q==",version="1"
             */
            if (authHeader == null ||
                authHeader.indexOf("Signature ") != 0 ||
                authHeader.indexOf("version=\"") < 0 ||
                authHeader.indexOf("headers=\"") < 0 ||
                authHeader.indexOf("keyId=\"") < 0 ||
                authHeader.indexOf("algorithm=\"") < 0 ||
                authHeader.indexOf("signature=\"") < 0) {
                return false;
            }
            return true;
        }

        @Override
        public AccessContext checkAccess(HttpMethod httpMethod,
                                         String requestPath,
                                         HttpHeaders httpHeaders,
                                         OpCode opCode,
                                         @Nullable String compartmentId,
                                         @Nullable String tableName,
                                         @Nullable AccessContext actx,
                                         Filter filter,
                                         LogContext lc)
            throws RequestException {

            /*
             * Allow for either a legacy Bearer token header
             * or a minimally-validated IAM Signature header, or simply
             * no auth header at all.
             */
            String authHeader = httpHeaders.get(AUTHORIZATION);

            boolean isIAMAuthHeader = false;

            if (authHeader != null && authHeader.length() > 0 &&
                validateLegacyAuth(authHeader) == false) {
                    isIAMAuthHeader = minimallyValidateIAMAuth(authHeader);
                    if (isIAMAuthHeader == false) {
                        throw new RequestException(
                            INVALID_AUTHORIZATION,
                            "Invalid authorization header " + authHeader);
                    }
            }

            /*
             * if we have a valid IAM header, and in special test mode to
             * simulate real IAM, see if we have this auth in a "cache", and
             * if so, is it "valid" (only after a certain time).
             * If not, add it to the cache with a 150-230ms
             * pause (to simulate IAM identity check) and return a
             * SECURITY_INFO_UNAVAILABLE error.
             * Note: real IAM auth check usually takes 30-50ms, but we make
             * that longer here to verify multiple client retries since the
             * client drivers have a fixed 100ms retry time on auth errors.
             */
            if (simulatedIAMCache != null && isIAMAuthHeader) {
                boolean isOK = false;
                Long okAfter = simulatedIAMCache.get(authHeader);
                if (okAfter == null) {
                    long okTime = System.currentTimeMillis() +
                                      (long)(Math.random() * 70.0) + 150;
                    simulatedIAMCache.put(authHeader, okTime);
                } else {
                    if (System.currentTimeMillis() > okAfter.longValue()) {
                        isOK = true;
                    }
                }
                if (isOK == false) {
                    throw new RequestException(
                        SECURITY_INFO_UNAVAILABLE,
                        "NotAuthenticated.");
                }
            }

            /* Filter the request */
            if (filter != null) {
                filter.filterRequest(opCode,
                                     null, /*TenantId*/
                                     null, /*PrincipalId*/
                                     tableName,
                                     lc);
            }

            return nullAccessContext;
        }

        @Override
        public AccessContext checkAccess(HttpMethod httpMethod,
                                         String uri,
                                         HttpHeaders headers,
                                         OpCode op,
                                         @Nullable String compartmentId,
                                         @Nullable String tableNameOrId,
                                         @Nullable byte[] payload,
                                         @Nullable AccessContext actx,
                                         Filter filter,
                                         LogContext lc)
            throws RequestException {

            checkAccess(httpMethod, uri, headers, op, compartmentId,
                        tableNameOrId, actx, filter, lc);

            if (actx == null) {
                actx = new InsecureAccessContext();
            }
            if (compartmentId != null) {
                actx.setCompartmentId(compartmentId);
            }
            return actx;
        }

        @Override
        public AccessContext checkWorkRequestAccess(
                HttpMethod httpMethod,
                String uri,
                HttpHeaders headers,
                OpCode op,
                @Nullable String compartmentId,
                @Nullable String workRequestId,
                @Nullable byte[] payload,
                Filter filter,
                LogContext lc)
            throws RequestException {

            checkAccess(httpMethod, uri, headers, op, compartmentId,
                        null /* tableName */, null /* actx*/, filter, lc);

            InsecureAccessContext actx = new InsecureAccessContext();
            if (compartmentId != null) {
                actx.setCompartmentId(compartmentId);
            }
            return actx;
        }
    }

    /**
     * The AccessContext used by rest interface in CloudSim.
     *
     * The compartmentId is used as namespace of the table in the store.
     */
    private static class InsecureAccessContext implements AccessContext {

        private String compartmentId;

        /* Used compartmentId as namespace of the table */
        @Override
        public String getNamespace() {
            return compartmentId;
        }

        @Override
        public String getPrincipalId() {
            return null;
        }

        @Override
        public String getTenantId() {
            return null;
        }

        @Override
        public Type getType() {
            return Type.INSECURE;
        }

        @Override
        public void setCompartmentId(String compartmentId) {
            this.compartmentId = compartmentId;
        }

        @Override
        public String getCompartmentId() {
            return compartmentId;
        }
    }
}
