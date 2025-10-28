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

import static oracle.nosql.proxy.protocol.HttpConstants.TOKEN_PREFIX;

import java.io.IOException;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.kv.StoreAccessTokenProvider;
import oracle.nosql.driver.ops.Request;

public class SecureTestUtil {
    /**
     * Configure NoSQLHandleConfig with authorization provider.
     */
    public static void setAuthProvider(NoSQLHandleConfig config,
                                       boolean security,
                                       String tenant) {
        setAuthProvider(config, security, false, tenant);
    }

    public static void setAuthProvider(NoSQLHandleConfig config,
                                       boolean security,
                                       boolean onprem,
                                       String tenant) {
        if (security) {
            if (onprem) {
                throw new IllegalArgumentException(
                    "setAuthProvider not supported (yet) for secure onprem");
            }
            config.setAuthorizationProvider(
                new TestSignatureProvider().setTenantId(tenant));
            return;
        }
        if (onprem) {
            config.setAuthorizationProvider(new StoreAccessTokenProvider());
        } else {
            config.setAuthorizationProvider(new AuthorizationProvider() {
                    @Override
                    public String getAuthorizationString(Request request) {
                        return getAuthHeader(tenant);
                    }

                    @Override
                    public void close() {
                    }
                });
        }
    }

    public static void setAuthProvider(NoSQLHandleConfig config,
                                       String configFile,
                                       String profile) {
        try {
            config.setAuthorizationProvider(
                    new SignatureProvider(configFile, profile));
        } catch (IOException ioe) {
            throw new IllegalArgumentException("Unable to load " + profile +
                                               " from configFile: " + ioe);
        }
    }

    /**
     * Get an authorization header with service access token.
     */
    public static String getAuthHeader(String tenantId, boolean security) {
        if (security) {
            return new TestSignatureProvider().setTenantId(tenantId)
                .getAuthorizationString(null);
        }
        return getAuthHeader(tenantId);
    }

    /**
     * Get an authorization header with access token.
     * @param needAccountAT whether need account access token
     */
    public static String getAuthHeader(String tenantId) {
        return TOKEN_PREFIX + tenantId;
    }
}
