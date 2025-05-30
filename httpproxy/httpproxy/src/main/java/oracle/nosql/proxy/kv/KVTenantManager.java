/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.kv;

import static oracle.nosql.proxy.protocol.HttpConstants.BEARER_PREFIX;
import static oracle.nosql.proxy.protocol.Protocol.INVALID_AUTHORIZATION;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import oracle.kv.KVSecurityException;
import oracle.kv.ExecutionFuture;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.impl.client.admin.DdlFuture;
import oracle.kv.impl.security.AuthContext;
import oracle.kv.impl.security.login.LoginToken;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.query.ExecuteOptions;
import oracle.kv.table.Table;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.RequestException;
import oracle.nosql.proxy.RequestLimits;
import oracle.nosql.proxy.sc.GetTableResponse;
import oracle.nosql.proxy.sc.LocalTenantManager;
import oracle.nosql.proxy.sc.TableUsageResponse;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableInfo.TableState;
import oracle.nosql.util.tmi.TableLimits;

public class KVTenantManager extends LocalTenantManager {

    private KVTenantManager(KVStore store,
                            String storeName,
                            String[] helperHosts) {
        super(store, storeName, false, helperHosts);
    }

    public static KVTenantManager createTenantManager(
        Config config) {
        final KVStoreConfig storeConfig = config.getTemplateKVStoreConfig();
        storeConfig.setStoreName(config.getStoreName());
        storeConfig.setHelperHosts(config.getHelperHosts());
        try {
            return new KVTenantManager(connectKVStore(storeConfig),
                                       config.getStoreName(),
                                       config.getHelperHosts());
        } catch (KVSecurityException kse) {
            throw kse;
        }
    }

    @Override
    public TableUsageResponse getTableUsage(AccessContext actx,
                                            String tableName,
                                            long startTimestamp,
                                            long endTimestamp,
                                            int startIndex,
                                            int limit,
                                            LogContext lc) {
        return new TableUsageResponse(
            ErrorResponse.build(
                ErrorCode.UNSUPPORTED_OPERATION,
                "GetTableUsage not supported"));
    }

    @Override
    public PrepareCB createPrepareCB(String namespace) {
        /*
         * Allow namespace
         */
        return new PrepareCB(namespace, true) {
            @Override
            public TableMetadataHelper getMetadataHelper() {
                return tableAPI.getTableMetadataHelper();
            }
        };
    }

    /**
     * Override to set the auth context in the options
     */
    @Override
    protected ExecuteOptions createExecuteOptions(String namespace,
                                                  String authString,
                                                  LogContext lc) {
        return new ExecuteOptions().setNamespace(namespace, true)
            .setLogContext(new oracle.kv.impl.util.contextlogger.LogContext(lc)).
            setAuthContext(createAuthContext(authString));
    }

    /**
     * On-prem list ALL tables, recursively
     */
    @Override
    protected Set<String> getTableNames(String namespace) {

        Set<String> namespaces = null;
        if (namespace == null) {
            namespaces = listNamespaces();
        } else {
            namespaces = new HashSet<String>();
            namespaces.add(namespace);
        }

        /* repeatable order */
        Set<String> results = new LinkedHashSet<String>();

        for (String ns : namespaces) {
            addTablesToSet(results, ns);
        }
        return results;
    }

    @Override
    protected void addTableNameToSet(Set<String> results, Table table) {
        results.add(table.getFullNamespaceName());
    }

    @Override
    protected TableLimits getTableLimits(TableImpl table) {
        return null;
    }

    @Override
    protected oracle.kv.impl.api.table.TableLimits makeKVTableLimits(
        TableLimits limits) {
        return null;
    }

    @Override
    protected ExecutionFuture setTableLimits(
        String namespace,
        String tableName,
        oracle.kv.impl.api.table.TableLimits limits) {
        throw new IllegalArgumentException(
            "Table limit operation is not supported");
    }

    @Override
    protected TableInfo makeTableInfo(String tableName,
                                      String namespace,
                                      AccessContext actx,
                                      TableState state,
                                      TableLimits limits) {

        /* embed namespace in table name, clear namespace */
        return new TableInfo(makeNamespaceName(namespace,
                                               tableName),
                             null, // namespace
                             state,
                             null, // limits
                             0);
    }

    @Override
    protected DdlFuture createDdlFuture(int planId,
                                        String authString) {
        return new DdlFuture(planId, executor, createAuthContext(authString));
    }

    /*
     * Convert authentication string to login token
     */
    static AuthContext parseAuthString(String authString) {

        if (authString == null) {
            return null;
        }

        if (!authString.substring(0, BEARER_PREFIX.length()).
            equalsIgnoreCase(BEARER_PREFIX)) {
            throw new IllegalArgumentException("Illegal auth header prefix");
        }

        final String tokenHex =
            authString.substring(BEARER_PREFIX.length(), authString.length());

        if (tokenHex.isEmpty()) {
            throw new IllegalArgumentException("No access token");
        }

        try {
            /* Parse token back to byte array and create login token */
            final LoginToken token =
                LoginToken.fromByteArray(
                    LoginToken.convertHexToBytes(tokenHex));
            return new AuthContext(token);
        } catch (Exception e) {
            throw new RequestException(
                INVALID_AUTHORIZATION,
                "Fail to decode authentication header: " + e.toString());
        }
    }

    @Override
    protected GetTableResponse handleGetTableException(String operationId,
                                                       Exception e,
                                                       LogContext lc) {
        if (e instanceof KVSecurityException) {
            throw (KVSecurityException)e;
        }
        return super.handleGetTableException(operationId, e, lc);
    }

    private AuthContext createAuthContext(String authString) {
        return parseAuthString(authString);
    }

    @Override
    public RequestLimits getRequestLimits() {
        return null;
    }

    /**
     * Returns 0 as the service (region) identifier
     * A 0 value required to indicate that this proxy
     * is an on prem environment
     */
    @Override
    public int getLocalRegionId() {
        return 0;
    }
}
