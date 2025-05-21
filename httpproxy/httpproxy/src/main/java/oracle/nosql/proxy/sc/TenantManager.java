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

package oracle.nosql.proxy.sc;

import java.util.concurrent.TimeoutException;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.Config;
import oracle.nosql.proxy.protocol.Protocol.OpCode;
import oracle.nosql.proxy.sc.TableUtils.PrepareCB;
import oracle.nosql.proxy.security.AccessContext;
import oracle.nosql.proxy.MonitorStats;
import oracle.nosql.proxy.util.TableCache;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.ErrorResponse;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableLimits;
import oracle.nosql.util.tmi.TenantLimits;

/**
 * TenantManager is an internal interface to abstract the operations
 * provided by the NoSQL cloud Tenant Manager. In a cloud deployment
 * these operations will translate to/from REST calls supported by the
 * Service Controller. In a test or non-cloud environment the implementation
 * will be mocked, stubbed out, or implemented in terms of a local store.
 *
 */
public interface TenantManager {

    /**
     * Gets the state of a table that has already been created.
     *
     * @param actx access context
     * @param tableName the name of the table
     * @param operationId an optional operation id which was returned in
     * a TableResult to a driver user. If supplied the TenantManager instance
     * should use it to check status of the associated operation and throw
     * an exception if there has been a failure.
     * @param internal whether this is an internal. For Cloud, internal
     * won't be throttled.
     * @param lc log context
     *
     * @return a GetTableResponse representing the table or any errors
     */
    GetTableResponse getTable(AccessContext actx,
                              String tableName,
                              String operationId,
                              boolean internal,
                              LogContext lc);


    /**
     * Creates a table
     *
     * @param actx access context
     * @param tableName the name of the table, scoped to a tenant
     * @param statement the DDL statement to use to create the table
     * @param ifNotExists if true indicates that if the table already exists,
     * @param limits the table limits, required.
     * @param isAutoReclaimable the flag indicates the table is auto reclaimable
     * @param retryToken the retry token
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    GetTableResponse createTable(AccessContext actx,
                                 String tableName,
                                 String statement,
                                 boolean ifNotExists,
                                 TableLimits limits,
                                 boolean isAutoReclaimable,
                                 String retryToken,
                                 LogContext lc);

    /**
     * Alters a table
     *
     * @param actx access context
     * @param tableName the name of the table, scoped to a tenant
     * @param statement the DDL statement to use to create the table. This
     * paramter is mutually exclusive with respect to limits
     * @param limits is specified if there is no statement. This parameter is
     * mutually exclusive with respect to statement.
     * @param matchETag the table ETag to be matched
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    GetTableResponse alterTable(AccessContext actx,
                                String tableName,
                                String statement,
                                TableLimits limits,
                                byte[] matchETag,
                                LogContext lc);

    /**
     * Drops a table
     *
     * @param actx access context
     * @param tableName the name of the table, scoped to a tenant
     * @param ifExists will be true if the query contains an "if exists"
     * clause indicating that if the table does not exist that is not a failure.
     * @param matchETag the table ETag to be matched
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    GetTableResponse dropTable(AccessContext actx,
                               String tableName,
                               boolean ifExists,
                               byte[] matchETag,
                               LogContext lc);

     /**
     * Creates an index
     *
     * @param actx access context
     * @param tableName the name of the table, scoped to a tenant
     * @param indexName the name of the index, scoped to a table
     * @param statement the DDL statement to use to create the table
     * @param ifNotExists if true indicates that if the table already exists,
     * return success
     * @param retryToken the retry token
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    GetTableResponse createIndex(AccessContext actx,
                                 String tableName,
                                 String indexName,
                                 String statement,
                                 boolean ifNotExists,
                                 String retryToken,
                                 LogContext lc);

    /**
     * Drops an index
     *
     * @param actx access context
     * @param tableName the name of the table, scoped to the tenant
     * @param indexName the name of the index
     * @param ifExists will be true if the query contains an "if exists"
     * clause indicating that if the table does not exist that is not a failure.
     * @param matchETag the index ETag to be matched
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    GetTableResponse dropIndex(AccessContext actx,
                               String tableName,
                               String indexName,
                               boolean ifExists,
                               byte[] matchETag,
                               LogContext lc);

    /**
     * Changes a table's compartment, the destination compartmentId is carried
     * in the AccessContext
     *
     * @param actx access context
     * @param tableName the name of the table
     * @param matchETag the table ETag to be matched
     * @param retryToken the retry token
     * @param lc log context
     *
     * @return a GetTableResponse representing the table and its state
     */
    default GetTableResponse changeCompartment(AccessContext actx,
                                               String tableName,
                                               byte[] matchETag,
                                               String retryToken,
                                               LogContext lc) {
        return null;
    }

   /**
     * Returns a list of tables owned by the tenant
     *
     * @param actx access context
     * @param startIndex if non-zero it specifies the index to use to start
     * returning tables
     * @param numTables if not 0 it is used to limit the number of tables
     * returned
     * @param lc log context
     *
     * @return ListTableResponse
     */
    ListTableResponse listTables(AccessContext actx,
                                 int startIndex,
                                 int numTables,
                                 LogContext lc);

    /**
      * Returns a list of TableInfo objects
      *
      * @param actx access context
      * @param namePattern a shell-globbing-style (*?[]) filter for names.
      * @param state the filter for lifecycle state.
      * @param sortBy The field to sort by.
      * @param isAscSort flag indicates if the sort order is ascending
      * @param startIndex if non-zero it specifies the index to use to start
      * returning tables
      * @param numTables if non-zero it is used to limit the number of tables
      * returned
      * @param lc log context
      *
      * @return ListTableInfoResponse
      */
     ListTableInfoResponse listTableInfo(AccessContext actx,
                                         String namePattern,
                                         String state,
                                         String sortBy,
                                         boolean isAscSort,
                                         int startIndex,
                                         int numTables,
                                         LogContext lc);

    /**
     * Gets store information so that the proxy can connect to the store
     * associated with the table.
     *
     * @param tenant id
     * @param tableName the name of the table, scoped to a tenant
     * @param lc log context
     *
     * @return a GetStoreResponse representing the table or any errors
     */
    GetStoreResponse getStoreInfo(String tenantId,
                                  String tableName,
                                  LogContext lc);

    /**
     * Gets dynamic usage information for a table.
     *
     * @param actx access context
     * @param tableName the name of the table
     * @param startTimestamp optional start timestamp, milliseconds since Epoch
     * @param endTimestamp optional end timestamp, milliseconds since Epoch
     * @param startIndex if non-zero it specifies the index to use to start
     * returning table usage entries.
     * @param limit optional max number of usage records returned
     * @param lc log context
     *
     * @return a GetTableUsageResponse representing the table or any errors
     */
    TableUsageResponse getTableUsage(AccessContext actx,
                                     String tableName,
                                     long startTimestamp,
                                     long endTimestamp,
                                     int startIndex,
                                     int limit,
                                     LogContext lc);

    /**
     * Gets information about indexes of a table.
     *
     * @param tenantId the tenant's identifier
     * @param actx access context
     * @param tableName the name of the table
     * @param indexName the name of the index
     * @param namePattern a shell-globbing-style (*?[]) filter for index names.
     * @param state the filter for lifecycle state.
     * @param sortBy The field to sort by.
     * @param isAscSort flag indicates if the sort order is ascending
     * @param startIndex if non-zero it specifies the index to use to start
     * returning tables
     * @param limit if non-zero it is used to limit the number of indexes
     * returned
     * @param lc log context
     *
     * @return IndexResponse representing the index or indexes requested
     */
    IndexResponse getIndexInfo(AccessContext actx,
                               String tableName,
                               String indexName,
                               String namePattern,
                               String state,
                               String sortBy,
                               boolean isAscSort,
                               int startIndex,
                               int limit,
                               LogContext lc);

    /**
     * Returns a TenantLimits instance for the named tenantId
     *
     * @param tenantId the tenant to use for the limits
     * @param lc log context
     *
     * @return TenantLimits representing the tenant limits.
     */
    TenantLimits getTenantLimits(String tenantId, LogContext lc);

    /**
     * Gets workRequestId from TableInfo object.
     */
    default String getWorkRequestId(TableInfo tableInfo, OpCode opCode) {
        return null;
    }

    /**
     * Gets the DDL work request information.
     *
     * @param actx access context
     * @param workRequestId the workRequest id
     * @param internal whether this is an internal. For Cloud, internal
     * won't be throttled.
     * @param lc log context
     *
     * @return GetDdlWorkRequestResponse
     */
    default GetDdlWorkRequestResponse getDdlWorkRequest(AccessContext actx,
                                                        String workRequestId,
                                                        boolean internal,
                                                        LogContext lc) {
        return null;
    }

    /**
     * Gets the work request information.
     *
     * @param actx access context
     * @param workRequestId the workRequest id
     * @param lc log context
     *
     * @return GetWorkRequestResponse
     */
    default GetWorkRequestResponse getWorkRequest(AccessContext actx,
                                                  String workRequestId,
                                                  boolean internal,
                                                  LogContext lc) {
        return null;
    }

    /**
     * Return a list of work requests owned by a compartment.
     *
     * @param actx access context, it contains the compartmentId.
     * @param startIndex if non-zero it specifies the index to use to start
     * returning workRequests
     * @param limit if non-zero it is used to limit the number of workRequests
     * returned
     * @param lc log context
     *
     * @return ListWorkRequestResponse
     */
    default ListWorkRequestResponse listWorkRequests(AccessContext actx,
                                                     String startIndex,
                                                     int limit,
                                                     LogContext lc) {
        return null;
    }

    /**
     * Lists all persistent rules
     *
     * @param lc log context
     *
     * @return ListRuleResponse
     */
    ListRuleResponse listRules(LogContext lc);

    /**
     * Sets table's activity state to ACTIVE
     *
     * @param tableOcid the table ocid
     * @param lc log context
     *
     * @return ListWorkRequestResponse
     */
    default GetTableResponse setTableActive(AccessContext actx,
                                            String tableOcid,
                                            LogContext lc) {
        return null;
    }

    /**
     * Adds replica to table
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param newReplicaServiceName the service name of new replica
     * @param readLimit read limit of new replica table
     * @param writeLimit write limit of new replica table
     * @param matchETag the ETag of the affected table
     * @param retryToken the retry token
     * @param lc the LogContext instance
     *
     * @return a GetTableResponse representing the table and its state
     */
    default GetTableResponse addReplica(AccessContext actx,
                                        String tableNameOrId,
                                        String newReplicaServiceName,
                                        int readLimit,
                                        int writeLimit,
                                        byte[] matchETag,
                                        String retryToken,
                                        LogContext lc) {
        return new GetTableResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "AddReplica is not supported"));
    }

    /**
     * Drops replica from table
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param replicaToRemoveServiceName the name of replica to be dropped.
     * @param matchETag the ETag of the affected table
     * @param lc the LogContext instance
     *
     * @return a GetTableResponse representing the table and its state
     */
    default GetTableResponse dropReplica(AccessContext actx,
                                         String tableNameOrId,
                                         String replicaToRemoveServiceName,
                                         byte[] matchETag,
                                         LogContext lc) {
        return new GetTableResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "DropReplica is not supported"));
    }

    /**
     * Executes internal ddl operation
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param operation the operation type
     * @param payload the payload of request
     * @param lc the LogContext instance
     *
     * @return WorkRequestIdResponse representing the work request Id.
     */
    default WorkRequestIdResponse doInternalDdl(AccessContext actx,
                                                String tableNameOrId,
                                                String operation,
                                                String payload,
                                                LogContext lc) {
        return new WorkRequestIdResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "ExecInternalDdl is not supported"));
    }

    /**
     * Gets replica stats information for a multi-region table.
     *
     * @param actx the AccessContext instance
     * @param tableNameOrId the target table name or OCID
     * @param replicaName the name of the replica
     * @param startTime start timestamp, milliseconds since Epoch
     * @param limit optional, default 1000
     * @param lc log context
     *
     * @return a ReplicaStatsResponse representing the table or any errors
     */
    ReplicaStatsResponse getReplicaStats(AccessContext actx,
                                         String tableNameOrId,
                                         String replicaName,
                                         long startTime,
                                         int limit,
                                         LogContext lc);
    /**
     * Gets the service level kms key information
     *
     * @param actx the AccessContext instance
     * @param internal whether this is an internal. For Cloud, internal
     * won't be throttled.
     * @param lc log context
     *
     * @return a GetKmsKeyInfoResponse representing the kms key information
     */
    default GetKmsKeyInfoResponse getKmsKey(AccessContext actx,
                                            boolean internal,
                                            LogContext lc) {
        return new GetKmsKeyInfoResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "getKmsKey is not supported"));
    }

    /**
     * Updates the service level kms key
     *
     * @param actx the AccessContext instance
     * @param kmsKeyId the kms key Id
     * @param kmsVaultId the kms vault Id
     * @param matchETag the index ETag to be matched
     * @param dryRun true if test this operation without actually executing it
     * @param lc the LogContext instance
     *
     * @return WorkRequestIdResponse representing the work request Id.
     */
    default WorkRequestIdResponse updateKmsKey(AccessContext actx,
                                               String kmsKeyId,
                                               String kmsVaultId,
                                               byte[] matchETag,
                                               boolean dryRun,
                                               LogContext lc) {
        return new WorkRequestIdResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "updateKmsKey is not supported"));
    }

    /**
     * Removes the kms key used by the service
     *
     * @param actx the AccessContext instance
     * @param matchETag the index ETag to be matched
     * @param dryRun true if test this operation without actually executing it
     * @param lc the LogContext instance
     *
     * @return WorkRequestIdResponse representing the work request Id.
     */
    default WorkRequestIdResponse removeKmsKey(AccessContext actx,
                                               byte[] matchETag,
                                               boolean dryRun,
                                               LogContext lc) {
        return new WorkRequestIdResponse(
                ErrorResponse.build(ErrorCode.UNSUPPORTED_OPERATION,
                                    "removeKmsKey is not supported"));
    }

    /**
     * Certain environments require that a newly-created store is
     * fully initialized before operating. Test environments and
     * Cloudsim, and perhaps others.
     * @param timeoutSecs time to wait
     * @throws TimeoutException on timeout
     */
    default void waitForStoreInit(int timeoutSecs) throws TimeoutException {
    }

    /**
     * Is the underlying store secure? Default to true for the cloud.
     * @return true if the store is secure
     */
    default boolean isSecureStore() {
        return true;
    }

    /**
     * Closes the instance. This may be a no-op.
     */
    void close();

    /**
     * Sets the logger
     */
    void setLogger(final SkLogger logger);

    /**
     * Allows TenantManager to create an appropriate TableCache
     */
    void createTableCache(Config config,
                          MonitorStats stats,
                          SkLogger logger);

    TableCache getTableCache();

    void shutDown();

    /**
     * Create a prepare callback for DDL/DML to determine the operation context.
     * This is abstracted to allow cloud vs on-premise behavior differences.
     *
     * @param namespace if set any string returned in the namespaceName
     *        callback must match.
     * @return PrepareCB representing the compiled result of DDL/DML statement
     */
    default PrepareCB createPrepareCB(String namespace) {
        return new PrepareCB(namespace, false);
    }

    /**
     * This region id is an integer value which is used when we need to persist
     * the region id in a space efficient format.
     *
     * The region id has some semantic meaning to KV. Region ids that are:
     *   > 0 : tell KV that this region was defined "externally" by the cloud
     *   == 0: tells KV that this is an on-prem MR table
     *   < 0 : a negative region id affects how CRDT processing works.
     *
     * By default, return a value that says the region was defined externally.
     *
     * @return the integer value that acts as the region id, which is saved
     * when we need to persist the region id in a space efficient format.
     */
    default public int getLocalRegionId() {
        /*
         * In implementations of the TenantManager that support on-prem,
         * the TenantManager must return 0. Here, it defaults to
         * externally defined region id.
         */
        return 1;
    }

    /**
     * Cloud multi-region tables only:
     * Convert the service name, which is a compact, textual id for this region
     * into a well known OCI region id.  This region name is used in any
     * messages and errors visible to the application.
     *
     * @param serviceName
     * @return OCI region name
     */
    default String translateToRegionName(String serviceName) {
        /*
         * In implementations of the TenantManager that support on-prem,
         * cloudsim or tests, no value is needed. The TenantManager used by
         * a proxy in a true cloud deployment or minicloud must
         * override this and return a valid OCI region name.
         */
        return null;
    }

    /**
     * Cloud multi-region tables only:
     * Checks if the specified remote region is a valid replication partner for
     * this region. If it is, return the service name for that remote region.
     * @param remoteRegionName
     * @return service name for remote region.
     */
    default String validateRemoteReplica(String remoteRegionName) {
        /*
         * In implementations of the TenantManager that support on-prem,
         * cloudsim or tests, no value is needed. The TenantManager used by
         * a proxy in a true cloud deployment or minicloud must
         * override this and return a valid service name for the remote region.
         */
        return null;
    }

    /**
     * @return the human readable, compact, textual string used to uniquely
     * represent this region. Meant for internal use, and should not be
     * returned to the application.
     */
    default String getLocalServiceName() {
        /*
         * In implementations of the TenantManager that support on-prem,
         * cloudsim or tests, no value is needed. The TenantManager used by
         * a proxy in a true cloud deployment or minicloud must
         * override this and return a valid service name.
         */
        return null;
    }
}
