/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import com.oracle.bmc.ClientConfiguration;
import com.oracle.bmc.ConfigFileReader;
import com.oracle.bmc.ClientConfiguration.ClientConfigurationBuilder;
import com.oracle.bmc.ConfigFileReader.ConfigFile;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.auth.SimpleAuthenticationDetailsProvider;
import com.oracle.bmc.auth.StringPrivateKeySupplier;
import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.nosql.NosqlClient;
import com.oracle.bmc.nosql.model.CreateIndexDetails;
import com.oracle.bmc.nosql.model.CreateTableDetails;
import com.oracle.bmc.nosql.model.IndexKey;
import com.oracle.bmc.nosql.model.TableCollection;
import com.oracle.bmc.nosql.model.TableLimits;
import com.oracle.bmc.nosql.model.TableLimits.CapacityMode;
import com.oracle.bmc.nosql.model.TableSummary;
import com.oracle.bmc.nosql.model.UpdateRowDetails;
import com.oracle.bmc.nosql.model.UpdateTableDetails;
import com.oracle.bmc.nosql.model.WorkRequest;
import com.oracle.bmc.nosql.model.WorkRequestError;
import com.oracle.bmc.nosql.requests.ChangeTableCompartmentRequest;
import com.oracle.bmc.nosql.requests.CreateIndexRequest;
import com.oracle.bmc.nosql.requests.CreateTableRequest;
import com.oracle.bmc.nosql.requests.DeleteIndexRequest;
import com.oracle.bmc.nosql.requests.DeleteTableRequest;
import com.oracle.bmc.nosql.requests.GetRowRequest;
import com.oracle.bmc.nosql.requests.GetTableRequest;
import com.oracle.bmc.nosql.requests.GetWorkRequestRequest;
import com.oracle.bmc.nosql.requests.ListTablesRequest;
import com.oracle.bmc.nosql.requests.ListWorkRequestErrorsRequest;
import com.oracle.bmc.nosql.requests.UpdateRowRequest;
import com.oracle.bmc.nosql.requests.UpdateTableRequest;
import com.oracle.bmc.nosql.responses.ChangeTableCompartmentResponse;
import com.oracle.bmc.nosql.responses.CreateIndexResponse;
import com.oracle.bmc.nosql.responses.CreateTableResponse;
import com.oracle.bmc.nosql.responses.DeleteIndexResponse;
import com.oracle.bmc.nosql.responses.DeleteTableResponse;
import com.oracle.bmc.nosql.responses.GetRowResponse;
import com.oracle.bmc.nosql.responses.GetTableResponse;
import com.oracle.bmc.nosql.responses.GetWorkRequestResponse;
import com.oracle.bmc.nosql.responses.ListTablesResponse;
import com.oracle.bmc.nosql.responses.ListWorkRequestErrorsResponse;
import com.oracle.bmc.nosql.responses.UpdateRowResponse;
import com.oracle.bmc.nosql.responses.UpdateTableResponse;
import com.oracle.bmc.requests.BmcRequest;
import com.oracle.bmc.retrier.RetryConfiguration;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.proxy.ProxyTestBase;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.tmi.DropInputs;
import oracle.nosql.util.tmi.TableDDLInputs;
import oracle.nosql.util.tmi.TableInfo;
import oracle.nosql.util.tmi.TableInfo.ActivityPhase;
import oracle.nosql.util.tmi.TableInfo.TableState;

public class RestAPITestBase extends ProxyTestBase {
    protected static final String TENANT_NOSQL_DEV =
        "ocid1.tenancy.oc1..aaaaaaaattuxbj75pnn3nksvzyidshdbrfmmeflv4kkemajroz2thvca4kba";
    /* default compartmentId used in mincloud test */
    private static final String MC_TEST_COMPARTMENT_ID =
        "ocid1.compartment.oc1..aaaaaaaaw2774bxkk4kndya4pl43ols5z263iupqvcpcjkoz52oieg5czvtq";
    /* default another compartmentId used in mincloud test */
    protected static String MC_TEST_COMPRATMENT_ID_FOR_UPDATE =
        "ocid1.compartment.oc1..aaaaaaaahy6aozjru5grkp2dhrhqfdwh4hihd6fpeafqdxvlfb6scf7hotnq";
    private static final String LOCAL_COMPARTMENT_ID = "test.compartment";

    private static final String USER_OCID = "ocid1.user.oc1..dummyuser";
    private static final String FINGER_PRTINT =
        "01:02:03:04:05:06:07:08:09:0A:0B:0C:0D:0E:0F:10";
    private static final String PRIVATE_KEY = genPrivateKey();

    private final static int DEFAULT_WAIT_MS = 20_000;
    private final static int DEFAULT_DELAY_MS = 500;

    protected final static TableLimits defaultLimits =
        TableLimits.builder()
            .maxReadUnits(100)
            .maxWriteUnits(100)
            .maxStorageInGBs(1)
            .capacityMode(CapacityMode.Provisioned)
            .build();

    protected NosqlClient client;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        /*
         * The rest interface is not yet enabled for onprem, disable rest API
         * related tests for onprem test
         */
        onprem = Boolean.getBoolean(ONPREM_PROP);
        Assume.assumeTrue("Skipping rest api test if onprem test", !onprem);

        /*
         * Set test tenantId for minicloud test to tenancy "nosqldev", this is
         * to work with MockIAMService that mimics to resolve 2 test
         * compartments (COMPRATMENT_ID and COMPARTMENT_ID_TO_MOVE) to
         * tenancy "nosqldev", see the methods getTenantId() of
         * proxy/src/main/java/oracle/nosql/proxy/security/iam/MockIAMService.java
         * in spartakv repo.
         */
        cloudRunning = Boolean.getBoolean(USEMC_PROP);
        if (Boolean.getBoolean(USEMC_PROP)) {
            System.setProperty(TENANT_ID_PROP, TENANT_NOSQL_DEV);
        }

        staticSetUp(tenantLimits);
    }

    static String getUserId() {
        return USER_OCID;
    }

    protected static String getCompartmentId() {
        if (TEST_COMPARTMENT_ID != null) {
            return TEST_COMPARTMENT_ID;
        }
        return (useMiniCloud ? MC_TEST_COMPARTMENT_ID : LOCAL_COMPARTMENT_ID);
    }

    static String getCompartmentIdMoveTo() {
        return (TEST_COMPARTMENT_ID_FOR_UPDATE != null) ?
                TEST_COMPARTMENT_ID_FOR_UPDATE :
                MC_TEST_COMPRATMENT_ID_FOR_UPDATE;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        client = getNoSQLClient();
        setOpThrottling(getTenantId(), NO_OP_THROTTLE);
        removeAllTables(getCompartmentId());
    }

    protected NosqlClient getNoSQLClient() {

        ClientConfigurationBuilder cfg = ClientConfiguration.builder();
        configClient(cfg);

        NosqlClient.Builder builder = NosqlClient.builder();
        builder.configuration(cfg.build());

        AbstractAuthenticationDetailsProvider provider;
        if (useCloudService) {
            ConfigFile config = null;
            try {
                config = ConfigFileReader.parse(OCI_CONFIG_FILE, OCI_PROFILE);
            } catch (IOException e) {
                fail("Unable to read config file: " + OCI_CONFIG_FILE);
            }
            provider = new ConfigFileAuthenticationDetailsProvider(config);
        } else {
            provider = SimpleAuthenticationDetailsProvider.builder()
                        .userId(getUserId())
                        .fingerprint(FINGER_PRTINT)
                        .tenantId(getTenantId())
                        .privateKeySupplier(
                            new StringPrivateKeySupplier(PRIVATE_KEY))
                        .build();
        }

        return builder.endpoint(getProxyEndpoint()).build(provider);
    }

    protected void configClient(ClientConfigurationBuilder builder) {
        /*
         * Now retries on below operations are enabled by default, disable
         * retries in the rest API test.
         *   o ListTables
         *   o GetTable
         *   o ListIndexes
         *   o GetIndex
         *   o GetRow
         *   o ListTableUsage
         *   o PrepareStatement
         *   o SummarizeStatement
         *   o ListWorkRequests
         *   o GetWorkRequest
         *   o ListWorkRequestErrors
         *   o ListWorkRequestLogs
         *   o CreateTable
         *   o CreateIndex
         *   o ChangeTableCompartment
         */
        builder.retryConfiguration(RetryConfiguration.NO_RETRY_CONFIGURATION);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        removeAllTables(getCompartmentId());
        if (client != null) {
            client.close();
        }
        setOpThrottling(getTenantId(), DEFAULT_OP_THROTTLE);
    }

    void removeAllTables(String comptId) {
        ListTablesRequest ltReq = ListTablesRequest.builder()
                .compartmentId(comptId)
                .build();
        ListTablesResponse ltRet = client.listTables(ltReq);
        TableCollection tables = ltRet.getTableCollection();

        /*
         * Sorted the tables in reverse order of name to make sure that the
         * child table will be dropped before its parent
         */
        Set<String> sorted =
            new TreeSet<>(String.CASE_INSENSITIVE_ORDER.reversed());
        for (TableSummary table : tables.getItems()) {
            if (table.getName().startsWith("SYS$")) {
                continue;
            }
            sorted.add(table.getName());
        }
        for (String name : sorted) {
            dropTable(comptId, name, true, true);
        }
    }

    GetTableResponse getTable(String tableNameOrId) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        return getTable(comptId, tableNameOrId);
    }

    GetTableResponse getTable(String cmptId, String tableNameOrId) {
        GetTableRequest req = GetTableRequest.builder()
                .compartmentId(cmptId)
                .tableNameOrId(tableNameOrId)
                .build();
        return client.getTable(req);
    }

    String getTableId(String tableName) {
        GetTableResponse gtRes = getTable(tableName);
        if (gtRes.getTable() != null) {
            return gtRes.getTable().getId();
        }
        return null;
    }

    /*
     * create table
     */
    void createTable(String tableName, String ddl) {
        createTable(tableName, ddl, defaultLimits);
    }

    String createTable(String tableName, String ddl, TableLimits limits) {
        return createTable(tableName, ddl, limits, true /* wait */);
    }

    String createTable(String tableName,
                       String ddl,
                       TableLimits limits,
                       boolean wait) {
        CreateTableRequest req = buildCreateTableRequest(getCompartmentId(),
                                                         tableName,
                                                         ddl,
                                                         limits);
        return executeDdl(req, wait);
    }

    CreateTableRequest buildCreateTableRequest(String cmptId,
                                               String tableName,
                                               String ddl,
                                               TableLimits limits) {
        CreateTableDetails.Builder payload = CreateTableDetails.builder()
                .compartmentId(cmptId)
                .name(tableName)
                .ddlStatement(ddl);
        if (limits != null) {
            payload.tableLimits(limits);
        }

        return CreateTableRequest.builder()
                .createTableDetails(payload.build())
                .build();
    }

    void createTable(String tableName,
                     String ddl,
                     TableLimits limits,
                     Map<String, String> freeformTags,
                     Map<String, Map<String, Object>> definedTags) {

        CreateTableDetails.Builder payload = CreateTableDetails.builder()
                .compartmentId(getCompartmentId())
                .name(tableName)
                .ddlStatement(ddl);
        if (limits != null) {
            payload.tableLimits(limits);
        }
        if (freeformTags != null) {
            payload.freeformTags(freeformTags);
        }
        if (definedTags != null) {
            payload.definedTags(definedTags);
        }

        CreateTableRequest ctReq = CreateTableRequest.builder()
                .createTableDetails(payload.build())
                .build();
        executeDdl(ctReq);
    }

    /*
     * drop table
     */
    void dropTable(String tableNameOrId) {
        dropTable(tableNameOrId, true /* ifExists */, true /* wait */);
    }

    String dropTable(String tableNameOrId, boolean ifExists, boolean wait) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        return dropTable(comptId, tableNameOrId, ifExists, wait);
    }

    String dropTable(String cmptId,
                     String tableNameOrId,
                     boolean ifExists,
                     boolean wait) {
        DeleteTableRequest req = DeleteTableRequest.builder()
                .compartmentId(cmptId)
                .tableNameOrId(tableNameOrId)
                .isIfExists(ifExists)
                .build();
        return executeDdl(req, wait);
    }

    /*
     * alter table schema
     */

    void alterTable(String tableNameOrId, String ddl) {
        alterTable(tableNameOrId, ddl, true /* wait */);
    }

    String alterTable(String tableNameOrId, String ddl, boolean wait) {
        UpdateTableRequest req = buildUpdateTableRequest(tableNameOrId, ddl);
        return executeDdl(req, wait);
    }

    UpdateTableRequest buildUpdateTableRequest(String tableNameOrId, String ddl) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        UpdateTableDetails info = UpdateTableDetails.builder()
                .compartmentId(comptId)
                .ddlStatement(ddl)
                .build();
        return UpdateTableRequest.builder()
                .tableNameOrId(tableNameOrId)
                .updateTableDetails(info)
                .build();
    }

    /*
     * update table limits
     */
    void updateTable(String tableNameOrId, TableLimits limits) {
        updateTable(tableNameOrId, limits, true /* wait */);
    }

    String updateTable(String tableNameOrId, TableLimits limits, boolean wait) {
        UpdateTableRequest req = buildUpdateTableRequest(tableNameOrId, limits);
        return executeDdl(req, wait);
    }

    UpdateTableRequest buildUpdateTableRequest(String tableNameOrId,
                                               TableLimits limits) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        UpdateTableDetails info = UpdateTableDetails.builder()
                .compartmentId(comptId)
                .tableLimits(limits)
                .build();
        return UpdateTableRequest.builder()
                .tableNameOrId(tableNameOrId)
                .updateTableDetails(info)
                .build();
    }

    /*
     * create index
     */

    void createIndex(String tableNameOrId, String indexName, String[] fields) {
        createIndex(tableNameOrId, indexName, fields, false /* ifNotExists */,
                    true /* wait */);
    }

    String createIndex(String tableNameOrId,
                       String indexName,
                       String[] fields,
                       boolean ifNotExists,
                       boolean wait) {
        CreateIndexRequest req =
            buildCreateIndexRequest(tableNameOrId, indexName, fields, ifNotExists);
        return executeDdl(req, wait);
    }

    String createIndex(String tableNameOrId,
                       String indexName,
                       List<IndexKey> keys,
                       boolean wait) {
        CreateIndexRequest req =
            buildCreateIndexRequest(tableNameOrId, indexName, keys, false);
        return executeDdl(req, wait);
    }

    CreateIndexRequest buildCreateIndexRequest(String tableNameOrId,
                                               String indexName,
                                               String[] fields,
                                               boolean ifNotExists) {
        List<IndexKey> keys = new ArrayList<>();
        for (String field : fields) {
            IndexKey key = IndexKey.builder()
                    .columnName(field)
                    .build();
            keys.add(key);
        }
        return buildCreateIndexRequest(tableNameOrId, indexName, keys,
                                       ifNotExists);
    }

    private CreateIndexRequest buildCreateIndexRequest(String tableNameOrId,
                                                       String indexName,
                                                       List<IndexKey> keys,
                                                       boolean ifNotExists) {

        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        CreateIndexDetails info = CreateIndexDetails.builder()
                .name(indexName)
                .compartmentId(comptId)
                .isIfNotExists(ifNotExists)
                .keys(keys)
                .build();
        return CreateIndexRequest.builder()
                .tableNameOrId(tableNameOrId)
                .createIndexDetails(info)
                .build();
    }

    /*
     * drop index
     */

    String dropIndex(String tableNameOrId, String indexName, boolean wait) {
        return dropIndex(tableNameOrId, indexName, false /* ifExists */, wait);
    }

    String dropIndex(String tableNameOrId,
                     String indexName,
                     boolean ifExists,
                     boolean wait) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        DeleteIndexRequest req = DeleteIndexRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId(comptId)
                .isIfExists(ifExists)
                .indexName(indexName)
                .build();
        return executeDdl(req, wait);
    }

    String executeDdl(BmcRequest<?> request) {
        return executeDdl(request, true /* wait */);
    }

    String executeDdl(BmcRequest<?> request, boolean wait) {
        String workRequestId = runDdlOp(request);
        if (workRequestId  == null) {
            return null;
        }
        if (wait) {
            waitForStatus(workRequestId, WorkRequest.Status.Succeeded);
        }
        return workRequestId;
    }

    void executeDdlFail(BmcRequest<?> request, String errorType) {
        executeDdlFail(request, 0, errorType);
    }

    void executeDdlFail(BmcRequest<?> request, int errCode, String errType) {
        String workRequestId;
        try {
            workRequestId = runDdlOp(request);
            if (cloudRunning) {
                waitForStatus(workRequestId, WorkRequest.Status.Failed);
                assertEquals(errType, getWorkRequestError(workRequestId));
            } else {
                waitForComplete(workRequestId);
                fail("expect to fail but not");
            }
        } catch (BmcException ex) {
            if (errCode > 0) {
                assertEquals(errCode, ex.getStatusCode());
            }
            assertEquals(errType , ex.getServiceCode());
            checkErrorMessage(ex);
        }
    }

    void waitForComplete(String workRequestId) {
        waitForStatus(workRequestId,
                      WorkRequest.Status.Succeeded,
                      WorkRequest.Status.Failed);
    }

    void waitForStatus(String workRequestId, WorkRequest.Status... states) {
        waitForStatus(workRequestId, null, states);
    }

    void waitForStatus(String workRequestId,
                       String errorCode,
                       WorkRequest.Status... states) {
        final int waitMs = DEFAULT_WAIT_MS;
        final int delayMs = DEFAULT_DELAY_MS;

        GetWorkRequestRequest req;
        GetWorkRequestResponse res;
        WorkRequest workReq;

        req = GetWorkRequestRequest.builder()
                .workRequestId(workRequestId)
                .build();
        long start = System.currentTimeMillis();
        while(true) {
            res = client.getWorkRequest(req);
            workReq = res.getWorkRequest();
            for (WorkRequest.Status state : states) {
                if (workReq.getStatus() == state) {
                    if (state == WorkRequest.Status.Failed) {
                        String error = getWorkRequestError(workRequestId);
                        if (errorCode != null) {
                            assertEquals(errorCode, error);
                        } else {
                            assertNotNull(error);
                        }
                    }
                    return;
                }
            }

            if (isWorkRequestCompleted(workReq.getStatus())) {
                String error = null;
                if (workReq.getStatus() == WorkRequest.Status.Failed) {
                    error = getWorkRequestError(workRequestId);
                }
                fail("WorkRequest done with state = " + workReq.getStatus() +
                     " but not the expected " + Arrays.toString(states) +
                     ": error=" + error + ", workRequestId=" + workRequestId);
            }

            if (System.currentTimeMillis() - start > waitMs) {
                fail("Not reach the specified status after wait " + waitMs +
                     "ms " + Arrays.toString(states));
                break;
            }
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static boolean isWorkRequestCompleted(WorkRequest.Status state) {
        return state == WorkRequest.Status.Succeeded ||
               state == WorkRequest.Status.Failed ||
               state == WorkRequest.Status.Canceled;
    }

    protected String getWorkRequestError(String workRequestId) {
        ListWorkRequestErrorsRequest req =
            ListWorkRequestErrorsRequest.builder()
                .workRequestId(workRequestId)
                .build();

        ListWorkRequestErrorsResponse res = client.listWorkRequestErrors(req);
        List<WorkRequestError> errors =
            res.getWorkRequestErrorCollection().getItems();
        if (errors.isEmpty()) {
            return null;
        }
        return errors.get(0).getCode();
    }

    private String runDdlOp(BmcRequest<?> req) {
        if (req instanceof CreateTableRequest) {
            CreateTableResponse res =
                client.createTable((CreateTableRequest)req);
            return res.getOpcWorkRequestId();
        }

        if (req instanceof UpdateTableRequest) {
            UpdateTableResponse res =
                client.updateTable((UpdateTableRequest)req);
            return res.getOpcWorkRequestId();
        }

        if (req instanceof DeleteTableRequest) {
            DeleteTableResponse res =
                client.deleteTable((DeleteTableRequest)req);
            return res.getOpcWorkRequestId();
        }

        if (req instanceof CreateIndexRequest) {
            CreateIndexResponse res =
                client.createIndex((CreateIndexRequest)req);
            return res.getOpcWorkRequestId();
        }

        if (req instanceof DeleteIndexRequest) {
            DeleteIndexResponse res =
                client.deleteIndex((DeleteIndexRequest)req);
            return res.getOpcWorkRequestId();
        }

        if (req instanceof ChangeTableCompartmentRequest) {
            ChangeTableCompartmentResponse res =
                client.changeTableCompartment(
                    (ChangeTableCompartmentRequest)req);
            return res.getOpcWorkRequestId();
        }

        fail("Invalid ddl operation request: " + req);
        return null;
    }

    /*
     * dml ops
     */

    String putRow(String tableNameOrId, Map<String, Object> row) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        UpdateRowDetails info = UpdateRowDetails.builder()
                    .compartmentId(comptId)
                    .value(row)
                    .build();
        UpdateRowRequest req = UpdateRowRequest.builder()
                    .tableNameOrId(tableNameOrId)
                    .updateRowDetails(info)
                    .build();
        UpdateRowResponse res = client.updateRow(req);
        assertNotNull(res);
        assertNotNull(res.getUpdateRowResult());
        assertNotNull(res.getUpdateRowResult().getVersion());
        return res.getUpdateRowResult().getVersion();
    }

    Map<String, Object> getRow(String tableNameOrId, List<String> key) {
        String comptId = isValidTableOcid(tableNameOrId) ?
                         null : getCompartmentId();
        GetRowRequest req = GetRowRequest.builder()
                .tableNameOrId(tableNameOrId)
                .compartmentId(comptId)
                .key(key)
                .build();
        GetRowResponse res = client.getRow(req);
        assertNotNull(res);
        assertNotNull(res.getRow());
        return res.getRow().getValue();
    }

    private static boolean isValidTableOcid(String tableNameOrId) {
        if (cloudRunning && tableNameOrId != null) {
            return tableNameOrId.startsWith("ocid1") &&
                   tableNameOrId.contains("nosqltable");
        }
        return false;
    }

    protected static void checkErrorMessage(BmcException ex) {
        if (ex == null || ex.getMessage() == null) {
            return;
        }
        assertFalse(ex.getMessage().contains("ocid1_nosqltable_"));
    }

    /*
     * methods directly call SC APIs.
     */

    /*
     * Sets the data store for free table if set is true, otherwise revoke the
     * setting.
     */
    protected static void setFreeTableStore(boolean set) {
        if (!useMiniCloud) {
            return;
        }

        final String pl = "[\n" +
            "  {\n" +
            "    \"version\": 2,\n" +
            "    \"storeName\": \"DStore1\",\n" +
            "    \"storeAttrs\": {\n" +
            "      \"freeStore\": " + (set ? true : false) + ",\n" +
            "      \"tenantId\": null,\n" +
            "      \"compartmentIds\": null\n" +
            "    }\n" +
            "  }\n" +
            "]";

        HttpResponse resp = new HttpRequest().doHttpPost(scDSConfigBase, pl);
        if (200 != resp.getStatusCode()) {
            fail("setFreeTableStore failed: " + resp);
        }
    }

    /*
     * Sets table's TableActivity state using SC rest call
     */
    protected static void setTableActivity(String tenantId,
                                           String tableOcid,
                                           long dmlMs,
                                           ActivityPhase phase) {

        if (!useMiniCloud) {
            return;
        }

        StringBuilder sb = new StringBuilder(tmUrlBase)
                .append("tables/")
                .append(tableOcid)
                .append("/actions/setActivity")
                .append("?tenantid=").append(tenantId)
                .append("&dmlms=").append(dmlMs)
                .append("&phase=").append(phase.name());
        String url = sb.toString();

        HttpResponse res = new HttpRequest().doHttpPut(url, null);
        if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException("setTableActivity failed: " +
                                            res.getOutput());
        }
    }

    /*
     * Creates table using SC rest call
     */
    protected static String scCreateTable(String tenantId,
                                          String compartmentId,
                                          String tableName,
                                          String ddl,
                                          oracle.nosql.util.tmi.TableLimits limits,
                                          boolean isFreeTable)
        throws Exception {

        if (!cloudRunning) {
            return null;
        }

        HttpRequest httpRequest = new HttpRequest().disableRetry();
        String url = tmUrlBase + "tables/" + tableName;

        /* re-create the table */
        TableDDLInputs tdi = new TableDDLInputs(ddl, tenantId, compartmentId,
                                                null, /* matchETag */
                                                true, /* ifNotExists */
                                                limits, null /* tags */,
                                                isFreeTable /* freeTable */,
                                                null /* retryToken*/);
        String payload = JsonUtils.print(tdi);
        HttpResponse res = httpRequest.doHttpPost(url, payload);
        if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException(
                "Recreate failed to create table " + res.getOutput());
        }
        TableInfo tif = JsonUtils.readValue(res.getOutput(), TableInfo.class);
        String operationId = tif.getOperationId();
        waitForCompletion(httpRequest, tenantId, compartmentId, tableName,
                          operationId, TableState.ACTIVE, 20000);
        return tif.getTableOcid();
    }

    protected static void scRecreateTable(String tenantId,
                                          String compartmentId,
                                          String tableName,
                                          String ddl)
        throws Exception {

        if (!useMiniCloud) {
            return;
        }

        HttpRequest httpRequest = new HttpRequest().disableRetry();

        /* drop existing table */
        DropInputs di = new DropInputs(true, tenantId, compartmentId, null);
        String payload = JsonUtils.print(di);
        String url = tmUrlBase + "tables/" + tableName;

        HttpResponse res = httpRequest.doHttpDelete(url, payload);
        if (res.getStatusCode() != HttpURLConnection.HTTP_OK) {
            throw new IllegalStateException(
                "Recreate failed to drop existing table " + res.getOutput());
        }

        TableInfo tif = JsonUtils.readValue(res.getOutput(), TableInfo.class);
        String operationId = tif.getOperationId();
        waitForCompletion(httpRequest, tenantId, compartmentId, tableName,
                          operationId, TableState.DROPPED, 20000);

        scCreateTable(tenantId, compartmentId, tableName, ddl,
                      new oracle.nosql.util.tmi.TableLimits(100, 100, 100),
                      false /* isFreeTable */);
    }

    private static void waitForCompletion(HttpRequest httpRequest,
                                          String tenantId,
                                          String compartmentId,
                                          String tableName,
                                          String operationId,
                                          TableState state,
                                          int waitMillis)
        throws Exception {

        int delayMS = 500;
        long startTime = System.currentTimeMillis();

        String url = tmUrlBase + "tables/" + tableName +
             "?tenantid=" + tenantId +
             "&compartmentid=" + compartmentId +
             "&operationid=" + operationId;

        while (true) {

            long curTime = System.currentTimeMillis();
            if ((curTime - startTime) > waitMillis) {
                throw new RequestTimeoutException(
                    waitMillis,
                    "Operation not completed in expected time");
            }

            HttpResponse res = httpRequest.doHttpGet(url);
            if (res.getStatusCode() == 200) {
                TableInfo tif = JsonUtils.readValue(res.getOutput(),
                                                    TableInfo.class);
                if (state == tif.getStateEnum()) {
                    return;
                }
            } else if (res.getStatusCode() == 404) {
                if (state == TableState.DROPPED) {
                    return;
                }
                throw new IllegalStateException("Table not found " + tableName);
            }
            Thread.sleep(delayMS);
        }
    }

    protected static void assertDefinedTags(
            Map<String, Map<String, Object>> exp,
            Map<String, Map<String, Object>> tags) {

        if (useCloudService) {
            /* ignore the default defined tag implicitly added in cloud */
            tags.remove("Oracle-Tags");
        }
        assertEquals(exp, tags);
    }

    /*
     * Set or clear the dedicated tenantId of the pod
     *
     * If the given tenantId is not null, assign the pod to the given tenantId.
     * Otherwise, clear the pod's dedicated tenantId.
     */
    protected static void setDedicatedTenantId(String tenantId) {

        if (!cloudRunning) {
            return;
        }

        final String url = "http://" + scHost + ":" + scPort +
                           "/V0/service/dsconfig";

        /* Get the current store configuration */
        HttpResponse res = new HttpRequest().doHttpGet(url, null);
        assertEquals(HttpURLConnection.HTTP_OK, res.getStatusCode());

        ArrayNode dsconfig = JsonUtils.parseJsonNode(res.getOutput()).asArray();
        ObjectNode storeAttrs = dsconfig.get(0).asObject()
                                        .get("storeAttrs").asObject();

        /*
         * Change the dedicated tenantId and compartmentIds in the store
         * configuration
         */
        if (tenantId != null) {
            storeAttrs.put("tenantId", tenantId)
                      .putArray("compartmentIds").add(tenantId);
        } else {
            storeAttrs.putNull("tenantId")
                      .putNull("compartmentIds");
        }

        /* Update the store configuration */
        res  = new HttpRequest().doHttpPost(url, JsonUtils.toJson(dsconfig));
        assertEquals(HttpURLConnection.HTTP_OK, res.getStatusCode());
    }

    private static String genPrivateKey() {
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
            kpg.initialize(2048);
            KeyPair kp = kpg.generateKeyPair();

            StringBuilder sb = new StringBuilder();
            sb.append("-----BEGIN PRIVATE KEY-----\n")
              .append(Base64.getEncoder().encodeToString(
                          kp.getPrivate().getEncoded()))
              .append("\n-----END PRIVATE KEY-----");
            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException(
                "Generate RSA private key file failed: " + e);
        }
    }
}
