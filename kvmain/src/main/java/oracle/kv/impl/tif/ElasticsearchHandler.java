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

package oracle.kv.impl.tif;

import static oracle.kv.impl.api.table.TimestampUtils.UTC_SUFFIX;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.http.HttpClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;

import oracle.kv.impl.admin.Admin;
import oracle.kv.impl.admin.IllegalCommandException;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.IndexField;
import oracle.kv.impl.api.table.NullValueImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.impl.tif.TransactionAgenda.Commit;
import oracle.kv.impl.tif.esclient.esRequest.BulkRequest;
import oracle.kv.impl.tif.esclient.esRequest.CATRequest;
import oracle.kv.impl.tif.esclient.esRequest.ClusterHealthRequest;
import oracle.kv.impl.tif.esclient.esRequest.CreateIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetHttpNodesRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetMappingRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexDocumentRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.MappingExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.PutMappingRequest;
import oracle.kv.impl.tif.esclient.esResponse.BulkResponse;
import oracle.kv.impl.tif.esclient.esResponse.CATResponse;
import oracle.kv.impl.tif.esclient.esResponse.ClusterHealthResponse;
import oracle.kv.impl.tif.esclient.esResponse.ClusterHealthResponse.ClusterHealthStatus;
import oracle.kv.impl.tif.esclient.esResponse.CreateIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetHttpNodesResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetMappingResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexAlreadyExistsException;
import oracle.kv.impl.tif.esclient.esResponse.IndexDocumentResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.MappingExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.PutMappingResponse;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClientBuilder;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClientBuilder.SecurityConfigCallback;
import oracle.kv.impl.tif.esclient.httpClient.SSLContextException;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonUtil;
import oracle.kv.impl.tif.esclient.restClient.ESAdminClient;
import oracle.kv.impl.tif.esclient.restClient.ESDMLClient;
import oracle.kv.impl.tif.esclient.restClient.ESRestClient;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.RestStatus;
import oracle.kv.impl.tif.esclient.restClient.monitoring.ESNodeMonitor;
import oracle.kv.impl.tif.esclient.restClient.monitoring.MonitorClient;
import oracle.kv.impl.tif.esclient.restClient.utils.ESLatestResponse;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.tif.esclient.security.TIFSSLContext;
import oracle.kv.impl.util.HostPort;
import oracle.kv.impl.util.server.LoggerUtils;
import oracle.kv.table.ArrayDef;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapDef;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Table;
import oracle.kv.table.TimestampValue;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import oracle.kv.impl.tif.esclient.httpClient.ESHttpMethods;
import oracle.kv.impl.tif.esclient.httpClient.HttpHost;

/**
 * Object representing an Elastic Search (ES) handler with all ES related
 * operations including ES index and mapping management and data operations.
 */
public class ElasticsearchHandler {

    /*
     * Property names of interest to index creation.
     */
    static final String SHARDS_PROPERTY = "ES_SHARDS";
    static final String REPLICAS_PROPERTY = "ES_REPLICAS";

    private static Logger logger;

    private final ESRestClient esRestClient;
    private final ESAdminClient adminClient;
    private final ESDMLClient client;
    private final MonitorClient esMonitoringClient;
    private final ESNodeMonitor esNodeMonitor;

    /*
     * Timeout values for the http client this ElasticsearchHandler uses
     * This timeout is used by ESSyncResponseListener and is based on
     * the expected bulk request processing time.
     */
    private static final int maxRetryTimeoutMillis = 120000;

    /* The name of fields used in mapping type for TIMESTAMP */
    private static final String DATE = "date";
    private static final String EMPTY_ANNOTATION = "{}";
    private static final String DEFAULT_ES_VERSION = "2.4.4";

    /*
     * Implementation Note:
     *
     * When a value stored in a kvstore column is of type java.sql.Timestamp,
     * (corresponding to the enum type TIMESTAMP), the precision specified
     * for the value (0 - 9) allows one to specify the day and time with
     * increasing levels of granularity; where the least precise representation
     * includes only the day, month, and year (that is, "yyyy-MM-dd"), and
     * the finest - or most precise - representation includes an instant
     * during the day that is accurate to the nanosecond (that is,
     * "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS").
     *
     * Unfortunately, Elasticsearch supports only millisecond granularity.
     * Specifically, the way that the day time is represented in Elasticsearch
     * is through the Elasticsearch "date" data type; which does not
     * translate directly to/from the java.sql.Timestamp type. Because of
     * this, any data value taken from the kvstore that is of type TIMESTAMP
     * will be mapped to the Elasticsearch "date" data type according to
     * following algorithm:
     *
     * If the user specifies that a value to be indexed in Elasticsearch
     * has type "date" and precision "millis" - that is, the user specifies
     * an annotation of {"type":"date","precision":"millis"} - then the
     * mapping sent to elastic search for that item will be specified as:
     *
     * {"type":"date",
     *  "format":"yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis"}
     *
     * On the other hand, if the user specifies no precision in the annotation,
     * or a precision of "nanos", then the following version-specific default
     * mapping will be used.
     *
     * For versions of Elasticsearch PRIOR TO VERSION 5 (ex. "2.4.4"):
     * {"type":"date", "format":"strict_date_optional_time||epoch_millis"}
     *
     * For Elasticsearch version 5 or greater:
     * {"type":"date"}
     *
     * where, when no "format" is specified in the mapping, Elasticsearch
     * will dynamically determine the appropriate format, based on the
     * value of the TIMESTAMP being indexed.
     *
     * This is done so that the broadest types of TIMESTAMP values can be
     * indexed in Elasticsearch without error. For example, suppose a
     * value such as "1997-11-17T08:33:59.735" is stored in a table of
     * the kvstore. If the user specifes an annotation of
     * {"type":"date","precision":"millis"}, then the full precision of the
     * value - including the milliseconds - will be indexed by Elasticsearch.
     * On the other hand, if the value stored in the kvstore has nanoseconds
     * precision - for example, "1997-11-17T08:33:59.735888822" - then
     * since Elasticsearch will throw an exception if an attempt is made to
     * map the value's nanosecond component, a default mapping must be
     * used that tells Elasticsearch to parse the value and ignore any
     * parts it can't handle (that is, the nanoseconds component). Thus,
     * if the user specifies {"type":"date","precision":"nanos"}, the
     * mapping that is sent to Elasticsearch is the same mapping that is
     * sent if the user specified no precision; that is, a mapping with
     * a format that allows Elasticsearch to handle a fairly wide range of
     * values being indexed.
     *
     * The constants defined below are used to translate the date annotation
     * specified by the user to the correct "date" type specification for
     * the mapping that is generated for indexing a TIMESTAMP value. Note
     * that although a specification of "precision":"nanos" currently
     * corresponds to the default mapping, if Elasticsearch produces a
     * release that handles nanoseconds, then the implementation of this
     * class will be changed accordingly; to generate a mapping that
     * handles nanoseconds in the TIMESTAMP correctly.
     *
     * One final implementation note regarding the values of the TIMESTAMP
     * related constants defined below, and how they are handled based
     * on the particular version of Elasticsearch being run:
     *
     * Although a simple mapping of {"type":"date"} - where there is no
     * format and Elasticsearch is left to dynamically determine the
     * format - can be used to successfully index a wide range of TIMESTAMP
     * types, for versions of Elasticsearch PRIOR TO VERSION 5, the specific
     * format that Elasticsearch uses when no format is specified must
     * be specified; that is, as explained in the following paragraph,
     * {"type":"date", "format":"strict_date_optional_time||epoch_millis"}
     * must be specified in the mapping that is generated rather than simply
     * specifying {"type":"date"}.
     *
     * Although {"type":"date"} can be sent to Elasticsearch without error,
     * because versions of Elasticsearch prior to version 5 always dynamically
     * set the format to "strict_date_optional_time||epoch_millis" when
     * no format is specified, this will cause the method createESIndexMapping
     * to fail. That method fails because prior to returning the newly
     * generated mapping, the method retrieves any mapping that may have
     * been previously registered with Elasticsearch for the current
     * index, and then compares the existing mapping to the new mapping
     * to ensure there are no conflicts. As a result, if the generic,
     * no-format {"type":"date"} mapping is used to index a TIMESTAMP value
     * in a cluster running a version of Elasticsearch prior to version 5,
     * then the new mapping will contain the specification {"type":"date"}
     * for the TIMESTAMP value, but Elasticsearch will return an
     * existing mapping with the non-matching specification,
     * {"type":"date", "format":"strict_date_optional_time||epoch_millis"};
     * which will cause the comparison to declare a conflict in the
     * mapping. Thus, to avoid failure when indexing values in versions of
     * Elasticsearch prior to version 5, the mapping that is generated for all
     * TIMESTAMP values except those that have millisecond precision will
     * specify the specific format: "strict_date_optional_time||epoch_millis".
     *
     * Compare this with what happens when the version of Elasticsearch
     * is version 5 or greater. In that case, if {"type":"date"} -- with no
     * "format" specification -- is include in the mapping, then no mapping
     * conflict such as that described above occurs. As a result, when the
     * Elasticsearch version is 5 or greater, the more desirable default
     * {"type":"date"} specification will be used for TIMESTAMP mappings.
     */
    private static final String TIMESTAMP_TO_ES2_DATE_DEFAULT =
        "{" +
        "\"type\":\"date\"" +
        "," +
        "\"format\":\"strict_date_optional_time||epoch_millis\"" +
        "}";

    private static final String TIMESTAMP_TO_ES5_DATE_DEFAULT =
        "{" +
        "\"type\":\"date\"" +
        "}";

    private static final String TIMESTAMP_TO_ES_DATE_MILLIS =
        "{" +
        "\"type\":\"date\"" +
        "," +
        "\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis\"" +
        "}";

    private static final String TIMESTAMP_TO_ES2_DATE_NANOS =
        TIMESTAMP_TO_ES2_DATE_DEFAULT;

    private static final String TIMESTAMP_TO_ES5_DATE_NANOS =
        TIMESTAMP_TO_ES5_DATE_DEFAULT;

    /*
     * These variables are set at Construction time. AdminClient instantiation
     * does a call to ES to get the values for this.
     *
     * Note that one JVM process can connect to only one ES Cluster.
     */
    private static String esVersion;
    private static String esClusterName;

    /*
     * This variable is set at the time of Register-ES plan.
     * ElasticsearchHandler is per store.
     *
     * This is the store name which is prefixed to all FTS index.
     *
     * One effect of STORE_NAME being null is that ensureCommit() will refresh
     * all indices on the ES Cluster.
     */
    private static final String STORE_NAME = null;

    private boolean doEnsureCommit;

    ElasticsearchHandler(ESRestClient esRestClient,
            MonitorClient esMonitoringClient,
            ESNodeMonitor esNodeMonitor,
            Logger inputLogger) {
        this.esRestClient = esRestClient;
        this.client = esRestClient.dml();
        this.adminClient = esRestClient.admin();
        this.esMonitoringClient = esMonitoringClient;
        this.esNodeMonitor = esNodeMonitor;
        logger = inputLogger;
        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class,
                                           "ElasticsearchHandler");
        }
        esVersion = this.adminClient.getESVersion();
        if (esVersion == null) {
            esVersion = DEFAULT_ES_VERSION;
        }
        esClusterName = this.adminClient.getClusterName();
        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler: elasticsearch version = " +
                        esVersion + ", clustername = " + esClusterName);
        }
    }

    /**
     * Closes the ES handler.
     */
    public void close() {
        if (esNodeMonitor != null) {
            esNodeMonitor.close();
        }
        if (esMonitoringClient != null) {
            esMonitoringClient.close();
        }
        if (esRestClient != null) {
            esRestClient.close();
        }
    }

    /*
     * Typically FTS would use only one instance of ElasticsearchHandler.
     * However, this single instance restriction, if required should be at the
     * TextIndexFeeder Layer and not here.
     */
    public static ElasticsearchHandler newInstance(
                                           final String clusterName,
                                           final String esMembers,
                                           final boolean isSecure,
                                           SecurityParams esSecurityParams,
                                           final int monitoringFixedDelay,
                                           Logger inputLogger)
        throws IOException {

        ESRestClient newEsRestClient = null;
        MonitorClient newEsMonitoringClient = null;
        ESNodeMonitor newEsNodeMonitor = null;

        ESHttpClient baseRestClient = null;
        ESHttpClient baseMonitoringClient = null;

        if (!isSecure) {
            esSecurityParams = null;
        }
        /*
         * Create ES Http Clients for restClient and monitoring Client. No need
         * to check connections as that is done during register-es-plan before
         * setting the SN parameters.
         *
         * For now, using same logger for low level httpclient and higher level
         * clients.
         */
        baseRestClient =
            ElasticsearchHandler.createESHttpClient(
                clusterName, esMembers, esSecurityParams, inputLogger);

        baseMonitoringClient =
            ElasticsearchHandler.createESHttpClient(
                clusterName, esMembers, esSecurityParams, inputLogger);

        newEsRestClient = new ESRestClient(baseRestClient, inputLogger);
        newEsMonitoringClient =
            new MonitorClient(baseMonitoringClient,
                              new ESLatestResponse(), inputLogger);

        final List<ESHttpClient> registeredESHttpClients =
            new ArrayList<ESHttpClient>();
        registeredESHttpClients.add(newEsMonitoringClient.getESHttpClient());
        registeredESHttpClients.add(newEsRestClient.getESHttpClient());
        newEsNodeMonitor =
            new ESNodeMonitor(newEsMonitoringClient, monitoringFixedDelay,
                              registeredESHttpClients, isSecure, inputLogger);
        newEsRestClient.getESHttpClient().setFailureListener(newEsNodeMonitor);

        newEsNodeMonitor.start();

        return new ElasticsearchHandler(newEsRestClient, newEsMonitoringClient,
                                        newEsNodeMonitor, inputLogger);
    }

    public ESRestClient getEsRestClient() {
        return esRestClient;
    }

    public ESAdminClient getAdminClient() {
        return adminClient;
    }

    public ESDMLClient getClient() {
        return client;
    }

    public MonitorClient getEsMonitoringClient() {
        return esMonitoringClient;
    }

    /**
     * Enables ensure commit
     */
    void enableEnsureCommit() {
        doEnsureCommit = true;
    }

    /**
     * Checks if an ES index exists
     *
     * @param indexName  name of ES index
     *
     * @return true if an ES index exists
     * @throws IOException
     */
    boolean existESIndex(String indexName) throws IOException {
        return (existESIndex(indexName, adminClient));
    }

    /**
     * Checks if an ES index mapping exists
     *
     * @param esIndexName  name of ES index
     *
     * @return true if a mapping exits
     * @throws IOException
     */
    boolean existESIndexMapping(String esIndexName)
        throws IOException {

        /* if no index, no mapping */
        if (!existESIndex(esIndexName)) {
            return false;
        }

        final MappingExistRequest request =
            new MappingExistRequest(esIndexName);
        final MappingExistResponse response =
            adminClient.mappingExists(request);

        /*
         * Create Index API may create an empty mapping.
         */
        if (response.exists()) {
            final GetMappingRequest getMappingReq =
                new GetMappingRequest(esIndexName);

            final GetMappingResponse getMappingResp =
                adminClient.getMapping(getMappingReq);
            ObjectNode jNode = JsonUtils.parseJsonObject(getMappingResp.mapping());
            if (!jNode.isEmpty() && jNode.findFirst("mappings") != null) {
                return !jNode.findFirst("mappings").isEmpty();
            }
            return false;
        }

        /* check if mapping exists in index */
        return response.exists();
    }

    /**
     *
     * Gets a json string representation of a mapping.
     *
     * @param esIndexName  name of ES index
     *
     * @return json string
     * @throws IOException
     */
    String getESIndexMapping(String esIndexName)
        throws IOException {

        /* if no index, no mapping */
        if (!existESIndex(esIndexName)) {
            return null;
        }
        final GetMappingRequest request = new GetMappingRequest(esIndexName);

        final GetMappingResponse response = adminClient.getMapping(request);

        return response.mapping();

    }

    /**
     * Creates an ES index with default property, the default number of shards
     * and replicas would be applied by ES.
     *
     * @param esIndexName  name of ES index
     * @throws Exception
     * @throws IllegalStateException
     */
    void createESIndex(String esIndexName) throws IOException {
        createESIndex(esIndexName, (Map<String, String>) null);
    }

    /**
     * Creates an ES index
     *
     * @param esIndexName  name of ES index
     * @param properties   Map of index properties, can be null
     * @throws Exception
     */
    void createESIndex(String esIndexName, Map<String, String> properties)
        throws IOException {

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler.createESIndex: " +
                        "[index = " + esIndexName +
                        ", properties = " + properties + "]");
        }
         
        final Map<String, Object> indexSettingsComplete = new LinkedHashMap<>();

        final Map<String, String> indexSettings =
            new LinkedHashMap<String, String>();

        if (properties != null) {
            final String shards = properties.get(SHARDS_PROPERTY);
            final String replicas = properties.get(REPLICAS_PROPERTY);

            if (shards != null) {
                if (Integer.parseInt(shards) < 1) {
                    throw new IllegalStateException
                        ("The " + SHARDS_PROPERTY + " value of " + shards +
                         " is not allowed.");
                }
                indexSettings.put("number_of_shards", shards);
            }
            if (replicas != null) {
                if (Integer.parseInt(replicas) < 0) {
                    throw new IllegalStateException
                        ("The " + REPLICAS_PROPERTY + " value of " + replicas +
                         " is not allowed.");
                }
                indexSettings.put("number_of_replicas", replicas);
            }
        }

        indexSettingsComplete.put("settings", indexSettings);
        CreateIndexResponse createResponse = null;
        try {
            final CreateIndexRequest createIndex =
                new CreateIndexRequest(esIndexName).settings(indexSettingsComplete);

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndex: " +
                            "sending REST " +
                            "index creation request to elasticsearch" +
                            "[index = " + esIndexName + "] ...");
            }

            createResponse = adminClient.createIndex(createIndex);

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndex: " +
                            "response received from elasticsearch" +
                            "[index = " + esIndexName +
                            ", response = " + createResponse + "]");
            }

        } catch (IndexAlreadyExistsException iae) {

            /*
             * That is OK; multiple repnodes will all try to create the index
             * at the same time, only one of them can win.
             *
             * Or this could be a restart of TIF case, so index could already
             * be existing.
             */

            logger.fine("ES index " + esIndexName + " has already been" +
                        "created");
            return;

        } catch (IOException e) {

            logger.log(Level.SEVERE, "index could not be created due to:" + e);
            throw e;
        }

        if (createResponse == null || !createResponse.isAcknowledged()) {

            final String msgPrefix =
                "ElasticsearchHandler.createESIndex: failed to " +
                "create elasticsearch index [index = " + esIndexName;

            String errMsg = null;
            if (createResponse != null) {
                errMsg = msgPrefix + ", acknowledged = " +
                         createResponse.isAcknowledged() + "]" +
                         ". Request not acknowledged.";
            } else {
                errMsg = msgPrefix + "], NULL response received from " +
                         "create index request";
            }

            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(" ");
                logger.warning(errMsg);
            }
            throw new IllegalStateException(errMsg);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ElasticsearchHandler.createESIndex: " +
                        "elasticsearch index " +
                        "created [index = " + esIndexName +
                        ", acknowledged = " + createResponse.isAcknowledged() +
                        ", error = " + createResponse.isError() + "]");
        }
    }

    /**
     * Deletes an ES index
     *
     * @param esIndexName  name of ES index
     *
     * @throws IllegalStateException
     */
    void deleteESIndex(String esIndexName) throws IllegalStateException {

        if (!deleteESIndex(esIndexName, adminClient, logger)) {
            logger.info("nothing to delete, ES index " + esIndexName +
                        " does not exist.");
        }

        logger.info("ES index " + esIndexName + " deleted");
    }

    /**
     * Returns all ES indices corresponding to text indices in the kvstore
     *
     * @param storeName  name of kv store
     *
     * @return list of all ES index names
     * @throws IOException
     */
    Set<String> getAllESIndexNamesInStore(final String storeName)
        throws IOException {

        return getAllESIndexInStoreInternal(storeName, adminClient);
    }

    /**
     * Creates an ES index mapping
     *
     * @param esIndexName  name of ES index
     * @param mappingSpec  mapping specification
     *
     * @throws IllegalStateException
     */
    void createESIndexMapping(String esIndexName,
                              JsonGenerator mappingSpec)
        throws IllegalStateException {


        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler.createESIndexMapping: " +
                        "index = " + esIndexName);
        }

        PutMappingResponse mresp = null;

        try {
            /* ensure the ES index exists */
            if (!existESIndex(esIndexName)) {
                throw new IllegalStateException("ES Index " + esIndexName +
                                                " does not exist");
            }

            /* ensure no pre-existing conflicting mapping */
            if (existESIndexMapping(esIndexName)) {
                /*
                 * It is not entirely create how create index API behaves.
                 * Create Index API can add empty mapping
                 */
                final String existingMapping = getESIndexMapping(esIndexName);

                if (!JsonUtils.parseJsonObject(existingMapping).isEmpty()) {
                    if (ESRestClientUtil.isMappingResponseEqual
                                                        (existingMapping,
                                                         mappingSpec,
                                                         esIndexName)) {
                        return;
                    }

                    throw new IllegalStateException
                    ("Mapping already exists in index " +
                     esIndexName + ", but differs from new mapping." +
                     "\nexisting mapping: " + existingMapping +
                     "\nnew mapping: " + mappingSpecStr(mappingSpec));
                }
            }

            final PutMappingRequest mappingReq =
                new PutMappingRequest(esIndexName, mappingSpec);

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndexMapping: " +
                            "sending REST mapping request to elasticsearch" +
                            "[index = " + esIndexName + "] ...");
            }

            mresp = adminClient.createMapping(mappingReq);
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndexMapping: " +
                            "response received from elasticsearch" +
                            "[index = " + esIndexName +
                            ", response = " + mresp + "]");
            }

        } catch (IOException ioe) {

            logger.warning("ElasticsearchHandler.createESIndexMapping: " +
                           "exception occured while trying to create " +
                           "mapping:" + ioe);
        }

        if (mresp == null || !mresp.isAcknowledged()) {

            String errMsg =
                "ElasticsearchHandler.createESIndexMapping: cannot " +
                "install mapping for elasticsearch index " +
                "[index = " + esIndexName;

            if (mresp != null) {
                errMsg = errMsg +
                    ", acknowledged = " + mresp.isAcknowledged() + "]";
            } else {
                errMsg = errMsg + ", response to mapping request = null]";
            }

            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(" ");
                logger.warning(errMsg);
            }
            throw new IllegalStateException(errMsg);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ElasticsearchHandler.createESIndexMapping: " +
                        "index mapping " +
                        "created [index = " + esIndexName + "]");
        }
    }

    /**
     * Fetches an entry from ES index
     *
     * @param esIndexName  name of ES index
     * @param key          key of entry to get
     *
     * @return response from ES index
     * @throws IOException
     */
    GetResponse get(String esIndexName, String key)
        throws IOException {

        final GetRequest req = new GetRequest(esIndexName, key);
        return client.get(req);

    }

    /**
     * Sends a document to ES for indexing
     *
     * @param document  document to index
     * @throws IOException
     */
    IndexDocumentResponse index(IndexOperation document) throws IOException {

        final IndexDocumentRequest req =
            new IndexDocumentRequest(
                    document.getESIndexName(),
                    document.getPkPath()).source(document.getDocument());

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler.index: " +
                        "sending document to elasticsearch for indexing " +
                            "[indexingRequest = " + req + "] ...");
        }

        final IndexDocumentResponse response = client.index(req);

        ensureCommit();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ElasticsearchHandler.index: " +
                        "document indexing complete " +
                            "[indexingResponse = " + response + "]");
        }

        return response;
    }

    /**
     * Deletes an entry from ES index
     *
     * @param esIndexName  name of ES index
     * @param key          key of entry to delete
     * @throws IOException
     */
    DeleteResponse del(String esIndexName, String key)
        throws IllegalStateException,
        IOException {

        final DeleteRequest delReq =
            new DeleteRequest(esIndexName, key);
        DeleteResponse response = null;
        try {
            response = client.delete(delReq);
        } catch (IOException e) {
            throw new IllegalStateException("Could not delete document:" +
                    esIndexName + ":" + key + " due to:" +
                    e.getCause());
        }

        ensureCommit();

        return response;
    }

    /**
     * Send a bulk operation to Elasticsearch
     *
     * @param batch  a batch of operations
     * @return  response from ES cluster, or null if the batch is empty.
     * @throws IOException
     */
    BulkResponse doBulkOperations(List<TransactionAgenda.Commit> batch)
        throws IOException {

        if (batch.size() == 0) {
            return null;
        }

        final BulkRequest bulkRequest = new BulkRequest();

        /* If operations were purged by an index deletion, the batch will
         * contain empty transactions.  If every transaction in the batch is
         * empty, then the bulkRequest will be empty.  We don't want to send an
         * empty bulkRequest to ES -- it will throw
         * ActionRequestValidationException.  So we keep track of the actual
         * number of operations here, and skip this request, declaring it
         * successful so that the empty transactions will be cleaned up.
         */
        int numberOfOperations = 0;
        for (Commit commit : batch) {

            /* apply each operation to ES index */
            for (IndexOperation op : commit.getOps()) {
                final IndexOperation.OperationType type = op.getOperation();
                if (type == IndexOperation.OperationType.PUT) {
                    bulkRequest.add(
                        new IndexDocumentRequest(op.getESIndexName(),
                                                 op.getPkPath()).source(
                                                     op.getDocument()));
                } else if (type == IndexOperation.OperationType.DEL) {
                    bulkRequest.add(new DeleteRequest(op.getESIndexName(),
                                                      op.getPkPath()));
                } else {
                    throw new IllegalStateException(
                                  "illegal op to ES index " +
                                  op.getOperation());
                }
                numberOfOperations++;
            }
        }

        if (numberOfOperations == 0) {
            return null;
        }

        /* Default timeout is one minute, which seems proper. */
        return client.bulk(bulkRequest);
    }

    /* sync ES to ensure commit */
    private void ensureCommit() throws IOException {
        if (doEnsureCommit) {
            /*
             * Get all indices in the ES for this store, and refresh them all.
             */
            final Set<String> indices = getAllESIndexNamesInStore(STORE_NAME);

            client.refresh(indices.toArray(new String[0]));
        }
    }

    /*------------------------------------------------------*/
    /* static functions, start with public static functions */
    /*------------------------------------------------------*/

    /**
     * Verify that the given Elasticsearch node exists by connecting to it.
     * This is a transient connection used only during configuration. This
     * method is called in the context of the Admin during plan construction.
     *
     * If the node doesn't exist/the connection fails, throw
     * IllegalCommandException, because the user provided an incorrect address.
     *
     * If the node exists/the connection succeeds, ask the node for a list of
     * its peers, and return that list as a String of hostname:port[,...].
     *
     * If storeName is not null, then we expect that an ES Index corresponding
     * to the store name should NOT exist. If such does exist, then
     * IllegalCommandException is thrown, unless the forceClear flag is set. If
     * forceClear is true, then the offending ES index will be summarily
     * removed.
     *
     * This method will check the cluster state of ES and verify the cluster
     * name matches the provided cluster name.
     *
     * @param clusterName  name of ES cluster
     * @param transportHp  host and port of ES node to connect
     * @param storeName    name of the NoSQL store, or null as described above
     * @param secure       user configuration requirement.
     *                     This value comes from the Plan command.
     *                     Based on this value SSLContext
     *                     can be null or not null.
     * @param secParams    SecurityParams configured on SN.
     *                     This will be used to create the
     *                     SSLContext used in SSLEngine.
     * @param forceClear   if true, allows deletion of the existing ES index
     * @param inputLogger  caller's logger, if any. Null is allowed in tests.
     *
     * @return list of discovered ES node and port
     */
    public static String getAllTransports(String clusterName,
                                          HostPort transportHp,
                                          String storeName,
                                          boolean secure,
                                          SecurityParams secParams,
                                          boolean forceClear,
                                          Logger inputLogger) {

        final String errorMsg =
            "Can't connect to an Elasticsearch cluster at ";

        /*
         * Create a Monitoring Client. Get All nodes in the cluster. Make sure
         * that clusterName given in the argument matches. Create a string out
         * of HttpHosts in the format: hostname:port[,...] Close the monitoring
         * client, as this is a one time connection for configuration.
         */

        final StringBuilder sb = new StringBuilder();
        MonitorClient monitorClient = null;
        ESRestClient restClient = null;

        List<HttpHost> availableNodes = null;

        GetHttpNodesResponse resp = null;
        if (!secure) {
            secParams = null;
        }
        try {

            final ESHttpClient baseHttpClient =
                createESHttpClient(clusterName, transportHp, secParams,
                                   inputLogger);

            monitorClient = new MonitorClient(baseHttpClient,
                                              new ESLatestResponse(),
                                              inputLogger);
            resp =
                monitorClient.getHttpNodesResponse(new GetHttpNodesRequest());

            if (resp != null &&
                    !resp.getClusterName().equals(clusterName.trim())) {

                throw new IllegalCommandException(errorMsg + transportHp +
                        " Given Cluster Name does not match the" +
                        " cluster name on ES side.");
            }

            if (resp != null) {
                if (!ESRestClientUtil.isEmpty(resp.getClusterName()) &&
                        resp.getClusterName().equals(clusterName)) {
                    availableNodes =
                        resp.getHttpHosts(secure ? ESHttpClient.Scheme.HTTPS :
                                                   ESHttpClient.Scheme.HTTP,
                                          inputLogger);
                }
            }

            if (availableNodes == null || availableNodes.isEmpty()) {
                throw new IllegalCommandException(errorMsg + transportHp +
                        " {" + clusterName + "}");
            }

            for (HttpHost node : availableNodes) {
                if (sb.length() != 0) {
                    sb.append(ParameterUtils.HELPER_HOST_SEPARATOR);
                }
                sb.append(node.getHostName()).append(":")
                  .append(node.getPort());
            }

            /*
             * since each es index corresponds to a text index, we do not
             * know exactly the es index name, but we know all es indices
             * starts with a prefix derived from store name, which can
             * be used to check if there are pre-existing es indices for
             * the particular store
             */
            if (storeName != null) {
                /* fetch list of all indices in ES under the store */

                restClient = new ESRestClient(baseHttpClient, inputLogger);

                final Set<String> allIndices =
                    getAllESIndexInStoreInternal(storeName,
                                                 restClient.admin());

                /* delete each existing ES index if force clear */
                String offendingIndexes = "";
                for (String indexName : allIndices) {
                    if (forceClear) {
                        deleteESIndex(indexName, restClient.admin(),
                                      inputLogger);
                    } else {
                        offendingIndexes += "  " + indexName + "\n";
                    }
                }
                if (!("".equals(offendingIndexes))) {
                    throw new IllegalCommandException
                        ("\nThe Elasticsearch cluster \"" + clusterName +
                         "\" already contains indexes\n" +
                         "corresponding to the NoSQL Database " +
                         "store \"" + storeName + "\".\n" +
                         "Here is a list of them:\n" +
                         offendingIndexes +
                         "This situation might occur if you " +
                         "register an ES cluster simultaneously with\n" +
                         "two NoSQL Database stores that have the same " +
                         "store name, which is not allowed;\n" +
                         "or if you have removed a NoSQL store " +
                         "to which the ES cluster was registered\n" +
                         "(which makes the ES indexes orphans), " +
                         "and then created the store again with \n" +
                         "the same name. If the offending indexes " +
                         "are no longer needed, you can remove\n" +
                         "them by re-issuing the 'plan register-es' " +
                         "command with the -force option.");
                }
            }

        } catch (IOException e) {

            final String errPrefix =
                "Problem executing command on Elasticsearch at " + transportHp;
            final String checkLogMsg =
                "For more information, check the store log files";
            final String cliErrMsg;
            String warnStr =
                "ElasticsearchHandler.getAllTransports [IOException]:\n";

            if (e.getCause() instanceof SSLContextException) {
                final String secErrMsg =
                    "Could not set up Security Context based on the current " +
                    " security configurations for each Elasticsearch node";

                cliErrMsg =
                    errPrefix + "\n" + secErrMsg + "\n" + checkLogMsg;

                warnStr = errPrefix + "\n" + warnStr + "\n" + secErrMsg +
                          "\n" + LoggerUtils.getStackTrace(e);

                inputLogger.warning(warnStr);
                throw new IllegalCommandException(cliErrMsg);
            }

            cliErrMsg = errPrefix + "\n" + checkLogMsg;
            warnStr = errPrefix + "\n" + warnStr + "\n" +
                          LoggerUtils.getStackTrace(e);

            inputLogger.warning(warnStr);
            throw new IllegalCommandException(cliErrMsg);

        } catch (Exception e) {

            final String errPrefix =
                "Problem executing command on Elasticsearch at " + transportHp;

            final String cliErrMsg = errPrefix + "\n" +
                "For more information, check the store log files";

            final String warnStr = errPrefix + "\n" +
                "ElasticsearchHandler.getAllTransports [Exception]:" +
                "\n" + e + "\n" + LoggerUtils.getStackTrace(e);
            inputLogger.warning(warnStr);
            throw new IllegalCommandException(cliErrMsg);

        } finally {
            /*
             * Both restClient and monitorClient will end up closing same
             * httpClient.
             */
            if (monitorClient != null) {
                monitorClient.close();
            }
            if (restClient != null) {
                restClient.close();
            }
        }

        return sb.toString();
    }

    /**
     * Return an indication of whether the ES cluster is considered "healthy".
     *
     * @param esMembers
     */
    public static boolean isClusterHealthy(String esMembers,
                                           ESAdminClient esAdminClient) {

        final ClusterHealthRequest req = new ClusterHealthRequest();
        ClusterHealthResponse resp = null;
        try {
            resp = esAdminClient.clusterHealth(req);
        } catch (IOException e) {
            /*
             * Could not execute an API call after few retries means cluster is
             * not healthy.
             */
            return false;
        }

        final ClusterHealthStatus status = resp.getClusterHealthStatus();

        /*
         * We want to insist on a GREEN status for the cluster when operations
         * such as creating or deleting an index are performed.  This is
         * because there are weird cases where a deletion would be undone
         * later, if some nodes are down when the deletion took place.
         * cf. https://github.com/elastic/elasticsearch/issues/13298
         *
         * The problem with requiring GREEN status is that ES's out-of-the-box
         * defaults result in a single-node cluster's status always being
         * YELLOW.  Requiring GREEN for index deletion would cause problems for
         * tire-kickers, and we don't want that.
         *
         * The compromise is to allow YELLOW status for single-node clusters,
         * but insist on GREEN for every other situation.
         */

        if (ClusterHealthStatus.GREEN.equals(status)) {
            return true;
        }

        /*
         * Turns out it's not so easy to distinguish between a "single-node
         * cluster" and a multi-node cluster that has only one node available.
         * I am not finding a reliable way to do it.
         *
         * It seems that ES lacks a notion of a persistent topology.  If a node
         * is not present, it's as if it never existed!
         *
         * Things I tried:
         *  - NodesInfoRequest will not reliably return information
         *    about nodes that aren't running.
         *
         *  - Looking at the number of unassigned shards - this only tells
         *    you that there aren't enough nodes to satisfy the number of
         *    replicas specified.
         *
         *  - for an unassigned shard there's a "reason" it is unassigned,
         *    which can be any of the values of the enum
         *    org.elasticsearch.cluster.routing.Reason.  This looked promising,
         *    as newly created indexes that can't satisfy their replica
         *    requirements give the reason value of INDEX_CREATED; but
         *    after a restart of the single ES node, this value changed to
         *    CLUSTER_RECOVERED, which is the same as for shards that were
         *    previously assigned to a missing node.  Dang it.
         *    The reason value NODE_LEFT seems promising, but it's also
         *    transient.
         *
         * So, for now we will user kvstore's knowledge of the number of nodes
         * in the cluster at register-es time.  This isn't 100% reliable because
         * the number of nodes might have changed since register-es, but that
         * should be uncommon.  We do advise users to issue register-es after
         * changing the ES cluster's topology, to update kvstore's list of
         * nodes.
         *
         */
        final int nRegisteredNodes = esMembers.split(",").length;
        if (ClusterHealthStatus.YELLOW.equals(status) &&
            resp.getNumberOfDataNodes() == 1 &&
            nRegisteredNodes == 1) {
            return true;
        }

        return false;
    }

    /**
     * Static version of existESIndex for use by getAllTransports, when no
     * ElasticsearchHandler object exists.  Called during es registration.
     *
     * @param indexName     The name of the ES index to check
     * @param esAdminClient The ES Admin client handle
     * @return              True if the index exists
     * @throws IOException
     */
    public static boolean existESIndex(String indexName,
                                       ESAdminClient esAdminClient)
        throws IOException {
        final IndexExistRequest existsRequest =
            new IndexExistRequest(indexName);
        final IndexExistResponse existResponse =
                                     esAdminClient.indexExists(existsRequest);
        return existResponse.exists();
    }

    /**
     * Static version of addingESIndex.
     *
     * @param esIndexName   name of ES index
     * @param esAdminClient ES Admin client handle
     *
     * @throws IndexAlreadyExistsException
     * @throws IllegalStateException
     */
    public static void createESIndex(String esIndexName,
                                     ESAdminClient esAdminClient)
        throws IndexAlreadyExistsException, IllegalStateException {

        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class,
                                           "ElasticsearchHandler");
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler.createESIndex: " +
                        "[index = " + esIndexName + "]");
        }

        CreateIndexResponse createResponse = null;
        try {

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndex: " +
                            "sending REST " +
                            "index creation request to elasticsearch" +
                            "[index = " + esIndexName + "] ...");
            }

            createResponse =
                esAdminClient.createIndex(new CreateIndexRequest(esIndexName));

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.createESIndex: " +
                            "response received from elasticsearch" +
                            "[index = " + esIndexName +
                            ", response = " + createResponse + "]");
            }

        } catch (IOException e) {

            throw new IllegalStateException(
                "ElasticsearchHandler.createESIndex: Failed to create " +
                "ES index [" + esIndexName + "], cause = " + e);
        }

        if (createResponse == null || !createResponse.isAcknowledged()) {

            final String msgPrefix =
                "ElasticsearchHandler.createESIndex: failed to " +
                "create elasticsearch index [index = " + esIndexName;

            String errMsg = msgPrefix + "]";

            if (logger.isLoggable(Level.WARNING)) {

                if (createResponse != null) {
                    errMsg = msgPrefix + ", acknowledged = " +
                             createResponse.isAcknowledged() + "]" +
                             ". Request not acknowledged.";
                }
                logger.warning(" ");
                logger.warning(errMsg);
            }

            throw new IllegalStateException(errMsg);
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ElasticsearchHandler.createESIndex: " +
                        "elasticsearch index " +
                        "created [index = " + esIndexName +
                        ", acknowledged = " + createResponse.isAcknowledged() +
                        ", error = " + createResponse.isError() + "]");
        }
    }

    /**
     * Static version of deleteEsIndex.
     *
     * @param indexName      name of the ES index to remove
     * @param esAdminClient  ES Admin client handle
     * @return               True if the index existed and was deleted
     */
    public static boolean deleteESIndex(String indexName,
                                        ESAdminClient esAdminClient,
                                        Logger inputLogger) {
        DeleteIndexResponse deleteIndexResponse = null;
        try {
            if (!existESIndex(indexName, esAdminClient)) {
                return false;
            }

            final DeleteIndexRequest deleteIndexRequest =
                new DeleteIndexRequest(indexName);

            deleteIndexResponse =
                esAdminClient.deleteIndex(deleteIndexRequest);
            if (!deleteIndexResponse.exists()) {
                inputLogger.warning("Index:" + indexName +
                        " could not get deleted because it did not exist.");
            }

        } catch (IOException e) {
            inputLogger.warning("ElasticsearchHandler.deleteESIndex: Delete " +
                           "Request Failed [index=" + indexName + "], " +
                           "cause = " + e);
            throw new IllegalStateException(
                "ElasticsearchHandler.deleteESIndex: Failed to delete ES " +
                "index [" + indexName + "], cause = " + e);
        }
        if (!deleteIndexResponse.isAcknowledged()) {
            throw new IllegalStateException(
                "ElasticsearchHandler.deleteESIndex: Failed to delete ES " +
                "index [" + indexName + "], " +
                "cause = 'delete request not acknowledged'");
        }
        return true;
    }

    /**
     * Returns all ES indices corresponding to text indices in the kvstore
     *
     * @param storeName  name of kv store
     * @param adminClient es client
     * @return  list of all ES index names
     * @throws IOException
     */
    static Set<String> getAllESIndexInStoreInternal
                                   (final String storeName,
                                    final ESAdminClient adminClient)
        throws IOException {
        Set<String> ret = Collections.emptySet();
        String prefix = null;
        if (storeName != null) {
            prefix = TextIndexFeeder.deriveESIndexPrefix(storeName);
        }

        /* fetch list of all indices in ES with prefix */
        final CATResponse resp =
            adminClient.catInfo(CATRequest.API.INDICES, null,
                                prefix, null, null);
        if (resp != null) {
            ret = resp.getIndices();
        }
        return ret;
    }

    static String constructMapping(IndexImpl index) {

        try {
            final JsonGenerator jsonGen = generateMapping(index);
            jsonGen.flush();
            return new String(
                              ((ByteArrayOutputStream) jsonGen
                                      .getOutputTarget()).toByteArray(),
                              "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(
                                            "Unable to serialize ES mapping" +
                                            " for text index due to UTF-8" +
                                            " enconding not supported " +
                                            index.getName(), e);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to serialize ES mapping for text index due to" +
                    " json generation issues " +
                    index.getName(), e);
       }
    }

    /*
     * Creates the JSON to describe an ES type mapping for this index.
     */
    static JsonGenerator generateMapping(IndexImpl index) {

        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class,
                                           "ElasticsearchHandler");
        }

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ElasticsearchHandler.generateMapping: " +
                        "index = " + index.getName());
        }

        final Table table = index.getTable();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        /*
         * The json mapping that is generated should be of the form:
         *
         * {
         *     "dynamic":false,
         *     "properties":{
         *
         *        "_pkey":{
         *            "enabled":false,
         *            "properties":{
         *                "_table":{"type":"string"},
         *                "id":{"type":"integer"}
         *             }
         *        },
         *
         *        "kvstoreJsonField_A":{
         *            "childA1":{
         *                "type":"object",
         *                "properties":{
         *                    "childA2":{
         *                        "type":"object",
         *                        "properties":{
         *                           "childA3":{"type":"integer"}
         *                        }
         *                    }
         *                }
         *            }
         *        },
         *
         *        "kvstoreJsonField_B":{
         *            "childB1":{
         *                "type":"object",
         *                "properties":{
         *                    "childB2":{
         *                        "type":"object",
         *                        "properties":{
         *                           "childB3":{"analyzer":"english",
         *                                      "type":"string"}
         *                        }
         *                    }
         *                }
         *            }
         *        }
         *    }
         * }
         *
         * When generating the mapping, the first thing to do is place the
         * header information in the JsonGenerator. That is, disable
         * "dynamic" typing, and specify the "p_key" object; everything
         * up to the first field that will be indexed (ex. "kvstoreJsonField"
         * in the example above).
         */
        try {
            final ESJsonBuilder jsonBuilder = new ESJsonBuilder(baos);
            final JsonGenerator jsonGen = jsonBuilder.jsonGenarator();
            jsonGen.writeStartObject(); /* Mapping Start */
            jsonGen.writeBooleanField("dynamic", false);
            jsonBuilder.startStructure("properties").startStructure("_pkey");
            jsonGen.writeBooleanField("enabled", false);
            jsonBuilder.startStructure("properties").startStructure("_table");
            /*
             * For unit tests, this static method is used directly without
             * any esHandler instantiation.
             * esVersion will be null in that case, as there is no
             * ES Cluster started in these tests. Default esVersion.
             */
            if (esVersion == null) {
                esVersion = "2.4.4";
            }
            if (esVersion.compareTo("5") > 0) {
                jsonGen.writeStringField("type", "keyword");
            } else {
                jsonGen.writeStringField("type", "string");
            }
            jsonGen.writeEndObject(); /* end _table structure */

            for (String keyField : table.getPrimaryKey()) {
                final String type = defaultESTypeFor(table.getField(keyField));
                if ("string".equals(type)) {
                    if (esVersion.compareTo("5") > 0) {
                        jsonBuilder.field(keyField,
                                          getMappingTypeInfo("keyword"));
                    } else {
                        jsonBuilder.field(keyField,
                                          getMappingTypeInfo("string"));
                    }
                } else {

                    logger.fine("ElasticsearchHandler.generateMapping: " +
                                "keyField = " + keyField +
                                ", mappingTypeInfo = " +
                                getMappingTypeInfo(type));
                    jsonBuilder.field(keyField, getMappingTypeInfo(type));
                }
            }
            jsonGen.writeEndObject(); /* end pkey properties */
            jsonGen.writeEndObject(); /* end pkey */

            /*
             * We want to preserve the letter case of field names in the ES
             * document type, but the name of the path in IndexField is
             * lower-cased. The field names in IndexImpl have their original
             * case intact. So we iterate over the list of String field names
             * and the list of IndexFields in parallel, so that we have the
             * unmolested name in hand when it's needed.
             */
            final List<IndexField> indexFields = index.getIndexFields();

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("ElasticsearchHandler.generateMapping: " +
                            "indexFields = " + indexFields);

                if (indexFields != null) {
                    logger.fine("ElasticsearchHandler.generateMapping: " +
                                "indexFields.size = " + indexFields.size());
                }

                logger.fine("ElasticsearchHandler.generateMapping: " +
                            "indexFields = " + indexFields);
            }

            /*
             * Loop thru the index fields, sorting, parsing, and grouping
             * the dot-separated components of each field. Using the example
             * above, the index fields will be of the form:
             *
             * kvstoreJsonField_A.childA1.childA2.childA3 {"type":"integer"}
             * kvstoreJsonField_B.childB1.childB2.childB3
             *                        {"type":"string","analyzer":"english"}
             *
             * The components of each such index field reflect a
             * parent.child.grandchild.etc. relationship, where all but the
             * last component is an object, and the last component is a
             * scalar with correponding type specified in the annotation map
             * associated with the given index field.
             */
            final Map<String, JsonPathNode> rootNodeMap = new HashMap<>();
            int indexFieldCounter = 0;
            for (String field : index.getFields()) {

                IndexField indexField = null;
                if (indexFields != null && indexFields.size() > 0) {
                    indexField = indexFields.get(indexFieldCounter++);
                }

                /*
                 * We have to parse the mappingSpec string so that it is
                 * copied correctly into the builder. The mappingSpec cannot
                 * be treated as a string, or it will be quoted in its
                 * entirety in the resulting JSON string.
                 */
                String annotation = index.getAnnotationForField(field);
                annotation =
                    (annotation == null ? EMPTY_ANNOTATION : annotation);

                if (logger.isLoggable(Level.FINE)) {
                    logger.fine("ElasticsearchHandler.generateMapping: " +
                                "field = " + field + ", " +
                                "annotation = " + annotation);
                }

                try (JsonParser parser = ESJsonUtil.createParser(annotation)) {

                    final Map<String, Object> m =
                        ESJsonUtil.parseAsMap(parser);

                    final String mappingFieldName = getMappingFieldName(field);

                    /*
                     * If no annotation was provided with the current
                     * indexField, or if the annotation that was provided
                     * does not contain a mapping specifying the elasticsearch
                     * "type" to associate with the current indexField in
                     * the elasticsearch mapping being generated by this
                     * method, then getMappingFieldType is called to determine
                     * the appropriate elasticsearch type to use.
                     *
                     * On the other hand, if an annotation with "type"
                     * specification was provided, then that annotation is
                     * used as-is in the elasticsearch mapping being
                     * generated here.
                     */
                    final Object typeAnnotation = m.get("type");
                    String type = null;

                    if (typeAnnotation == null) {
                        type = getMappingFieldType(indexField);
                    } else {
                        type = typeAnnotation.toString();
                    }

                    if ("string".equals(type) || "text".equals(type)) {
                        if (esVersion.compareTo("5") > 0) {
                            m.putAll(getMappingTypeInfo("text"));
                        } else {
                            m.putAll(getMappingTypeInfo("string"));
                        }
                    } else {
                        m.putAll(getMappingTypeInfo(type, m));
                    }

                    if (logger.isLoggable(Level.FINE)) {
                        logger.fine("ElasticsearchHandler.generateMapping: " +
                                    "field = " + field + ", " +
                                    "mappingFieldName = " + mappingFieldName +
                                    ", map = " + m);
                    }

                    /*
                     * Place the components making up the json path into an
                     * array so that they can be processed separately.
                     * For example, if field = parentNode.child1.child2.child3,
                     * then nodeNames[] = [parentNode, child1, child2, child3].
                     *
                     * Note that if the field being indexed is a MAP type,
                     * then the CREATE FULLTEXT INDEX command used to index
                     * the field(s) of the table must specify such MAP fields
                     * using the format, 'mapFieldname.values()'. For that
                     * case, the field must be handled differently than the
                     * other field types. For a field name of the form
                     * 'mapFieldname.values()', rather than splitting the
                     * name into two components ('mapFieldname' & 'values()'),
                     * a single component with the '.' replaced with '/' is
                     * constructed. This is done so that the map can be
                     * indexed in Elasticsearch as an ES array rather than
                     * an ES object type.
                     */
                    String[] nodeNames = field.split("\\s*\\.\\s*");
                    if (indexField != null) {
                        /* If MAP type, override the default nodeNames. */
                        if ((indexField.getFirstDef()).getType()
                                 .equals(FieldDef.Type.MAP)) {
                            nodeNames = new String[] { mappingFieldName };
                        }
                    }

                    final String curRootName = nodeNames[0];
                    JsonPathNode curRootNode = rootNodeMap.get(curRootName);
                    if (curRootNode == null) {
                        curRootNode = new JsonPathNode(curRootName);
                    }
                    JsonPathNode prevNode = null;
                    for (int k = nodeNames.length - 1; k > 0; k--) {
                        final String curName = nodeNames[k];
                        final JsonPathNode curNode = new JsonPathNode(curName);

                        if (prevNode == null) {
                            curNode.setAnnotation(annotation, m, esVersion);
                        } else {
                            curNode.addChild(prevNode);
                        }
                        prevNode = curNode;
                    }
                    if (prevNode == null) {
                        curRootNode.setAnnotation(annotation, m, esVersion);
                    } else {
                        curRootNode.addChild(prevNode);
                    }
                    rootNodeMap.put(curRootName, curRootNode);
                }
            }

            /*
             * Walk the contents of the map constructed above, generating
             * appropriate Json object (parent) and Json scalar (child)
             * specifications for each element of the map.
             */
            for (Map.Entry<String, JsonPathNode> entry :
                                                 rootNodeMap.entrySet()) {

                final String curRootNodeName = entry.getKey();
                final JsonPathNode curRootNode = entry.getValue();

                /*
                 * Sort the children of the root/parent node so that
                 * each child path with the same child node name are
                 * grouped together, and thus, mapped together. For
                 * example, suppose the user enters the following paths:
                 *
                 * parentNode.child1.grandChild1A
                 * parentNode.child2.grandChild2A
                 * parentNode.child3.grandChild3A
                 * parentNode.child3.grandChild3B
                 * parentNode.child2.grandChild2B
                 * parentNode.child1.grandChild1C
                 * parentNode.child2.grandChild2C
                 * parentNode.child3.grandChild3C
                 * parentNode.child1.grandChild1C
                 *
                 * Then sorting the curRootNode will result in a map with
                 * elements having a sort order reflecting all the child1
                 * nodes first, followed by the child2 nodes, and then the
                 * child3 nodes. Note that the grandchild nodes are not
                 * sorted. For the example above, the sortedChildMap that
                 * is produced will reflect components ordered as follows:
                 *
                 * child1.grandChild1A
                 * child1.grandChild1C
                 * child1.grandChild1B
                 *
                 * child2.grandChild2A
                 * child2.grandChild2B
                 * child2.grandChild2C
                 *
                 * child3.grandChild3A
                 * child3.grandChild3B
                 * child3.grandChild3C
                 *
                 * This is done to support invoking buildMappingFromNodeMap()
                 * recursively in a depth-first (rather than breadth-first)
                 * manner. That is, map child1 and its descendents first,
                 * then child2 and its descendents, and so on.
                 */
                final Map<String, List<JsonPathNode>> sortedChildMap =
                    getSortedChildMap(curRootNode);

                if (isScalar(curRootNode)) {

                    writeScalarToMapping(curRootNode, jsonBuilder);
                    jsonGen.writeEndObject();

                } else { /* not scalar ==> object */

                    jsonBuilder.startStructure(curRootNodeName);
                    jsonGen.writeStringField("type", "object");
                    jsonBuilder.startStructure("properties");

                    /* Recurse thru the current node's children. */
                    buildMappingFromNodeMap(sortedChildMap, jsonBuilder);

                    jsonGen.writeEndObject(); /* end curRootNode properties */
                    jsonGen.writeEndObject(); /* end curRootNode structure */
                }
            }/* end loop thru rootNodeMap */

            /* Do final termination of the mapping generated above. */
            jsonGen.writeEndObject(); /* top level properties end */
            jsonGen.writeEndObject(); /* mapping structure end */

            if (logger.isLoggable(Level.FINE)) {
                logger.fine("    ");
                logger.fine("ElasticsearchHandler.generateMapping: index " +
                            "mapping complete [index = " +
                            index.getName() + "]");

                final String mappingSpec = mappingSpecStr(jsonGen);
                logger.fine(" ");
                logger.fine("ElasticsearchHandler.generateMapping: " +
                            "mappingSpec as JSON = " + mappingSpec);
                logger.fine(" ");
                logger.fine("ElasticsearchHandler.generateMapping: " +
                            "mappingSpec as MAP  = " +
                            JsonUtils.fromJson(mappingSpec, HashMap.class));
                logger.fine(" ");
            }
            return jsonGen;

        } catch (IOException e) {
            throw new IllegalStateException
                ("Unable to serialize ES mapping for text index " +
                 index.getName(), e);
        }
    }

    /**
     * Based on the given type parameter, constructs a (key,value) pair in
     * which the key is the String "type" and the value is the name of the
     * Object type represented by the value of the type parameter. For
     * example, if the value input for the type parameter is "long", then
     * the (key,value) pair that is constructed is, ("type","long").
     *
     * The (key,value) pair that is constructed is returned as an element
     * of the Map that is ultimately returned.
     *
     * Note that a type parameter with the value "date" is handled specially.
     * In this case, the Map that is returned will contain elements having
     * a form like one of the following:
     *
     * {"type":"date","format":"strict_date_optional_time||epoch_millis"}
     * or
     * {"type":"date"}
     * or
     * {"type":"date","format":
     *                "yyyy-MM-dd'T'HH:mm:ss.SSS||yyyy-MM-dd||epoch_millis"}
     *
     * The form of the element that is added to the returned Map will depend
     * on the contents of the given typeMap parameter, as well as the
     * version of Elasticsearch. For more information on how the "date" type
     * handled, see the implementation note presented at the beginning
     * of this class.
     *
     * @throws IOException
     */
    private static Map<String, Object> getMappingTypeInfo(final String type)
        throws IOException {

        return getMappingTypeInfo(type, null);
    }

    private static Map<String, Object> getMappingTypeInfo(
                                           final String type,
                                           final Map<String, Object> typeMap)
        throws IOException {

        if (type.startsWith(DATE)) {

            int precision = 0;

            if (typeMap != null) {

                final Object precisionObj = typeMap.get("precision");
                final String precisionStr = (precisionObj != null ?
                    precisionObj.toString().toLowerCase() : null);

                if (precisionStr != null) {
                    if ("millis".equals(precisionStr)) {
                        precision = 6;
                    } else if ("nanos".equals(precisionStr)) {
                        precision = 9;
                    }
                }
            }
            return getTimestampTypeProps(precision);
        }

        final Map<String, Object> map = new HashMap<String, Object>(1);
        map.put("type", type);
        return map;
    }

    /**
     * Returns a map that contains mapping type information for TIMESTAMP type.
     */
    private static Map<String, Object> getTimestampTypeProps(int precision)
        throws IOException {

        final String timestampType = precisionToTimestampType(precision);
        return ESJsonUtil.parseAsMap(ESJsonUtil.createParser(timestampType));
    }

    /**
     * Returns the annotation to use for an elasticsearch "date" type,
     * based on the specified precision. For more information on the
     * logic employed by this method, see the implementation note at
     * the beginning of this class.
     */
    private static String precisionToTimestampType(final int precision) {

        if (precision == 3) {
            return  TIMESTAMP_TO_ES_DATE_MILLIS;
        }

        if (esVersion.compareTo("5") > 0) {
            if (precision > 3) {
                return  TIMESTAMP_TO_ES5_DATE_NANOS;
            }
            return  TIMESTAMP_TO_ES5_DATE_DEFAULT;
        }

        /* ES version less than 5. Use version 2 specific values. */
        if (precision > 3) {
            return  TIMESTAMP_TO_ES2_DATE_NANOS;
        }
        return  TIMESTAMP_TO_ES2_DATE_DEFAULT;
    }

    /*
     * Mangle a table field's name so that it works as an ES mapping field
     * name.  In particular, the '.' character is not allowed in mappings,
     * so we substitute '/' for '.'.
     *
     * Note that if the field is a MAP, then the field name input to this
     * method will take one of the following forms: "mapName.values()",
     * "mapName.keys()", or "mapName.keyname"; depending on what
     * part of the map is to be indexed. If the field name input has the
     * suffix ".values()" or ".keys()", then the suffix ".values()" is
     * replaced with "/values" and similarly, ".keys()" is replaced with
     * "/keys". For the single key case, as well as for the names of all
     * other field types, any '.' characters in the given name are each
     * replaced with '/'; so that Elasticsearch will accept the name
     * returned by this method as an indexable field.
     */
    private static String getMappingFieldName(String field) {

        String retField = field;

        final String valuesSuffix = TableImpl.VALUES; /* "values()" */
        final String keysSuffix = TableImpl.KEYS; /* "keys()" */
        final String parenSuffix = "()";

        /* 1. Remove "()" from the input field. */
        final int parenSuffixIndx = retField.lastIndexOf(parenSuffix);
        if (retField.endsWith(valuesSuffix)) {
            if (parenSuffixIndx > -1) {
                retField = retField.substring(0, parenSuffixIndx);
            }
        } else if (retField.endsWith(keysSuffix)) {
            if (parenSuffixIndx > -1) {
                retField = retField.substring(0, parenSuffixIndx);
            }
        }

        /* 2. Replace "." character(s) with "/". */
        retField = retField.replace('.', '/');

        return retField;
    }

    /*
     * Return the default type for the field represented by the iField.
     */
    private static String getMappingFieldType(IndexField ifield) {

        if (logger == null) {
            logger = LoggerUtils.getLogger(ElasticsearchHandler.class,
                                           "ElasticsearchHandler");
        }

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(" ");
            logger.finest("ElasticsearchHandler.getMappingFieldType: " +
                          "IndexField = " + ifield);
        }

        /* The possibilities are as follows:
         *
         * 1. ifield represents a scalar type.
         *
         * 2. ifield represents an Array
         *    2a. The array contains a scalar type.
         *    2b. The array contains a record and ifield refers to a
         *        scalar type field in the record.
         *
         * 3. ifield represents a Record and refers to a scalar field
         *    in the Record.
         *
         * 4. ifield represents a Map
         *    4a. ifield refers to the Map's string keys (endswith '.keys()').
         *    4b. ifield refers to the Map's values (endswith '.values()').
         *    4c. ifield refers to a specific key in the map.
         */

        FieldDef fdef = ifield.getFirstDef();
        int stepIdx = 0;
        String fieldName = ifield.getStep(stepIdx++);

        logger.finest("ElasticsearchHandler.getMappingFieldType: fieldDef = " +
                      fdef + ", fieldName = " + fieldName);

        switch (fdef.getType()) {
        case STRING:
        case INTEGER:
        case LONG:
        case BOOLEAN:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP:
            /* case 1 */
            logger.finest("ElasticsearchHandler.getMappingFieldType: " +
                          "'scalar' ==> return default ES " +
                          "type = " + defaultESTypeFor(fdef));
            return defaultESTypeFor(fdef);

        case ARRAY:
            final ArrayDef adef = fdef.asArray();
            fdef = adef.getElement();
            if (!fdef.isComplex()) {
                /* case 2a. */
                logger.finest("ElasticsearchHandler.getMappingFieldType: " +
                              "non-complex ARRAY ==> return default ES " +
                              "type = " + defaultESTypeFor(fdef));
                return defaultESTypeFor(fdef);
            }
            if (!fdef.isRecord()) {
                throw new IllegalStateException
                    ("Array type " + fdef + " not allowed as an index field.");
            }
            /* case 2b. */
            stepIdx++; /* Skip over the ifield placeholder "[]" */
            //$FALL-THROUGH$
         case RECORD:
            /* case 3. */
            fieldName = ifield.getStep(stepIdx++);
            fdef = fdef.asRecord().getFieldDef(fieldName);
            logger.finest("ElasticsearchHandler.getMappingFieldType: RECORD " +
                          "==> return default ES type = " +
                          defaultESTypeFor(fdef));
            return defaultESTypeFor(fdef);
         case MAP:
            final MapDef mdef = fdef.asMap();
            fieldName = ifield.getStep(stepIdx++);
            if (TableImpl.KEYS.equalsIgnoreCase(fieldName)) {
                /* case 4a. Keys are always strings. */
                logger.finest("ElasticsearchHandler.getMappingFieldType: " +
                              "mapFields.keys() specified ==> return " +
                              "default ES type for string = " +
                              defaultESTypeFor(
                                  FieldDefImpl.Constants.stringDef));
                return defaultESTypeFor(FieldDefImpl.Constants.stringDef);
            }
            /* case 4b and 4c are the same from a schema standpoint. */
            fdef = mdef.getElement();

            if (logger.isLoggable(Level.FINEST)) {
                if (TableImpl.VALUES.equalsIgnoreCase(fieldName)) {
                    logger.finest(
                        "ElasticsearchHandler.getMappingFieldType: " +
                        "mapField.values() specified ==> return " +
                        "default ES type for the NoSql type of each " +
                        "value in the map = " + defaultESTypeFor(fdef));
                } else {
                    logger.finest(
                        "ElasticsearchHandler.getMappingFieldType: " +
                        "mapField.keyName specified ==> return " +
                        "default ES type for the NoSql type of the " +
                        "map value corresponding to the key = " +
                        defaultESTypeFor(fdef));
                }
            }
            return defaultESTypeFor(fdef);

        default:
            throw new IllegalStateException
               ("Fields of type " + fdef + " aren't allowed as index fields.");
        }
    }

    /*
     * Returns a put operation containing a JSON document suitable for
     * indexing at an ES index, based on the given RowImpl.
     *
     * @param esIndexName  name of es index to which the put operation is
     *                     created
     * @param row          row from which to create a put operation
     * @return  a put index operation to an es index; if null, it means
     *                     that no significant content was found.
     */
    static IndexOperation makePutOperation(IndexImpl index,
                                           String esIndexName,
                                           RowImpl row) {

        return makePutOperation(
                   index, esIndexName, row,
                   LoggerUtils.getLogger(ElasticsearchHandler.class,
                                         "ElasticsearchHandler"));
    }

    static IndexOperation makePutOperation(final IndexImpl index,
                                           final String esIndexName,
                                           final RowImpl row,
                                           final Logger inLogger) {

        final Table table = index.getTable();
        assert (table == row.getTable());

        /* The encoded string form of the row's primary key. */
        final String pkPath =
            TableKey.createKey(table, row, false).getKey().toString();

        try {
            /* root object */
            final ESJsonBuilder document =
                ESJsonBuilder.builder().startStructure()
                .startStructure("_pkey")
                .field("_table", table.getFullName()); /* nested primary key */

            for (String keyField : table.getPrimaryKey()) {
                new DocEmitter(keyField, document).putValue(row.get(keyField));
            }

            document.endStructure(); /* end of primary key object */

            final List<IndexField> indexFields = index.getIndexFields();
            inLogger.finest("ElasticsearchHandler.makePutOperation: " +
                            "indexFields = " + indexFields);

            int indexFieldCounter = 0;
            boolean contentToIndex = false;
            String rootNodeName = "";
            for (String field : index.getFields()) {
                final IndexField indexField =
                                     indexFields.get(indexFieldCounter++);
                FieldDef fdef = indexField.getFirstDef();
                String curStep = indexField.getStep(0);

                if (fdef.getType() == FieldDef.Type.JSON &&
                        !rootNodeName.isEmpty() &&
                        rootNodeName.equals(curStep)) {
                    continue;
                } else if (fdef.getType() == FieldDef.Type.JSON) {
                    rootNodeName = curStep;
                }
                
                if (addValue(indexField, row,
                             getMappingFieldName(field), document, inLogger)) {
                    contentToIndex = true;
                }
            }

            if (!contentToIndex) {
                return null;
            }

            document.endStructure(); /* end of root object */

            if (inLogger.isLoggable(Level.FINEST)) {
                inLogger.finest(" ");
                inLogger.finest("------ DOCUMENT (makePutOperation) -----");
                inLogger.finest(mappingSpecStr(document.jsonGenarator()));
                inLogger.finest("----------------------------------------");
                inLogger.finest(" ");
            }

            return new IndexOperation(esIndexName, pkPath,
                                      document.byteArray(),
                                      IndexOperation.OperationType.PUT);
        } catch (IOException e) {
            throw new IllegalStateException
                ("Unable to serialize ES" + " document for text index " +
                 index.getName(), e);
        }
    }

    /*
     * Add a field to the JSON document using the value implied by indexField
     * and row.  A return value of false indicates that no indexable content
     * was found.
     *
     * @param indexField instance of IndexImpl.IndexField which represents
     *        a TablePath describing the 'steps' of the field to be indexed.
     * @param row the table row containing the field to be indexed.
     * @param mappingFieldName the name to use when indexing the field in
     *        Elasticsearch. If the indexField is a scalar value, then
     *        this name will be the same as the first and only step in
     *        the given indexField. The method getMappingFieldName()
     *        describes the cases where this name is not the same as that
     *        for a field that is a scalar.
     * @param document the Json document to which to add the desired value.
     * @param inLogger the logger to use for debug output.
     *
     * @return true if the desired value is successfully added (emitted) to
     *         the Json; false otherwise.
     */
    private static boolean addValue(final IndexField indexField,
                                    final RowImpl row,
                                    final String mappingFieldName,
                                    final ESJsonBuilder document,
                                    final Logger inLogger) throws IOException {

        FieldDef fdef = indexField.getFirstDef();
        int stepIdx = 0;
        String curStep = indexField.getStep(stepIdx++);

        String rootNodeName = mappingFieldName;
        if (fdef.getType() == FieldDef.Type.JSON) {
            rootNodeName = curStep;
        }

        /*
         * Emit the field name lazily; if there is nothing to index,
         * don't bother indexing the field at all.
         *
         * Note that if the field type is JSON, then when emitting, the
         * field name initially supplied to the emitter is handled differently.
         * When not JSON, the field name is the mappingFieldName. In that case,
         * if the given indexField is a dot-separated path, the dots are
         * replaced with '/' and the mappingFieldName is input to the emitter
         * as a single field name; for example, if indexField = 'a.b.c', then
         * the mappingFieldName input to the emitter will be ='a/b/c'.
         * On the other hand, if the field type is JSON, then each component
         * in the dot-separated path is handled separately; where the
         * last component in a given path is treated as a scalar mapping
         * in the emitted document, and all other components in that path
         * are emitted as JSON object structures.
         */
        final DocEmitter emitter = new DocEmitter(rootNodeName, document);

        switch (fdef.getType()) {

                /* Scalar types are easy. */
        case STRING:
        case INTEGER:
        case LONG:
        case BOOLEAN:
        case FLOAT:
        case DOUBLE:
        case TIMESTAMP:
            emitter.putValue(row.get(curStep));
            break;

            /* An array can contain either scalars or records.
             * If it's an array of records, one field of the record will
             * be indicated by IndexField.
             */
        case ARRAY:
            if (row.get(curStep).equals(NullValueImpl.getInstance())) {
                /*
                 * This field is not there in the row and hence will not be
                 * there in the ES document for this row.
                 */
                break;
            }
            final ArrayValue aValue = row.get(curStep).asArray();
            final ArrayDef adef = fdef.asArray();
            fdef = adef.getElement();
            if (fdef.isComplex()) {
                if (!fdef.isRecord()) {
                    throw new IllegalStateException
                        ("Array type " + fdef +
                         " not allowed as an index field.");
                }
                stepIdx++; /* Skip over the ifield placeholder "[]" */
                curStep = indexField.getStep(stepIdx++);
            }
            for (FieldValue element : aValue.toList()) {
                if (element.isRecord()) {
                    emitter.putArrayValue(element.asRecord().get(curStep));
                } else {
                    emitter.putArrayValue(element);
                }
            }
            break;

            /*
             * A record will have one field indicated for indexing.
             */
        case RECORD:
            if (row.get(curStep).equals(NullValueImpl.getInstance())) {
                /*
                 * This field is not there in the row and hence will not be
                 * there in the ES document for this row.
                 */
                break;
            }
            final RecordValue rValue = row.get(curStep).asRecord();
            curStep = indexField.getStep(stepIdx++);
            emitter.putValue(rValue.get(curStep));
            break;

            /*
             * An index field can specify that all keys, all values, or one
             * value corresponding to a given key be included in the index.
             */
        case MAP:
            if (row.get(curStep).equals(NullValueImpl.getInstance())) {
                /*
                 * This field is not there in the row and hence will not be
                 * there in the ES document for this row.
                 */
                break;
            }
            final MapValue mValue = row.get(curStep).asMap();
            final Map<String, FieldValue> mFields = mValue.getFields();

            if (indexField.isMapKeys()) {

                /* Index all the keys from the map. */
                for (String key : mFields.keySet()) {
                    emitter.putArrayString(key);
                }

            } else if (indexField.isMapValues()) {

                /* Index all values from the map. */
                for (Entry<String, FieldValue> entry : mFields.entrySet()) {
                    emitter.putArrayValue(entry.getValue());
                }

            } else {

                /*
                 * Index the single map value for the specified key. To do
                 * this, step over the '.' in "mapF.key', so the new curStep
                 * is the key to use to retrieve the value to index in ES.
                 */
                curStep = indexField.getStep(stepIdx++);
                emitter.putValue(mFields.get(curStep));
            }
            break;

        case JSON:

            /* Capture the components (nodes) making up the path. */
            final String[] nodeNamesArray =
                               indexField.toString().split("\\s*\\.\\s*");

            /*
             * Currently, JSON data is stored in a kvstore table under a
             * single column name (the rootNodeName above) as a single
             * JSON document. Based on how JSON data is currently stored
             * in the table, the nodeNames should always contain at least
             * 2 elements: the rootNodeName and at least one child node
             * (ex. root.a, root.b.c, root.d.e.f, etc.)
             */
            if (nodeNamesArray == null || nodeNamesArray.length <= 1) {
                throw new IllegalStateException(
                    "Invalid IndexField [" + indexField.toString() + "]: " +
                    "the IndexField must consist of at lease 2 nodes " +
                    "(parent node plus child nodes)");
            }

            final ArrayList<String> nodeNames = new ArrayList<>();
            for (int i = 0; i < nodeNamesArray.length; i++) {
                nodeNames.add(nodeNamesArray[i]);
            }

            if (inLogger.isLoggable(Level.FINEST)) {
                inLogger.finest(" ");
                for (int i = 0; i < nodeNamesArray.length; i++) {
                    inLogger.finest("ElasticsearchHandler.addValue: " +
                        "nodeNames[" + i + "] = " + nodeNamesArray[i]);
                }
            }

            /*
             * The first node in the array is the child of the parent node;
             * where successive nodes are the grandchild, great-grandchild,
             * etc. of the parent node. So the parent node should always be
             * specified to ElasticSearch in the emitter as a JSON object
             * structure. And the last node in the array should be a scalar.
             */

            final String curParentNodeName = nodeNames.remove(0);
            emitter.putJsonObject(curParentNodeName); /* Parent is an object */
            buildJsonDocumentForEsUtil(row.get(rootNodeName).asMap().getFields(),
                emitter, inLogger);

            emitter.endJsonObject();

            if (inLogger.isLoggable(Level.FINEST)) {
                inLogger.finest(" ");
                inLogger.finest("----- DOCUMENT SUB-TOTAL (addValue) ----");
                inLogger.finest(mappingSpecStr(document.jsonGenarator()));
                inLogger.finest("----------------------------------------");
                inLogger.finest(" ");
            }
            break;

        default:

            throw new IllegalStateException
                ("Unexpected type in ElasticsearchHandler.addValue\n" + fdef);
        }
        emitter.end();
        return emitter.emitted();
    }

    /*
     * DocEmitter is a helper class for writing fields into an XContentBuilder.
     * It delays writing the field name, so that if it is discovered that there
     * is no content of interest, it can avoid writing anything at all.
     */
    private static class DocEmitter {

        private final String fieldName;
        private final ESJsonBuilder document;
        private boolean emitted;
        private boolean emittingArray;

        DocEmitter(String fieldName, ESJsonBuilder document) {
            this.fieldName = fieldName;
            this.document = document;
            this.emitted = false;
            this.emittingArray = false;
        }

        private void startEmittingMaybe() throws IOException {
            if (!emitted) {
                document.field(fieldName);
                emitted = true;
            }
        }

        private void startEmittingArrayMaybe() throws IOException {
            startEmittingMaybe();
            if (!emittingArray) {
                document.startArray();
                emittingArray = true;
            }
        }

        void putString(String val) throws IOException {
            if (val == null) {
                return;
            }
            startEmittingMaybe();
            document.value(val);
        }

        void putValue(FieldValue val) throws IOException {
            if (val == null || val.isNull()) {
                return;
            }
            if (val.isTimestamp()) {
                putTimestamp(val.asTimestamp());
            } else {
                putString(val.toString());
            }
        }

        private void putTimestamp(TimestampValue tsv) throws IOException {
            final String ts = tsv.toString();
            if (ts != null && !ts.isEmpty() && ts.endsWith(UTC_SUFFIX)) {
                /* remove the timezone char */
                putString(ts.substring(0, ts.length() - 1));
                return;
            }
            putString(ts);
        }

        void putArrayString(String val) throws IOException {
            if (val == null || "".equals(val)) {
                return;
            }
            startEmittingArrayMaybe();
            putString(val);
        }

        void putArrayValue(FieldValue val) throws IOException {
            if (val == null || val.isNull()) {
                return;
            }
            startEmittingArrayMaybe();
            if (val.isTimestamp()) {
                putTimestamp(val.asTimestamp());
            } else {
                putString(val.toString());
            }
        }

        void end() throws IOException {
            if (emittingArray) {
                document.endArray();
                emittingArray = false;
            }
        }

        boolean emitted() {
            return emitted;
        }

        /*
         * Methods used when building a document from content consisting
         * of complex JSON; that is, JSON consisting of nested objects,
         * arrays, and scalars, not just scalar-based JSON for which the
         * methods above are used.
         */

        void putJsonObject(final String jsonObjectName) throws IOException {
            if (jsonObjectName == null) {
                return;
            }
            document.startStructure(jsonObjectName);
            emitted = true;
        }

        void putJsonScalar(final String fName, final FieldValue fValue)
            throws IOException {

            if (fName == null) {
                return;
            }

            if (fValue == null) {
                document.nullField(fName);
                return;
            }

            switch (fValue.getType()) {

                case STRING:
                    document.field(fName, fValue.asString().get());
                    break;
                case INTEGER:
                    document.field(fName, fValue.asInteger().get());
                    break;
                case LONG:
                    document.field(fName, fValue.asLong().get());
                    break;
                case FLOAT:
                    document.field(fName, fValue.asFloat().get());
                    break;
                case DOUBLE:
                    document.field(fName, fValue.asDouble().get());
                    break;
                case NUMBER:
                    document.field(fName, fValue.asNumber().get());
                    break;
                case BOOLEAN:
                    final String valStr =
                        (fValue.asBoolean().get() ? "true" : "false");
                    document.field(fName, valStr);
                    break;
                case TIMESTAMP:
                    document.field(
                        fName, fValue.asTimestamp().get().toString());
                    break;
                case ANY_JSON_ATOMIC:
                    assert (fValue.isJsonNull());
                    document.nullField(fName);
                    break;
                case JSON:
                    if (fValue.isJsonNull()) {
                        document.nullField(fName);
                    } else {
                        document.field(
                            fName, fValue.toJsonString(false));
                    }
                    break;
                default:
                    throw new IllegalStateException
                        ("DocEmitter.putJsonScalar: unexpected type [" +
                         fValue.getType() + "]");
            }
            emitted = true;
        }

        void endJsonObject() throws IOException {
            document.endStructure();
        }

        void startJsonArray(final String jsonArrayName) throws IOException {
            document.field(jsonArrayName);
            document.startArray();
            emitted = true;
        }

        void putJsonArrayValue(final FieldValue fieldValue)
            throws IOException {

            if (fieldValue == null) {
                document.nullField(fieldName);
                return;
            }

            switch (fieldValue.getType()) {

                case STRING:
                    document.field(null, fieldValue.asString().get());
                    break;
                case INTEGER:
                    document.field(fieldValue.asInteger().get());
                    break;
                case LONG:
                    document.field(fieldValue.asLong().get());
                    break;
                case FLOAT:
                    document.field(fieldValue.asFloat().get());
                    break;
                case DOUBLE:
                    document.field(fieldValue.asDouble().get());
                    break;
                case NUMBER:
                    document.field(fieldValue.asNumber().get());
                    break;
                case BOOLEAN:
                    final String valStr =
                        (fieldValue.asBoolean().get() ? "true" : "false");
                    document.field(null, valStr);
                    break;
                case TIMESTAMP:
                    document.field(
                        null, fieldValue.asTimestamp().get().toString());
                    break;

                default:
                    throw new IllegalStateException
                        ("DocEmitter.putScalar: unexpected type [" +
                         fieldValue.getType() + "]");
            }
            emitted = true;
        }

        void endJsonArray() throws IOException {
            document.endArray();
        }
    }

    /*
     * Returns a delete operation containing a JSON document suitable for
     * indexing at an ES index, based on the given RowImpl.
     *
     * @param esIndexName  name of es index to which the delete operation is
     *                     created
     * @param row          row from which to create a delete operation
     *
     * @return  a delete operation to an es index
     */
    static IndexOperation makeDeleteOperation(IndexImpl index,
                                              String esIndexName,
                                              RowImpl row) {

        final Table table = index.getTable();
        assert table == row.getTable();

        /* The encoded string form of the row's primary key. */
        final String pkPath =
            TableKey.createKey(table, row, false).getKey().toString();

        return new IndexOperation(esIndexName,
                                  pkPath,
                                  null,
                                  IndexOperation.OperationType.DEL);
    }

    /*
     * Provides a default translation between NoSQL types and ES types.
     *
     * The TIMESTAMP type is translated to "date<precision>" e.g. "date3" for
     * TIMESTAMP(3)
     *
     * @param fdef field definition in NoSQL DB
     *
     * @return ES type translated from field type
     */
    static String defaultESTypeFor(FieldDef fdef) {
        final FieldDef.Type t = fdef.getType();
        switch (t) {
            case STRING:
            case INTEGER:
            case LONG:
            case BOOLEAN:
            case FLOAT:
            case DOUBLE:
                return t.toString().toLowerCase();
            case TIMESTAMP:
                return "date" + fdef.asTimestamp().getPrecision();
            case ARRAY:
            case BINARY:
            case FIXED_BINARY:
            case MAP:
            case RECORD:
            case ENUM:
            default:
                throw new IllegalStateException
                    ("Unexpected default type mapping requested for " + t);
        }
    }

    /*
     * Returns true if f represents a retriable failure.  Some ES errors
     * indicate that a there is a problem with the document that was sent to
     * ES.  Such documents will never succeed in being indexed and so should
     * not be retried. The status code is intended for REST request statuses,
     * and not all of the possible values are relevant to the bulk request.  I
     * have chosen to list all possible values in the switch statement anyway;
     * any that seem irrelevant are simply relegated to the "not retriable"
     * category.
     *
     * @param f  A Failure object from a BulkItemResponse.
     *
     * @return   Boolean indication of whether the failure should be re-tried.
     */
    static boolean isRetriable(RestStatus status) {
        switch (status) {
        case BAD_GATEWAY:
        case CONFLICT:
        case GATEWAY_TIMEOUT:
        case INSUFFICIENT_STORAGE:
        case INTERNAL_SERVER_ERROR:
        case TOO_MANY_REQUESTS: /*
                                 * Returned if we try to use a client node that
                                 * has been shut down; which would be a bug
                                 */
        case SERVICE_UNAVAILABLE: /*
                                   * Returned if the shard has insufficient
                                   * replicas - this is the significant one
                                   */

            return true;

        case ACCEPTED:
        case BAD_REQUEST:
        case CONTINUE:
        case CREATED:
        case EXPECTATION_FAILED:
        case FAILED_DEPENDENCY:
        case FOUND:
        case FORBIDDEN:
        case GONE:
        case HTTP_VERSION_NOT_SUPPORTED:
        case LENGTH_REQUIRED:
        case LOCKED:
        case METHOD_NOT_ALLOWED:
        case MOVED_PERMANENTLY:
        case MULTIPLE_CHOICES:
        case MULTI_STATUS:
        case NON_AUTHORITATIVE_INFORMATION:
        case NOT_ACCEPTABLE:
        case NOT_FOUND:
        case NOT_IMPLEMENTED:
        case NOT_MODIFIED:
        case NO_CONTENT:
        case OK:
        case PARTIAL_CONTENT:
        case PAYMENT_REQUIRED:
        case PRECONDITION_FAILED:
        case PROXY_AUTHENTICATION:
        case REQUESTED_RANGE_NOT_SATISFIED:
        case REQUEST_ENTITY_TOO_LARGE:
        case REQUEST_TIMEOUT:
        case REQUEST_URI_TOO_LONG:
        case RESET_CONTENT:
        case SEE_OTHER:
        case SWITCHING_PROTOCOLS:
        case TEMPORARY_REDIRECT:
        case UNAUTHORIZED:
        case UNPROCESSABLE_ENTITY:
        case UNSUPPORTED_MEDIA_TYPE:
        case USE_PROXY:
        default:

            return false;
        }
    }

    /* Convenience Static Utility Methods */

    /*
     * For use in the context of KV AdminService
     */
    /**
     * For use in the context of KV AdminService
     *
     * The parameter secure can be checked by the StorageNode Parameter
     * ES_CLUSTER_SECURE.
     *
     * @param clusterName - ES Cluster name.
     * @param esMembers - hostport pair of registered ES Node.
     * @param secure - true means ES Cluster is available on https.(TLS)
     * @param admin - The admin instance
     * @return - ESRestClient
     * @throws IOException - Exception thrown if Client could not be created.
     */
    public static ESRestClient createESRestClient(String clusterName,
                                                  String esMembers,
                                                  boolean secure,
                                                  Admin admin)
        throws IOException {
        ESHttpClient baseHttpClient = null;
        if (!secure) {
            baseHttpClient = createESHttpClient(clusterName, esMembers,
                                                admin.getLogger());
        } else {
            baseHttpClient = createESHttpClient(clusterName, esMembers,
                                                admin.getParams()
                                                     .getSecurityParams(),
                                                admin.getLogger());
        }
        return new ESRestClient(baseHttpClient, admin.getLogger());
    }

    /*
     * Non secure ES Client.
     */
    public static ESRestClient createESRestClient(String clusterName,
                                                  String esMembers,
                                                  Logger inputLogger)
        throws IOException {
        ESHttpClient baseHttpClient = null;
        baseHttpClient =
            createESHttpClient(clusterName, esMembers, inputLogger);
        return new ESRestClient(baseHttpClient, inputLogger);
    }

    /*
     * The caller has to make sure that ES Cluster is set up in a secure
     * fashion.
     *
     * This method does not check that because register-es plan makes sure that
     * if KVStore is running in secured mode, that ES Cluster has to be
     * registered for HTTPS.
     *
     * Whether ESCluster is secured or not, can be checked by StorageNode
     * Parameter, SEARCH_CLUSTER_SECURE.
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  String esMembers,
                                                  SecurityParams secParams,
                                                  Logger inputLogger)
        throws IOException {
        if (ESRestClientUtil.isEmpty(esMembers)) {
            throw new IllegalArgumentException();
        }
        final HostPort[] hps = HostPort.parse(esMembers.split(","));
        return createESHttpClient(clusterName, hps, secParams, inputLogger);
    }

    /*
     * The caller has to make sure that ES Cluster is set up in a secure
     * fashion.
     *
     * This method does not check that because register-es plan makes sure that
     * if KVStore is running in secured mode, that ES Cluster has to be
     * registered for HTTPS.
     *
     * Whether ESCluster is secured or not, can be checked by StorageNode
     * Parameter, SEARCH_CLUSTER_SECURE.
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort hostPort,
                                                  SecurityParams secParams,
                                                  Logger inputLogger)
        throws IOException {

        final HostPort[] hostPorts = new HostPort[1];
        hostPorts[0] = hostPort;

        return createESHttpClient(
                   clusterName, hostPorts, secParams, inputLogger);

    }

    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  SecurityParams secParams,
                                                  Logger inputLogger)
        throws IOException {

        return createESHttpClient(clusterName, hostPorts, secParams,
                                  inputLogger, maxRetryTimeoutMillis);
    }

    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  SecurityParams secParams,
                                                  Logger inputLogger,
                                                  int retryTimeout)
        throws IOException {

        Logger clientLogger = inputLogger;
        if (clientLogger == null) {
            clientLogger = logger;
        }

        if (secParams == null || !secParams.isSecure()) {
            return createESHttpClient(clusterName, hostPorts, clientLogger);
        }

        final AtomicReference<char[]> ksPwdAR = new AtomicReference<char[]>();

        SSLContext sslContext = null;
        try {
            sslContext = TIFSSLContext.makeSSLContext(
                             secParams, ksPwdAR, clientLogger);

            /*
             * No need to check connections as that is done during
             * register-es-plan before setting the SN parameters.
             *
             * For now, using same logger for low level httpclient and higher
             * level clients.
             */
            final ESHttpClient client =
                createESHttpClient(hostPorts, true,
                                   sslContext, retryTimeout, clientLogger);
            if (verifyClusterName(clusterName, client)) {
                return client;
            }

            throw new IOException("ClusterName does not match on ES Cluster");

        } catch (SSLContextException e) {
            throw new IOException(e);
        } finally {
            if (ksPwdAR.get() != null) {
                Arrays.fill(ksPwdAR.get(), ' ');
            }
        }
    }

    /*
     * Currently setting up ES Client does not check if the given ES Node
     * Members is hosting the cluster given by the clusterName parameter.
     */
    private static ESHttpClient createESHttpClient(String clusterName,
                                                   String esMembers,
                                                   Logger inputLogger)
        throws IOException {
        if (ESRestClientUtil.isEmpty(esMembers)) {
            throw new IllegalArgumentException();
        }

        final HostPort[] hps = HostPort.parse(esMembers.split(","));

        return createESHttpClient(clusterName, hps, inputLogger);

    }

    private static ESHttpClient createESHttpClient(String clusterName,
                                                   HostPort[] hps,
                                                   Logger inputLogger)
        throws IOException {
        if (hps == null || hps.length == 0) {
            throw new IllegalArgumentException();
        }

        final ESHttpClient client = createESHttpClient(hps, false, null,
                                                       maxRetryTimeoutMillis,
                                                       inputLogger);
        if (verifyClusterName(clusterName, client)) {
            return client;
        }

        throw new IOException("ClusterName does not match on ES Cluster");

    }

    /*
     * FOR TEST PURPOSES ONLY - SSLContext Creation is private to this class. *
     */
    public static ESHttpClient createESHttpClient(String clusterName,
                                                  HostPort[] hostPorts,
                                                  boolean secure,
                                                  SSLContext sslContext,
                                                  int retryTimeout,
                                                  Logger inputLogger)
        throws IOException {

        final ESHttpClient client =
            createESHttpClient(hostPorts, secure,
                               sslContext, retryTimeout, inputLogger);

        if (verifyClusterName(clusterName, client)) {
            return client;
        }

        throw new IOException("ClusterName does not match on ES Cluster");

    }

    /**
     *
     * @param hostPorts  ES Node HostPorts.
     * @param secure  A boolean variable coming from the user end.
     * @param sslContext  An SSLContext containing keystore info. Note that
     * the keystore password is filled with null after the method is done. if
     * secure is true, needs SSLContext. Caller method should create this in
     * case, user configures a secure connection.
     * @param retryTimeout  timeout before retries give up.
     * @return  ESHttpClient instance.
     */

    private static ESHttpClient createESHttpClient(
                                                   HostPort[] hostPorts,
                                                   boolean secure,
                                                   SSLContext sslContext,
                                                   int retryTimeout,
                                                   Logger inputLogger) {
        if (hostPorts == null || hostPorts.length == 0) {
            throw new IllegalArgumentException("hostPorts is required");
        }

        ESHttpClient baseHttpClient = null;
        final HttpHost[] httpHosts = new HttpHost[hostPorts.length];
        int i = 0;
        for (HostPort hostPort : hostPorts) {
            httpHosts[i++] = new HttpHost(secure? "https" : "http",
                                          hostPort.hostname(), 
                                          hostPort.port());
        }
        final ESHttpClientBuilder builder =
            new ESHttpClientBuilder(httpHosts).setMaxRetryTimeoutMillis
                                               (retryTimeout)
                                              .setLogger(inputLogger);

        if (secure) {
            if (sslContext == null) {
                throw new IllegalArgumentException();
            }
            builder.setSecurityConfigCallback(new SecurityConfigCallback() {

                @Override
                public HttpClient.Builder addSecurityConfig
                                              (HttpClient.Builder
                                               httpClientBuilder) {
                    return httpClientBuilder.sslContext(sslContext);
                }

            });
        }
        baseHttpClient = builder.build();
        return baseHttpClient;
    }

    public static boolean verifyClusterName(String clusterName,
                                            ESHttpClient httpClient)
        throws IOException {
        if (ESRestClientUtil.isEmpty(clusterName)) {
            return false;
        }
        JsonParser parser = null;
        try {
            String clusterNameFromResp;
            final RestResponse resp =
                httpClient.executeSync(ESHttpMethods.HttpGet, "");
            parser = ESRestClientUtil.initParser(resp);
            JsonToken token;
            String currentFieldName;
            while ((token = parser.nextToken()) != JsonToken.END_OBJECT) {

                if (token.isScalarValue()) {
                    currentFieldName = parser.currentName();
                    if ("cluster_name".equals(currentFieldName)) {
                        clusterNameFromResp = parser.getText();
                        if (clusterName.equals(clusterNameFromResp)) {
                            return true;
                        }
                    }

                }
            }

        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        return false;

    }

    /** Convenience method used for debug output. */
    static String mappingSpecStr(final JsonGenerator jsonGenerator)
        throws IOException, UnsupportedEncodingException {
        if (jsonGenerator == null) {
            return null;
        }
        jsonGenerator.flush();
        return new String(((ByteArrayOutputStream) jsonGenerator.
                              getOutputTarget()).toByteArray(), "UTF-8");
    }

    private static boolean isScalar(final JsonPathNode node) {

        final String nodeName = node.getName();
        final String annotation = node.getAnnotation();
        final List<JsonPathNode> children = node.getChildren();

        if (annotation == null && children == null) {
            throw new IllegalStateException("annotation and children both " +
                                            "null [node=" + nodeName + "]");
        }

        if (annotation != null && children != null) {
            throw new IllegalStateException("annotation and children " +
                                            "non-null [node=" + nodeName +
                                            ", annotation=" + annotation +
                                            ", children=" + children + "]");
        }

        if (annotation != null) {
            return true;
        }
        return false;
    }

    private static boolean isScalar(final FieldValue fieldValue) {
        if (fieldValue == null) {
            return true;
        }

        switch (fieldValue.getType()) {
            case STRING:
            case INTEGER:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case NUMBER:
            case BOOLEAN:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
        }

    private static void buildJsonDocumentForEsUtil(
                            final Map<String, FieldValue> fieldMap,
                            final DocEmitter emitter,
                            final Logger inLogger) throws IOException {
        if (fieldMap == null || fieldMap.size() == 0 || emitter == null) {
            return;
        }

        for (Map.Entry<String, FieldValue> entry : fieldMap.entrySet()) {
             buildJsonDocumentForEs(entry.getKey(), entry.getValue(),
                                    emitter, inLogger);
        }
    }



    private static void buildJsonDocumentForEs(final String fieldName, 
                            final FieldValue fieldVal, final DocEmitter emitter,
                            final Logger inLogger) throws IOException {

        if (inLogger.isLoggable(Level.FINEST)) {
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldName = " + fieldName +
                          ", fieldVal = " + fieldVal);
        }

        if (fieldVal == null) {
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldVal = null. Done building document.");
            /* Reached end of path. Previous invocation was scalar. Done. */
            return;
        }

        if (inLogger.isLoggable(Level.FINEST)) {
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldVal isComplex    = " + fieldVal.isComplex());
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldVal isMap        = " + fieldVal.isMap());
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldVal type         = " + fieldVal.getType());
            inLogger.finest("ElasticsearchHandler.buildJsonDocumentForEs: " +
                          "fieldVal.isJsonNull() = " + fieldVal.isJsonNull());
        }

        if (isScalar(fieldVal) || fieldVal.isJsonNull()) {
            emitter.putJsonScalar(fieldName, fieldVal);
        } else {

            /*
             * If fieldVal is not scalar, then it is emitted as a JSON object
             * structure; after which its children are handled. And under
             * normal conditons, it will be a MapValue or an ArrayValue.
             * If it is a MapValue, it is treated as a JSON nested object,
             * where the elements of the map are walked/recursed. On the
             * other hand, if the fieldVal is an ArrayValue, then all the
             * elements of the array are written to the document in a loop
             * rather than recursing.
             */
            if (fieldVal.isMap()) {
                emitter.putJsonObject(fieldName);
                buildJsonDocumentForEsUtil(
                  fieldVal.asMap().getFields(), emitter, inLogger);
                emitter.endJsonObject();
            } else if (fieldVal.isArray()) {

                final ArrayValue arrayVal = fieldVal.asArray();

                /* Elements must be scalar, not complex. */
                for (FieldValue arrayElement : arrayVal.toList()) {

                    final FieldDef.Type elementType = arrayElement.getType();
                    switch (elementType) {

                        case STRING:
                        case INTEGER:
                        case LONG:
                        case FLOAT:
                        case DOUBLE:
                        case NUMBER:
                        case BOOLEAN:
                        case TIMESTAMP:
                            continue;
                        default:
                            throw new IllegalStateException
                                ("ElasticsearchHandler.addValue: unexpected " +
                                 "type in JSON field [" + fieldName + "] - " +
                                 "field is array type, but at least 1 " +
                                 "element in the array is non-scalar " +
                                 "[type=" + elementType + "]. All elements " +
                                 "of the array must be scalar.");
                    }
                }
                emitter.startJsonArray(fieldName);
                for (FieldValue arrayElement : arrayVal.toList()) {
                    emitter.putJsonArrayValue(arrayElement);
                }
                emitter.endJsonArray();

            } else {

                throw new IllegalStateException
                    ("ElasticsearchHandler.addValue: unexpected type in " +
                     "JSON field [" + fieldName + ":" +
                     fieldVal.getType() + "]. " +
                     "Must be either scalar, map, or array type");
            }
        }
    }

    private static void buildMappingFromNodeMap(
                       final Map<String, List<JsonPathNode>> nodeMap,
                       final ESJsonBuilder jsonBuilder) throws IOException {

        if (nodeMap == null || nodeMap.size() == 0 || jsonBuilder == null) {
            return;
        }

        final JsonGenerator jsonGen = jsonBuilder.jsonGenarator();

        for (Map.Entry<String, List<JsonPathNode>> entry :
                                                       nodeMap.entrySet()) {

            final String outerName = entry.getKey();
            final Map<String, List<JsonPathNode>> childNodeMap =
                                                      new HashMap<>();

            for (JsonPathNode primaryNode : entry.getValue()) {

                if (primaryNode == null) {
                    throw new IllegalStateException("For given outer object " +
                        "[" + outerName + "]: null primary node in list");
                }

                final String primaryNodeName = primaryNode.getName();

                if (!outerName.equals(primaryNodeName)) {
                    throw new IllegalStateException("Outer object name != " +
                        "primary node name [" + outerName + " != " +
                        primaryNodeName + "]");
                }

                if (isScalar(primaryNode)) {

                    writeScalarToMapping(primaryNode, jsonBuilder);
                    continue;
                }

                for (JsonPathNode primaryChildNode :
                         primaryNode.getChildren()) {

                    final String primaryChildNodeName =
                        primaryChildNode.getName();

                    List<JsonPathNode> secondaryChildNodeList =
                        childNodeMap.get(primaryChildNodeName);

                    if (secondaryChildNodeList == null) {
                        secondaryChildNodeList = new ArrayList<>();
                    }
                    secondaryChildNodeList.add(primaryChildNode);

                    childNodeMap.put(
                        primaryChildNodeName, secondaryChildNodeList);
                }
            }

            boolean terminateOuterProperties = false;
            if (childNodeMap != null && childNodeMap.size() > 0) {

                jsonBuilder.startStructure(outerName);
                jsonGen.writeStringField("type", "object");
                jsonBuilder.startStructure("properties");

                terminateOuterProperties = true;
            }

            for (Map.Entry<String, List<JsonPathNode>> mapEntry :
                                                     childNodeMap.entrySet()) {
                final String entryName = mapEntry.getKey();
                final List<JsonPathNode> entryList = mapEntry.getValue();
                final Map<String, List<JsonPathNode>> nextChildMap =
                    new HashMap<>();
                nextChildMap.put(entryName, entryList);
                buildMappingFromNodeMap(nextChildMap, jsonBuilder);
            }

            if (terminateOuterProperties) {
                jsonGen.writeEndObject();
            }
            jsonGen.writeEndObject();
        }
    }

    private static Map<String, List<JsonPathNode>> getSortedChildMap(
                                   final JsonPathNode parentNode) {

        final List<JsonPathNode> children = parentNode.getChildren();

        if (children == null) {
            return null;
        }

        /*
         * Walk the children of the parentNode, sorting the children with the
         * same node name into separate groups, organized by name.
         */
        final Map<String, List<JsonPathNode>> childMap = new HashMap<>();
        for (JsonPathNode curChildNode : children) {
            final String curChildName = curChildNode.getName();
            List<JsonPathNode> curChildList = childMap.get(curChildName);
            if (curChildList == null) {
                curChildList = new ArrayList<>();
            }
            curChildList.add(curChildNode);
            childMap.put(curChildName, curChildList);
        }
        return childMap;
    }

    private static void writeScalarToMapping(
                       final JsonPathNode jsonPathNode,
                       final ESJsonBuilder esJsonBuilder) throws IOException {

        if (jsonPathNode == null || esJsonBuilder == null) {
            return;
        }

        final JsonParser annotationParser =
            ESJsonUtil.createParser(jsonPathNode.getAnnotation());
        final Map<String, Object> annotationMap =
           ESJsonUtil.parseAsMap(annotationParser);

        esJsonBuilder.startStructure(jsonPathNode.getName());
        for (Map.Entry<String, Object> annotationEntry :
                annotationMap.entrySet()) {

            final String annotationKey = annotationEntry.getKey();
            String annotationVal = annotationEntry.getValue().toString();

            /*
             * Elasticsearch version 5.0 or greater does not know about
             * type "string". It only knows about type "text" (for full
             * text search) and type "keyword" (for whole value keyword
             * search that must be mapped as 'not_analyzed'. For versions
             * prior to 5.0, Elasticsearch only knows about type "string".
             *
             * Change the annotation associated with the given path node
             * to the appropriate type ("string" or "text"), based on the
             * current version of Elasticsearch. That is,
             *
             * If ES version is 5 or greater and the annotation contains
             * "type":"string", then change to "type":"text".
             *
             * If ES version is less than 5 and the annotation contains
             * "type":"text" or "type":"keyword", then change to
             * "type":"string".
             */
            if ("type".equals(annotationKey)) {
                if (esVersion.compareTo("5") > 0) { /* Version >= 5.x */
                    if ("string".equals(annotationVal)) {
                        annotationVal = "text";
                    }
                } else { /* Pre-5.x version */
                    if ("text".equals(annotationVal) ||
                        "keyword".equals(annotationVal)) {
                        annotationVal = "string";
                    }
                }
            }
            esJsonBuilder.field(annotationKey, annotationVal);
        }
    }

    private static class JsonPathNode {

        private final String name;
        private String annotation;
        private List<JsonPathNode> children;

        JsonPathNode(String nodeName) {
            this.name = nodeName;
        }

        String getName() {
            return name;
        }

        String getAnnotation() {
            return annotation;
        }

        void setAnnotation(final String newAnnotation,
                           final Map<String, Object> propertiesMap,
                           final String elasticsearchVersion) {

            if (propertiesMap == null) {
                annotation = newAnnotation;
                return;
            }

            final Object typeObj = propertiesMap.get("type");
            if (typeObj == null) {
                annotation = newAnnotation;
                return;
            }
            final String type = typeObj.toString();

            /* Handle date type below. Handle other types here. */
            if (!("date").equals(type)) {

                if (EMPTY_ANNOTATION.equals(newAnnotation)) {
                    annotation =
                        "{" + "\"type\":\"" + type + "\"" + "}";
                    return;
                }
                annotation = newAnnotation;
                return;
            }

            /* Type is 'date'; which requires special handling. */

            boolean isMillis = false;
            boolean isNanos = false;

            final Object precisionObj = propertiesMap.get("precision");
            if (precisionObj != null) {
                final String precision = precisionObj.toString();

                if ("millis".equals(precision)) {
                    isMillis = true;
                } else if ("nanos".equals(precision)) {
                    isNanos = true;
                }
            }

            if (isMillis) {
                annotation = TIMESTAMP_TO_ES_DATE_MILLIS;
                return;
            }

            if (elasticsearchVersion.compareTo("5") > 0) {
                if (isNanos) {
                    annotation = TIMESTAMP_TO_ES5_DATE_NANOS;
                    return;
                }
                /* No precision specified. Use default. */
                annotation = TIMESTAMP_TO_ES5_DATE_DEFAULT;
            } else {
                if (isNanos) {
                    annotation = TIMESTAMP_TO_ES2_DATE_NANOS;
                    return;
                }
                /* No precision specified. Use default. */
                annotation = TIMESTAMP_TO_ES2_DATE_DEFAULT;
            }
            return;
        }

        List<JsonPathNode> getChildren() {
            return children;
        }

        void addChild(final JsonPathNode child) {
            if (children == null) {
                children = new ArrayList<>();
            }
            children.add(child);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof JsonPathNode)) {
                return false;
            }
            if (obj == this) {
                return true;
            }

            final JsonPathNode obj1 = this;
            final JsonPathNode obj2 = (JsonPathNode) obj;

            if (obj1.name == null || obj2.name == null) {
                return false;
            }
            if (!(obj1.name).equals(obj2.name)) {
                return false;
            }

            if (obj1.annotation == null && obj2.annotation != null) {
                return false;
            }
            if (obj1.annotation != null &&
                    !(obj1.annotation).equals(obj2.annotation)) {
                return false;
            }

            if (obj1.children == null && obj2.children != null) {
                return false;
            }
            if (obj1.children != null &&
                    !(obj1.children).equals(obj2.children)) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {

            final int pm = 37;
            int hc = 11;
            int hcSum = 0;

            if (name != null) {
                hcSum = hcSum + name.hashCode();
            }
            if (annotation != null) {
                hcSum = hcSum + annotation.hashCode();
            }
            if (children != null) {
                hcSum = hcSum + children.hashCode();
            }

            hc = (pm * hc) + hcSum;
            return hc;
        }

        @Override
        public String toString() {
            final StringBuilder buf = new StringBuilder();
            buf.append("[");
            buf.append("name=");
            buf.append(name);
            buf.append(", ");
            buf.append("annotation=");
            buf.append(annotation);
            buf.append(", ");
            buf.append("children=");
            buf.append(children);
            buf.append("]");
            return buf.toString();
        }
    }
}
