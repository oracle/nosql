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

package oracle.kv.impl.tif.esclient.restClient;

import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esRequest.CATRequest;
import oracle.kv.impl.tif.esclient.esRequest.ClusterHealthRequest;
import oracle.kv.impl.tif.esclient.esRequest.CreateIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetIndexRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetIndexRequest.Feature;
import oracle.kv.impl.tif.esclient.esRequest.GetMappingRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.MappingExistRequest;
import oracle.kv.impl.tif.esclient.esRequest.PutMappingRequest;
import oracle.kv.impl.tif.esclient.esRequest.VersionInfoRequest;
import oracle.kv.impl.tif.esclient.esResponse.CATResponse;
import oracle.kv.impl.tif.esclient.esResponse.ClusterHealthResponse;
import oracle.kv.impl.tif.esclient.esResponse.CreateIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetMappingResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexAlreadyExistsException;
import oracle.kv.impl.tif.esclient.esResponse.IndexExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.MappingExistResponse;
import oracle.kv.impl.tif.esclient.esResponse.PutMappingResponse;
import oracle.kv.impl.tif.esclient.esResponse.VersionInfoResponse;

public class ESAdminClient {

    private final Logger logger;
    private final ESRestClient esRestClient;

    private String esVersion = null;
    private String luceneVersion = null;
    private String clusterName = null;

    ESAdminClient(ESRestClient esRestClient, Logger logger) {
        this.esRestClient = esRestClient;
        this.logger = logger;
        try {
            final VersionInfoResponse resp = getESVersionInfo
                                                 (new VersionInfoRequest());
            esVersion = resp.version();
            luceneVersion = resp.luceneVersion();
            clusterName = resp.clusterName();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "ES Admin Client failed to instantiate.",
                       e);

        }
    }

    public CreateIndexResponse createIndex(CreateIndexRequest req)
        throws IOException,
        IndexAlreadyExistsException {
        CreateIndexResponse resp = new CreateIndexResponse();

        if (logger.isLoggable(Level.FINE)) {
            logger.fine(" ");
            logger.fine("ESAdminClient.createIndex: elasticsearch request " +
                        "['create index' " + req.index() + "]");
            logger.fine(" ");
        }

        resp = esRestClient.executeSync(req, resp);

        if (logger.isLoggable(Level.FINE)) {
            logger.fine("ESAdminClient.createIndex: elasticsearch response " +
                        "[index=" + req.index() +
                        ", acknowledged=" + resp.isAcknowledged() +
                        ", already-exists=" + resp.alreadyExists() +
                        ", error=" + resp.isError() + "]");
            logger.fine(" ");
        }

        if (resp.alreadyExists()) {
            throw new IndexAlreadyExistsException("Index:" + req.index() +
                                                  " already exists");
        }
        return resp;
    }

    public DeleteIndexResponse deleteIndex(DeleteIndexRequest req)
        throws IOException {
        final DeleteIndexResponse resp = new DeleteIndexResponse();
        return esRestClient.executeSync(req, resp);
    }

    public PutMappingResponse createMapping(PutMappingRequest req)
        throws IOException {
        final PutMappingResponse resp = new PutMappingResponse();
        return esRestClient.executeSync(req, resp);
    }

    /**
     * If mapping exists it gets the mapping response as a string.
     *
     * One of the usage of this function could be at places, where
     * existsMapping API is issued first to first check if mapping exists, if
     * yes then GetMapping is issued.
     *
     * In case, the checks are only ensuring mapping exists, following pattern
     * would be better as it saves on network I/O.
     *
     * GetMappingResponse response = adminClient.getMapping(req); if
     * (response.isFound()) String mappingSpec = response.mapping();
     *
     * @param req - A GetMappingRequest
     * @return A GetMappingResponse containing the mapping if it exists.
     * @throws IOException
     */
    public GetMappingResponse getMapping(GetMappingRequest req)
        throws IOException {
        final GetMappingResponse resp = new GetMappingResponse();
        return esRestClient.executeSync(req, resp);
    }

    public MappingExistResponse mappingExists(MappingExistRequest req)
        throws IOException {
        final MappingExistResponse resp = new MappingExistResponse();
        return esRestClient.executeSync(req, resp);
    }

    public IndexExistResponse indexExists(IndexExistRequest req)
        throws IOException {
        final IndexExistResponse resp = new IndexExistResponse();
        return esRestClient.executeSync(req, resp);
    }

    /*
     * Note that from ES 5.5 GetIndexRequest can only take one feature name.
     * Comma separated feature names are deprecated. Hence two seperate APIs
     * for feature names.
     *
     * Mapping is not available currently. For Mappings use GetMappingRequest.
     */

    public GetIndexResponse getIndexSettings(GetIndexRequest req)
        throws IOException {
        final GetIndexResponse resp = new GetIndexResponse();
        return esRestClient.executeSync(req.featureName(Feature.SETTINGS),
                                        resp);
    }

    public ClusterHealthResponse clusterHealth(ClusterHealthRequest req)
        throws IOException {
        final ClusterHealthResponse resp = new ClusterHealthResponse();
        return esRestClient.executeSync(req, resp);
    }

    public CATResponse catInfo(CATRequest.API api,
                               String index,
                               String indexPrefix,
                               String indexSuffix,
                               Map<String, String> params) throws IOException {
        final CATRequest req = new CATRequest(api);
        req.params(params).index(index).indexPrefix(indexPrefix)
           .indexSuffix(indexSuffix);
        final CATResponse resp = new CATResponse();
        return esRestClient.executeSync(req, resp);

    }

    public VersionInfoResponse getESVersionInfo(VersionInfoRequest req)
        throws IOException {
        final VersionInfoResponse resp = new VersionInfoResponse();
        return esRestClient.executeSync(req, resp);
    }

    public String getESVersion() {
        return esVersion;
    }

    public String getLuceneVersion() {
        return luceneVersion;
    }

    public String getClusterName() {
        return clusterName;
    }

}
