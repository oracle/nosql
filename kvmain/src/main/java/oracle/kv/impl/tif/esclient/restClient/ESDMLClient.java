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
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esRequest.BulkRequest;
import oracle.kv.impl.tif.esclient.esRequest.DeleteRequest;
import oracle.kv.impl.tif.esclient.esRequest.GetRequest;
import oracle.kv.impl.tif.esclient.esRequest.IndexDocumentRequest;
import oracle.kv.impl.tif.esclient.esRequest.RefreshRequest;
import oracle.kv.impl.tif.esclient.esRequest.SearchRequest;
import oracle.kv.impl.tif.esclient.esResponse.BulkResponse;
import oracle.kv.impl.tif.esclient.esResponse.DeleteResponse;
import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexDocumentResponse;
import oracle.kv.impl.tif.esclient.esResponse.RefreshResponse;
import oracle.kv.impl.tif.esclient.esResponse.SearchResponse;

public class ESDMLClient {

    private final Logger logger;

    private final ESRestClient esRestClient;

    ESDMLClient(ESRestClient esRestClient, Logger logger) {
        this.esRestClient = esRestClient;
        this.logger = logger;
    }

    public IndexDocumentResponse index(IndexDocumentRequest req)
        throws IOException {
        IndexDocumentResponse resp = new IndexDocumentResponse();
        return esRestClient.executeSync(req, resp);
    }

    public DeleteResponse delete(DeleteRequest req) throws IOException {
        DeleteResponse resp = new DeleteResponse();
        return esRestClient.executeSync(req, resp);
    }

    public GetResponse get(GetRequest req) throws IOException {
        GetResponse resp = new GetResponse();
        try {
            resp = esRestClient.executeSync(req, resp);
        } catch (IOException e) {
            if (e.getCause() instanceof ESException) {
                if (((ESException) e.getCause()).errorStatus()
                                                .equals(RestStatus.NOT_FOUND)) {
                    logger.info("Index " + req.index()
                            + " does not Exist. No Get Result.");
                }
            } else {
                throw e;
            }
        }
        return resp;
    }

    public SearchResponse search(SearchRequest req) throws IOException {
        SearchResponse resp = new SearchResponse();
        try {
            resp = esRestClient.executeSync(req, resp);
        } catch (IOException e) {
            if (e.getCause() instanceof ESException) {
                if (((ESException) e.getCause())
                                    .errorStatus()
                                    .equals(RestStatus
                                            .NOT_FOUND)) {
                    logger.info("Index " + req.index() + " does not Exist." +
                                " No search results.");
                }
            } else {
                throw e;
            }
        }
        return resp;
    }

    public BulkResponse bulk(BulkRequest req) throws IOException {
        BulkResponse resp = new BulkResponse();
        return esRestClient.executeSync(req, resp);
    }

    public boolean refresh(String... indices) throws IOException {
        RefreshRequest req = new RefreshRequest(indices);
        RefreshResponse resp = new RefreshResponse();
        esRestClient.executeSync(req, resp);
        return resp.isSuccess();
    }

}
