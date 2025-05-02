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
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esRequest.ESRequest;
import oracle.kv.impl.tif.esclient.esRequest.ESRequest.RequestType;
import oracle.kv.impl.tif.esclient.esRequest.ESRestRequestGenerator;
import oracle.kv.impl.tif.esclient.esResponse.CreateIndexResponse;
import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.esResponse.ESResponse;
import oracle.kv.impl.tif.esclient.esResponse.InvalidResponseException;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient.FailureListener;
import oracle.kv.impl.tif.esclient.httpClient.HttpHost;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.util.server.LoggerUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;

/**
 * A Rest Client for Elasticsearch which executes REST request and parses the
 * REST Response from ES.
 *
 * Currently FTS works on a synchronous communication with ES. Most of the
 * requests are bulk requests which need to be synchronous to preserve event
 * ordering.
 *
 * This client only provides a synchronous request execution. The monitoring
 * client which works independently provides asynchronous communication with
 * ES.
 *
 */
public class ESRestClient {

    private final Logger logger;
    private final ESHttpClient httpClient;
    private ESAdminClient adminClient;
    private ESDMLClient dmlClient;
    private final ReentrantLock lock = new ReentrantLock();

    public ESRestClient(ESHttpClient httpClient, Logger logger) {
        this.httpClient = httpClient;
        if (logger == null) {
            logger = LoggerUtils.getLogger(
                         ESRestClient.class, "[esRestClient]");
        }
        this.logger = logger;
    }

    /*
     * It is possible that first few admin APIs get executed in different
     * adminClient object. But that is fine.
     */
    public ESAdminClient admin() {
        if (adminClient != null) {
            return adminClient;
        }
        lock.lock();
        try {
            adminClient = new ESAdminClient(this, logger);
        } finally {
            lock.unlock();
        }
        return adminClient;
    }

    public ESDMLClient dml() {
        if (dmlClient != null) {
            return dmlClient;
        }
        lock.lock();
        try {
            dmlClient = new ESDMLClient(this, logger);
        } finally {
            lock.unlock();
        }
        return dmlClient;
    }

    public <R extends ESRequest<R>, S extends ESResponse> S
            executeSync(R req, S resp) throws IOException {

        @SuppressWarnings("unchecked")
        final JsonResponseObjectMapper<S> respMapper =
            (JsonResponseObjectMapper<S>) resp;
        final ESRestRequestGenerator reqGenerator =
            (ESRestRequestGenerator) req;
        return executeSync(req, reqGenerator, respMapper);

    }

    @SuppressWarnings("unchecked")
    public <R extends ESRequest<R>, S extends ESResponse> S executeSync
                                      (R req,
                                       ESRestRequestGenerator reqGenerator,
                                       JsonResponseObjectMapper<S> respMapper)
        throws IOException {

        final RestRequest restReq = reqGenerator.generateRestRequest();
        RestResponse restResp = null;
        JsonParser parser = null;
        S esResponse = null;
        try {
            restResp =
                httpClient.executeSync(restReq.method(), restReq.endpoint(),
                                       restReq.params(), restReq.entity(),
                                       restReq.headers());

            final RequestType reqType = req.requestType();

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest("ESRestClient.executeSync: elasticsearch " +
                            "request type = " + reqType);
            }

            int respStatus = 0;
            switch (reqType) {
                case GET_MAPPING:
                case EXIST_INDEX:
                case MAPPING_EXIST:
                case REFRESH:
                    esResponse = respMapper.buildFromRestResponse(restResp);

                    respStatus =
                       restResp.getStatusCode();
                    esResponse.statusCode(respStatus);

                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest(
                            "ESRestClient.executeSync: " +
                            "elasticsearch response status =  " + respStatus);
                    }
                    break;
                default:
                    parser = ESRestClientUtil.initParser(restResp);
                    esResponse = respMapper.buildFromJson(parser);

                    respStatus =
                       restResp.getStatusCode();
                    esResponse.statusCode(respStatus);

                    if (logger.isLoggable(Level.FINE)) {
                        if (esResponse instanceof CreateIndexResponse) {
                            logger.fine(
                                "ESRestClient.executeSync: " +
                                "elasticsearch response status = " +
                                respStatus);
                            final CreateIndexResponse ciResp =
                                (CreateIndexResponse) esResponse;
                            logger.fine(
                                "ESRestClient.executeSync: " +
                                "elasticsearch CreateIndexResponse " +
                                "acknowledged   = " + ciResp.isAcknowledged());
                            logger.fine(
                                "ESRestClient.executeSync: " +
                                "elasticsearch CreateIndexResponse " +
                                "already-exitss = " + ciResp.alreadyExists());
                            logger.fine(
                                "ESRestClient.executeSync: " +
                                "elasticsearch CreateIndexResponse " +
                                "error          = " + ciResp.isError());
                            logger.fine(" ");
                        }
                    }
                    break;
            }

        } catch (ESException e) {
            /*
             * Logging the exception reads from the HttpEntity.ESException
             * constructor converts it into a repeatable entity,
             */
            final RequestType reqType = req.requestType();
            final RestStatus errStatus = e.errorStatus();

            if (logger.isLoggable(Level.INFO)) {
                logger.info(
                    "ESRestClient.executeAsync: Exception thrown from the " +
                    "low level http client of elasticsearch upon REST " +
                    "request [request = " + restReq +
                    ", requestType = " + reqType +
                    ", errorStatus = " + errStatus +
                    ", exception = " + e + "]");
            }

            if (e.errorStatus() == RestStatus.NOT_FOUND) {
                switch (reqType) {
                    case EXIST_INDEX:
                    case MAPPING_EXIST:
                    case DELETE_INDEX:
                    case DELETE:
                    case CAT:
                    case REFRESH:
                        esResponse = respMapper.buildErrorReponse(e);
                        break;
                    default:
                        throw new IOException(e);

                }
            } else if (e.errorStatus() == RestStatus.BAD_REQUEST) {
                switch (reqType) {
                    case CREATE_INDEX:
                        esResponse = respMapper.buildErrorReponse(e);
                        break;
                    default:
                        throw new IOException(e);

                }
            }

        } catch (InvalidResponseException ire) {
            if (restResp.isSuccess() || restResp.isRetriable()) {
                /*
                 * TODO: log this exception, this might be a parsing exception,
                 * which may not result in any problems, in case response got
                 * sufficiently parsed.
                 *
                 * In case response was not sufficiently parsed, that will be
                 * taken care separately.
                 */
                if (respMapper instanceof ESResponse) {
                    ((ESResponse) respMapper).statusCode(
                        restResp.getStatusCode());
                }
                logger.info("Invalid Response Exception - response can" +
                            " not be parsed fully." +
                            ire);
            } else {
                throw new IOException(ire);
            }

        } catch (JsonParseException jsonParseEx) {

            final String errPrefix =
                "ESRestClient.executeAsync: JsonParseException thrown " +
                "from the low level http client of elasticsearch upon " +
                "REST request";
            /*
             * When the document a given request attempts to access and parse
             * does not exist, the low-level method ESJsonUtil.validateToken
             * will generate a stack trace along with an error message
             * indicating:
             *
             *   'Failed to parse object: expecting token of
             *    type [START_OBJECT] but found [null]'
             *
             * Although the stack trace does not result in failure or exit
             * of the system, it nevertheless could be alarming or confusing
             * for users. As a result, it is probably better to catch and log
             * the issue, rather than allowing the exception thrown by
             * ESJsonUtil.validateToken to propagate out to the user.
             */
            if (restResp != null) {

                if (logger.isLoggable(Level.WARNING)) {

                    final int respStatus =
                       restResp.getStatusCode();

                    String errSuffix = "error parsing document";
                    if (respStatus == RestStatus.NOT_FOUND.getStatus()) {
                        errSuffix = "non-existent document";
                    }

                    final String errMsg = errPrefix +
                        " [request = " + restReq +
                        ", requestType = " + req.requestType() +
                        ", response = " + restResp +
                        ", respStatus = " + respStatus +
                        ", isRetriable = " + restResp.isRetriable() + "]" +
                        " - " + errSuffix + ": " + jsonParseEx.getMessage();

                    logger.warning(errMsg);
                }
            } else {
                logger.warning(errPrefix);
                throw jsonParseEx;
            }

        } catch (IOException ie) {

            final String errPrefix =
                "ESRestClient.executeAsync: IOException thrown " +
                "from the low level http client of elasticsearch upon " +
                "REST request";

            if (restResp != null &&
                    (restResp.isSuccess() || restResp.isRetriable())) {
                // TODO: log this exception, this must be a parsing exception.
                if (respMapper instanceof ESResponse) {
                    // set status code in the response.
                    ((ESResponse) respMapper).statusCode(restResp
                                                         .getStatusCode());
                }
                final String errMsg = errPrefix +
                    ": non-null response received [success = " +
                    restResp.isSuccess() + ", retriable = " +
                    restResp.isRetriable() + "]";

                logger.warning(errMsg);
            } else {
                logger.warning(errPrefix);
                throw ie;
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        if (esResponse != null) {
            return esResponse;
        }
        if (restResp != null) {
            return respMapper.buildFromRestResponse(restResp);
        }
        return (S) respMapper;

    }

    public List<HttpHost> getAvailableNodes() {
        return httpClient.getAvailableNodes();
    }

    public void setAvailableNodes(List<HttpHost> availableNodes) {
        logger.finest("Setting availableNodes to: " + availableNodes);
        httpClient.setAvailableNodes(availableNodes);
    }

    public void close() {
        if (httpClient != null) {
            final FailureListener failureListener =
                httpClient.getFailureListener();
            if (failureListener != null) {
                failureListener.close();
            }
            httpClient.close();
        }
    }

    List<HttpHost> getAllESHttpNodes() {
        return httpClient.getAllESHttpNodes();
    }

    public ESHttpClient getESHttpClient() {
        return httpClient;
    }

}
