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

package oracle.kv.impl.tif.esclient.restClient.monitoring;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.tif.esclient.esRequest.ESRequest;
import oracle.kv.impl.tif.esclient.esRequest.ESRestRequestGenerator;
import oracle.kv.impl.tif.esclient.esRequest.GetHttpNodesRequest;
import oracle.kv.impl.tif.esclient.esResponse.ESResponse;
import oracle.kv.impl.tif.esclient.esResponse.GetHttpNodesResponse;
import oracle.kv.impl.tif.esclient.esResponse.InvalidResponseException;
import oracle.kv.impl.tif.esclient.esResponse.TimeOrderedResponse;
import oracle.kv.impl.tif.esclient.httpClient.ESHttpClient;
import oracle.kv.impl.tif.esclient.httpClient.ESResponseListener;
import oracle.kv.impl.tif.esclient.httpClient.HttpHost;
import oracle.kv.impl.tif.esclient.restClient.RestRequest;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;
import oracle.kv.impl.tif.esclient.restClient.utils.ESLatestResponse;
import oracle.kv.impl.tif.esclient.restClient.utils.ESRestClientUtil;
import oracle.kv.impl.util.RateLimitingLogger;

import com.fasterxml.jackson.core.JsonParser;

/**
 * Monitoring APIs are executed asynchronously.
 * 
 * For now, the only use case for a monitoring client is to serve
 * ESNodeMonitor. And that function is as follows:
 * 
 * <li>Execute GetHttpNodesRequest asynchronously.</li> 
 * <li>onSuccess of this asynchronous execution, wrap the RestResponse in a
 * TimeOrderedResponse,thereby time stamping it.</li> 
 * <li>Try setting this TimeOrderedResponse to an ESLatestResponse instance 
 * called latestAvailableNodes. </li>
 * <li> Then build a GetHttpNodeResponse from the
 * RestResponse which got set as latest. </li>
 * 
 * <p>
 * Monitoring Client work over a different low level ESHttpClient, than the
 * ESRestClient.
 * </p>
 * 
 * <p>
 * It is preferable to use a different MonitorinClient for a different
 * monitoring request type.
 * </p>
 * <p>
 * Currently Async responses are not queued anywhere for consumption by other
 * thread. The current use case is only to store the latest state of available
 * ES Nodes. This class only specializes to this particular requirement.
 * </p>
 * <p>
 * In future, when more use cases arise, this class will be generalized
 * accordingly.
 * </p>
 */

public class MonitorClient implements AutoCloseable {

    private final Logger logger;
    private final RateLimitingLogger<String> rateLimitLogger;
    private final ESHttpClient httpClient;

    /*
     * A priority Queue which enqueues asynchronous response and dequeues the
     * latest response based on the time the response was received by the
     * client.
     * 
     * TimeOrderedResponse will wrap a RestResponse of GetHttpNodesResponse.
     * And the natural ordering for TimeOrderedResponse is based on most recent
     * response.
     * 
     * private final PriorityBlockingQueue<TimeOrderedResponse> responseQueue;
     * private final int waitOnQueueMillis = 3000;
     */
    private final ESLatestResponse latestNodesResponse;

    public MonitorClient(ESHttpClient httpClient,
            ESLatestResponse latestNodesResponse,
            Logger logger) {
        this.httpClient = httpClient;
        this.latestNodesResponse = latestNodesResponse;
        this.logger = logger;
        this.rateLimitLogger = new RateLimitingLogger<>(60 * 1000, 10, logger);
    }

    /*
     * This is used for scheduled monitoring API, and is executed
     * asynchronously.
     */
    public GetHttpNodesResponse getHttpNodesResponse(GetHttpNodesRequest req)
        throws IOException {
        GetHttpNodesResponse resp = new GetHttpNodesResponse();
        executeAsync(req);
        RestResponse restResponse = null;
        JsonParser parser = null;
        try {
            TimeOrderedResponse wrapperTimeResponse =
                    latestNodesResponse.get(1000,
                                            TimeUnit.MILLISECONDS);
            restResponse = wrapperTimeResponse.getResponse();
            if (restResponse != null) {

                parser = ESRestClientUtil.initParser(restResponse);
                resp.buildFromJson(parser);
            } else if (wrapperTimeResponse.getException() != null) {
                throw new IOException(wrapperTimeResponse.getException());
            }
        } catch (InvalidResponseException e) {
            logger.log(Level.WARNING,"There might be an issue in parsing " +
                    "the response structure", e);
            throw new IOException(e);
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }

        return resp;
    }

    public <R extends ESRequest<R>, S extends ESResponse> void
            executeAsync(R req) {

        ESRestRequestGenerator reqGenerator = (ESRestRequestGenerator) req;
        executeAsyncLatestResponse(reqGenerator, this.latestNodesResponse);

    }

    private <R extends ESRequest<R>, S extends ESResponse> void
            executeAsyncLatestResponse(
                                       ESRestRequestGenerator reqGenerator,
                                       ESLatestResponse latestResponse) {

        RestRequest restReq = reqGenerator.generateRestRequest();

        ESResponseListener responseListener = new ESResponseListener() {

            @Override
            public void onSuccess(RestResponse response) {
                TimeOrderedResponse nowResponse =
                    new TimeOrderedResponse(response, System.nanoTime());

                latestResponse.setIfLatest(nowResponse);

            }

            @Override
            public void onFailure(Exception exception) {
                final String exObj = exception.toString();
                final String failureMsg =
                    "Monitoring Request Failed with exception: " + exObj;
                rateLimitLogger.log(exObj, Level.WARNING, failureMsg);
            }

            @Override
            public void onRetry(Exception exception) {
                final String exObj = exception.getClass().getName();
                final String failureMsg = "Retries are done by ESHttpClient";
                rateLimitLogger.log(exObj, Level.WARNING, failureMsg);
            }

        };

        httpClient.executeAsync(restReq.method(), restReq.endpoint(),
                                restReq.params(), responseListener,
                                restReq.headers());

    }

    public List<HttpHost> getAvailableNodes() {
        return httpClient.getAvailableNodes();
    }

    /**
     * set availableNode to all the esHttpClients registered with this
     * monitoring client.
     * 
     * @param availableNodes - List of HttpHosts of ES Cluster which are up and
     *            running.
     * @param esHttpClients - List of ESHttpClients on which httpHosts are set.
     */
    /*
     * Note that esHttpClient.setAvailableNodes is synchronized.
     * 
     * This method is synchronized so that the thread which captures a
     * particular snapshot of availableNodes in time, should be able to set
     * that snapshot on all registered clients. This way all clients have same
     * set of available server nodes.
     * 
     * There is a very simpler lock order so should not be an issue.
     * MonitoringClient object -> httpClient Object.
     */
    public synchronized void setAvailableNodes(
                                               List<HttpHost> availableNodes,
                                               List<ESHttpClient> esHttpClients) {
        logger.fine("Setting availableNodes to: " + availableNodes);
        for (ESHttpClient esHttpClient : esHttpClients) {
            esHttpClient.setAvailableNodes(availableNodes);
        }
    }

    @Override
    public void close() {
        httpClient.close();
    }

    /*
     * If set of available nodes change from what was set, then set available
     * nodes with the new list.
     * 
     * The method name is a misnomer in the way that it suggests something
     * atomic like CAS. This is not atomic method.
     * 
     * 
     * This method is called from different threads on the same object and
     * calling it concurrently is very probable as well.
     * 
     * Following are the reasons for not doing this operation under a mutex:
     * 
     * getAvailableNodes gets into a synchronized method
     * httpClient.getAvailable nodes. httpClient.setAvailableNodes is
     * synchronized as well.
     * 
     * acquiring locks in this method may lead to deadlock. and there is no
     * need for it, as following scenario is acceptable:
     * 
     * 1) threadA gets current availableNodes on httpclient as node1, node2,
     * node3 2) threadA wants to set it to node1,node2,node3. 3) In the mean
     * time threadB sets availableNodes on httpClient to node1,node2 4) threadA
     * which had the latest available nodes node1,node2,node3 will not set it
     * as comparison will evaluate to true. 5) This is a transient problem as
     * next periodic event will eventually correct this problem. 6) Moreover
     * making the operation atomic will not solve this problem either.
     */
    public void compareAndSet(
                              List<HttpHost> availableNodes,
                              List<ESHttpClient> esHttpClients) {
        if (availableNodes == null || availableNodes.size() == 0 ||
                esHttpClients == null || esHttpClients.size() == 0) {
            return;
        }
        List<HttpHost> current = getAvailableNodes();
        current.sort(ESHttpClient.httpHostComparator);
        availableNodes.sort(ESHttpClient.httpHostComparator);
        if (current.size() != availableNodes.size()) {
            setAvailableNodes(availableNodes, esHttpClients);
            return;
        }

        boolean diff = false;
        Iterator<HttpHost> newList = availableNodes.iterator();
        for (HttpHost host : current) {
            if (!newList.next().equals(host)) {
                diff = true;
                break;
            }
        }
        if (diff) {
            setAvailableNodes(availableNodes, esHttpClients);
        }

    }

    List<HttpHost> getAllESHttpNodes() {
        return httpClient.getAllESHttpNodes();
    }

    public ESHttpClient getESHttpClient() {
        return httpClient;
    }

}
