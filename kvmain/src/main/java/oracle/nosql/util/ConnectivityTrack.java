/*-
 * Copyright (C) 2011, 2020 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util;

import java.util.List;

import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.util.HttpRequest.ConnectionHandler;
import oracle.nosql.util.HttpRequest.HttpMethod;
import oracle.nosql.util.fault.RequestFaultException;
import oracle.nosql.util.ph.HealthStatus;

/**
 * A helper class to track HTTP request.
 * It tracks HTTP request succeeded or failed. And then help to check the target
 * connectivity.
 */
public class ConnectivityTrack {

    /* connectivity check interval */
    private final static int CONNECTIVITY_INTERVAL = 60_000;

    /* last successful request time. */
    private long lastSuccessTime = 0;
    /* last failed request time. */
    private long lastFailureTime = 0;
    private final String targetName;
    private final int failureTolerance;
    private int failureCount;

    /**
     * @param targetName is used to report which target connectivity status.
     * @param failureTolerance When there is connectivity failure, it will
     * report YELLOW status by default. Once the consecutive failure count is
     * more than failureTolerance, it will report RED status.
     */
    public ConnectivityTrack(String targetName, int failureTolerance) {
        this.targetName = targetName;
        this.failureTolerance = failureTolerance;
        failureCount = 0;
    }

    /**
     * Check target connectivity.
     * 1) If all target requests are successful in last interval,
     *    we think target connectivity is fine.
     * 2) If no target request or some target requests failed in last interval,
     *    using the list of harmless API URLs to check target connectivity.
     * @param urlList is the list of URLs to ping target. These URLs should be
     *        harmless. Note: only HTTP GET URL is supported.
     * @param errors to add health error message for podhealth.
     * @return the summary health status for podhealth.
     */
    public HealthStatus checkConnectivity(HttpRequest request,
                                          ConnectionHandler handler,
                                          List<String> urlList,
                                          List<String> errors,
                                          SkLogger logger) {
        long expiredTime = System.currentTimeMillis() - CONNECTIVITY_INTERVAL;
        if (lastSuccessTime >= expiredTime && lastFailureTime < expiredTime) {
            /*
             * All target http requests are successful during last interval,
             * target connectivity is fine.
             */
            return healthyStatus();
        }
        /*
         * There isn't any target http request, or some target http requests
         * failed in last interval.
         */
        if (urlList == null || urlList.isEmpty()) {
            if (lastFailureTime >= expiredTime) {
                final String errorMsg = "Some requests to " + targetName +
                    " failed during last interval";
                return unhealthyStatus(errorMsg, errors, logger);
            }
            /*
             * Unknown status: no request during last interval and
             * no ping urls.
             */
            return HealthStatus.GREEN;
        }
        /*
         * Use target harmless API urls to check target connectivity.
         * TODO check in parallel if urlList size > 3.
         */
        for (String url : urlList) {
            try {
                doHttpRequest(request, HttpMethod.GET, url, null /* payload */,
                              handler, null /* LogContext */);
            } catch(Exception e) {
                final String errorMsg = "Ping request to " + targetName +
                    " failed, url is " + url + ", exception is " + e;
                return unhealthyStatus(errorMsg, errors, logger);
            }
        }
        return healthyStatus();
    }

    /*
     * Do http request, and record success or failure time.
     */
    public HttpResponse doHttpRequest(HttpRequest request,
                                      HttpMethod method,
                                      String url,
                                      String payload,
                                      ConnectionHandler handler,
                                      LogContext lc)
        throws RequestFaultException {

        HttpResponse response = null;
        try {
            switch (method) {
            case GET:
                response = request.doHttpGet(url, handler, lc);
                break;
            case POST:
                response = request.doHttpPost(url, payload, handler, lc);
                break;
            case PUT:
                response = request.doHttpPut(url, payload, handler, lc);
                break;
            case DELETE:
                response = request.doHttpDelete(url, payload, handler, lc);
                break;
            }
        } catch (Exception e) {
            lastFailureTime = System.currentTimeMillis();
            throw e;
        }
        lastSuccessTime = System.currentTimeMillis();
        return response;
    }

    private HealthStatus healthyStatus() {
        failureCount = 0;
        return HealthStatus.GREEN;
    }

    private HealthStatus unhealthyStatus(final String errorMsg,
                                         final List<String> errors,
                                         final SkLogger logger) {
        logger.warning(errorMsg);
        errors.add(errorMsg);
        ++failureCount;
        if (failureCount >= failureTolerance) {
            return HealthStatus.RED;
        }
        return HealthStatus.YELLOW;
    }
}
