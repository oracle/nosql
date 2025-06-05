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
import java.util.Map;

/**
 * Struct to package together the output from an HttpRequest
 */
public class HttpResponse {

    private final int statusCode;
    private final String output;

    /*
     * Retry information collected using the HttpRequestRetryHelper to
     * issue the HttpRequest, including total number of executions and
     * total wait time to get this response.
     */
    private int totalExecutions;
    private long totalWaitTime;
    private Map<String, List<String>> headers;

    public HttpResponse(int statusCode, String output) {
        this(statusCode, output, null /* headers */);
    }

    public HttpResponse(int statusCode,
                        String output,
                        Map<String,List<String>> headers) {
        this.statusCode = statusCode;
        this.output = output;
        this.headers = headers;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getOutput() {
        return output;
    }

    public Map<String,List<String>> getHeaders() {
        return headers;
    }

    public String getHeaderAsString(String key) {
        if (headers != null && headers.containsKey(key)) {
            List<String> values = headers.get(key);
            if (!values.isEmpty()) {
                return values.get(0);
            }
        }
        return null;
    }

    /**
     * Set total number of HTTP request executions. Used in the
     * {@link HttpRequest} to collect the total number of request
     * executions until get this response.
     * @param totalExecutions total number of executions.
     * @return this HttpResponse
     */
    public HttpResponse setTotalExecutions(int totalExecutions) {
        this.totalExecutions = totalExecutions;
        return this;
    }

    /**
     * Set total wait time of HTTP request executions. Used in the
     * {@link HttpRequest} to collect the total wait time of request
     * executions untill get this response.
     * @param totalWaitTime total wait time of executions.
     * @return this HttpResponse
     */
    public HttpResponse setTotalWaitTime(long totalWaitTime) {
        this.totalWaitTime = totalWaitTime;
        return this;
    }

    /**
     * Returns total number of HTTP request executions if using
     * {@link HttpRequest} to get this HTTP response, which
     * indicates the total number of request executions until get this response.
     */
    public int getTotalExecutionNum() {
        return totalExecutions;
    }

    /**
     * Returns total wait time of HTTP request executions if using
     * {@link HttpRequest} to get this HTTP response, which
     * indicates the total wait time of request executions until get
     * this response.
     */
    public long getTotalWaitTime() {
        return totalWaitTime;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "HttpResponse [statusCode=" + statusCode + ", output=" + output
                + "]";
    }
}
