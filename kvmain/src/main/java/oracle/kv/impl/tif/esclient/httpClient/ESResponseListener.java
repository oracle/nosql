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

package oracle.kv.impl.tif.esclient.httpClient;

import oracle.kv.impl.tif.esclient.esResponse.ESException;
import oracle.kv.impl.tif.esclient.restClient.RestResponse;

public interface ESResponseListener {

    /**
     * Method invoked if the request yielded a successful response
     */
    void onSuccess(RestResponse response);

    /**
     * Method invoked if the request failed. There are two main categories of failures: 
     * connection failures (usually {@link java.io.IOException}s, or responses 
     * that were treated as errors based on their error response code
     * ({@link ESException}s).
     */
    void onFailure(Exception exception);

    /**
     * ESResponseListener also serves as tracking listener.
     * The exception which allows retries are added as suppressed exception.
     * @param exception
     */
    void onRetry(Exception exception);
}
