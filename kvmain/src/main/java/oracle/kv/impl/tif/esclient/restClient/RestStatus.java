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

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import oracle.kv.impl.tif.esclient.esResponse.ESWriteResponse;

/**
 * The ES status used by Elasticsearch REST API.
 * These status' are obtained from the link given below:
 * Info about status can be seen here or in ElasticSearch docs.
 * http://xbib.org/elasticsearch/1.3.1/apidocs/org/elasticsearch/rest/RestStatus.html
 * 
 *
 */

public enum RestStatus {

    CONTINUE(100),

    SWITCHING_PROTOCOLS(101),

    OK(200),

    CREATED(201),

    ACCEPTED(202),

    NON_AUTHORITATIVE_INFORMATION(203),

    NO_CONTENT(204),

    RESET_CONTENT(205),

    PARTIAL_CONTENT(206),

    MULTI_STATUS(207),

    MULTIPLE_CHOICES(300),

    MOVED_PERMANENTLY(301),

    FOUND(302),

    SEE_OTHER(303),

    NOT_MODIFIED(304),

    USE_PROXY(305),

    TEMPORARY_REDIRECT(307),

    BAD_REQUEST(400),

    UNAUTHORIZED(401),

    PAYMENT_REQUIRED(402),

    FORBIDDEN(403),

    NOT_FOUND(404),

    METHOD_NOT_ALLOWED(405),

    NOT_ACCEPTABLE(406),

    PROXY_AUTHENTICATION(407),

    REQUEST_TIMEOUT(408),

    CONFLICT(409),

    GONE(410),

    LENGTH_REQUIRED(411),

    PRECONDITION_FAILED(412),

    REQUEST_ENTITY_TOO_LARGE(413),

    REQUEST_URI_TOO_LONG(414),

    UNSUPPORTED_MEDIA_TYPE(415),

    REQUESTED_RANGE_NOT_SATISFIED(416),

    EXPECTATION_FAILED(417),

    UNPROCESSABLE_ENTITY(422),

    LOCKED(423),

    FAILED_DEPENDENCY(424),

    TOO_MANY_REQUESTS(429),

    INTERNAL_SERVER_ERROR(500),

    NOT_IMPLEMENTED(501),

    BAD_GATEWAY(502),

    SERVICE_UNAVAILABLE(503),

    GATEWAY_TIMEOUT(504),

    HTTP_VERSION_NOT_SUPPORTED(505),

    INSUFFICIENT_STORAGE(506);

    private static final Map<Integer, RestStatus> CodeMap;
    static {
        RestStatus[] values = values();
        Map<Integer, RestStatus> codeToStatus = new HashMap<>(values.length);
        for (RestStatus value : values) {
            codeToStatus.put(value.status, value);
        }
        CodeMap = unmodifiableMap(codeToStatus);
    }

    private int status;

    RestStatus(int status) {
        this.status = (short) status;
    }

    public int getStatus() {
        return status;
    }

    public static RestStatus status(int successfulShards,
                                    int totalShards,
                                    ESWriteResponse
                                    .ShardInfo
                                    .ReplicaFailure... failures) {
        if (failures.length == 0) {
            if (successfulShards == 0 && totalShards > 0) {
                return RestStatus.SERVICE_UNAVAILABLE;
            }
            return RestStatus.OK;
        }
        RestStatus status = RestStatus.OK;
        if (successfulShards == 0 && totalShards > 0) {
            for (ESWriteResponse.ShardInfo
                    .ReplicaFailure failure : failures) {
                RestStatus shardStatus = failure.status();
                if (shardStatus.getStatus() >= status.getStatus()) {
                    status = failure.status();
                }
            }
            return status;
        }
        return status;
    }

    /**
     * Turn a status code into a {@link RestStatus}, returning null
     * if we don't know that status.
     */
    public static RestStatus fromCode(int code) {
        return CodeMap.get(code);
    }
}
