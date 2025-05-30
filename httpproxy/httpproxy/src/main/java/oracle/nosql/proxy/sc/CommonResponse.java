/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy.sc;

import static oracle.nosql.proxy.protocol.BinaryProtocol.ETAG_MISMATCH;
import static oracle.nosql.proxy.protocol.BinaryProtocol.EVOLUTION_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ILLEGAL_ARGUMENT;
import static oracle.nosql.proxy.protocol.BinaryProtocol.ILLEGAL_STATE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INDEX_EXISTS;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INDEX_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INDEX_NOT_FOUND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INSUFFICIENT_PERMISSION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INVALID_AUTHORIZATION;
import static oracle.nosql.proxy.protocol.BinaryProtocol.INVALID_RETRY_TOKEN;
import static oracle.nosql.proxy.protocol.BinaryProtocol.NO_ERROR;
import static oracle.nosql.proxy.protocol.BinaryProtocol.OPERATION_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.OPERATION_NOT_SUPPORTED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.RESOURCE_EXISTS;
import static oracle.nosql.proxy.protocol.BinaryProtocol.RESOURCE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.SERVER_ERROR;
import static oracle.nosql.proxy.protocol.BinaryProtocol.SERVICE_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_DEPLOYMENT_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_EXISTS;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_NOT_FOUND;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TABLE_NOT_READY;
import static oracle.nosql.proxy.protocol.BinaryProtocol.TENANT_DEPLOYMENT_LIMIT_EXCEEDED;
import static oracle.nosql.proxy.protocol.BinaryProtocol.UNKNOWN_ERROR;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.ErrorResponse;

/**
 * A base class for various Response instances. This class factors out the
 * common state of success/failure and error information.
 *
 * Success/failure is determined by the HTTP response code and further
 * discrimination of errors is contained in errorCode, if it's available.
 */
public abstract class CommonResponse {
    private final int httpResponse;
    private final ErrorResponse error;

    /*
     * An error map to map ErrorCode strings to integer error codes used by
     * the proxy and drivers
     */
    private static final Map<String, Integer> errorMap = createErrorMap();

    CommonResponse(int httpResponse) {
        this.httpResponse = httpResponse;
        this.error = null;
    }

    CommonResponse(ErrorResponse error) {

        httpResponse = error.getStatus();
        this.error = error;
    }

    /**
     * Returns the HTTP response code
     */
    public int getHttpResponse() {
        return httpResponse;
    }

    /**
     * Returns the JSON payload for the response. Used by the REST
     * interfaces
     */
    public String getPayload() {
        if (!getSuccess()) {
            if (error != null) {
                try {
                    return JsonUtils.prettyPrint(error);
                } catch (IllegalArgumentException iae) {
                    /* plan B */
                    return error.getDetail();
                }
            }
            return "{}";
        }
        return successPayload();
    }

    /**
     * Must be overridden by sub-classes. Used by REST interfaces
     */
    public abstract String successPayload();

    /**
     * status code larger than 200 but smaller than 300 is success.
     */
    public boolean getSuccess() {
        return (httpResponse >= 200) && (httpResponse < 300);
    }

    /**
     * Returns the error code. No error is indicated by 0. This is used by
     * the driver/proxy protocol.
     */
    public int getErrorCode() {
        if (error != null) {
            return mapErrorCode(error);
        }
        return NO_ERROR;
    }

    /**
     * Returns an error string on failure, null on success. This is a subset of
     * the ErrorResponse, used by the driver when not using REST.
     */
    public String getErrorString() {
        if (error != null) {
            return error.getDetail();
        }
        return null;
    }

    @Override
    public String toString() {
        return "CommonResponse [httpResponse=" + httpResponse + ", error="
            + error + "]";
    }

    /**
     * Maps ErrorCode types to protocol error codes used by the driver/proxy
     * protocol and not REST.
     */
    private static int mapErrorCode(ErrorResponse error) {
        String errorType = error.getType();
        /*
         * Most errors map directly. 502-504 should be retry-able as they
         * indicate an internal system issue, usually.
         */
        if (error.getStatus() < 502 ||
            error.getStatus() > 504) {
            Integer val = errorMap.get(errorType);
            if (val == null) {
                // TODO: log, throw?
                return UNKNOWN_ERROR;
            }
            return val;
        }
        /* this will be retry-able */
        return SERVICE_UNAVAILABLE;
    }

    private static Map<String, Integer> createErrorMap() {
        return new HashMap<String, Integer>() {
            private static final long serialVersionUID = 1L;
            {
                put(ErrorCode.TYPE_ILLEGAL_ARGUMENT, ILLEGAL_ARGUMENT);
                put(ErrorCode.TYPE_INCORRECT_STATE, ILLEGAL_STATE);
                put(ErrorCode.TYPE_INDEX_ALREADY_EXISTS, INDEX_EXISTS);
                put(ErrorCode.TYPE_INDEX_NOT_FOUND, INDEX_NOT_FOUND);
                put(ErrorCode.TYPE_NO_ERROR, NO_ERROR);
                put(ErrorCode.TYPE_RESOURCE_EXISTS, RESOURCE_EXISTS);
                put(ErrorCode.TYPE_RESOURCE_NOT_FOUND, RESOURCE_NOT_FOUND);
                put(ErrorCode.TYPE_SERVICE_UNAVAILABLE, SERVICE_UNAVAILABLE);
                put(ErrorCode.TYPE_SERVER_ERROR, SERVER_ERROR);
                put(ErrorCode.TYPE_TABLE_ALREADY_EXISTS, TABLE_EXISTS);
                put(ErrorCode.TYPE_TABLE_NOT_FOUND, TABLE_NOT_FOUND);
                put(ErrorCode.TYPE_ETAG_MISMATCH, ETAG_MISMATCH);
                put(ErrorCode.TYPE_INVALID_RETRY_TOKEN, INVALID_RETRY_TOKEN);
                put(ErrorCode.TYPE_UNKNOWN_ERROR, UNKNOWN_ERROR);
                put(ErrorCode.TYPE_UNKNOWN_INT_ERROR, UNKNOWN_ERROR);
                put(ErrorCode.TYPE_UNSUPPORTED_OP, OPERATION_NOT_SUPPORTED);
                put(ErrorCode.TYPE_TABLE_LIMIT_EX, TABLE_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_INDEX_LIMIT_EX, INDEX_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_EVO_LIMIT_EX, EVOLUTION_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_TABLE_DEPLOY_LIMIT_EX,
                    TABLE_DEPLOYMENT_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_TENANT_DEPLOY_LIMIT_EX,
                    TENANT_DEPLOYMENT_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_OPERATION_RATE_LIMIT_EX,
                    OPERATION_LIMIT_EXCEEDED);
                put(ErrorCode.TYPE_INSUFFICIENT_PERMISSION,
                    INSUFFICIENT_PERMISSION);
                put(ErrorCode.TYPE_INVALID_AUTHORIZATION,
                    INVALID_AUTHORIZATION);
                put(ErrorCode.TYPE_TABLE_NOT_READY, TABLE_NOT_READY);
            }
        };
    }
}
