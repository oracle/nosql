/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
package oracle.nosql.proxy.rest;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Error code used for the error response of rest API, each ErrorCode defines
 * the errorCode type and the associated HTTP status code.
 *
 * See createErrorCodeMap() in RestDataService for the mapping between proxy
 * error codes defined in Protocol.java to the ErrorCodes in this class.
 */
public enum ErrorCode {
    INVALID_PARAMETER {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INVALID_PARAMETER;
        }
    },
    ROW_SIZE_LIMITED_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_ROW_SIZE_LIMITED_EXCEEDED;
        }
    },
    KEY_SIZE_LIMITED_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_KEY_SIZE_LIMITED_EXCEEDED;
        }
    },
    BATCH_OP_NUMBER_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_BATCH_OP_NUMBER_LIMIT_EXCEEDED;
        }
    },
    REQUEST_SIZE_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_REQUEST_SIZE_LIMIT_EXCEEDED;
        }
    },
    TABLE_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TABLE_LIMIT_EXCEEDED;
        }
    },
    INDEX_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INDEX_LIMIT_EXCEEDED;
        }
    },
    EVOLUTION_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_EVOLUTION_LIMIT_EXCEEDED;
        }
    },
    SIZE_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_SIZE_LIMIT_EXCEEDED;
        }
    },
    TABLE_DEPLOYMENT_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TABLE_DEPLOYMENT_LIMIT_EXCEEDED;
        }
    },
    TENANT_DEPLOYMENT_LIMIT_EXCEEDED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TENANT_DEPLOYMENT_LIMIT_EXCEEDED;
        }
    },
    CANNOT_PARSE_REQUEST {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.BAD_REQUEST;
        }

        @Override
        public String getErrorCode() {
            return TYPE_CANNOT_PARSE_REQUEST;
        }
    },
    INVALID_AUTHORIZED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.UNAUTHORIZED;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INVALID_AUTHORIZED;
        }
    },
    INSUFFICIENT_PERMISSION {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_NOT_AUTHORIZED_OR_NOT_FOUND;
        }
    },
    NOT_FOUND {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_NOT_FOUND;
        }
    },
    TABLE_NOT_FOUND {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TABLE_NOT_FOUND;
        }
    },
    INDEX_NOT_FOUND {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INDEX_NOT_FOUND;
        }
    },
    RESOURCE_NOT_FOUND {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_RESOURCE_NOT_FOUND;
        }
    },
    METHOD_NOT_IMPLEMENTED {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_METHOD_NOT_IMPLEMENTED;
        }
    },
    SECURITY_INFO_UNAVAILABLE {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.SERVICE_UNAVAILABLE;
        }

        @Override
        public String getErrorCode() {
            return TYPE_SERVICE_AVAILABLE;
        }
    },
    CANNOT_CANCEL_WORK_REQUEST {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.NOT_FOUND;
        }

        @Override
        public String getErrorCode() {
            return TYPE_CANNOT_CANCEL_WORK_REQUEST;
        }
    },
    TABLE_ALREADY_EXISTS {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.CONFLICT;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TABLE_ALREADY_EXISTS;
        }
    },
    INDEX_ALREADY_EXISTS {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.CONFLICT;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INDEX_ALREADY_EXISTS;
        }
    },
    RESOURCE_ALREADY_EXISTS {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.CONFLICT;
        }

        @Override
        public String getErrorCode() {
            return TYPE_RESOURCE_ALREADY_EXISTS;
        }
    },
    INVALID_RETRY_TOKEN {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.CONFLICT;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INVALID_RETRY_TOKEN;
        }
    },
    ETAG_MISMATCH {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.PRECONDITION_FAILED;
        }

        @Override
        public String getErrorCode() {
            return TYPE_ETAG_MISMATCH;
        }
    },
    TOO_MANY_REQUESTS {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.TOO_MANY_REQUESTS;
        }

        @Override
        public String getErrorCode() {
            return TYPE_TOO_MANY_REQUESTS;
        }
    },
    INTERNAL_SERVER_ERROR {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        @Override
        public String getErrorCode() {
            return TYPE_INTERNAL_SERVER_ERROR;
        }
    },
    REQUEST_TIMEOUT {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        @Override
        public String getErrorCode() {
            return TYPE_REQUEST_TIMEOUT;
        }
    },
    RETRY_AUTHENTICATION {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        @Override
        public String getErrorCode() {
            return TYPE_RETRY_AUTHENTICATION;
        }
    },
    UNKNOWN_ERROR {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        @Override
        public String getErrorCode() {
            return TYPE_UNKNOWN_ERROR;
        }
    },
    ILLEGAL_STATE {
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.INTERNAL_SERVER_ERROR;
        }

        @Override
        public String getErrorCode() {
            return TYPE_ILLEGAL_STATE;
        }
    },
    SERVICE_UNAVAILABLE{
        @Override
        public HttpResponseStatus getHttpStatusCode() {
            return HttpResponseStatus.SERVICE_UNAVAILABLE;
        }

        @Override
        public String getErrorCode() {
            return TYPE_SERVICE_AVAILABLE;
        }
    };

    /**
     * Type of ErrorCode constants
     */
    /* 400 Bad Request*/
    private static final String TYPE_INVALID_PARAMETER =
        "InvalidParameter";
    private static final String TYPE_ROW_SIZE_LIMITED_EXCEEDED =
        "RowSizeLimitExceeded";
    private static final String TYPE_KEY_SIZE_LIMITED_EXCEEDED =
        "KeySizeLimitExceeded";
    private static final String TYPE_BATCH_OP_NUMBER_LIMIT_EXCEEDED =
        "BatchOpNumberLimitExceeded";
    private static final String TYPE_REQUEST_SIZE_LIMIT_EXCEEDED =
        "RequestSizeLimitExceeded";
    private static final String TYPE_TABLE_LIMIT_EXCEEDED =
        "TableLimitExceeded";
    private static final String TYPE_INDEX_LIMIT_EXCEEDED =
        "IndexLimitExceeded";
    private static final String TYPE_EVOLUTION_LIMIT_EXCEEDED =
        "EvolutionLimitExceeded";
    private static final String TYPE_SIZE_LIMIT_EXCEEDED = "SizeLimitExceeded";
    private static final String TYPE_TABLE_DEPLOYMENT_LIMIT_EXCEEDED =
        "TableDeploymentLimitExceeded";
    private static final String TYPE_TENANT_DEPLOYMENT_LIMIT_EXCEEDED =
        "TenantDeploymentLimitExceeded";
    private static final String TYPE_CANNOT_PARSE_REQUEST =
        "CannotParseRequest";

    /* 401 Unauthorized */
    private static final String TYPE_INVALID_AUTHORIZED =
        "InvalidAuthorization";

    /* 404 Not Found */
    private static final String TYPE_NOT_FOUND = "NotFound";
    private static final String TYPE_TABLE_NOT_FOUND = "TableNotFound";
    private static final String TYPE_INDEX_NOT_FOUND = "IndexNotFound";
    private static final String TYPE_RESOURCE_NOT_FOUND = "ResourceNotFound";
    private static final String TYPE_METHOD_NOT_IMPLEMENTED =
        "MethodNotImplemented";
    private static final String TYPE_NOT_AUTHORIZED_OR_NOT_FOUND =
        "NotAuthorizedOrNotFound";
    private static final String TYPE_CANNOT_CANCEL_WORK_REQUEST =
        "CantCancelWorkRequest";

    /* 409 Conflict */
    private static final String TYPE_TABLE_ALREADY_EXISTS = "TableAlreadyExists";
    private static final String TYPE_INDEX_ALREADY_EXISTS = "IndexAlreadyExists";
    private static final String TYPE_RESOURCE_ALREADY_EXISTS =
        "ResourceAleadyExists";
    private static final String TYPE_INVALID_RETRY_TOKEN =
        "InvalidatedRetryToken";

    /* 412 Precondition fail */
    private static final String TYPE_ETAG_MISMATCH = "ETagMismatch";

    /* 429 Too Many Request */
    private static final String TYPE_TOO_MANY_REQUESTS = "TooManyRequests";

    /* 500 Internal Server Error */
    private static final String TYPE_INTERNAL_SERVER_ERROR =
        "InternalServerError";
    private static final String TYPE_REQUEST_TIMEOUT = "RequestTimeout";
    private static final String TYPE_RETRY_AUTHENTICATION =
        "RetryAuthentication";
    private static final String TYPE_UNKNOWN_ERROR = "UnknownError";
    private static final String TYPE_ILLEGAL_STATE = "IllegalState";

    /* 503 */
    private static final String TYPE_SERVICE_AVAILABLE = "ServiceAvailable";

    abstract HttpResponseStatus getHttpStatusCode();
    abstract String getErrorCode();
}
