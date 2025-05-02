/*-
 * Copyright (C) 2011, 2019 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.util.fault;

/**
 * Error code catalogs. Each ErrorCode defines the generic reason-phrase and the
 * associated HTTP status code. Note that unlike the base HTTP spec, there can
 * be different reason-phrases associated with the same HTTP status code.
 * Further details about the specific context of the error are captured in the
 * associated response which implements the error response model in rfc7807
 *
 * The error codes are shared by the Proxy and all web tier components.
 */
public enum ErrorCode {
    NO_ERROR {
        @Override
        public String getHttpReasonPhrase() {
            return "OK";
        }

        @Override
        public int getHttpStatusCode() {
            return 200;
        }

        @Override
        public String getType() {
            return TYPE_NO_ERROR;
        }
    },
    INVALID_AUTHORIZATION {
        @Override
        public String getHttpReasonPhrase() {
            return "Invalid Authorization";
        }

        @Override
        public int getHttpStatusCode() {
            return 403;
        }

        @Override
        public String getType() {
            return TYPE_INVALID_AUTHORIZATION;
        }
    },
    INSUFFICIENT_PERMISSION {
        @Override
        public String getHttpReasonPhrase() {
            return "Insufficient Permission";
        }

        @Override
        public int getHttpStatusCode() {
            return 403;
        }

        @Override
        public String getType() {
            return TYPE_INSUFFICIENT_PERMISSION;
        }
    },
    RESOURCE_NOT_FOUND {
        @Override
        public String getHttpReasonPhrase() {
            return "Resource not found";
        }

        @Override
        public int getHttpStatusCode() {
            return 404;
        }

        @Override
        public String getType() {
            return TYPE_RESOURCE_NOT_FOUND;
        }
    },
    UNSUPPORTED_HTTP_VERB {
        @Override
        public String getHttpReasonPhrase() {
            return "Unsupported http verb";
        }

        @Override
        public int getHttpStatusCode() {
            return 405;
        }

        @Override
        public String getType() {
            return TYPE_UNSUPPORTED_VERB;
        }
    },
    RESOURCE_ALREADY_EXISTS {
        @Override
        public String getHttpReasonPhrase() {
            return "Resource already exists";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_RESOURCE_EXISTS;
        }
    },
    INTERNAL_SERVER_ERROR {
        @Override
        public String getHttpReasonPhrase() {
            return "Internal server error";
        }

        @Override
        public int getHttpStatusCode() {
            return 500;
        }

        @Override
        public String getType() {
            return TYPE_SERVER_ERROR;
        }
    },
    SERVICE_UNAVAILABLE {
        @Override
        public String getHttpReasonPhrase() {
            return "Service unavailable";
        }

        @Override
        public int getHttpStatusCode() {
            return 503;
        }

        @Override
        public String getType() {
            return TYPE_SERVICE_UNAVAILABLE;
        }
    },
    UNSUPPORTED_OPERATION {
        @Override
        public String getHttpReasonPhrase() {
            return "Unsupported operation";
        }

        @Override
        public int getHttpStatusCode() {
            return 501;
        }

        @Override
        public String getType() {
            return TYPE_UNSUPPORTED_OP;
        }
    },
    UNKNOWN_ERROR {
        @Override
        public String getHttpReasonPhrase() {
            return "Unknown error";
        }

        @Override
        public int getHttpStatusCode() {
            return 501;
        }

        @Override
        public String getType() {
            return TYPE_UNKNOWN_ERROR;
        }
    },
    UNKNOWN_INTERNAL_ERROR {
        @Override
        public String getHttpReasonPhrase() {
            return "Unknown internal error";
        }

        @Override
        public int getHttpStatusCode() {
            return 501;
        }

        @Override
        public String getType() {
            return TYPE_UNKNOWN_INT_ERROR;
        }
    },
    INCORRECT_STATE {
        @Override
        public String getHttpReasonPhrase() {
            return "Incorrect service state";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_INCORRECT_STATE;
        }
    },
    TABLE_NOT_FOUND {
        @Override
        public String getHttpReasonPhrase() {
            return "Table not found";
        }

        @Override
        public int getHttpStatusCode() {
            return 404;
        }

        @Override
        public String getType() {
            return TYPE_TABLE_NOT_FOUND;
        }
    },
    INDEX_NOT_FOUND {
        @Override
        public String getHttpReasonPhrase() {
            return "Index not found";
        }

        @Override
        public int getHttpStatusCode() {
            return 404;
        }

        @Override
        public String getType() {
            return TYPE_INDEX_NOT_FOUND;
        }
    },
    ILLEGAL_ARGUMENT {
        @Override
        public String getHttpReasonPhrase() {
            return "Request arguments validation error";
        }

        @Override
        public int getHttpStatusCode() {
            return 400;
        }

        @Override
        public String getType() {
            return TYPE_ILLEGAL_ARGUMENT;
        }
    },
    TABLE_ALREADY_EXISTS {
        @Override
        public String getHttpReasonPhrase() {
            return "Table already exists";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_TABLE_ALREADY_EXISTS;
        }
    },
    INDEX_ALREADY_EXISTS {
        @Override
        public String getHttpReasonPhrase() {
            return "Index already exists";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_INDEX_ALREADY_EXISTS;
        }
    },
    INVALID_RETRY_TOKEN {
        @Override
        public String getHttpReasonPhrase() {
            return "Invalid retry token";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_INVALID_RETRY_TOKEN;
        }
    },
    TABLE_NOT_READY {
        @Override
        public String getHttpReasonPhrase() {
            return "Table is not ready for this operation";
        }

        @Override
        public int getHttpStatusCode() {
            return 409;
        }

        @Override
        public String getType() {
            return TYPE_TABLE_NOT_READY;
        }
    },
    ETAG_MISMATCH {
        @Override
        public String getHttpReasonPhrase() {
            return "ETag mismatch";
        }

        @Override
        public int getHttpStatusCode() {
            return 412;
        }

        @Override
        public String getType() {
            return TYPE_ETAG_MISMATCH;
        }
    },
    INDEX_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Number of indexes per table exceeds limit";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_INDEX_LIMIT_EX;
        }
    },
    TABLE_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Number of tables per tenant exceeds limit";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_TABLE_LIMIT_EX;
        }
    },
    EVOLUTION_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Number of schema evolutions per table exceeds limit";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_EVO_LIMIT_EX;
        }
    },
    TABLE_DEPLOYMENT_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Requested deployment resource per table exceeds limit";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_TABLE_DEPLOY_LIMIT_EX;
        }
    },
    TENANT_DEPLOYMENT_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Requested deployment resource per tenant exceeds limit";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_TENANT_DEPLOY_LIMIT_EX;
        }
    },
    OPERATION_RATE_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Operation rate limit has been exceeded";
        }

        @Override
        public int getHttpStatusCode() {
            return 429;
        }

        @Override
        public String getType() {
            return TYPE_OPERATION_RATE_LIMIT_EX;
        }
    },
    SERVICE_LIMIT_EXCEEDED {
        @Override
        public String getHttpReasonPhrase() {
            return "Insufficient service capacity for this table";
        }

        @Override
        public int getHttpStatusCode() {
            return 503;
        }

        @Override
        public String getType() {
            return TYPE_SERVICE_UNAVAILABLE;
        }
    };

    /**
     * Type of ErrorCode constants
     */
    public static final String TYPE_NO_ERROR = "OK";
    public static final String TYPE_RESOURCE_NOT_FOUND = "ResourceNotFound";
    public static final String TYPE_UNSUPPORTED_VERB = "UnsupportedHttpVerb";
    public static final String TYPE_RESOURCE_EXISTS = "ResourceAlreadyExists";
    public static final String TYPE_SERVER_ERROR = "InternalServerError";
    public static final String TYPE_SERVICE_UNAVAILABLE = "ServiceUnavailable";
    public static final String TYPE_UNKNOWN_INT_ERROR = "UnknownInternalError";
    public static final String TYPE_UNKNOWN_ERROR = "UnknownError";
    public static final String TYPE_UNSUPPORTED_OP = "UnsupportedOperation";
    public static final String TYPE_INCORRECT_STATE = "IncorrectServiceState";
    public static final String TYPE_TABLE_NOT_FOUND = "TableNotFound";
    public static final String TYPE_INDEX_NOT_FOUND = "IndexNotFound";
    public static final String TYPE_ILLEGAL_ARGUMENT = "IllegalArgument";
    public static final String TYPE_TABLE_ALREADY_EXISTS = "TableAlreadyExists";
    public static final String TYPE_INDEX_ALREADY_EXISTS = "IndexAlreadyExists";
    public static final String TYPE_INVALID_RETRY_TOKEN = "InvalidRetryToken";
    public static final String TYPE_ETAG_MISMATCH = "ETagMismatch";
    public static final String TYPE_TABLE_LIMIT_EX = "TableLimitExceeded";
    public static final String TYPE_INDEX_LIMIT_EX = "IndexLimitExceeded";
    public static final String TYPE_EVO_LIMIT_EX = "EvolutionLimitExceeded";
    public static final String TYPE_TABLE_DEPLOY_LIMIT_EX =
        "TableDeploymentLimitExceeded";
    public static final String TYPE_TENANT_DEPLOY_LIMIT_EX =
        "TenantDeploymentLimitExceeded";
    public static final String TYPE_OPERATION_RATE_LIMIT_EX =
        "OperationRateLimitExceeded";
    public static final String TYPE_INSUFFICIENT_PERMISSION =
        "InsufficientPermission";
    public static final String TYPE_INVALID_AUTHORIZATION =
        "InvalidAuthorization";
    public static final String TYPE_TABLE_NOT_READY = "TableNotReady";

    /**
     * Return the ErrorCode instance with specified type.
     *
     * @param type the URI reference that identifies the problem.
     * @return the ErrorCode identified by given type, return null if cannot
     * find an ErrorCode with specified type.
     */
    public static ErrorCode getErrorCode(String type) {
        for (ErrorCode v : ErrorCode.values()) {
            if (type.equalsIgnoreCase(v.getType())) {
                return v;
            }
        }
        return null;
    }

    /**
     * Returns a short, human-readable summary of the problem reason phrase.
     */
    public abstract String getHttpReasonPhrase();

    /**
     * A URI reference that identifies the problem type.
     *
     * TODO: Regarding to rfc7807, this is required field in the error report,
     * which is supposed to be documentation URI that describes the error.
     * We don't have one right now, so use a String to represent the relative
     * URI as rfc7807 suggested.
     */
    public abstract String getType();

    /**
     * Returns the associated HTTP status code of this error code.
     */
    public abstract int getHttpStatusCode();
}
