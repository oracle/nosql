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

package oracle.nosql.util.http;

/**
 * Constants used for HTTP headers and paths
 */
public class HttpConstants {

    /**
     * The http header that identifies the client scoped unique request id
     * associated with each request. The request header is returned by the
     * server, as part of the response and serves to associate the response
     * with the request.
     *
     * Note: We could use stream ids to associate a request and response.
     * However, the current handler pipeline on the client side operates at the
     * http level rather than the frame level, and consequently does not have
     * access to the stream id.
     */
    public static final String REQUEST_ID_HEADER = "x-nosql-request-id";

    /**
     * The version number associated with the serialization. The server will
     * use this version number when deserializing the http request and
     * serializing the http response.
     */
    public static final String REQUEST_SERDE_VERSION_HEADER =
        "x-nosql-serde-version";

    /**
     * A header for transferring a LogContext on an http request.
     */
    public static final String REQUEST_LOGCONTEXT_HEADER =
        "x-nosql-logcontext";

    /**
     * A header identifying the originating nosql service component.
     * (Currently set only by the proxy).
     */
    public static final String REQUEST_ORIGIN_HEADER =
        "x-nosql-originating-component";

    /**
     * Currently the only possible value for REQUEST_ORIGIN_HEADER.
     */
    public static final String REQUEST_ORIGIN_PROXY = "proxy";

    /**
     * Headers possibly set by the load balancer service to indicate original
     * IP address
     */
    public static final String X_REAL_IP_HEADER = "x-real-ip";
    public static final String X_FORWARDED_FOR_HEADER = "x-forwarded-for";

    /**
     * The name of the content type header
     */
    public static final String CONTENT_TYPE = "Content-Type";

    /**
     * The name of the content length header
     */
    public static final String CONTENT_LENGTH = "Content-Length";

    /*
     * Keep alive header
     */
    public static final String KEEP_ALIVE = "keep-alive";

    public static final String CONNECTION = "Connection";

    public static final String ACCEPT = "Accept";

    public static final String USER_AGENT = "User-Agent";

    /*
     * Content type values
     */
    public static final String APPLICATION_JSON =
        "application/json; charset=UTF-8";
    /* should this be "binary" ? */
    public static final String OCTET_STREAM = "application/octet-stream";

    /*
     * Headers required for security. These need to be in each response
     */
    public static final String X_CONTENT_TYPE_OPTIONS = "X-content-type-options";
    public static final String X_CONTENT_TYPE_OPTIONS_VALUE = "nosniff";
    public static final String CONTENT_DISPOSITION = "content-disposition";
    public static final String CONTENT_DISPOSITION_VALUE =
        "attachment; filename=api.json";

    /*
     * The name of the Authorization header
     */
    public static final String AUTHORIZATION = "Authorization";

    /*
     * The Access Token prefix in authorization header
     */
    public static final String TOKEN_PREFIX = "Bearer ";


    /*
     * Path Components
     */

    /**
     * The current version of the protocol
     */
    public static final String NOSQL_VERSION = "V0";

    /**
     * The service name prefix for public NoSQL services in the proxy
     */
    public static final String NOSQL_PATH_NAME = "nosql";

    /**
     * The service name of the proxy NoSQL data service (the driver protocol)
     */
    public static final String DATA_PATH_NAME = "data";

    /**
     * The service name of the proxy table service, which is a REST service
     * exposed to the public
     */
    public static final String TABLE_PATH_NAME = "tables";

    /**
     * The service name of the health service
     */
    public static final String HEALTH_PATH_NAME = "health";

    /**
     * The service name of the version service
     */
    public static final String VERSION_PATH_NAME = "version";

    /**
     * The service name of the tools name
     */
    public static final String TOOLS_PATH_NAME = "tools";

    /**
     * The service name of the logControl service
     */
    public static final String LOGCONTROL_PATH_NAME = "logcontrol";

    /**
     * The service name of administration command interfaces
     */
    public static final String ADMIN_PATH_NAME = "admin";

    /**
     * The service name of the tools prefix
     */
    public static final String TOOLS_PREFIX = makePath(NOSQL_VERSION,
                                                       TOOLS_PATH_NAME);

    /**
     * The service name of the nosql prefix
     */
    public static final String NOSQL_PREFIX = makePath(NOSQL_VERSION,
                                                       NOSQL_PATH_NAME);

    /**
     * The path denoting a NoSQL request
     */
    public static final String NOSQL_DATA_PATH = makePath(NOSQL_PREFIX,
                                                          DATA_PATH_NAME);

    /**
     * The path denoting a NoSQL table request
     */
    public static final String NOSQL_TABLE_PATH = makePath(NOSQL_PREFIX,
                                                           TABLE_PATH_NAME);

    /**
     * The path denoting a health service request
     */
    public static final String HEALTH_PATH =  makePath(NOSQL_VERSION,
                                                       HEALTH_PATH_NAME);

    /**
     * The path denoting a version service request
     */
    public static final String VERSION_PATH = makePath(TOOLS_PREFIX,
                                                       VERSION_PATH_NAME);

    /**
     * The path to logcontrol requests
     */
    public static final String LOGCONTROL_PATH = makePath(TOOLS_PREFIX,
                                                          LOGCONTROL_PATH_NAME);

    /**
     * The path to administrative command web service
     */
    public static final String NOSQL_ADMIN_PATH =  makePath(NOSQL_PREFIX,
                                                            ADMIN_PATH_NAME);

    /**
     * Path component indicating table usage
     */
    public static final String TABLE_USAGE = "usage";

    /**
     * Path component indicating table history
     */
    public static final String TABLE_HISTORY = "history";

    /**
     * Path component indicating store info (internal use by tenant manager)
     */
    public static final String TABLE_STOREINFO = "storeinfo";

    /**
     * Path component indicating indexes operation
     */
    public static final String TABLE_INDEXES = "indexes";

    /*
     * Query Parameters used by GET operations
     */

    /**
     * Tenant id, required for all paths
     */
    public static final String TENANT_ID = "tenantid";

    /**
     * Compartment id
     */
    public static final String COMPARTMENT_ID = "compartmentid";

    /**
     * If exists, used for drop table, index
     */
    public static final String IF_EXISTS = "ifexists";

    /**
     * verb, used by retrieve request history
     */
    public static final String VERB = "verb";

    /**
     * Operation id, used optionally by GET table, calling the SC
     */
    public static final String OPERATION_ID = "operationid";

    /**
     * Used by list tables for paging (history, list)
     */
    public static final String START_INDEX = "start_index";

    /**
     * Used for numeric limits to return objects (usage, history, list)
     */
    public static final String LIMIT = "limit";

    /**
     * Used to specify a log level for LogControlService.
     */
    public static final String LOG_LEVEL = "level";

    /**
     * Used to specify an entrypoint for LogControlService.
     */
    public static final String ENTRYPOINT = "entrypoint";

    /**
     * Used by usage to return a range of records
     */
    public static final String START_TIMESTAMP = "start_timestamp";
    public static final String END_TIMESTAMP = "end_timestamp";

    /**
     * Used for paginating results that might be voluminous.
     */
    public static final String PAGE_SIZE = "pagesz";
    public static final String PAGE_NUMBER = "pageno";
    /**
     * Used for list table and get indexes
     */
    public static final String NAME_ONLY = "name_only";
    public static final String NAME_PATTERN = "name_pattern";
    public static final String STATE = "state";
    public static final String SORT_BY = "sort_by";
    public static final String SORT_ORDER_ASC = "sort_order_asc";

    /**
     * Used for WorkRequest related APIs
     */
    public static final String WORK_REQUEST_ID = "workRequestId";

    /**
     * Used for Backfill to RQS API
     */
    public static final String INCLUDE_DROPPED_TABLES = "include_dropped_tables";
    public static final String ID = "id";

    /**
     * Use this key to represent non-exist entry key for admin sub service
     * lookup.
     */
    public static final String NULL_KEY = "NULL";

    /**
     * Prefix of the basic authentication information.
     */
    public static final String BASIC_PREFIX = "Basic ";

    /**
     * Prefix of the authorization field for access token.
     */
    public static final String BEARER_PREFIX = "Bearer ";

    /**
     * Creates a URI path from the arguments
     */
    private static String makePath(String ... s) {
        StringBuilder sb = new StringBuilder();
        sb.append(s[0]);
        for (int i = 1; i < s.length; i++) {
            sb.append("/");
            sb.append(s[i]);
        }
        return sb.toString();
    }
}
