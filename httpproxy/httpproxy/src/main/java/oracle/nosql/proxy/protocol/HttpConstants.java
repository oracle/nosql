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

package oracle.nosql.proxy.protocol;

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
     * A header for returning the proxy and kv versions used for a request
     *
     * Format of value is:
     *  "proxy=<version> kv=<version>"
     */
    public static final String PROXY_VERSION_HEADER = "x-nosql-version";

    /**
     * A header for returning the specific serial version of the proxy.
     * The serial version can change to indicate new features or modified
     * semantics and can be used by the SDKs to conditionally use them.
     *
     * Format of value is: "serial_version" where it is a simple integer,
     * e.g. "5"
     */
    public static final String PROXY_SERIAL_VERSION_HEADER =
        "x-nosql-serial-version";

    /**
     * A header for transferring the compartment id on an http request.
     */
    public static final String REQUEST_COMPARTMENT_ID = "x-nosql-compartment-id";

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
    public static final String APPLICATION_JSON_NOCHARSET = "application/json";

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
     * The default namespace, for onprem only
     */
    public static final String DEFAULT_NAMESPACE = "X-Nosql-Default-Ns";

    /*
     * Time delayed due to rate limiting, in milliseconds
     */
    public static final String X_RATELIMIT_DELAY = "X-Nosql-RL-Delay-Ms";

    /*
     * Overall time spent in request, in microseconds
     */
    public static final String X_NOSQL_US = "X-Nosql-Us";

    /*
     * Path Components
     */

    /**
     * The V0 version of the protocol
     */
    public static final String NOSQL_VERSION_0 = "V0";

    /**
     * The current version of the protocol
     */
    public static final String NOSQL_VERSION = "V2";

    /**
     * The service name prefix for public NoSQL services in the proxy
     */
    public static final String NOSQL_PATH_NAME = "nosql";

    /**
     * The service name of the proxy NoSQL data service (the driver protocol)
     */
    public static final String DATA_PATH_NAME = "data";

    /**
     * The service name of the tools name
     */
    public static final String TOOLS_PATH_NAME = "tools";

    /**
     * The service name of the logControl service
     */
    public static final String LOGCONTROL_PATH_NAME = "logcontrol";

    /**
     * The service name of the cacheUpdate service
     */
    public static final String CACHEUPDATE_PATH_NAME = "cacheupdate";

    /**
     * The service name of the filers service
     */
    public static final String FILTERS_PATH_NAME = "filters";

    /**
     * The path denoting a NoSQL request
     */
    public static final String NOSQL_DATA_PATH = makePath(NOSQL_PATH_NAME,
                                                          DATA_PATH_NAME);

    /**
     * The path to logcontrol requests
     */
    public static final String LOGCONTROL_PATH = makePath(TOOLS_PATH_NAME,
                                                          LOGCONTROL_PATH_NAME);

    /**
     * The path to update proxy security cache
     */
    public static final String CACHEUPDATE_PATH = makePath(TOOLS_PATH_NAME,
                                                       CACHEUPDATE_PATH_NAME);

    /**
     * The path to filters requests
     */
    public static final String FILTERS_PATH = makePath(TOOLS_PATH_NAME,
                                                       FILTERS_PATH_NAME);

    /**
     * The service name and path of the health service
     */
    public static final String HEALTH_PATH = "health";

    /**
     * KV-only security constants for login, logout, renew services
     */
    public static final String KVSECURITY_PREFIX = makePath(NOSQL_PATH_NAME,
                                                            "security");

    public static final String KVLOGIN_PATH = makePath(KVSECURITY_PREFIX,
                                                       "login");
    public static final String KVLOGOUT_PATH = makePath(KVSECURITY_PREFIX,
                                                       "logout");
    public static final String KVRENEW_PATH = makePath(KVSECURITY_PREFIX,
                                                       "renew");

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

    /**
     * Path component indicating move compartment operation
     */
    public static final String ACTIONS = "actions";
    public static final String CHANGE_COMPARTMENT = "changeCompartment";

    /**
     * Path component indicating setting table activity
     */
    public static final String SET_ACTIVITY = "setActivity";

    /**
     * Path components for add/drop replica;
     */
    public static final String REPLICA = "replica";
    public static final String CROSS_REGION_DDL = "crossregionddl";

    /**
     * Path component for get replica stats
     */
    public static final String REPLICA_STATS = "replicastats";

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
     * Table name
     */
    public static final String TABLE_NAME = "tablename";

    /**
     * Table id
     */
    public static final String TABLE_ID = "tableid";

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

    public static final String RETURN_COLLECTION = "returnCollection";

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
     * Used by setTableActive to specify the timestamp of the dml operation
     * which activates the table in IDLE state.
     */
    public static final String DML_MS = "dmlms";

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
     * The signature prefix in authorization header
     */
    public static final String SIGNATURE_PREFIX = "Signature ";

    /**
     * Creates a URI path from the arguments
     */
    public static String makePath(String ... s) {
        StringBuilder sb = new StringBuilder();
        sb.append(s[0]);
        for (int i = 1; i < s.length; i++) {
            sb.append("/");
            sb.append(s[i]);
        }
        return sb.toString();
    }

    public static boolean pathInURIAllVersions(String uri, String path) {
        final int offset = 3;
        if (uri.startsWith(NOSQL_VERSION) ||
            uri.startsWith(NOSQL_VERSION_0)) {
            return uri.startsWith(path, offset);
        }
        return false;
    }
}
