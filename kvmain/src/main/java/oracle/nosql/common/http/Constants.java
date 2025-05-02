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

package oracle.nosql.common.http;

/**
 * Constants used for HTTP headers and paths
 */
public class Constants {

    /**
     * The name of the content length header
     */
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_TYPE = "Content-Type";

    /*
     * Keep alive header
     */
    public static final String KEEP_ALIVE = "keep-alive";

    public static final String CONNECTION = "Connection";

    public static final String SET_COOKIE = "Set-Cookie";


    /*
     * Content type values
     */

    /*
     * Headers required for security. These need to be in each response
     */
    public static final String X_CONTENT_TYPE_OPTIONS = "X-content-type-options";
    public static final String X_CONTENT_TYPE_OPTIONS_VALUE = "nosniff";
    public static final String CONTENT_DISPOSITION = "content-disposition";
    public static final String CONTENT_DISPOSITION_VALUE =
        "attachment; filename=api.json";

    /**
     * Headers possibly set by the load balancer service to indicate original
     * IP address
     */
    public static final String X_REAL_IP = "x-real-ip";
    public static final String X_FORWARDED_FOR = "x-forwarded-for";

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
     * The service name of administration command interfaces
     */
    public static final String ADMIN_PATH_NAME = "admin";

    /**
     * The service name of the nosql prefix
     */
    public static final String NOSQL_PREFIX = makePath(NOSQL_VERSION,
                                                       NOSQL_PATH_NAME);

    /**
     * The path to administrative command web service
     */
    public static final String NOSQL_ADMIN_PATH =  makePath(NOSQL_PREFIX,
                                                            ADMIN_PATH_NAME);

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
