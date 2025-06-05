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

package oracle.nosql.util.ph;

import oracle.nosql.util.ssl.SSLConfig;

/**
 * PodHealth server REST API URL.
 */
public class URL {
    /* Environment property for PH host:port */
    private static final String PH_HOSTPORT_ENV_PROP = "PH_HOSTPORT";

    private static String phUrl =
        (SSLConfig.isInternalSSLEnabled() ? "https://" : "http://") +
        System.getenv(PH_HOSTPORT_ENV_PROP);

    private static final String VERSION = "/V0";
    private static final String PREFIX = "/ph";

    public static final String POD_HEALTH_PATH =
        VERSION + PREFIX + "/podhealth";

    public static final String HEALTH_TOPO_PATH =
        VERSION + PREFIX + "/healthtopo";

    public static final String TARGET_HEALTH_PATH =
        VERSION + PREFIX + "/targethealth";

    public static final String CONFIG_PATH =
        VERSION + PREFIX + "/config";

    public static final String STATUS_PATH =
        VERSION + PREFIX + "/status";

    public static String getPhUrl() {
        return phUrl;
    }
}
