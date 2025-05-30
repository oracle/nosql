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

package oracle.nosql.proxy.cloud;

import java.util.List;

import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.security.AccessChecker;
import oracle.nosql.util.HttpServerHealth;
import oracle.nosql.util.ph.HealthReportAgent.HealthSource;
import oracle.nosql.util.ph.HealthStatus;

/**
 * ProxyHealthSource reports whether this proxy is in good health. It checks if
 *  - the main proxy functionality is working
 *  - it can do authorization
 *  - its httpserver is operational, and it can accept incoming requests
 */
public class ProxyHealthSource extends HealthSource {

    private final CloudDataService service;
    private final AccessChecker ac;
    private final HttpServerHealth httpServerHealth;

    public ProxyHealthSource(CloudDataService service,
                             AccessChecker ac,
                             HttpServerHealth httpServerHealth) {
        this.service = service;
        this.ac = ac;
        this.httpServerHealth = httpServerHealth;
    }

    @Override
    public HealthStatus getStatus(String componentName,
                                  String componentId,
                                  String hostName,
                                  SkLogger logger,
                                  List<String> errors) {
        HealthStatus result = service.checkStatus(errors);
        HealthStatus acStatus = ac.checkConnectivity(errors, logger);
        if (acStatus.ordinal() > result.ordinal()) {
            result = acStatus;
        }
        HealthStatus shStatus = httpServerHealth.checkHealth(errors, logger);
        if (shStatus.ordinal() > result.ordinal()) {
            result = shStatus;
        }
        return result;
    }
}
