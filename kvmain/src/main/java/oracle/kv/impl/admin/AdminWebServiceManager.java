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

package oracle.kv.impl.admin;

import static oracle.kv.impl.util.ObjectUtil.checkNull;
import static oracle.nosql.common.http.Constants.ADMIN_PATH_NAME;

import java.util.logging.Logger;

import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.admin.web.AdminWebService;

import oracle.nosql.common.http.HttpServer;
import oracle.nosql.common.http.LogControl;
import oracle.nosql.common.http.ServiceRequestHandler;
import oracle.nosql.common.sklogger.SkLogger;

import io.netty.handler.ssl.SslContext;

/**
 * A class to encapsulate the admin web service. The admin web service requires
 * classes in oracle.common.http packages, which are not available for KVLocal.
 * KVLocal doesn't use the admin web service, so keep the references separate
 * so that they will not be resolved.
 */
class AdminWebServiceManager {
    private Logger logger;
    private HttpServer webServer;
    private AdminWebService adminWebService;

    /** Create an instance and create the underlying admin web service. */
    AdminWebServiceManager(Logger logger,
                           CommandService secureCommandService,
                           LoginService loginService,
                           StorageNodeParams snp,
                           int httpPort,
                           int httpsPort,
                           SslContext ctx) {
        this.logger = checkNull("logger", logger);

        try {

            final SkLogger skLogger = new SkLogger(logger);
            final LogControl logControl = new LogControl();
            final ServiceRequestHandler handler =
                new ServiceRequestHandler(logControl, skLogger);
            adminWebService =
                new AdminWebService(secureCommandService,
                                    loginService,
                                    snp.getHostname(),
                                    snp.getRegistryPort(),
                                    skLogger)
                .initService();
            handler.addService(ADMIN_PATH_NAME, adminWebService);

            webServer = new HttpServer(snp.getHostname(),
                                       httpPort, /* httpPort */
                                       httpsPort, /* httpsPort */
                                       0, /* numAcceptThreads */
                                       0, /* numWorkerThreads */
                                       0, /* maxRequestSize */
                                       0, /* maxChunkSize */
                                       0, /* idleReadTimeout */
                                       handler,
                                       ctx,
                                       skLogger);
            logger.info("Started web service");
        } catch (Exception e) {
            logger.severe("Fail to start up HTTP server: " + e);
            throw new IllegalStateException(e);
        }
    }

    /** Update the logger. */
    void resetLogger(Logger newLogger) {
        checkNull("logger", newLogger);
        logger = newLogger;
        if (webServer != null) {
            webServer.getLogger().resetLogger(newLogger);
        }
    }

    /** Shutdown the underlying admin web service. */
    void shutdown() {
        if (webServer == null) {
            return;
        }
        try {
            adminWebService.shutdown();
            webServer.shutdown();
            webServer = null;
            adminWebService = null;
        } catch (Exception e) {
            final String message = "Can't shutdown admin web server";
            logger.severe(message);
            throw new IllegalStateException(message, e);
        }
    }
}
