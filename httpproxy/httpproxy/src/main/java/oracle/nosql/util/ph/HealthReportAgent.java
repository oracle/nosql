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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import oracle.nosql.common.sklogger.ScheduleStart;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.util.HostNameResolver;
import oracle.nosql.util.HttpRequest;
import oracle.nosql.util.HttpRequest.ConnectionHandler;
import oracle.nosql.util.HttpResponse;
import oracle.nosql.util.ph.record.InstanceHealthRecord;
import oracle.nosql.util.ssl.SSLConfig;
import oracle.nosql.util.ssl.SSLConnectionHandler;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * It will report component health data to PodHealth server periodically.
 * The default HealthSource will report GREEN status to PodHealth server.
 * Component can override the default HealthSource to execute more health
 * check.
 */
public class HealthReportAgent {

    private static long INTERVAL = 60_000;
    private static String COMPONENT_NAME_ENV = "COMPONENT_NAME";
    private static String COMPONENT_ID_ENV = "COMPONENT_ID";

    private static final HttpRequest request = new HttpRequest();
    private static final Gson gson = new GsonBuilder()
        .registerTypeAdapter(InstanceHealthRecord.class,
                             new InstanceHealthRecord.SerDe())
        .serializeNulls()
        .create();

    private final String hostName;
    private final String componentName;
    private final String componentId;
    private final long interval;
    private String reportUrl;
    private ConnectionHandler sslHandler;
    private final SkLogger logger;
    private final HealthSource source;
    private final ScheduledExecutorService executor;
    private Future<?> collectorFuture;

    public HealthReportAgent(boolean isGlobalComponent,
                             SkLogger logger,
                             HealthSource source) {
        this(isGlobalComponent, INTERVAL, logger, source);
    }

    public HealthReportAgent(boolean isGlobalComponent,
                             long interval,
                             SkLogger logger,
                             HealthSource source) {
        this.hostName = HostNameResolver.getHostName();
        this.componentName = System.getenv(COMPONENT_NAME_ENV);
        this.componentId = System.getenv(COMPONENT_ID_ENV);
        final String phUrl = URL.getPhUrl();
        if (phUrl != null) {
            if (isGlobalComponent) {
                reportUrl = phUrl + URL.TARGET_HEALTH_PATH +
                            "?targetType=global";
            } else {
                reportUrl = phUrl + URL.TARGET_HEALTH_PATH +
                            "?targetType=webtier";
            }

            if (SSLConfig.isInternalSSLEnabled()) {
                request.disableHostVerification();
                sslHandler = SSLConnectionHandler.getOCICertHandler(logger);
                if (sslHandler == null) {
                    logger.log(Level.WARNING,
                               "Initializing SSL ConnectionHandler failed, " +
                               "health report may fail");
                }
            }
        }
        this.interval = interval;
        this.logger = logger;
        this.source = source;
        executor = new ScheduledThreadPoolExecutor(1);
    }

    /**
     * The agent executor won't start if any environment is not set.
     */
    public void start() {
        if (hostName == null || componentName == null || componentId == null ||
            reportUrl == null) {
            return;
        }
        if (collectorFuture != null) {
            collectorFuture.cancel(true);
        }
        final long nowMs = System.currentTimeMillis();
        final long delayMs = ScheduleStart.calculateDelay(interval,
                                                          nowMs);
        collectorFuture =
            executor.scheduleAtFixedRate(new HealthReportTask(),
                                         delayMs, interval,
                                         TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (collectorFuture != null) {
            collectorFuture.cancel(true);
        }
        if (executor != null) {
            executor.shutdown();
        }
    }

    private class HealthReportTask implements Runnable {

        @Override
        public void run() {
            try {
                final long now = System.currentTimeMillis();
                final List<String> errors = new ArrayList<>();
                final HealthStatus status = source.getStatus(componentName,
                                                             componentId,
                                                             hostName,
                                                             logger,
                                                             errors);
                final InstanceHealthRecord record =
                    new InstanceHealthRecord(now, status, errors, "0", null,
                                             componentName, componentId,
                                             hostName);
                final String dataPayload = gson.toJson(record);
                final HttpResponse response = request.doHttpPost(reportUrl,
                                                                 dataPayload,
                                                                 sslHandler,
                                                                 null /* lc */);
                logger.fine(response.toString());
            } catch (Exception e) {
                logger.info(reportUrl + " report health data error: " + e);
            }
        }
    }

    public static class HealthSource {
        protected HealthStatus getStatus(String componentName,
                                         String componentId,
                                         String hostName,
                                         SkLogger logger,
                                         List<String> errors) {
            return HealthStatus.GREEN;
        }
    }
}
