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

package oracle.kv.impl.sna.collector;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.impl.admin.CommandResult;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.fault.CommandFaultException;
import oracle.kv.impl.util.FileNames;
import oracle.kv.util.ErrorMessage;

/**
 * Publish collected monitor data to the logstash input files.
 *
 * Each type of metric goes into its own file, in order to make it easy to
 * describe the format of the metric and create a logstash input plugin
 * descriptor. The FileCollectorRecorder uses the java.util.logging package as
 * a publishing mechanism and maintains a collection of loggers, one per file.
 *
 * The amount of disk space that may be used by the collector files is
 * controlled by the global parameter, collectorStoragePerComponent. A SN's
 * collection limit is therefore the limitPerComponent * (snCapacity + 1),
 * because it uses one chunk per component, and one chunk for itself.
 *
 * We have to divide this per component budget into the different types of
 * metrics (files) generated by a component. Currently we do a static division,
 * and split the total size between stat types as following:
 *
 * SN and Admin will use one limitPerComponent size.
 *   PING_TYPE use 90%
 *   PLAN_TYPE use 10%
 * Each RN will use one limitPerComponent size.
 *   RNENV_TYPE use 30%
 *   RNEVENT_TYPE use 10%
 *   RNEXCEPTION_TYPE use 10%
 *   RNOP_TYPE use 20%
 *   RNTABLE_TYPE use 20%
 *   RNJVM_TYPE uses 5%
 *   RNENDPOINTGROUP_TYPE uses 5%
 *
 * There is a minimum amount of disk space for each metric type, so final
 * calculation is max(minAmountOfSpace, limitPerComponent * set percentage.)
 */
public class FileCollectorRecorder implements CollectorRecorder {

    /*
     * Each metric has a rotating set of files, specified by a file count and
     * file size. Each metric type has the file count, but its file size may
     * vary based on the percentage of the per-component budget it gets.
     * A metric's minimum storage is FILE_COUNT * MIN_FLE_SIZE
     */
    private final int FILE_COUNT = 10;
    private final int MIN_FILE_SIZE = 10000;

    /* Specified by global parameter: collectorStoragePerComponent */
    private long limitPerComponent;

    /* SN capacity */
    private int nComps;

    /* We have different type of stats to store, one type one Logger. */
    private Map<MetricType, Logger> loggers;
    /* Logger file name pattern for each type */
    private Map<MetricType, String> logPatterns;

    private void initPatternAndLogger(MetricType mType,
                                      File workingDir,
                                      String loggerPrefix,
                                      String metricFileName) {

        /* Setup the log file pattern for a given metric. */
        String pattern = workingDir + File.separator +
            metricFileName + "_%g." + FileNames.COLLECTOR_FILE_SUFFIX;
        logPatterns.put(mType, pattern);

        /* Each metric gets its own logger */
        Logger logger = Logger.getLogger(loggerPrefix + "." + mType.name());
        logger.setUseParentHandlers(false);
        loggers.put(mType, logger);
    }

    public FileCollectorRecorder(StorageNodeParams snp, GlobalParams gp) {

        File workingDir = FileNames.getCollectorDir(snp.getRootDirPath(),
                                                    gp.getKVStoreName(),
                                                    snp.getStorageNodeId());
        if (!workingDir.exists()) {
            workingDir.mkdirs();
        }

        logPatterns = new HashMap<MetricType, String>();
        loggers = new HashMap<MetricType, Logger>();

        final String loggerPrefix = snp.getRootDirPath() +
                                    FileCollectorRecorder.class.getName();

        initPatternAndLogger(MetricType.PING,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_PING_FILE_NAME);

        initPatternAndLogger(MetricType.PLAN,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_PLAN_FILE_NAME);

        initPatternAndLogger(MetricType.RNOP,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNOP_FILE_NAME);

        initPatternAndLogger(MetricType.RNEXCEPTION,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNEXCEPTION_FILE_NAME);

        initPatternAndLogger(MetricType.RNENV,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNENV_FILE_NAME);

        initPatternAndLogger(MetricType.RNEVENT,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNEVENT_FILE_NAME);

        initPatternAndLogger(MetricType.RNTABLE,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNTABLE_FILE_NAME);

        initPatternAndLogger(MetricType.RNJVM,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNJVM_FILE_NAME);

        initPatternAndLogger(MetricType.RNENDPOINTGROUP,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_RNENDPOINTGROUP_FILE_NAME);

        initPatternAndLogger(MetricType.LOGGINGSTAT,
                             workingDir,
                             loggerPrefix,
                             FileNames.COLLECTOR_LOGGINGSTATS_FILE_NAME);

        /* set limitPerComponent to -1 to force updating log handlers */
        limitPerComponent = -1;
        nComps = -1;
        updateParams(gp, snp);
    }

    /**
     * Calculate how much space each type of metric should get.
     */
    @Override
    public synchronized void updateParams(GlobalParams gp,
                                          StorageNodeParams snp) {
        boolean needUpdateHandler = false;
        if (gp != null) {
            long newLimitPerComponent = gp.getCollectorStoragePerComponent();
            if (limitPerComponent == -1 ||
                limitPerComponent != newLimitPerComponent) {
                limitPerComponent = newLimitPerComponent;
                needUpdateHandler = true;
            }
        }
        if (snp != null) {
            int newNComps = snp.getCapacity();
            if (nComps == -1 ||
                nComps != newNComps) {
                nComps = newNComps;
                needUpdateHandler = true;
            }
        }
        if (!needUpdateHandler) {
            return;
        }

        /* Ping and plans share the SN's allotment of space */
        final int pingLimit = Math.max((int) (0.9 * limitPerComponent),
                                       MIN_FILE_SIZE);
        final int planLimit = Math.max((int) (0.1 * limitPerComponent),
                                       MIN_FILE_SIZE);

        updateLoggerHandler(MetricType.PING, pingLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.PLAN, planLimit, FILE_COUNT);

        /*
         * A metric file on a SN stores one kind of metric, for all the RNs
         * on that SN.
         * TODO: this assumes that all the components on a SN are RNs.
         * In the future, adjust if an arbiter generates metrics.
         */
        final int rnEnvLimit =
            Math.max((int) (0.3 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnEventLimit =
            Math.max((int) (0.1 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnExceptionLimit =
            Math.max((int) (0.1 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnOpLimit =
            Math.max((int) (0.2 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnTableLimit =
            Math.max((int) (0.2 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnJVMLimit =
            Math.max((int) (0.03 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int rnEndpointGroupLimit =
            Math.max((int) (0.03 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);
        final int loggingStatsLimit =
            Math.max((int) (0.03 * limitPerComponent * nComps),
                     MIN_FILE_SIZE);

        updateLoggerHandler(MetricType.RNENV, rnEnvLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.RNEVENT, rnEventLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.RNEXCEPTION, rnExceptionLimit,
                            FILE_COUNT);
        updateLoggerHandler(MetricType.RNOP, rnOpLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.RNTABLE, rnTableLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.RNJVM, rnJVMLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.RNENDPOINTGROUP,
                            rnEndpointGroupLimit, FILE_COUNT);
        updateLoggerHandler(MetricType.LOGGINGSTAT, loggingStatsLimit,
                            FILE_COUNT);
    }

    @Override
    public synchronized void record(MetricType mType, String stat) {
        Logger logger = loggers.get(mType);
        if (logger == null) {
            return;
        }
        logger.info(stat);
    }

    @Override
    public void close() {
        for (Logger logger : loggers.values()) {
            try {
                for (Handler handler : logger.getHandlers()) {
                    handler.close();
                }
            } catch (Exception e) {}
        }
    }

    private void updateLoggerHandler(MetricType mType,
                                     int newLimitSize,
                                     int newFileCount) {

        Logger logger = loggers.get(mType);
        final String pattern = logPatterns.get(mType);
        if (logger == null || pattern == null) {
            return;
        }
        Handler[] handlers = logger.getHandlers();
        CollectorLogHandler existingHandler = null;
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof CollectorLogHandler) {
                    existingHandler = (CollectorLogHandler) h;
                    break;
                }
            }
        }

        if (existingHandler != null) {
            if (existingHandler.limit == newLimitSize &&
                existingHandler.count == newFileCount) {
                return;
            }
            existingHandler.close();
            logger.removeHandler(existingHandler);
        }

        try {
            final int limitSizePerFile = newLimitSize / newFileCount;
            CollectorLogHandler handler =
                new CollectorLogHandler(pattern, limitSizePerFile,
                                        newFileCount, true /* append */);
            handler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            });
            logger.addHandler(handler);
        } catch (Exception e) {
            throw new CommandFaultException(e.getMessage(), e,
                                            ErrorMessage.NOSQL_5200,
                                            CommandResult.NO_CLEANUP_JOBS);
        }
    }

    private class CollectorLogHandler extends java.util.logging.FileHandler {

        int limit;
        int count;

        public CollectorLogHandler(String pattern,
                                   int limit,
                                   int count,
                                   boolean append)
            throws IOException, SecurityException {

            super(pattern, limit, count, append);
            /* log all message. */
            setLevel(Level.ALL);
            this.limit = limit;
            this.count = count;
        }
    }
}
