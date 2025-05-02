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

package oracle.kv.impl.util.server;

import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import oracle.kv.KVVersion;
import oracle.kv.impl.admin.AdminServiceParams;
import oracle.kv.impl.admin.param.AdminParams;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.ClientId;
import oracle.kv.impl.arb.ArbNodeService;
import oracle.kv.impl.fault.ProcessExitCode;
import oracle.kv.impl.monitor.AdminDirectHandler;
import oracle.kv.impl.monitor.AgentRepository;
import oracle.kv.impl.monitor.LogToMonitorHandler;
import oracle.kv.impl.monitor.MonitorAgentHandler;
import oracle.kv.impl.monitor.MonitorKeeper;
import oracle.kv.impl.rep.RepNodeService;
import oracle.kv.impl.topo.AdminId;
import oracle.kv.impl.topo.ArbNodeId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.ResourceId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.CommonLoggerUtils;
import oracle.kv.impl.util.ConfigurableService.ServiceStatus;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.FormatUtils;
import oracle.kv.util.CompressingFileHandler;
import oracle.kv.util.FileHandler;
import oracle.kv.util.NonCompressingFileHandler;
import oracle.nosql.common.contextlogger.ContextFormatter;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.ReplicatedEnvironment.State;
import com.sleepycat.je.rep.StateChangeEvent;

/**
 * General utilities for creating and formatting java.util loggers and handlers.
 */
public class LoggerUtils {

    /*
     * A general Logger provided by this class is hooked up to three handlers:
     *
     *  1. ConsoleHandler, which will display to stdout on the local machine
     *  2. FileHandler, which will display to the <resourceId>.log file on the
     *     local machine.
     *     Note that special loggers are created to also connect to the .perf
     *     and .stat files.
     *  3. MonitorHandler, which will funnel output to the Monitor. If this
     *     service is remote and implements a MonitorAgent, the output is
     *     saved in the buffered repository implemented by the MonitorAgent.
     *     If the service is local to the Admin process, the handler publishes
     *     it directly to Monitoring.
     *
     * Shared Handlers:
     *
     * Each logging resource must share a FileHandler, because different
     * FileHandlers are seen as conflicting by java.util.logging, and will open
     * unique files. Likewise, each logging resource shares a MonitorHandler,
     * which funnels logging output into the monitor agent buffer, for later
     * pickup by the monitoring system.
     *
     * This mapping is managed with the FileHandler and MonitorHandler map,
     * which are keyed by kvstore name and resource id. Note that when the
     * caller wants to obtain a logger, this obliges the caller to have the
     * kvstore name in hand. FileHandlers also require additional parameters to
     * configure the count and limit of files. The MonitorAgentHandler must be
     * configured to bound the size of the recording repository.
     *
     * Making the kvstore name and required parameters available when logging
     * means that parameter classes are passed downward many levels. That
     * creates a minor dissonance in pattern. We've centralized the file
     * handler map in this static map, whereas we've passed the parameter class
     * downward. Arguably we could have also passed the handler maps, or some
     * handle to it, in much the same way as we pass the parameters.
     *
     * We consciously chose not to do so. The handler map shouldn't be
     * referenced by the parameter class, and we don't want to pass additional
     * parameters downward.
     */

    /**
     * Directs logging output to a file per service, on the local node. The
     * file handlers are kept in a single global map to make it easier to
     * clean up upon exit.
     */
    private static final ConcurrentMap<ServiceHandlerKey, FileHandler>
        FILE_HANDLER_MAP = new ConcurrentHashMap<>();

    /**
     * Directs iostat style perf output to a file on the admin node.
     */
    private static final ConcurrentMap<ServiceHandlerKey, FileHandler>
        PERF_FILE_HANDLER_MAP = new ConcurrentHashMap<>();

    /**
     * Directs rep/environment stat output to a file on the admin node.
     */
    private static final ConcurrentMap<ServiceHandlerKey, FileHandler>
        STAT_FILE_HANDLER_MAP = new ConcurrentHashMap<>();

    /**
     * Directs logging output to a single monitor agent repository per service,
     * on the local node.
     */
    private static final ConcurrentMap<ServiceHandlerKey, LogToMonitorHandler>
        MONITOR_HANDLER_MAP = new ConcurrentHashMap<>();

    /**
     * Tracks counts of SEVERE, WARNING, and INFO log entries.
     */
    private static final ConcurrentMap<ServiceHandlerKey, LoggingStatsHandler>
        LOGGING_STATS_HANDLER_MAP = new ConcurrentHashMap<>();

    /**
     * A single bootstrap log file that is not associated with a store.  This
     * is created by the SNA and used for logging and debugging bootstrap
     * startup state.
     */
    private static volatile Logger bootstrapLogger;

    /**
     * The name of the system property to set to revert to using FileHandler in
     * case CompressingFileHandler or NonCompressingFileHandler cause trouble.
     * Remove when the new facilities seem trustworthy.
     */
    private static final String USE_FILE_HANDLER_PROPERTY =
        "oracle.kv.useFileHandler";

    /** Whether to use FileHandler. */
    private static final boolean USE_FILE_HANDLER =
        Boolean.getBoolean(USE_FILE_HANDLER_PROPERTY);

    /**
     * Return a String to be used in error messages and other usage situations
     * that describes where the storewide logging file is.
     */
    public static String getStorewideLogName(String rootDirPath,
                                             String kvStoreName) {
        File loggingDir = FileNames.getLoggingDir
            (new File(rootDirPath), kvStoreName);
        return loggingDir.getPath() +
            File.separator + kvStoreName + "_{0..N}." +
            FileNames.LOG_FILE_SUFFIX;
    }

    /**
     * Get the single bootstrap logger.  There may be one of these for the SNA
     * and one for the bootstrap admin, but not in the same process.
     */
    public static Logger getBootstrapLogger(String kvdir,
                                            String filename,
                                            String label) {

        final Logger currentBootstrapLogger = bootstrapLogger;
        if (currentBootstrapLogger != null) {
            return currentBootstrapLogger;
        }
        final Logger newBootstrapLogger = Logger.getLogger(filename);
        bootstrapLogger = newBootstrapLogger;
        newBootstrapLogger.setUseParentHandlers(false);

        String logFilePattern = makeFilePattern(kvdir, filename,
                                                FileNames.LOG_FILE_SUFFIX);
        try {
            FileHandler newHandler =
                new oracle.kv.util.FileHandler(logFilePattern,
                                               1000000, /* limit */
                                               20, /* count */
                                               true /* append */);
            newHandler.setFormatter(new ContextFormatter(label));
            newBootstrapLogger.addHandler(newHandler);
            addConsoleHandler(newBootstrapLogger, label);
        } catch (IOException e) {
            throw new IllegalStateException("Problem creating bootstrap " +
                                            "log file: " + logFilePattern);
        }
        /*
         * Note that we do not collect stats for WARNING, SEVERE, and INFO
         * logging for the bootstrap logger.
         */
        return newBootstrapLogger;
    }

    /**
     * This flavor of logger logs only to the console, and is for use by the
     * client library, which does not have disk access nor monitoring.
     */
    public static Logger getLogger(Class<?> cl,
                                   ClientId clientId) {
        return LoggerUtils.getLogger(cl,
                                     clientId.toString(),
                                     clientId,
                                     null,  /* globalParams */
                                     null); /* storageNodeParams */
    }

    /**
     * For logging with no resource id, which could be from a pre-registration
     * StorageNodeAgent, or from tests. Monitoring goes only to console, and
     * does not go to a file nor to monitoring.
     * @param label descriptive name used to prefix logging output.
     */
    public static Logger getLogger(Class<?> cl, String label) {
        return LoggerUtils.getLogger(cl,
                                     label,
                                     null,  /* resourceId */
                                     null,  /* globalParams */
                                     null); /* storageNodeParams */
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and the Monitor.
     */
    public static Logger getLogger(Class<?> cl,
                                   AdminServiceParams params) {
        AdminId adminId = params.getAdminParams().getAdminId();
        return LoggerUtils.getLogger(cl,
                                     adminId.toString(),
                                     adminId,
                                     params.getGlobalParams(),
                                     params.getStorageNodeParams());
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its local MonitorAgent.
     */
    public static Logger getLogger(Class<?> cl,
                                   RepNodeService.Params params) {

        RepNodeId repNodeId = params.getRepNodeParams().getRepNodeId();
        return LoggerUtils.getLogger(cl,
                                     repNodeId.toString(),
                                     repNodeId,
                                     params.getGlobalParams(),
                                     params.getStorageNodeParams(),
                                     params.getRepNodeParams());
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its local MonitorAgent.
     */
    public static Logger getLogger(Class<?> cl,
                                   GlobalParams globalParams,
                                   StorageNodeParams storageNodeParams) {

        StorageNodeId storageNodeId = storageNodeParams.getStorageNodeId();
        return LoggerUtils.getLogger(cl,
                                     storageNodeId.toString(),
                                     storageNodeId,
                                     globalParams,
                                     storageNodeParams);
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its local MonitorAgent.
     */
    public static Logger getLogger(Class<?> cl,
                                   ArbNodeService.Params params) {

        ArbNodeId arbNodeId = params.getArbNodeParams().getArbNodeId();
        return LoggerUtils.getLogger(cl,
                                     arbNodeId.toString(),
                                     arbNodeId,
                                     params.getGlobalParams(),
                                     params.getStorageNodeParams());
    }

    /**
     * Get a logger which will only log to the  resource's logging file. It's
     * meant for temporary use, to log information at service shutdown.
     * Global and StorageNodeParams must not be null.
     */
    public static Logger getFileOnlyLogger
        (Class<?> cl,
         ResourceId resourceId,
         GlobalParams globalParams,
         StorageNodeParams storageNodeParams,
         RepNodeParams repNodeParams) {

        Logger logger = Logger.getLogger(cl.getName() + ".TEMP_" + resourceId);
        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasFileHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                }
            }
        }

        if (!hasFileHandler) {
            String logDir = storageNodeParams.getRootDirPath();
            boolean isRoot = true;
            if ((repNodeParams != null) &&
                (repNodeParams.getLogDirectoryPath() != null)) {
                logDir = repNodeParams.getLogDirectoryPath();
                isRoot = false;
            }
            addLogFileHandler(logger,
                              resourceId.toString(),
                              resourceId.toString(),
                              globalParams.getKVStoreName(),
                              new File(logDir),
                              storageNodeParams.getLogFileLimit(),
                              storageNodeParams.getLogFileCount(),
                              storageNodeParams.getLogFileCompression(),
                              isRoot);
        }
        return logger;
    }

    /**
     * Get a logger which will only log to the resource's perf file.
     */
    public static Logger getPerfFileLogger(Class<?> cl,
                                           GlobalParams globalParams,
                                           StorageNodeParams snParams ) {

        String kvName = globalParams.getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + ".PERF_" + kvName);
        logger.setUseParentHandlers(false);

        Handler[] handlers = logger.getHandlers();
        boolean hasFileHandler = false;
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                    break;
                }
            }
        }

        if (hasFileHandler) {
            return logger;
        }

        /* Send this logger's output to a storewide .perf file. */
        addFileHandler(PERF_FILE_HANDLER_MAP,
                       logger,
                       new Formatter() {
                           @Override
                           public String format(LogRecord record) {
                               return record.getMessage() + "\n";
                           }
                       },
                       kvName,
                       kvName,
                       new File(snParams.getRootDirPath()),
                       FileNames.PERF_FILE_SUFFIX,
                       snParams.getLogFileLimit(),
                       snParams.getLogFileCount(),
                       snParams.getLogFileCompression(),
                       true /* isRoot */);
        return logger;
    }

    /**
     * Get a logger which will only log to the resource's stats file.
     */
    public static Logger getStatFileLogger(Class<?> cl,
                                           GlobalParams globalParams,
                                           StorageNodeParams snParams ) {

        String kvName = globalParams.getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + ".STAT_" + kvName);
        logger.setUseParentHandlers(false);

        Handler[] handlers = logger.getHandlers();
        boolean hasFileHandler = false;
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                    break;
                }
            }
        }

        if (hasFileHandler) {
            return logger;
        }

        /* Send this logger's output to a storewide .stat file. */
        addFileHandler(STAT_FILE_HANDLER_MAP,
                       logger,
                       new ContextFormatter(kvName),
                       kvName,
                       kvName,
                       new File(snParams.getRootDirPath()),
                       FileNames.STAT_FILE_SUFFIX,
                       snParams.getLogFileLimit(),
                       snParams.getLogFileCount(),
                       snParams.getLogFileCompression(),
                       true /* isRoot */);

        return logger;
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its Monitor handler.
     */
    public static Logger getLogger(Class<?> cl,
                                   String prefix,
                                   ResourceId resourceId,
                                   GlobalParams globalParams,
                                   StorageNodeParams storageNodeParams) {
        return getLogger(cl, prefix, resourceId, globalParams,
                         storageNodeParams, null);
    }

    /**
     * Obtain a logger which sends output to the console, its local logging
     * file, and its Monitor handler.
     */
    public static Logger getLogger(Class<?> cl,
                                   String prefix,
                                   ResourceId resourceId,
                                   GlobalParams globalParams,
                                   StorageNodeParams storageNodeParams,
                                   RepNodeParams repNodeParams) {

        Logger logger = Logger.getLogger(cl.getName() + "." + resourceId);
        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasConsoleHandler = false;
        boolean hasFileHandler = false;
        boolean hasAdminDirectHandler = false;
        boolean hasLoggingStatsHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.ConsoleHandler) {
                    hasConsoleHandler = true;
                } else if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                } else if (h instanceof
                           oracle.kv.impl.monitor.LogToMonitorHandler) {
                    hasAdminDirectHandler = true;
                } else if (h instanceof LoggingStatsHandler) {
                    hasLoggingStatsHandler = true;
                }
            }
        }

        if (!hasConsoleHandler) {
            addConsoleHandler(logger, prefix);
        }

        /*
         * Only loggers that belong to kvstore classes that know their kvstore
         * directories, and are components with resource ids, log into a file
         */
        if (globalParams != null) {
            if ((storageNodeParams != null) && (!hasFileHandler)) {
                String logDir = storageNodeParams.getRootDirPath();
                boolean isRoot = true;
                if ((repNodeParams != null) &&
                    (repNodeParams.getLogDirectoryPath() != null)) {
                    logDir = repNodeParams.getLogDirectoryPath();
                    isRoot = false;
                }
                addLogFileHandler(logger,
                                  prefix,
                                  resourceId.toString(),
                                  globalParams.getKVStoreName(),
                                  new File(logDir),
                                  storageNodeParams.getLogFileLimit(),
                                  storageNodeParams.getLogFileCount(),
                                  storageNodeParams.getLogFileCompression(),
                                  isRoot);
            }

            /*
             * If this service has a monitorHandler registered, connect to that
             * handler. TODO: Do we need to check for a current monitor handler?
             */
            if (!hasAdminDirectHandler) {
                addMonitorHandler(logger,
                                  globalParams.getKVStoreName(),
                                  resourceId);
            }

            if (!hasLoggingStatsHandler) {
                addLoggingStatsHandler(logger, resourceId,
                                       globalParams.getKVStoreName());
            }
        }

        return logger;
    }

    /**
     * This logger displays output for the store wide view provided by the
     * monitoring node. Logging input comes from MonitorAgents and data sent
     * directly to the Monitor, and the resulting output is displayed to the
     * console and to a store wide log file. Used only by the Monitor. TODO:
     * funnel to UI.
     */
    public static Logger
        getStorewideViewLogger(Class<?> cl,  AdminServiceParams adminParams) {

        String storeName = adminParams.getGlobalParams().getKVStoreName();
        Logger logger = Logger.getLogger(cl.getName() + "." + storeName);

        logger.setUseParentHandlers(false);

        /* Check whether the logger already has existing handlers. */
        boolean hasConsoleHandler = false;
        boolean hasFileHandler = false;

        /*
         * [#18277] Add null check of logger.getHandlers() because the Resin
         * app server's implementation of logging can return null instead of an
         * empty array.
         */
        Handler[] handlers = logger.getHandlers();
        if (handlers != null) {
            for (Handler h : handlers) {
                if (h instanceof oracle.kv.util.StoreConsoleHandler) {
                    hasConsoleHandler = true;
                } else if (h instanceof oracle.kv.util.FileHandler) {
                    hasFileHandler = true;
                }
            }
        }

        if (!hasConsoleHandler) {
            Handler handler = new oracle.kv.util.StoreConsoleHandler();
            handler.setFormatter(new ContextFormatter());
            logger.addHandler(handler);
        }

        /**
         * Use log file count and limit from AdminParams, not StorageNodeParams
         */
        if (!hasFileHandler) {
            StorageNodeParams snp = adminParams.getStorageNodeParams();
            GlobalParams gp = adminParams.getGlobalParams();
            AdminParams ap = adminParams.getAdminParams();
            addLogFileHandler(logger,
                              null, // label
                              gp.getKVStoreName(),
                              gp.getKVStoreName(),
                              new File(snp.getRootDirPath()),
                              ap.getLogFileLimit(),
                              ap.getLogFileCount(),
                              snp.getLogFileCompression(),
                              true /* isRoot */);
        }
        return logger;
    }

    /**
     * Each service that implements a MonitorAgent which collects logging
     * output should register a handler in its name, before any loggers come
     * up.
     * @param kvName
     * @param resourceId
     * @param agentRepository
     */
    public static void
        registerMonitorAgentBuffer(String kvName,
                                   ResourceId resourceId,
                                   AgentRepository agentRepository) {

        /*
         * Create a handler that is just a pass through to the monitor agent's
         * buffer.
         */
        MonitorAgentHandler handler = new MonitorAgentHandler(agentRepository);

        String resourceName = resourceId.toString();
        handler.setFormatter(new ContextFormatter(resourceName));
        MONITOR_HANDLER_MAP.put(new ServiceHandlerKey(resourceName, kvName),
                                handler);

    }

    public static void registerMonitorAdminHandler(String kvName,
                                                   AdminId adminId,
                                                   MonitorKeeper admin) {
        LogToMonitorHandler handler = new AdminDirectHandler(admin);
        String resourceName = adminId.toString();
        handler.setFormatter(new ContextFormatter(resourceName));
        MONITOR_HANDLER_MAP.put(new ServiceHandlerKey(resourceName, kvName),
                                handler);
    }

    /**
     * Attach a handler which directs output to the monitoring system. If this
     * is a remote service, the logging output goes to the agent repository. If
     * this is a service that is on the Admin process, the logging output goes
     * directly to the Monitor.
     */
    private static void addMonitorHandler(Logger logger,
                                          String kvName,
                                          ResourceId resourceId) {

        Handler handler =
            MONITOR_HANDLER_MAP.get(new ServiceHandlerKey
                                    (resourceId.toString(), kvName));
        if (handler != null) {
            logger.addHandler(handler);
        }
    }

    /**
     * Attach a handler which directs output to stdout.
     */
    private static void addConsoleHandler(Logger logger, String prefix) {

        Handler handler = new oracle.kv.util.ConsoleHandler();
        handler.setFormatter(new ContextFormatter(prefix));
        logger.addHandler(handler);
    }

    /**
     * Add a handler that sends this logger's output to a .log file.
     */
    private static void addLogFileHandler(Logger logger,
                                          String label,
                                          String resourceName,
                                          String kvName,
                                          File rootDir,
                                          int fileLimit,
                                          int fileCount,
                                          boolean fileCompression,
                                          boolean isRoot) {
        addFileHandler(FILE_HANDLER_MAP, logger, new ContextFormatter(label),
                       resourceName, kvName, rootDir,
                       FileNames.LOG_FILE_SUFFIX, fileLimit, fileCount,
                       fileCompression, isRoot);
    }

    /**
     * Attach a handler which directs output to a logging file on the node.
     */
    private static void addFileHandler
        (ConcurrentMap<ServiceHandlerKey,FileHandler> map,
         Logger logger,
         Formatter formatter,
         String resourceName,
         String kvName,
         File rootDir,
         String suffix,
         int fileLimit,
         int fileCount,
         boolean fileCompression,
         boolean isRoot) {

        /*
         * Avoid calling new FileHandler unnecessarily, because the FileHandler
         * constructor will actually create the log file. Check the map first
         * to see if a handler exists.
         */
        ServiceHandlerKey handlerKey =
            new ServiceHandlerKey(resourceName, kvName);
        Handler existing = map.get(handlerKey);

        /*
         * A FileHandler exists, just connect it to this logger unless the
         * parameters have changed, in which case close the existing one and
         * make a new one.
         */
        if (existing != null) {
            oracle.kv.util.FileHandler fh =
                (oracle.kv.util.FileHandler) existing;
            if (fh.getLimit() == fileLimit && fh.getCount() == fileCount) {
                logger.addHandler(existing);
                return;
            }
            existing.close();
            map.remove(handlerKey);
            existing = null;
        }

        /* A FileHandler does not exist yet, so create one. */
        FileHandler newHandler;
        try {
            String logFilePattern;
            if (isRoot) {
                FileNames.makeLoggingDir(rootDir, kvName);
                logFilePattern = makeFilePattern
                    (FileNames.getLoggingDir(rootDir, kvName).getPath(),
                     resourceName, suffix);
            } else {
                FileNames.makeRNLoggingDir(rootDir);
                logFilePattern = makeFilePattern(rootDir.getPath(),
                                                 resourceName, suffix);
            }

            /*
             * Convert illegal values to defaults to avoid illegal argument
             * exceptions for these values since the associated parameters do
             * not enforce these requirements. These conversions match the way
             * that the java.util.logging package handles these values when
             * they are specified in the logging configuration file.
             */
            if (fileLimit < 0) {
                fileLimit = 0;
            }
            if (fileCount < 1) {
                fileCount = 1;
            }

            if (USE_FILE_HANDLER) {
                newHandler = new FileHandler(logFilePattern,
                                             fileLimit,
                                             fileCount,
                                             true /* append */);
            } else {
                /*
                 * Only include extra logging for standard logging files, not
                 * stat or perf files where those entries would not fit with
                 * the format of other entries
                 */
                final boolean extraLogging =
                    logFilePattern.endsWith(FileNames.LOG_FILE_SUFFIX);
                if (fileCompression) {
                    newHandler = new CompressingFileHandler(
                        logFilePattern, fileLimit, fileCount, extraLogging);
                } else {
                    newHandler = new NonCompressingFileHandler(
                        logFilePattern, fileLimit, fileCount, extraLogging);
                }
            }
            newHandler.setFormatter(formatter);
        } catch (SecurityException e) {
            throw new IllegalStateException
                ("Problem creating log file for logger " + resourceName, e);
        } catch (IOException e) {
            throw new IllegalStateException
                ("Problem creating log file for logger " + resourceName, e);
        }

        existing = map.putIfAbsent(handlerKey, newHandler);
        if (existing == null) {
            logger.addHandler(newHandler);
        } else {
            /*
             * Something else beat us to the unch and registered a
             * FileHandler, so we won't be using the one we created. Release
             * its files.
             */
            newHandler.close();
            logger.addHandler(existing);
        }
    }

    /**
     * Follow a consistent naming convention for all KV log files.
     */
    private static String makeFilePattern(String parent,
                                          String fileName,
                                          String suffix) {
        return parent + File.separator + fileName + "_%g." + suffix;
    }

    private static void addLoggingStatsHandler(Logger logger,
                                               ResourceId resourceId,
                                               String storeName) {
        logger.addHandler(getLoggingStatsHandler(resourceId, storeName));
    }

    private static
        LoggingStatsHandler getLoggingStatsHandler(ResourceId resourceId,
                                                   String storeName) {
        final String resourceName = resourceId.toString();
        return LOGGING_STATS_HANDLER_MAP.computeIfAbsent(
            new ServiceHandlerKey(resourceName, storeName),
            k -> new LoggingStatsHandler());
    }

    /**
     * Get information about logging entries of different levels for the
     * resource with the specified resource ID in the store with the specified
     * name.
     */
    public static LoggingCounts getLoggingStatsCounts(ResourceId resourceId,
                                                      String storeName) {
        return getLoggingStatsHandler(resourceId, storeName);
    }

    /**
     * Records information about logging entries of different levels.
     */
    public interface LoggingCounts {

        /**
         * Returns an AtomicLong that maintains the count of SEVERE log
         * entries.
         */
        AtomicLong getSevere();

        /**
         * Returns an AtomicLong that maintains the count of WARNING (and
         * SEC_WARNING) log entries.
         */
        AtomicLong getWarning();

        /**
         * Returns an AtomicLong that maintains the count of INFO (and
         * SEC_INFO) log entries.
         */
        AtomicLong getInfo();
    }

    /**
     * A logging handler that counts logging entries at SEVERE, WARNING (or
     * SEC_WARNING), or INFO (or SEC_INFO).
     */
    private static class LoggingStatsHandler extends Handler
            implements LoggingCounts {

        private final AtomicLong severe = new AtomicLong();
        private final AtomicLong warning = new AtomicLong();
        private final AtomicLong info = new AtomicLong();

        LoggingStatsHandler() {
            setLevel(Level.INFO);
        }

        /* LogCounts  */

        @Override
        public AtomicLong getSevere() {
            return severe;
        }

        @Override
        public AtomicLong getWarning() {
            return warning;
        }

        @Override
        public AtomicLong getInfo() {
            return info;
        }

        /* Handler */

        @Override
        public void publish(LogRecord record) {
            if (record == null) {
                return;
            }
            final int levelValue = record.getLevel().intValue();
            if (levelValue >= Level.SEVERE.intValue()) {
                severe.getAndIncrement();
            } else if (levelValue >= Level.WARNING.intValue()) {
                warning.getAndIncrement();
            } else if (levelValue >= Level.INFO.intValue()) {
                info.getAndIncrement();
            }
        }

        /** No action needed on flush */
        @Override
        public void flush() { }

        /** No action needed on close */
        @Override
        public void close() { }

        /* Object */

        @Override
        public String toString() {
            return "LoggingStatsHandler[severe=" + severe +
                " warning=" + warning + " info=" + info + "]";
        }
    }

    /**
     * Close all FileHandler and Monitor Handlers for the given kvstore.
     * FileHandlers created by any KVStore process need to be released when the
     * process is shutdown. This method is thread safe, because the close()
     * call is reentrant and the FileHandlerMap is a concurrent map.
     * @param kvName if null, all file handlers for all kvstores running in this
     * process are closed. If a value is supplied, only those handlers that
     * work on behalf of the given store are close.
     */
    public static void closeHandlers(String kvName) {
        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 PERF_FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                PERF_FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,FileHandler> entry :
                 STAT_FILE_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                STAT_FILE_HANDLER_MAP.remove(entry.getKey());
            }
        }

        for (Map.Entry<ServiceHandlerKey,LogToMonitorHandler> entry :
                 MONITOR_HANDLER_MAP.entrySet()) {
            if (entry.getKey().belongs(kvName)) {
                entry.getValue().close();
                MONITOR_HANDLER_MAP.remove(entry.getKey());
            }
        }
    }

    /**
     * Close all FileHandlers in the process.
     */
    public static void closeAllHandlers() {
        closeHandlers(null);
        final Logger oldBootstrapLogger = bootstrapLogger;
        if (oldBootstrapLogger != null) {
            Handler[] handlers = oldBootstrapLogger.getHandlers();
            if (handlers != null) {
                for (Handler h : handlers) {
                    h.close();
                }
            }
        }
        bootstrapLogger = null;
    }

    /**
     * Get the value of a specified Logger property.
     */
    public static String getLoggerProperty(String property) {
        return CommonLoggerUtils.getLoggerProperty(property);
    }

    /**
     * Utility method to return a String version of a stack trace
     */
    public static String getStackTrace(Throwable t) {
        return CommonLoggerUtils.getStackTrace(t);
    }

    /**
     * File and MonitorHandlers are unique to each service within a kvstore
     * instance.
     */
    private static class ServiceHandlerKey {

        private final String resourceName;
        private final String kvName;

        ServiceHandlerKey(String resourceName, String kvName) {
            this.resourceName = resourceName;
            this.kvName = kvName;
        }

        /**
         * Return true if kvStoreName is null, or if it matches this key's
         * store.
         */
        boolean belongs(String kvStoreName) {
            if (kvStoreName == null) {
                return true;
            }

            return kvName.equals(kvStoreName);
        }

        /**
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                + ((kvName == null) ? 0 : kvName.hashCode());
            result = prime * result
                + ((resourceName == null) ? 0 : resourceName.hashCode());
            return result;
        }

        /**
         * @see java.lang.Object#equals(java.lang.Object)
         */
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            ServiceHandlerKey other = (ServiceHandlerKey) obj;

            if (kvName == null) {
                if (other.kvName != null) {
                    return false;
                }
            } else if (!kvName.equals(other.kvName)) {
                return false;
            }

            if (resourceName == null) {
                if (other.resourceName != null) {
                    return false;
                }
            } else if (!resourceName.equals(other.resourceName)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return kvName + "/" + resourceName;
        }
    }

    /**
     * Remove the given log handler from any associated loggers.
     */
    public static void removeHandler(Handler handler) {
        LogManager lm = LogManager.getLogManager();

        Enumeration<String> loggerNames = lm.getLoggerNames();

        while (loggerNames.hasMoreElements()) {
            String name = loggerNames.nextElement();
            Logger logger = lm.getLogger(name);
            if (logger != null) {
                Handler[] handlers = logger.getHandlers();
                if (handlers != null) {
                    for (Handler h : handlers) {
                        if (h == handler) {
                            logger.removeHandler(h);
                        }
                    }
                }
            }
        }
    }

    /**
     * Additional logging level defined for security audit logging.
     *
     * Note that the new order of logging level after introduced security
     * logging level is as below:
     * - SEVERE
     * - SEC_WARNING
     * - WARNING
     * - SEC_INFO
     * - INFO
     *
     * Security auditing logging only collects a limited set of logging
     * information of security-relevant activities.
     */
    public static class SecurityLevel extends Level {

        private static final long serialVersionUID = 1L;

        private static final int SEC_WARNING_VALUE = 950;
        private static final int SEC_INFO_VALUE = 850;

        public static final SecurityLevel SEC_WARNING =
            new SecurityLevel("SEC_WARNING", SEC_WARNING_VALUE);
        public static final SecurityLevel SEC_INFO =
            new SecurityLevel("SEC_INFO", SEC_INFO_VALUE);

        private SecurityLevel(String name, int value) {
            super(name, value);
        }

        /**
         * Relies on the unique value defined for security logging level
         * to resolve and return designated object.
         */
        private Object readResolve() {
            if (intValue() == SEC_WARNING_VALUE) {
                return SEC_WARNING;
            } else if (intValue() == SEC_INFO_VALUE) {
                return SEC_INFO;
            }
            throw new RuntimeException(
                "Encounter unrecognized value in resolving security level: " +
                intValue());
        }

        static Level parseSecurity(String name) {
            switch (name) {
            case "SEC_WARNING":
            case "950":
                return SEC_WARNING;
            case "SEC_INFO":
            case "850":
                return SEC_INFO;
            default:
                return null;
            }
        }
    }

    /**
     * Parse a level name into a Level or SecurityLevel. The argument string
     * may consist of either a level name or an integer value.
     *
     * @param name string to parse
     * @return the level object
     * @throws IllegalArgumentException if the name is not associated with a
     * level
     */
    public static Level parseLevel(String name) {
        Level level = SecurityLevel.parseSecurity(name);
        if (level != null) {
            return level;
        }
        return Level.parse(name);
    }

    /**
     * Logs a message including the NoSQL, JE, and Java software versions. Also
     * arranges for any logger file handlers to log that same message whenever
     * the log files are rotated, so we can be sure to have that information
     * available in each log file.
     */
    public static void logSoftwareVersions(Logger logger) {
        final String msg =
            "Software versions: " +
            "NoSQL version: " + KVVersion.CURRENT_VERSION +
            ", JE version: " + JEVersion.CURRENT_VERSION +
            ", Java version: " + System.getProperty("java.version");
        logger.info(msg);
        for (final Handler handler : logger.getHandlers()) {
            if (handler instanceof FileHandler) {
                final FileHandler fileHandler = (FileHandler) handler;
                fileHandler.setInitialMessage(msg);
            }
        }
    }

    /**
     * Logs a JE replication StateChangeEvent with tag "LIFECYCLE". The entry
     * has the format: LIFECYCLE Replication state change: state=<i>state</i>
     * type={MONITOR,ELECTABLE,SECONDARY,ARBITER,EXTERNAL,PRIMARY}
     * master={<i>node-name</i>,none} time=<i>utc-date-time</i>
     *
     * @param logger the logger
     * @param sce the event
     * @param nodeType the type of the node experiencing the event, either JE
     * NodeType or KV AdminType
     */
    public static void logStateChangeEvent(Logger logger,
                                           StateChangeEvent sce,
                                           Object nodeType) {
        final State state = sce.getState();
        logger.info("LIFECYCLE " +
                    "Replication state change:" +
                    " state=" + state +
                    " type=" + nodeType +
                    " master=" +
                    ((state.isMaster() || state.isReplica()) ?
                     sce.getMasterNodeName() :
                     "none") +
                    " time=" +
                    FormatUtils.formatDateTimeMillis(sce.getEventTime()));
    }

    /**
     * Logs a service status event with tag "LIFECYCLE". The entry has the
     * format: LIFECYCLE Service status change: from=<i>old-status</i>
     * to=<i>new-status</i> <i>(optional)</i>afterCrash
     * <i>(optional)</i>reason=<i>reason-text</i>
     *
     * @param logger the logger
     * @param oldStatus the old service status
     * @param newStatus the new service status
     * @param startingAfterCrash whether the service is starting after a crash
     * @param reason an explanation for the event or null
     */
    public static void logServiceStatusEvent(Logger logger,
                                             ServiceStatus oldStatus,
                                             ServiceStatus newStatus,
                                             boolean startingAfterCrash,
                                             String reason) {
        final StringBuilder msg =
            new StringBuilder("LIFECYCLE Service status change:");
        msg.append(" from=").append(oldStatus);
        msg.append(" to=").append(newStatus);
        if (startingAfterCrash) {
            msg.append(" afterCrash");
        }
        if (reason != null) {
            msg.append(" reason=" + reason);
        }
        logger.info(msg.toString());
    }

    /**
     * Logs a service monitor event with tag "LIFECYCLE" when a service is
     * created.
     *
     * @param logger the logger
     * @param serviceId the ID of the created service
     */
    public static void logServiceMonitorCreateEvent(Logger logger,
                                                    ResourceId serviceId) {
        logServiceMonitorEvent(logger, serviceId, "CREATE");
    }

    /**
     * Logs a service monitor event with tag "LIFECYCLE" when a service is
     * destroyed.
     *
     * @param logger the logger
     * @param serviceId the ID of the destroyed service
     */
    public static void logServiceMonitorDestroyEvent(Logger logger,
                                                     ResourceId serviceId) {
        logServiceMonitorEvent(logger, serviceId, "DESTROY");
    }

    /**
     * Logs a service monitor event with tag "LIFECYCLE" when a monitored
     * service exits.
     *
     * @param logger the logger
     * @param serviceId the ID of the exiting service
     * @param exitCode the exit code of the exiting process
     * @param restart whether the process will be restarted
     */
    public static void logServiceMonitorExitEvent(Logger logger,
                                                  ResourceId serviceId,
                                                  int exitCode,
                                                  boolean restart) {
        final StringBuilder sb = new StringBuilder();
        sb.append("EXIT cause=");
        if (exitCode == 0) {
            sb.append("STOP");
        } else {
            sb.append(ProcessExitCode.injectedFault(exitCode) ?
                      "INJECTED_FAULT" :
                      "FAULT");
            sb.append(" exitCode=");
            sb.append(ProcessExitCode.description(exitCode));
            sb.append(" restart=");
            sb.append(restart ?
                      "YES" :
                      !ProcessExitCode.needsRestart(exitCode) ?
                      "NO" :
                      "DENIED");
        }
        logServiceMonitorEvent(logger, serviceId, sb.toString());
    }

    /**
     * Logs a service monitor event with tag "LIFECYCLE" when a service is
     * started.
     *
     * @param logger the logger
     * @param serviceId the ID of the started service
     */
    public static void logServiceMonitorStartEvent(Logger logger,
                                                   ResourceId serviceId) {
        logServiceMonitorEvent(logger, serviceId, "START");
    }

    /**
     * Log a service monitor event. The entry has the format: LIFECYCLE Service
     * monitor event: service=<i>service</i> event={CREATE,DESTROY,START,EXIT
     * cause={STOP,{INJECTED_FAULT,FAULT} <i>exitCode=<description>
     * restart={YES,NO,DENIED}}}
     */
    private static void logServiceMonitorEvent(Logger logger,
                                               ResourceId serviceId,
                                               String eventMsg) {
        logger.info("LIFECYCLE Service monitor event:" +
                    " service=" + serviceId +
                    " event=" + eventMsg);
    }

    /**
     * Security auditing logging info format class.
     */
    public static class KVAuditInfo {

        private static String BASIC_INFO_FORMAT = "KVAuditInfo [User: %s, " +
            "ClientHost: %s, AuthHost: %s, Operation Desc: %s";

        /**
         * Format failed operation log info.
         *
         * @param user       User perform this operation
         * @param clientHost Host from which the request originated
         * @param authHost   Host that originated a forwarded request
         * @param opDesc     Operation description
         * @param reason     Operation failure reason
         * @return formatted log info for failed operation
         */
        public static String failure(final String user,
                                     final String clientHost,
                                     final String authHost,
                                     final String opDesc,
                                     final String reason) {

            return String.format(
                BASIC_INFO_FORMAT + ", Status: FAILED, Reason: %s]",
                user, clientHost, authHost, opDesc, reason);
        }

        public static String failure(final String user,
                                     final String clientHost,
                                     final String opDesc,
                                     final String reason) {

            return failure(user, clientHost, "", opDesc, reason);
        }

        /**
         * Format successful operation log info.
         *
         * @param user       User perform this operation
         * @param clientHost Host from which the request originated
         * @param authHost   Host that originated a forwarded request
         * @param opDesc     Operation description
         * @return formatted log info for successful operation
         */
        public static String success(final String user,
                                     final String clientHost,
                                     final String authHost,
                                     final String opDesc) {

            return String.format(BASIC_INFO_FORMAT + ", Status: SUCCEEDED]",
                                 user, clientHost, authHost, opDesc);
        }

        public static String success(final String user,
                                     final String clientHost,
                                     final String opDesc) {

            return success(user, clientHost, "", opDesc);
        }
    }

    /**
     * Simple utility to perform a thread dump
     */
    private static Semaphore dumpSemaphore = new Semaphore(1);
    private static AtomicLong lastDumpMs = new AtomicLong(0);

    /**
     * Dumps all the threads in the running VM. This method is typically used
     * to debug the source of stalls, potentially due to a deadlock.
     *
     * To avoid excessive logging, it will not write a dump, if one is already
     * in progress, or there was a recent dump within interDumpIntervalMs.
     */
    public static boolean fullThreadDump(Logger logger,
                                         int interDumpIntervalMs,
                                         String dumpMessage) {

        if (!dumpSemaphore.tryAcquire()) {
            /* A thread dump is already in progress. */
            return false;
        }

        try {
            final long nowMs = System.currentTimeMillis();
            if ((interDumpIntervalMs > 0) &&
                (nowMs - lastDumpMs.get()) < interDumpIntervalMs) {
                /*
                 * Don't dump thread too frequently to avoid overwhelming the
                 * logs.
                 */
                return false;
            }

            lastDumpMs.set(nowMs);
            logger.info(dumpMessage);
            final Map<Thread, StackTraceElement[]> stackTraces =
                Thread.getAllStackTraces();

            for (Map.Entry<Thread, StackTraceElement[]> stme :
                 stackTraces.entrySet()) {

                final Thread thread = stme.getKey();
                logger.info(thread + (thread.isDaemon() ? " daemon" : ""));
                for (StackTraceElement ste : stme.getValue()) {
                    logger.info("  " + ste);
                }
            }
        } finally {
            dumpSemaphore.release();
        }

        return true;
    }
}
