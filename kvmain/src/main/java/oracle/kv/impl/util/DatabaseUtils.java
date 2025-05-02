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

package oracle.kv.impl.util;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.topo.ResourceId;
import oracle.nosql.common.json.JsonUtils;

import oracle.nosql.common.json.ObjectNode;
import com.sleepycat.je.Database;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockNotAvailableException;
import com.sleepycat.je.LockTimeoutException;
import com.sleepycat.je.VerifyConfig;
import com.sleepycat.je.VerifyError;
import com.sleepycat.je.VerifyListener;
import com.sleepycat.je.VerifySummary;
import com.sleepycat.je.rep.InsufficientAcksException;
import com.sleepycat.je.rep.InsufficientReplicasException;
import com.sleepycat.je.rep.ReplicaWriteException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.impl.RepParams;
import com.sleepycat.je.util.DbVerifyLog;
import com.sleepycat.je.util.VerifyLogError;
import com.sleepycat.je.util.VerifyLogListener;
import com.sleepycat.je.util.VerifyLogSummary;
import com.sleepycat.je.utilint.StoppableThread;

/**
 * Collection of utilities for JE Database operations
 */
public class DatabaseUtils {

    public volatile static TestHook<ReplicatedEnvironment> VERIFY_ERROR_HOOK;
    public volatile static TestHook<VerificationThread> VERIFY_INTERRUPT_HOOK;
    public volatile static TestHook<Object[]> VERIFY_CORRUPTDATA_HOOK;
    public volatile static TestHook<VerificationInfo> VERIFY_CORRUPTFILE_HOOK;

    /**
     * Prevent instantiation.
     */
    private DatabaseUtils() {
    }

    /**
     * Handles an exception opening a replicated DB. Returns
     * true if the open should be retried otherwise the exception is
     * re-thrown.
     *
     * @param re the exception from the open
     * @param logger a logger
     * @param dbName name of DB that was opened
     * @return true if the open should be retried
     */
    public static boolean handleException(RuntimeException re,
                                          Logger logger,
                                          String dbName) {
        try {
            throw re;
        } catch (ReplicaWriteException | UnknownMasterException de) {

            /*
             * Master has not had a chance to create the database as
             * yet, or the current environment (in the replica, or
             * unknown) state is lagging or the node has become a
             * replica. Wait, giving the environment
             * time to catch up and become current.
             */
            logger.log(Level.FINE,
                       "Failed to open database for {0}. {1}",
                       new Object[] {dbName, de.getMessage()});
            return true;
        } catch (InsufficientReplicasException ire) {
            logger.log(Level.FINE,
                       "Insufficient replicas when creating " +
                       "database {0}. {1}",
                       new Object[] {dbName, ire.getMessage()});
            return true;
        } catch (InsufficientAcksException iae) {
            logger.log(Level.FINE,
                       "Insufficient acks when creating database {0}. {1}",
                       new Object[] {dbName, iae.getMessage()});
            /*
             * Database has already been created locally, ignore
             * the exception.
             */
            return false;
        } catch (IllegalStateException ise) {
            logger.log(Level.FINE,
                       "Problem accessing database {0}. {1}",
                       new Object[] {dbName, ise.getMessage()});
            return true;
        } catch (LockTimeoutException lte) {
            logger.log(Level.FINE, "Failed to open database for {0}. {1}",
                       new Object[] {dbName, lte.getMessage()});
            return true;
        } catch (LockNotAvailableException lna) {
            logger.log(Level.FINE, "Failed to open database for {0}. {1}",
                       new Object[] {dbName, lna.getMessage()});
            return true;
        }
    }

    /*
     * Resets the members of the JE replication group, replacing the group
     * members with the single member associated with the specified
     * environment.  This method does what DbResetRepGroup.reset does, but
     * using the specified configuration properties rather reading the
     * configuration from the environment directory.  Note that the
     * configuration arguments will be modified.
     *
     * @param envDir the node's replicated environment directory
     * @param envConfig the environment configuration
     * @param repConfig the replicated environment configuration
     * @see com.sleepycat.je.rep.util.DbResetRepGroup#reset
     */
    /* TODO: Consider creating a JE entrypoint to do this */
    public static void resetRepGroup(File envDir,
                                     EnvironmentConfig envConfig,
                                     ReplicationConfig repConfig) {
        final Durability durability =
            new Durability(Durability.SyncPolicy.SYNC,
                           Durability.SyncPolicy.SYNC,
                           Durability.ReplicaAckPolicy.NONE);

        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(durability);
        repConfig.setHelperHosts(repConfig.getNodeHostPort());

        /* Force the re-initialization upon open. */
        repConfig.setConfigParam(RepParams.RESET_REP_GROUP.getName(), "true");

        /* Open the environment, thus replacing the group. */
        final ReplicatedEnvironment repEnv =
            new ReplicatedEnvironment(envDir, repConfig, envConfig);

        repEnv.close();
    }

    /**
     * Returns true if the specified database handle needs to be refreshed.
     * Specifically, true is returned if db is null or the database's
     * environment is null or does not match the current environment.
     *
     * @param db the database to check, or null
     * @param current the current environment
     * @return true if the specified database handle needs refreshing
     */
    public static boolean needsRefresh(Database db, Environment current) {
        if (db == null) {
            return true;
        }

        final Environment dbEnv = db.getEnvironment();
        if (dbEnv == null) {
            return true;
        }

        /* If the old and current envs match, no need to refresh */
        if (dbEnv == current) {
            return false;
        }

        /* The old and current env are different, the old should be invalid */
        if (dbEnv.isValid()) {
            throw new IllegalStateException("Database needs refreshing, but " +
                                            "references a valid environment");
        }
        return true;
    }

    /**
     * Verify data for a node.
     * @param env the environment for the node
     * @param verifyBtreeConfig properties for verifyBtree
     * @param logListener listener for logVerify
     * @param logDelay delay between log file reads
     * @param logger
     * @throws IOException
     */
    public static void verifyData(ReplicatedEnvironment env,
                                  VerifyConfig verifyBtreeConfig,
                                  VerifyLogListener logListener,
                                  long logDelay,
                                  Logger logger) throws IOException {
        logger.info("Stop running scheduled verification.");
        EnvironmentMutableConfig mutableConfig = env.getMutableConfig();
        String oldScheduledConfig =
            mutableConfig.getConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER);
        try {
            /* cancel any running scheduled verification */
            mutableConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER,
                                         "false");
            /* for testing */
            assert TestHookExecute.doHookIfSet(DatabaseUtils.VERIFY_ERROR_HOOK,
                                               env);

            /* btree verification */
            if (verifyBtreeConfig != null) {
                logger.info("Start JE btree verification.");
                env.verify(verifyBtreeConfig);
            }

            /* log file verification */
            if (logListener != null) {
                DbVerifyLog logVerify = new DbVerifyLog(env, 0, logListener);
                if (logDelay >= 0) {
                    logVerify.setReadDelay(logDelay, TimeUnit.MILLISECONDS);
                }

                logger.info("Start JE log verification.");
                logVerify.verifyAll();

            }
        } finally {
            /*
             * set the configuration for scheduled verification back to previous
             * value.
             */
            mutableConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER,
                                         oldScheduledConfig);
        }
    }

    public static class VerificationThread extends StoppableThread {
        private final ReplicatedEnvironment env;
        private final VerificationInfo info;
        private final VerificationOptions options;
        private final Logger logger;

        private volatile boolean isShutdown = false;

        private static final int THREAD_SOFT_SHUTDOWN_MS = 10000;
        private static final int MAXIMUM_CORRUPT_KEYS_NUM = 5000;

        public VerificationThread(ReplicatedEnvironment env,
                                  VerificationInfo info,
                                  VerificationOptions options,
                                  Logger logger) {
            super("VerificationThread");
            this.env = env;
            this.info = info;
            this.options = options;
            this.logger = logger;

        }

        @Override
        public void run() {
            try {
                VerifyConfig config = null;
                if (options.verifyBtree) {
                    VerifyListener listener = getBtreeVerifyListener();
                    config = new VerifyConfig();
                    if (options.btreeDelay >= 0) {
                        config.setBatchDelay(options.btreeDelay,
                                             TimeUnit.MILLISECONDS);
                    }
                    config.setVerifyDataRecords(options.verifyRecord);
                    config.setVerifySecondaries(options.verifyIndex);
                    config.setListener(listener);
                }

                VerifyLogListener logListener = null;
                if (options.verifyLog) {
                    logListener = getLogVerifyListener();
                }
                verifyData(env, config, logListener, options.logDelay, logger);

            } catch (Exception e) {
                String errorMsg = "Verification fails. Exception: " +
                    e.getMessage();
                info.setException(errorMsg);
            } finally {
                /*
                 * If the state is not COMPLETED but it is not ERROR it means
                 * that JE didn't call the OnEnd() callback and didn't throw an
                 * exception either. This is unlikely to happen and would
                 * indicate a possible bug in JE.
                 */
                if (info.getCurrentState() !=
                    VerificationInfo.State.COMPLETED &&
                    info.getCurrentState() != VerificationInfo.State.ERROR) {
                    info.setException("UNKNOWN ERROR.");
                }
                initiateSoftShutdown();
            }
        }

        @Override
        protected synchronized int initiateSoftShutdown() {
            isShutdown = true;
            notifyAll();
            return THREAD_SOFT_SHUTDOWN_MS;
        }

        protected synchronized boolean checkForDone() {
            return isShutdown;
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
        private VerifyLogListener getLogVerifyListener() {
            VerifyLogListener listener = new VerifyLogListener() {

                @Override
                public boolean onBegin(int totalFiles) {
                    info.startVerify();
                    return !isShutdown;
                }

                @Override
                public void onEnd(VerifyLogSummary summary) {
                    info.completeVerify();

                }

                @Override
                public boolean onFile(long file,
                                      List<VerifyLogError> errors,
                                      boolean deleted) {
                    if (!errors.isEmpty()) {
                        /*
                         * Copy errors into a new ArrayList because
                         * JE reuses the input list.
                         */
                        info.logFileErrorList.put(file, new ArrayList<>(errors));
                    }
                    return !isShutdown;
                }

                @Override
                public boolean onRead(long file,
                                      int bytesRead) {
                    return !isShutdown;
                }

            };
            return listener;
        }

        private VerifyListener getBtreeVerifyListener() {
            VerifyListener listener = new VerifyListener() {
                @Override
                public boolean onBegin() {
                    info.startVerify();
                    assert TestHookExecute.
                        doHookIfSet(DatabaseUtils.VERIFY_INTERRUPT_HOOK, null);
                    return !isShutdown;
                }

                @Override
                public boolean
                    onDatabase(String dbName,
                               List<com.sleepycat.je.VerifyError> errors) {
                    if (errors != null && !errors.isEmpty()) {
                        info.dbErrorList.
                            put(dbName, new ArrayList<VerifyError>(errors));
                    }
                    return !isShutdown;
                }

                @Override
                public void onEnd(VerifySummary summary) {
                    if (!options.verifyLog) {
                        info.completeVerify();
                    }
                    if (options.showMissingFiles) {
                        assert TestHookExecute.
                            doHookIfSet(DatabaseUtils.VERIFY_CORRUPTFILE_HOOK,
                                        info);
                        info.missingFilesReferenced.
                            addAll(summary.getMissingFilesReferenced());
                        info.reservedFilesReferenced.
                            addAll(summary.getReservedFilesReferenced());
                        info.reservedFilesRepaired.
                            addAll(summary.getReservedFilesRepaired());
                    }
                }

                @Override
                public boolean
                    onOtherError(List<com.sleepycat.je.VerifyError> errors) {
                    info.otherErrorList.addAll(errors);
                    return !isShutdown;
                }

                @Override
                public boolean
                    onRecord(String dbName,
                             byte[] priKey,
                             byte[] secKey,
                             List<com.sleepycat.je.VerifyError> errors) {
                    if (errors != null && !errors.isEmpty()) {
                        if (info.recordErrorList.size() <
                                MAXIMUM_CORRUPT_KEYS_NUM) {
                            List<byte[]> keys = new ArrayList<byte[]>();
                            keys.add(dbName.getBytes());
                            keys.add(priKey);
                            keys.add(secKey);
                            info.recordErrorList.put(keys,
                                new ArrayList<VerifyError>(errors));
                        } else {
                            /* stop verification since the number of
                             * corrupt keys has exceed the limit. */
                            info.moreCorruptKeys = true;
                            return false;
                        }
                    }
                    return !isShutdown;
                }

            };
            return listener;
        }
    }

    /**
     * Manages verification of databases on a node, which can be a rn or admin.
     * Only one running verification is allowed.
     */
    public static class VerificationManager {
        private VerificationInfo info; /* Verification status and result */
        private volatile VerificationThread curThread;

        private ReplicatedEnvironment env;
        private final ResourceId id;
        private final Logger logger;
        private VerificationOptions curOptions;
        private int curPlanId;

        public VerificationManager(ResourceId id,
                                   Logger logger) {
            this.id = id;
            this.logger = logger;
            info = null;
        }

        public synchronized void setEnv(ReplicatedEnvironment env) {
            this.env = env;
        }

        public synchronized VerificationInfo getInfo() {
            return info;
        }

        public synchronized VerificationThread getThread() {
            return curThread;
        }

        /* Kicks off a verification, or checks the status of the running
         * verification when options is null. */
        public synchronized VerificationInfo
            startAndPollVerification(VerificationOptions options,
                                     int planId) {
            if (env == null) {
                throw new IllegalStateException("Environment cannot be null.");
            }
            if (info != null) {
                if (planId < curPlanId) {
                    /* An old plan polls for status. Return a result notifying
                     * the old plan that its verification has been interrupted
                     * by a new plan. */
                    VerificationInfo oldInfo =
                        new VerificationInfo(false, false, id);
                    oldInfo.interruptedByAnotherPlan = true;
                    oldInfo.completeVerify();
                    return oldInfo;

                }
                if (!info.getCurrentState().isDone()) {
                    /*verification is in progress*/
                    if (planId != curPlanId) {
                        if (options != null) {
                            /* A new plan initiates a new verification, so
                             * interrupt the current verification and
                             * starts a new one. */
                            interruptVerification();
                            startNewVerification(options, planId);
                            return info;
                        }
                        /* The node crushed before and there are two or more
                         * verification plans running in parallel. Now the newer
                         * plan polls for status, so ask it to resend
                         * options.  */
                        return null;
                    }
                    return info;

                }
                /*check the completed time for the last verification.*/
                long curTime = System.currentTimeMillis();
                if ((options == null && curPlanId == planId) ||
                    (options != null &&
                     curTime - info.getLastVerifyTime() < options.expiredTime &&
                     options.equals(curOptions) && !info.isInterrupted)) {
                    return info;
                }

            }
            if (options == null) {
                /* ask admin to send options. */
                curPlanId = Math.max(planId, curPlanId);
                info = null;
                return info;
            }

            assert TestHookExecute.doHookIfSet(
            DatabaseUtils.VERIFY_CORRUPTDATA_HOOK,
            new Object[] { env, id });


            startNewVerification(options, planId);
            return info;
        }

        private void startNewVerification(VerificationOptions options,
                                          int planId) {
            info = new VerificationInfo(options.verifyBtree, options.verifyLog,
                                        id);
            curThread = new VerificationThread(env, info, options, logger);

            /*start verify.*/
            curOptions = options;
            curPlanId = planId;
            curThread.start();
        }

        /* Interrupts the running verification. */
        public synchronized boolean interruptVerification() {
            if (curThread == null || info == null ||
                info.getCurrentState().isDone()) {

                return false;
            }
            try {
                curThread.join(curThread.initiateSoftShutdown());
            } catch (InterruptedException e) {
                /* ignore */
            }
            info.isInterrupted = true;
            return true;
        }

    }

    public static class VerificationInfo implements Serializable {

        private static final long serialVersionUID = 1L;

        public static enum State {
            NOT_STARTED, /* verification requested but not started yet */
            INPROGRESS,
            COMPLETED() {
                @Override
                public boolean isDone() { return true; }
            },
            ERROR() {
                @Override
                public boolean isDone() { return true; }
            };

            public boolean isDone() { return false; }
        }

        private State currentState = State.NOT_STARTED;

        private long completeTime = 0l;

        private String exceptionMsg = null;

        public final boolean verifyBtree;

        public final boolean verifyLog;

        public final ResourceId id;

        public final Map<List<byte[]>, List<VerifyError>> recordErrorList =
            new HashMap<>();

        public final Map<String, List<VerifyError>> dbErrorList =
            new HashMap<>();

        public final List<VerifyError> otherErrorList = new ArrayList<>();

        public final Map<Long, List<VerifyLogError>> logFileErrorList =
            new HashMap<>();

        public final Set<Long> missingFilesReferenced = new HashSet<>();

        public final Set<Long> reservedFilesReferenced = new HashSet<>();

        public final Set<Long> reservedFilesRepaired = new HashSet<>();

        public boolean moreCorruptKeys = false;

        public boolean isInterrupted = false;

        public boolean interruptedByAnotherPlan = false;

        public VerificationInfo(boolean verifyBtree,
                                boolean verifyLog,
                                ResourceId id) {
            this.verifyBtree = verifyBtree;
            this.verifyLog = verifyLog;
            this.id = id;
        }

        public void setNotStarted() {
            currentState = State.NOT_STARTED;
        }

        public State getCurrentState() {
            return currentState;
        }

        public void startVerify() {
            currentState = State.INPROGRESS;
        }

        public void completeVerify() {
            currentState = State.COMPLETED;
            completeTime = System.currentTimeMillis();
        }

        public void setException(String errorMsg) {
            currentState = State.ERROR;
            this.exceptionMsg = errorMsg;
            completeTime = System.currentTimeMillis();
        }

        public long getLastVerifyTime() {
            return completeTime;
        }

        public String getExceptionMsg() {
            return exceptionMsg;
        }

        public boolean noBtreeCorruptions() {
            return recordErrorList.isEmpty() && dbErrorList.isEmpty() &&
                   otherErrorList.isEmpty() &&
                   missingFilesReferenced.isEmpty() &&
                   reservedFilesReferenced.isEmpty() &&
                   reservedFilesRepaired.isEmpty();
        }

        public boolean noLogFileCorruptions() {
            return logFileErrorList.isEmpty();
        }

        public ObjectNode getJson(boolean showMissingFiles) {
            ObjectNode jsonNodeTop = JsonUtils.createObjectNode();
            ObjectNode jsonNode = jsonNodeTop.putObject("" + id);

            if (getCurrentState() == VerificationInfo.State.ERROR) {
                jsonNode.put("Verification failed. Error", getExceptionMsg());
                return jsonNodeTop;
            }
            if (interruptedByAnotherPlan) {
                jsonNode.put("INTERRUPTED", "Verification interrupted " +
                    "by another plan.");
                return jsonNodeTop;
            }

            if (isInterrupted) {
                jsonNode.put("INTERRUPTED", "Verification interrupted " +
                    "by users.");
            }
            if (verifyBtree) {
                if (!noBtreeCorruptions()) {
                    ObjectNode jsonBtreeVerify =
                        jsonNode.putObject("Btree Verify");
                    if (!dbErrorList.isEmpty()) {
                        putDbCorruptionJson(jsonBtreeVerify);
                    }

                    putKeyCorruptionJson(jsonBtreeVerify);

                    if (showMissingFiles) {
                        putFileCorruptionJson(jsonBtreeVerify);
                    }
                    if (!otherErrorList.isEmpty()) {
                        putOtherErrorJson(jsonBtreeVerify);
                    }
                } else {
                    jsonNode.put("Btree Verify", "No Btree Corruptions");
                }
            }

            if (verifyLog) {
                if (!noLogFileCorruptions()) {
                    putVerifyLogJson(jsonNode);
                } else {
                    jsonNode.put("Log File Verify", "No Log File Corruptions");
                }
            }
            return jsonNodeTop;
        }

        private void putDbCorruptionJson(ObjectNode jsonBtreeVerify) {
            ObjectNode jsonDbErrors = jsonBtreeVerify.
                putObject("Database corruptions");
            jsonDbErrors.put("Total Number", dbErrorList.size());
            ObjectNode jsonErrorList = jsonDbErrors.putObject("List");
            for (String dbName : dbErrorList.keySet()) {
                List<VerifyError> errors = dbErrorList.get(dbName);
                ObjectNode jsonDbName = jsonErrorList.putObject(dbName);
                int i = 1;
                for (VerifyError error : errors) {
                    jsonDbName.put("" + i, error.getMessage());
                    i++;
                }
            }
        }

        private void putKeyCorruptionJson(ObjectNode jsonBtreeVerify) {
            if (!recordErrorList.isEmpty()) {
                ObjectNode jsonCorruptKeys =
                        jsonBtreeVerify.putObject("Corrupt Keys");
                jsonCorruptKeys.put("Total number",
                                    recordErrorList.size());
                ObjectNode jsonErrorList = jsonCorruptKeys.putObject("List");
                for (List<byte[]> keys : recordErrorList.keySet()) {
                    List<VerifyError> errors = recordErrorList.get(keys);
                    ObjectNode jsonKey = jsonErrorList.
                        putObject("dbName: " + (keys.get(0) == null ?
                                    "null" : new String(keys.get(0))) +
                                  " priKey: " + (keys.get(1) == null ?
                                      "null" : new String(keys.get(1))) +
                                  " secKey: " + (keys.get(2) == null ?
                                      "null" : new String(keys.get(2))));
                    int i = 1;
                    for (VerifyError error : errors) {
                        jsonKey.put("" + i, error.getProblem() + ": " +
                                    error.getMessage());
                        i++;
                    }

                }

                if (moreCorruptKeys) {
                    jsonErrorList.putObject("Number of corrupt keys " +
                        "exceeds the limit. There are more to report. " +
                        "Please fix the reported corruption first and run " +
                        "verification again.");
                }
            }

        }

        private void putFileCorruptionJson(ObjectNode jsonBtreeVerify) {
            if (!missingFilesReferenced.isEmpty() ||
                !reservedFilesReferenced.isEmpty() ||
                !reservedFilesRepaired.isEmpty()) {
                ObjectNode jsonMissingFile = jsonBtreeVerify.
                    putObject("Missing Files Referenced");
                jsonMissingFile.put("Total number",
                    missingFilesReferenced.size());
                if (!missingFilesReferenced.isEmpty()) {
                    ObjectNode jsonCorruptFilesList = jsonMissingFile.
                        putObject("List");
                    int i = 1;
                    for (Long file : missingFilesReferenced) {
                        jsonCorruptFilesList.put("" + i, "" + file);
                        i++;
                    }
                }
                ObjectNode jsonReservedFile = jsonBtreeVerify.
                    putObject("Reserved Files Referenced");
                jsonReservedFile.put("Total number",
                    reservedFilesReferenced.size());
                if (!reservedFilesReferenced.isEmpty()) {
                    ObjectNode jsonReservedFilesList = jsonReservedFile.
                        putObject("List");
                    int i = 1;
                    for (Long file : reservedFilesReferenced) {
                        jsonReservedFilesList.put("" + i, "" + file);
                        i++;
                    }
                }
                ObjectNode jsonRepairedFile =
                    jsonBtreeVerify.putObject("Reserved Files " +
                        "Repaired");
                jsonRepairedFile.put("Total number",
                    reservedFilesRepaired.size());
                if (!reservedFilesRepaired.isEmpty()) {
                    ObjectNode jsonRepairedFilesList = jsonRepairedFile.
                        putObject("List");
                    int i = 1;
                    for (Long file : reservedFilesRepaired) {
                        jsonRepairedFilesList.put("" + i, "" + file);
                        i++;
                    }
                }
            }
        }

        private void putOtherErrorJson(ObjectNode jsonBtreeVerify) {
            ObjectNode jsonOtherErrors = jsonBtreeVerify.
                putObject("Other Errors");
            jsonOtherErrors.put("Total number", otherErrorList.size());
            ObjectNode jsonOtherErrorList = jsonOtherErrors.putObject("List");
            int i = 1;
            for (VerifyError error : otherErrorList) {
                jsonOtherErrorList.put("" + i, error.getMessage());
                i++;
            }
        }

        private void putVerifyLogJson(ObjectNode jsonNode) {
            ObjectNode jsonLogVerify = jsonNode.putObject("Log File Verify");
            ObjectNode jsonLogError = jsonLogVerify.putObject("Log File Error");
            jsonLogError.put("Total number", logFileErrorList.size());
            ObjectNode jsonLogErrorList = jsonLogError.putObject("List");
            for (Long file : logFileErrorList.keySet()) {
                ObjectNode jsonLogName = jsonLogErrorList.
                    putObject("Log File " + file);
                int i = 1;
                for (VerifyLogError error : logFileErrorList.get(file)) {
                    jsonLogName.put("" + i, error.getMessage());
                    i++;
                }
            }
        }


    }

    public static class VerificationOptions implements Serializable {
        /** 
         * Default values for verification options used by Admin CLI and
         * KVLocal API
         */
        public static final boolean DEFAULT_VERIFY_LOG = true;
        public static final boolean DEFAULT_VERIFY_BTREE = true;
        public static final boolean DEFAULT_VERIFY_INDEX = true;
        public static final boolean DEFAULT_VERIFY_DATARECORD = false;
        public static final long DEFAULT_BTREE_DELAY = -1;
        public static final long DEFAULT_LOG_DELAY = -1;
        public static final long DEFAULT_EXPIRED_TIME = 10 * 60000;/*10 mins*/
        public static final boolean DEFAULT_SHOW_MISSING_FILES = false;
        /**
         *
         */
        private static final long serialVersionUID = 1L;

        public final boolean verifyBtree;
        public final boolean verifyLog;
        public final boolean verifyIndex;
        public final boolean verifyRecord;
        public final long btreeDelay;
        public final long logDelay;
        public final long expiredTime;
        public final boolean showMissingFiles;

        /**
         * Options for verification set by users
         * @param verifyBtree verifies the b-tree in memory containing
         *        valid references if it's true
         * @param verifyLog verifies the checksum of each data record
         *        in JE log files if it's true
         * @param verifyIndex runs verification on index. It can be combined
         *        with verifyBtree.
         * @param verifyRecord verifies data records on disk. It can be combined
         *        with verifyBtree.
         * @param btreeDelay configures the delay time between batches
         *        (1000 records) for verifyBtree
         * @param logDelay configures the the delay time between file reads
         *        for verifyLog
         * @param expiredTime the amount of time for which an existing
         *        verification will be considered valid.
         * @param showMissingFiles shows the corrupt files, including missing
         *        files and reserved files.
         */
        public VerificationOptions(boolean verifyBtree,
                                   boolean verifyLog,
                                   boolean verifyIndex,
                                   boolean verifyRecord,
                                   long btreeDelay,
                                   long logDelay,
                                   long expiredTime,
                                   boolean showMissingFiles) {
            this.verifyBtree = verifyBtree;
            this.verifyLog = verifyLog;
            this.verifyIndex = verifyIndex;
            this.verifyRecord = verifyRecord;
            this.btreeDelay = btreeDelay;
            this.logDelay = logDelay;
            this.expiredTime = expiredTime;
            this.showMissingFiles = showMissingFiles;

        }

        public boolean equals(VerificationOptions other) {
            if (other == null) {
                return false;
            }
            return verifyBtree == other.verifyBtree &&
                verifyLog == other.verifyLog &&
                verifyIndex == other.verifyIndex &&
                verifyRecord == other.verifyRecord &&
                btreeDelay == other.btreeDelay &&
                logDelay == other.logDelay &&
                showMissingFiles == other.showMissingFiles;

        }
    }
}
