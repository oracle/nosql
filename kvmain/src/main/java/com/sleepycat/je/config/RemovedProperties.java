/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * When moving to 24.2, we decide to remove numerous deprecated
 * ConfigParams, methods and classes. In order to minimize the effect
 * on the user side, log a message of those unsupported settings in
 * je.properties file and ignore them, instead of throwing an
 * IllegalArgumentException.
 * This class keeps track of those removed ConfigParams and is used as a
 * dictionary.
 */
public class RemovedProperties {
    private static final Set<String> properties;

    static {
        Set<String> constants = new HashSet<>();
        constants.add("je.evictor.wakeupInterval");
        constants.add("je.evictor.useMemoryFloor");
        constants.add("je.evictor.nodeScanPercentage");
        constants.add("je.evictor.evictionBatchPercentage");
        constants.add("je.maxOffHeapMemory");
        constants.add("je.offHeap.evictBytes");
        constants.add("je.offHeap.checksum");
        constants.add("je.env.runOffHeapEvictor");
        constants.add("je.env.sharedLatches");
        constants.add("je.offHeap.coreThreads");
        constants.add("je.offHeap.maxThreads");
        constants.add("je.offHeap.keepAlive");
        constants.add("je.checkpointer.wakeupInterval");
        constants.add("je.cleaner.minFilesToDelete");
        constants.add("je.cleaner.retries");
        constants.add("je.cleaner.restartRetries");
        constants.add("je.cleaner.calc.recentLNSizes");
        constants.add("je.cleaner.calc.minUncountedLNs");
        constants.add("je.cleaner.calc.initialAdjustments");
        constants.add("je.cleaner.calc.minProbeSkipFiles");
        constants.add("je.cleaner.calc.maxProbeSkipFiles");
        constants.add("je.cleaner.cluster");
        constants.add("je.cleaner.clusterAll");
        constants.add("je.cleaner.rmwFix");
        constants.add("je.txn.serializableIsolation");
        constants.add("je.lock.oldLockExceptions");
        constants.add("je.log.groupCommitInterval");
        constants.add("je.log.groupCommitThreshold");
        constants.add("je.log.useNIO");
        constants.add("je.log.directNIO");
        constants.add("je.log.chunkedNIO");
        constants.add("je.nodeDupTreeMaxEntries");
        constants.add("je.tree.maxDelta");
        constants.add("je.compressor.purgeRoot");
        constants.add("je.evictor.deadlockRetry");
        constants.add("je.cleaner.adjustUtilization");
        constants.add("je.cleaner.maxBatchFiles");
        constants.add("je.cleaner.foregroundProactiveMigration");
        constants.add("je.cleaner.backgroundProactiveMigration");
        constants.add("je.cleaner.lazyMigration");
        constants.add("je.rep.preserveRecordVersion");
        constants.add("je.rep.minRetainedVLSNs");
        constants.add("je.rep.repStreamTimeout");
        constants.add("je.rep.replayCostPercent");
        constants.add("java.util.logging.FileHandler.on");
        constants.add("java.util.logging.ConsoleHandler.on");
        constants.add("java.util.logging.DbLogHandler.on");
        constants.add("java.util.logging.level.lockMgr");
        constants.add("java.util.logging.level.recovery");
        constants.add("java.util.logging.level.evictor");
        constants.add("java.util.logging.level.cleaner");

        properties = Collections.unmodifiableSet(constants);
    }

    // For testing
    public static Iterator<String> getPropertyNames() {
        return properties.iterator();
    }

    public static boolean isRemoved(String name) {
        return properties.contains(name);
    }
}