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

package com.sleepycat.je.dbi;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.evictor.Evictor;
import com.sleepycat.je.latch.LatchSupport;
import com.sleepycat.je.rep.ReplicationConfig;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.utilint.TestHook;
import com.sleepycat.je.utilint.TestHookExecute;

/**
 * Singleton collection of environments.  To open an env call {@link #openEnv}.
 * The {@link #removeEnvironment} method must be called as the last step of
 * closing an environment.
 * <p>
 * A env pool is used to ensure that each env has only a single handle open at
 * a time. It is also used for sharing an {@link Evictor} among multiple envs,
 * when a {@link EnvironmentConfig#SHARED_CACHE} is configured. In the future
 * it could be used (with modifications) for sharing of additional resources
 * among envs.
 * <p>
 * When synchronizing on two or more of the following objects, the
 * synchronization order must be: (i) Environment, (ii) DbEnvPool, (iii)
 * EnvironmentImpl, (iv) Evictor.
 */
public class DbEnvPool {
    /* Singleton instance. */
    private static DbEnvPool pool = new DbEnvPool();

    /*
     * Collection of environment handles, mapped by canonical directory
     * name->EnvironmentImpl object.
     */
    private final Map<String, EnvironmentImpl> envs;

    /* Environments (subset of envs) that share the global cache. */
    private final Set<EnvironmentImpl> sharedCacheEnvs;

    /* Test hook, during Environment creation. */
    private TestHook<EnvironmentImpl> beforeFinishInitHook;

    /**
     * Enforce singleton behavior.
     */
    private DbEnvPool() {
        envs = new HashMap<String, EnvironmentImpl>();
        sharedCacheEnvs = new HashSet<EnvironmentImpl>();
    }

    /**
     * Access the singleton instance.
     */
    public static DbEnvPool getInstance() {
        return pool;
    }

    public void setBeforeFinishInitHook(TestHook<EnvironmentImpl> hook) {
        beforeFinishInitHook = hook;
    }

    public synchronized int getNSharedCacheEnvironments() {
        return sharedCacheEnvs.size();
    }

    private EnvironmentImpl getAnySharedCacheEnv() {
        Iterator<EnvironmentImpl> iter = sharedCacheEnvs.iterator();
        return iter.hasNext() ? iter.next() : null;
    }

    /**
     * Creates an env impl instance, adds it to the global set of envs, and
     * then calls {@link EnvironmentImpl#finishInit} to run recovery and
     * perform other initialization. Recovery (finishInit) is performed as a
     * separate step without synchronization to prevent other environment
     * openings from being blocked by synchronization during recovery.
     *
     * @return a new {@link RepImpl} if repConfig is non-null, or an {@link
     * EnvironmentImpl} if repConfig is null.
     */
    public EnvironmentImpl openEnv(File envHome,
                                   EnvironmentConfig config,
                                   ReplicationConfig repConfig)
        throws UnsupportedOperationException,
               EnvironmentNotFoundException,
               EnvironmentLockedException {

        final String environmentKey;
        final EnvironmentImpl envImpl;
        synchronized (this) {
            environmentKey = getEnvironmentMapKey(envHome);
            if (envs.containsKey(environmentKey)) {
                throw new UnsupportedOperationException(
                    "There is already an open environment handle for the" +
                    " environment directory");
            }

            /*
             * If a shared cache is used, get another (any other, doesn't
             * matter which) environment that is sharing the global cache.
             */
            final EnvironmentImpl sharedCacheEnv = config.getSharedCache() ?
                getAnySharedCacheEnv() : null;

            /*
             * Note that the environment is added to the SharedEvictor before
             * the EnvironmentImpl ctor returns, by RecoveryManager.buildTree.
             */
            envImpl =
                (repConfig == null) ?
                 new EnvironmentImpl(envHome, config, sharedCacheEnv) :
                 new RepImpl(envHome, config, sharedCacheEnv, repConfig);
            assert config.getSharedCache() == envImpl.getSharedCache();

            envs.put(environmentKey, envImpl);
            addToSharedCacheEnvs(envImpl);
        }

        /*
         * If finishInit() fails in any way, make sure it is removed from the
         * 'envs' map. If it isn't, this will block all future attempts to open
         * the environment.
         */
        TestHookExecute.doHookIfSet(beforeFinishInitHook, envImpl);
        boolean success = false;
        try {
            envImpl.finishInit(config);
            synchronized(this) {
                finishAdditionOfSharedCacheEnv(envImpl);
            }
            success = true;
        } finally {
            if (!success) {
                synchronized(this) {
                    envs.remove(environmentKey);
                    sharedCacheEnvs.remove(envImpl);
                }
            }
        }

        return envImpl;
    }

    /* Add this environment into sharedCache environments list. */
    private void addToSharedCacheEnvs(EnvironmentImpl envImpl)
        throws DatabaseException {

        if (envImpl.getSharedCache()) {
            if (sharedCacheEnvs.contains(envImpl)) {
                throw EnvironmentFailureException.unexpectedState();
            }
            sharedCacheEnvs.add(envImpl);
        }
    }

    /* Post-processing of SharedCacheEnv addition, after recovery is done. */
    private void finishAdditionOfSharedCacheEnv(EnvironmentImpl envImpl)
        throws DatabaseException {

        if (envImpl.getSharedCache()) {
            if (!sharedCacheEnvs.contains(envImpl)) {
                throw EnvironmentFailureException.unexpectedState();
            }
            assert envImpl.getEvictor().checkEnv(envImpl);
            resetSharedCache(-1, envImpl);
        }
    }

    /**
     * Called by EnvironmentImpl.setMutableConfig to perform the
     * setMutableConfig operation while synchronized on the DbEnvPool.
     *
     * In theory we shouldn't need to synchronize here when
     * envImpl.getSharedCache() is false; however, we synchronize
     * unconditionally to standardize the synchronization order and avoid
     * accidental deadlocks.
     */
    synchronized void setMutableConfig(EnvironmentImpl envImpl,
                                       EnvironmentMutableConfig mutableConfig)
        throws DatabaseException {

        envImpl.doSetMutableConfig(mutableConfig);
        if (envImpl.getSharedCache()) {
            resetSharedCache(envImpl.getMemoryBudget().getMaxMemory(),
                             envImpl);
        }
    }

    /**
     * Removes an EnvironmentImpl from the pool after it has been closed.
     * Note that the environment was removed from the SharedEvictor by
     * EnvironmentImpl.shutdownEvictor.
     */
    synchronized void removeEnvironment(EnvironmentImpl envImpl) {

        final String environmentKey =
            getEnvironmentMapKey(envImpl.getEnvironmentHome());

        final boolean found = envs.remove(environmentKey) != null;

        if (sharedCacheEnvs.remove(envImpl)) {

            assert found && envImpl.getSharedCache();
            assert !envImpl.getEvictor().checkEnv(envImpl);

            if (sharedCacheEnvs.isEmpty()) {
                envImpl.getEvictor().shutdown();
            } else {
                envImpl.getMemoryBudget().subtractCacheUsage();
                resetSharedCache(-1, null);
            }
        } else {
            assert !found || !envImpl.getSharedCache();
        }

        /*
         * Latch notes may only be cleared when there is no possibility that
         * any environment is open.
         */
        if (envs.isEmpty()) {
            LatchSupport.clear();
        }
    }

    /**
     * For unit testing only.
     */
    public synchronized void clear() {
        envs.clear();
    }

    /**
     * For unit testing only.
     */
    public synchronized Collection<EnvironmentImpl> getEnvImpls() {
        return envs.values();
    }

    /**
     * For unit testing only.
     */
    public synchronized boolean isOpen(final File home) {
        return envs.containsKey(getEnvironmentMapKey(home));
    }

    /* Use the canonical path name for a normalized environment key. */
    private String getEnvironmentMapKey(File file)
        throws DatabaseException {

        try {
            return file.getCanonicalPath();
        } catch (IOException e) {
            /* No env is available, can't throw EnvironmentFailedException. */
            throw EnvironmentFailureException.unexpectedException(e);
        }
    }

    /**
     * Resets the memory budget for all environments with a shared cache.
     *
     * @param newMaxMemory is the new total cache budget or is less than 0 if
     * the total should remain unchanged.  A total greater than zero is given
     * when it has changed via setMutableConfig.
     *
     * @param skipEnv is an environment that should not be reset, or null.
     * Non-null is passed when an environment has already been reset because
     * it was just created or the target of setMutableConfig.
     */
    private void resetSharedCache(long newMaxMemory, EnvironmentImpl skipEnv)
        throws DatabaseException {

        for (EnvironmentImpl envImpl : sharedCacheEnvs) {

            /*
             * To avoid spurious exceptions, don't reset invalid envs that have
             * not yet been removed.  They aren't usable, and we expect them
             * to be closed and removed very soon.
             */
            if (envImpl != skipEnv && envImpl.isValid()) {
                envImpl.getMemoryBudget().reset(newMaxMemory,
                                                false /*newEnv*/,
                                                envImpl.getConfigManager());
            }
        }
    }
}
