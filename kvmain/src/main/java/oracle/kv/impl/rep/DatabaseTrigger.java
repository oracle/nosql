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

package oracle.kv.impl.rep;

import java.util.logging.Logger;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.trigger.ReplicatedDatabaseTrigger;
import com.sleepycat.je.trigger.TransactionTrigger;
import com.sleepycat.je.trigger.Trigger;

/**
 * Utility class for implementing a JE database trigger.
 */
public abstract class DatabaseTrigger implements TransactionTrigger,
                                                 ReplicatedDatabaseTrigger {
    private String dbName;

    /**
     * Returns true if the component using the trigger is shut down.
     */
    protected abstract boolean isShutdown();

    /**
     * Returns a logger.
     */
    protected abstract Logger getLogger();

    /**
     * Returns the replicated environment or null. This call should not wait.
     */
    protected abstract ReplicatedEnvironment getEnv();
    
    @Override
    public void repeatTransaction(Transaction t) { }

    @Override
    public void repeatAddTrigger(Transaction t) { }

    @Override
    public void repeatRemoveTrigger(Transaction t) { }

    @Override
    public void repeatCreate(Transaction t) { }

    @Override
    public void repeatRemove(Transaction t) { }

    @Override
    public void repeatTruncate(Transaction t) { }

    @Override
    public void repeatRename(Transaction t, String string) { }

    @Override
    public void repeatPut(Transaction t, DatabaseEntry key,
                          DatabaseEntry newData) { }

    @Override
    public void repeatDelete(Transaction t, DatabaseEntry key) { }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public Trigger setDatabaseName(String string) {
        dbName = string;
        return this;
    }

    @Override
    public String getDatabaseName() {
        return dbName;
    }

    @Override
    public void addTrigger(Transaction t) { }

    @Override
    public void removeTrigger(Transaction t) { }

    @Override
    public void put(Transaction t, DatabaseEntry key, DatabaseEntry oldData,
                    DatabaseEntry newData) { }

    @Override
    public void delete(Transaction t, DatabaseEntry key,
                       DatabaseEntry oldData) { }

    @Override
    public void commit(Transaction t) { }

    @Override
    public void abort(Transaction t) { }

    /**
     * Returns true if the replicated environment is valid and this is a
     * replica.
     */
    protected boolean canContinue() {
        if (isShutdown()) {
            return false;
        }

        final ReplicatedEnvironment repEnv = getEnv();
        try {
            if ((repEnv != null) && repEnv.getState().isReplica()) {
                return true;
            }
            getLogger().fine("Environment changed, ignoring trigger");
        } catch (EnvironmentFailureException efe) {
            /* It's in the process of being re-established. */
            getLogger().fine("Environment changing, ignoring trigger");
        } catch (IllegalStateException ise) {
            /* A closed environment. */
            getLogger().fine("Environment closed, ignoring trigger");
        }
        return false;
    }
}
