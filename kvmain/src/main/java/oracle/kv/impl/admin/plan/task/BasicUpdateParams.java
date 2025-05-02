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

package oracle.kv.impl.admin.plan.task;

import java.util.logging.Level;

import oracle.kv.impl.admin.plan.AbstractPlan;
import oracle.kv.impl.topo.ResourceId;

import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.util.ReplicationGroupAdmin;

/**
 * Base class for classes that update parameters.
 */
abstract class BasicUpdateParams extends SingleJobTask {
    private static final long serialVersionUID = 1L;

    final AbstractPlan plan;

    BasicUpdateParams(AbstractPlan plan) {
        this.plan = plan;
    }

    @Override
    protected AbstractPlan getPlan() {
        return plan;
    }

    /**
     * Delete a node from the JE replication group.
     */
    static boolean deleteMember(AbstractPlan plan,
                                ReplicationGroupAdmin jeAdmin,
                                String targetNodeName,
                                ResourceId targetId) {

        plan.getLogger().log(Level.FINE, "{0} deleting member: {1}",
                             new Object[]{plan, targetNodeName});
        final long timeout = 90000;
        final long check = 1000;
        final long stop = System.currentTimeMillis() + timeout;
        while (true) {
            try {
                jeAdmin.deleteMember(targetNodeName);
                return true;
            } catch (IllegalArgumentException iae) {
                /* Already a secondary, ignore */
                return true;
            } catch (UnknownMasterException | ReplicaStateException ume) {
                if (System.currentTimeMillis() > stop) {
                    logError(plan, targetId,
                             "the master was not found for deleteMember: " +
                             ume);
                    plan.getLogger().log(Level.INFO, "Exception", ume);
                    return false;
                }
                plan.getLogger().info(
                    "Waiting to retry deleteMember after unknown master");
                try {
                    Thread.sleep(check);
                } catch (InterruptedException e) {
                    logError(plan, targetId,
                             "waiting for the master for deleteMember was" +
                             " interrupted");
                    return false;
                }
            } catch (MemberActiveException mae) {
                /* This is unlikely as we just stopped the node */
                logError(plan, targetId,
                         "it is active when calling deleteMember");
                return false;
            } catch (MemberNotFoundException mnfe) {
                /* Already deleted, ignore */
                return true;
            } catch (MasterStateException mse) {
                logError(plan, targetId,
                         "it was the master when calling deleteMember");
                return false;
            } catch (OperationFailureException ofe) {
                logError(plan, targetId, "unexpected exception: " + ofe);
                return false;
            }
        }
    }

    /**
     * Log an error that occurred while updating parameters.
     */
    static void logError(AbstractPlan plan,
                         ResourceId targetId,
                         String cause) {
        plan.getLogger().log(
            Level.INFO, "{0} couldn''t update parameters for {1} because {2}",
            new Object[] {plan, targetId, cause });
    }
}
