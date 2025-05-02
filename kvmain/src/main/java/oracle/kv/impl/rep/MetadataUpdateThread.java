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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.kv.impl.api.RequestDispatcher;
import oracle.kv.impl.api.rgstate.RepGroupState;
import oracle.kv.impl.api.rgstate.RepNodeState;
import oracle.kv.impl.api.rgstate.UpdateThread;
import oracle.kv.impl.api.rgstate.UpdateThreadPoolExecutor.UpdateTask;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.metadata.MetadataInfo;
import oracle.kv.impl.rep.admin.ClientRepNodeAdminAPI;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.registry.RegistryUtils;

import com.sleepycat.je.rep.ReplicatedEnvironment;

/**
 * Thread to update the RNs metadata.
 */
public class MetadataUpdateThread extends UpdateThread {

    private final RepNode repNode;

    /**
     *  The time interval between executions of a concurrent update pass.
     */
    private static final int UPDATE_THREAD_PERIOD_MS = 2000;

    /**
     * Used to ensure that there is only one pull request running at one time.
     */
    private final Map<MetadataType, Semaphore> pullsInProgress =
            new EnumMap<MetadataType, Semaphore>(MetadataType.class);

    /**
     * Index of next group to update.
     */
    private int nextGroupIndex = 0;

    /**
     * Creates the RN state update thread.
     *
     * @param requestDispatcher the request dispatcher associated with this
     * thread
     *
     * @param repNode the rep node
     *
     * @param logger the logger used by the update threads
     */
    public MetadataUpdateThread(RequestDispatcher requestDispatcher,
                                RepNode repNode,
                                final Logger logger) {
        super(requestDispatcher, UPDATE_THREAD_PERIOD_MS,
              requestDispatcher.getExceptionHandler(), logger);
        this.repNode = repNode;
        for (final MetadataType type : MetadataType.values()) {
            pullsInProgress.put(type, new Semaphore(1));
        }
    }

    /**
     * Updates metadata for one group.
     */
    @Override
    protected void doUpdate() {
        /* Select a group to query */
        final Topology topo = repNode.getTopology();

        if (topo == null) {
            return; /* Not yet initialized */
        }
        final Set<RepGroupId> groups = topo.getRepGroupIds();

        /*
         * Nothing to update if only one group. We don't shut down the update
         * thread because the store may expand.
         */
        if (groups.size() < 2) {
            return;
        }

        /* Find the next group. */
        List<RepGroupId> sortedGroupIds = new ArrayList<RepGroupId>(groups);
        Collections.sort(sortedGroupIds);
        final int mygid = repNode.getRepNodeId().getGroupId();
        RepGroupId nextgid;

        while (true) {
            if (nextGroupIndex >= sortedGroupIds.size()) {
                nextGroupIndex = 0;
            }
            nextgid = sortedGroupIds.get(nextGroupIndex++);
            if (nextgid.getGroupId() == mygid) {
                continue;
            }
            break;
        }

        final RepGroupState rgState = getRepGroupState(nextgid);

        /*
         * Note that the size being passed in is the size of the entire store,
         * not the group, since although each update covers just one RG,
         * successive stalls at each RG could end up stalling many more threads
         * that the number needed for just one RG.
         */
        threadPool.tunePoolSize(topo.getRepNodeIds().size());

        /* Check all types of metadata except Topology */
        for (final MetadataType checkType : MetadataType.values()) {
            if (shutdown.get()) {
                return;
            }
            if (checkType == MetadataType.TOPOLOGY) {
                continue;
            }
            try {
                checkMetadata(checkType, rgState);
            } catch (Exception e) {

                /*
                 * If shutdown, simply return, even if there was an
                 * exception
                 */
                if (shutdown.get()) {
                    return;
                }

                /* Log and go on with check and update other metadata types. */
                logOnFailure(repNode.getRepNodeId(), e,
                             "Exception attempting to update " + checkType +
                             " metadata");
            }
        }
    }

    /**
     * Checks if the specified metadata type for the group represented by
     * rgState needs updating. This may result in this node being updated.
     *
     * @param type the metadata type to check
     * @param rgState the group state
     */
    private void checkMetadata(MetadataType type, RepGroupState rgState) {
        final int localSeqNum = repNode.getMetadataSeqNum(type);

        final int rgSeqNum = rgState.getSeqNum(type);

        /*
         * Check if we need an update. Note that except for the very first
         * check when reqSeqNum will be initialized to UNKNOWN_SEQ_NUM, we
         * will not know to pull unless our metadata is updated. This means
         * the system can't rely on pull for propagation.
         */
        assert Metadata.UNKNOWN_SEQ_NUM < Metadata.EMPTY_SEQUENCE_NUMBER;
        if (rgSeqNum >= localSeqNum) {
            return;
        }

        final RepNodeState mState = rgState.getMaster();

        if (mState == null) {
            addUpdateTasksForGroup(rgState, type);
        } else {
            if (!needsResolution(mState)) {
                logger.log(Level.FINE,
                           "Try update metadata for master: {0}", mState);
                threadPool.execute(new UpdateMetadata(rgState, mState, type, true));
            }
        }
    }

    private void addUpdateTasksForGroup(RepGroupState rgState,
                                        MetadataType type) {
        logger.log(Level.FINE,
                   "Try update metadata for every node in {0}.", rgState);
        for (RepNodeState rnState : rgState.getRepNodeStates()) {
            if (shutdown.get()) {
                return;
            }
            if (needsResolution(rnState)) {
                continue;
            }
            threadPool.execute(
                    new UpdateMetadata(rgState, rnState, type, false));
        }
    }

    /**
     * Task to update metadata to or from a RN.
     */
    private class UpdateMetadata implements UpdateTask {
        private final RepGroupState rgState;
        private final RepNodeState rnState;

        /* The metadata type to be pulled. */
        private final MetadataType type;

        /* Are we trying to update a master? */
        private final boolean isMaster;

        private UpdateMetadata(RepGroupState rgState,
                               RepNodeState rnState,
                               MetadataType type,
                               boolean isMaster) {
            this.rgState = rgState;
            this.rnState = rnState;
            this.type = type;
            this.isMaster = isMaster;
        }

        @Override
        public RepNodeId getResourceId() {
           return rnState.getRepNodeId();
        }

        @Override
        public void run() {
            logger.log(Level.FINE, "Starting {0}", this);
            final RepNodeId targetRNId = rnState.getRepNodeId();
            try {
                final RegistryUtils regUtils = requestDispatcher.getRegUtils();
                if (regUtils == null) {
                    /*
                     * The request dispatcher has not initialized itself as
                     * yet. Retry later.
                     */
                    return;
                }

                final ClientRepNodeAdminAPI rnAdmin =
                    regUtils.getClientRepNodeAdmin(
                        targetRNId, requestDispatcher.getProtocols());

                /* Start by getting the latest seq # from the RN */
                final int rgSeqNum = rgState.updateSeqNum(type,
                                            rnAdmin.getMetadataSeqNum(type));
                final int localSeqNum = repNode.getMetadataSeqNum(type);

                /* If we have newer MD, push, if behind, pull */
                if (localSeqNum > rgSeqNum) {
                    boolean success = push(rgSeqNum, regUtils);

                    /*
                     * If we have trouble update a master, update everyone in
                     * the group.
                     */
                    if (isMaster && !success) {
                        addUpdateTasksForGroup(rgState, type);
                    }
                } else if (localSeqNum < rgSeqNum) {
                    pull(localSeqNum, regUtils);
                }
            } catch (Exception e) {
                logOnFailure(targetRNId,  e,
                             "Exception updating " + targetRNId);
            }
        }

        /**
         * Push metadata changes to a RN. Incremental changes are
         * sent if available, otherwise, the entire metadata is pushed.
         */
        private boolean push(int rgSeqNum, RegistryUtils regUtils) {
            final RepNodeId targetRNId = rnState.getRepNodeId();
            try {
                final ClientRepNodeAdminAPI rnAdmin =
                    regUtils.getClientRepNodeAdmin(
                        targetRNId, requestDispatcher.getProtocols());
                final Metadata<?> md = repNode.getMetadata(type);

                final int updatedSeqNum;

                /* Attempt to update the node with deltas */
                final MetadataInfo info = md.getChangeInfo(rgSeqNum);
                if (info.isEmpty()) {
                    /* Cannot generate a delta so send entire metadata */
                    rnAdmin.updateMetadata(md);
                    updatedSeqNum = rnAdmin.getMetadataSeqNum(type);
                } else {
                    updatedSeqNum = rnAdmin.updateMetadata(info);
                }
                rgState.updateSeqNum(type, updatedSeqNum);

                if (updatedSeqNum < rgSeqNum) {
                    return false;
                }

                if (info.isEmpty()) {
                    logger.log(Level.FINE,
                               "Pushed {0} metadata changes [{1}] to {2}",
                               new Object[]{type, info, targetRNId});
                } else {
                    logger.log(Level.FINE, "Pushed {0} to {1}",
                               new Object[]{md, targetRNId});
                }
                return true;
            } catch (Exception e) {
                logOnFailure(targetRNId,  e,
                             "Failed pushing " +  type + " metadata to " +
                             targetRNId + ", target metadata seq number:" +
                             rgSeqNum);
                return false;
            }
        }

        /**
         * Pull metadata from the node.
         */
        private void pull(int localSeqNum, RegistryUtils regUtils) {
            final ReplicatedEnvironment repEnv = repNode.getEnv(1);
            if ((repEnv == null) || !repEnv.getState().isMaster()) {
                return;
            }

            if (!pullsInProgress.get(type).tryAcquire()) {
                /* A pull is already in progress. */
                return;
            }

            final RepNodeId targetRNId = rnState.getRepNodeId();
            try {
                final ClientRepNodeAdminAPI rnAdmin =
                    regUtils.getClientRepNodeAdmin(
                        targetRNId, requestDispatcher.getProtocols());

                /* Attempt to update from deltas */
                final MetadataInfo info =
                                        rnAdmin.getMetadata(type, localSeqNum);
                if (!info.isEmpty()) {
                    rgState.updateSeqNum(type, info.getSequenceNumber());
                    repNode.updateMetadata(info);
                    logger.log(Level.FINE,
                               "Pulled {0} metadata changes from {1}, " +
                               "updated to: {2}",
                               new Object[]{type, targetRNId,
                                            repNode.getMetadata(type)});
                    return;
                }

                /* Deltas are not available, try getting the entire metadata */
                final Metadata<?> md = rnAdmin.getMetadata(type);
                if (md != null) {
                    rgState.updateSeqNum(type, md.getSequenceNumber());
                    if (repNode.updateMetadata(md)) {
                        logger.log(Level.FINE,
                                   "Pulled {0} from {1}",
                                   new Object[]{md, targetRNId});
                    } else {
                        /*
                         * The update could have failed for numerous,
                         * non-serious reasons, so simply log.
                         */
                        logger.log(Level.FINE,
                                   "Unable to update {0} pulled from {1}",
                                    new Object[]{md, targetRNId});
                    }
                }
            } catch (Exception e) {
                logOnFailure(targetRNId, e,
                             "Failed pulling " + type + " metadata from " +
                             targetRNId);
            } finally {
                pullsInProgress.get(type).release();
            }
        }

        @Override
        public String toString() {
            return "UpdateThread[" + type + ", " + rnState.getRepNodeId() + "]";
        }
    }
}
