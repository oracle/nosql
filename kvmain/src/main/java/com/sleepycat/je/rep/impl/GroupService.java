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

package com.sleepycat.je.rep.impl;

import static com.sleepycat.je.rep.impl.RepParams.GROUP_NAME;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.impl.RepGroupProtocol.AddNode;
import com.sleepycat.je.rep.impl.RepGroupProtocol.DeleteMember;
import com.sleepycat.je.rep.impl.RepGroupProtocol.FailReason;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupRequest;
import com.sleepycat.je.rep.impl.RepGroupProtocol.OverrideQuorum;
import com.sleepycat.je.rep.impl.RepGroupProtocol.RemoveMember;
import com.sleepycat.je.rep.impl.RepGroupProtocol.TransferMaster;
import com.sleepycat.je.rep.impl.RepGroupProtocol.UpdateAddress;
import com.sleepycat.je.rep.impl.RepGroupProtocol.UpdateGroup;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.TextProtocol.RequestProcessor;
import com.sleepycat.je.rep.impl.node.RepNode;
import com.sleepycat.je.rep.net.DataChannel;
import com.sleepycat.je.rep.utilint.ServiceDispatcher;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingRunnable;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ExecutingService;
import com.sleepycat.je.utilint.LoggerUtils;

public class GroupService extends ExecutingService
    implements RequestProcessor {

    /* The replication node */
    final RepNode repNode;
    final RepGroupProtocol protocol;

    /**
     * List of channels for in-flight requests.
     * The channel is in this collection while the request is being processed,
     * and must be removed before sending any response.
     *
     * @see #cancel
     * @see #unregisterChannel
     */
    private final Collection<DataChannel> activeChannels =
        new ArrayList<>();

    private final Logger logger;

    /* Identifies the Group Service. */
    public static final String SERVICE_NAME = "Group";

    /* Map different kinds of requests to different processors. */
    private Map<Class<? extends RequestMessage>, RequestProcessor>
        processMap = new HashMap<>();

    public GroupService(ServiceDispatcher dispatcher, RepNode repNode) {
        super(SERVICE_NAME, dispatcher);
        this.repNode = repNode;

        final DbConfigManager configManager =
            repNode.getRepImpl().getConfigManager();
        String groupName = configManager.get(GROUP_NAME);
        protocol =
            new RepGroupProtocol(groupName,
                                 repNode.getNameIdPair(),
                                 repNode.getRepImpl(),
                                 repNode.getRepImpl().getChannelFactory());
        logger = LoggerUtils.getLogger(getClass());
        initProcessMap();
    }

    private void initProcessMap() {
        processMap.put(AddNode.class, (req) -> {
            return process((AddNode) req);
        });
        processMap.put(DeleteMember.class, (req) -> {
            return process((DeleteMember) req);
        });
        processMap.put(GroupRequest.class, (req) -> {
            return process((GroupRequest) req);
        });
        processMap.put(OverrideQuorum.class, (req) -> {
            return process((OverrideQuorum) req);
        });
        processMap.put(RemoveMember.class, (req) -> {
            return process((RemoveMember) req);
        });
        processMap.put(TransferMaster.class, (req) -> {
            return process((TransferMaster) req);
        });
        processMap.put(UpdateAddress.class, (req) -> {
            return process((UpdateAddress) req);
        });
        processMap.put(UpdateGroup.class, (req) -> {
            return process((UpdateGroup) req);
        });
    }

    @Override
    protected void cancel() {
        Collection<DataChannel> channels;
        synchronized (this) {
            channels = new ArrayList<>(activeChannels);
            activeChannels.clear();
        }
        if (!channels.isEmpty()) {
            LoggerUtils.warning
                (logger, repNode.getRepImpl(),
                 "In-flight GroupService request(s) canceled: node shutdown");
        }
        for (DataChannel channel : channels) {
            try {
                PrintWriter out =
                    new PrintWriter(Channels.newOutputStream(channel), true);
                ResponseMessage rm =
                    protocol.new Fail(FailReason.DEFAULT, "shutting down");
                out.println(rm.wireFormat());
            } finally {
                if (channel.isOpen()) {
                    try {
                        channel.close();
                    }
                    catch (IOException e) {
                        LoggerUtils.warning
                            (logger, repNode.getRepImpl(),
                             "IO error on channel close: " + e.getMessage());
                    }
                }
            }
        }
    }

    /* Dynamically invoked process methods */
    public ResponseMessage process(RequestMessage requestMessage) {
        RequestProcessor processor = processMap.get(requestMessage.getClass());
        return processor.process(requestMessage);
    }

    /**
     * Wraps the replication group as currently cached on this node in
     * a Response message and returns it.
     */
    public ResponseMessage process(GroupRequest groupRequest) {
        RepGroupImpl group = repNode.getGroup();
        if (group == null) {
            return protocol.new Fail(groupRequest, FailReason.DEFAULT,
                                     "no group info yet");
        }
        return protocol.new GroupResponse(groupRequest, group);
    }

    /**
     * Removes a current member from the group.
     *
     * @param removeMember the request identifying the member to be removed.
     *
     * @return OK message if the member was removed from the group.
     */
    public ResponseMessage process(RemoveMember removeMember) {
        final String nodeName = removeMember.getNodeName();
        try {
            ensureMaster();
            repNode.removeMember(nodeName);
            return protocol.new OK(removeMember);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(removeMember, FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(removeMember, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(removeMember, FailReason.IS_REPLICA,
                                     e.getMessage());
        }  catch (DatabaseException e) {
            return protocol.new Fail(removeMember, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Deletes a current member from the group, which marks the node as removed
     * and deletes its entry from the rep group DB.
     *
     * @param deleteMember the request identifying the member to be deleted
     *
     * @return OK message if the member was deleted from the group
     */
    public ResponseMessage process(DeleteMember deleteMember) {
        final String nodeName = deleteMember.getNodeName();
        try {
            ensureMaster();
            RepGroupImpl group = repNode.getGroup();
            RepNodeImpl node = group.getNode(nodeName);
            if (node == null) {
                return protocol.new Fail(deleteMember,
                    FailReason.MEMBER_NOT_FOUND, "Could not find node " +
                    nodeName + " in the group on remote node " +
                    repNode.getName());
            }
            /*
             * Transient nodes are not stored persistently, so they need to be
             * deleted by a different path.  Note this path is only used when
             * editing the name, id, or type of an existing transient node when
             * using ReplicationGroupAdmin.putNode.  If the transient node is
             * not delete in that path it could lead to zombie entries in the
             * in memory node name and node id maps of the group.
             * Using ReplicationGroupAdmin.deleteMember to delete a transient
             * node will result in an exception.
             */
            if (node.getType().hasTransientId()) {
                group.removeTransientNode(node);
            } else {
                repNode.removeMember(nodeName, true);
            }
            return protocol.new OK(deleteMember);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(deleteMember, FailReason.MEMBER_NOT_FOUND,
                                     e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(deleteMember, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(deleteMember, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (MemberActiveException e) {
            return protocol.new Fail(deleteMember, FailReason.MEMBER_ACTIVE,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(deleteMember, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Update the network address for a dead replica.
     *
     * @param updateAddress the request identifying the new network address for
     * the node.
     *
     * @return OK message if the address is successfully updated.
     */
    public ResponseMessage process(UpdateAddress updateAddress) {
        try {
            ensureMaster();
            repNode.updateAddress(updateAddress.getNodeName(),
                                  updateAddress.getNewHostName(),
                                  updateAddress.getNewPort());
            return protocol.new OK(updateAddress);
        } catch (MemberNotFoundException e) {
            return protocol.new Fail(
                updateAddress, FailReason.MEMBER_NOT_FOUND, e.getMessage());
        } catch (MasterStateException e) {
            return protocol.new Fail(updateAddress, FailReason.IS_MASTER,
                                     e.getMessage());
        } catch (ReplicaStateException e) {
            return protocol.new Fail(updateAddress, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(updateAddress, FailReason.DEFAULT,
                                     e.getMessage());
        }
    }

    /**
     * Adds a node in the in-memory RepGroup and in the RepGroupDB if it is not
     * a transient type. This is used by ReplicationGroupAdmin to repair
     * replication group information, and as such does not perform many of the
     * usual consistency checks.
     *
     * @param addNode the node to be added to the RepGroup.
     * @return OK message if the node is successfully added.
     */
    public ResponseMessage process(AddNode addNode) {
        final RepNodeImpl node = addNode.getNode();
        try {
            ensureMaster();
            if (node.getType().hasTransientId()) {
                /* Transient nodes are only updated in memory */
                repNode.addTransientIdNode(node);
            } else {
                repNode.getRepGroupDB().updateMember(node, true);
            }
            return protocol.new OK(addNode);
        } catch (ReplicaStateException e) {
            return protocol.new Fail(addNode, FailReason.IS_REPLICA,
                            e.getMessage());
        } catch (DatabaseException | IllegalStateException e) {
            return protocol.new Fail(addNode, FailReason.DEFAULT,
                            e.getMessage());
        }
    }

    /**
     * Updates the group information in both the in-memory RepGroup and in the
     * RepGroupDB. This is used by ReplicationGroupAdmin to repair replication
     * group information, and as such does not perform many of the usual
     * consistency checks.
     *
     * @param group the updated group information
     * @return OK message if the group was successfully updated.
     */
    public ResponseMessage process(UpdateGroup group) {
        final RepGroupImpl repGroup = group.getGroup();
        try {
            ensureMaster();
            repNode.getRepGroupDB().updateGroup(repGroup);
        } catch (ReplicaStateException e) {
            return protocol.new Fail(group, FailReason.IS_REPLICA,
                            e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(group, FailReason.DEFAULT, e.getMessage());
        }
        return protocol.new OK(group);
    }

    /**
     * Overrides the quorum size with the new value, used to update the
     * replication group information database when there are not enough
     * nodes up to form quorum.
     *
     * @param override is the new quorum size.
     * @return always the OK message.
     */
    public ResponseMessage process(OverrideQuorum override) {
        final int newQuorumSize = override.getQuorumSize();
        final RepImpl rep = repNode.getRepImpl();
        final ReplicationMutableConfig config = rep.cloneRepMutableConfig();
        config.setElectableGroupSizeOverride(newQuorumSize);
        rep.setRepMutableConfig(config);
        return protocol.new OK(override);
    }

    /**
     * Transfer the master role from the current master to one of the specified
     * replicas.
     *
     * @param transferMaster the request identifying nodes to be considered for
     * the role of new master
     * @return null
     */
    public ResponseMessage process(TransferMaster transferMaster) {
        try {
            ensureMaster();
            final String nodeList = transferMaster.getNodeNameList();
            final Set<String> replicas = parseNodeList(nodeList);
            final long timeout = transferMaster.getTimeout();
            final boolean force = transferMaster.getForceFlag();
            String winner = repNode.transferMaster(
                replicas, timeout, force, transferMaster.getReason());
            return protocol.new TransferOK(transferMaster, winner);
        } catch (ReplicaStateException e) {
            return protocol.new Fail(transferMaster, FailReason.IS_REPLICA,
                                     e.getMessage());
        } catch (MasterTransferFailureException e) {
            return protocol.new Fail(transferMaster, FailReason.TRANSFER_FAIL,
                                     e.getMessage());
        } catch (DatabaseException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.getMessage());
        } catch (IllegalArgumentException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.toString());
        } catch (IllegalStateException e) {
            return protocol.new Fail(transferMaster, FailReason.DEFAULT,
                                     e.toString());
        }
    }

    private Set<String> parseNodeList(String list) {
        Set<String> set = new HashSet<>();
        StringTokenizer st = new StringTokenizer(list, ",");
        while (st.hasMoreTokens()) {
            set.add(st.nextToken());
        }
        return set;
    }

    private void ensureMaster() throws ReplicaStateException {
        if (!repNode.isMaster()) {
            throw new ReplicaStateException
                ("GroupService operation can only be performed at master");
        }
    }

    synchronized private void registerChannel(DataChannel dc) {
        activeChannels.add(dc);
    }

    /**
     * Removes the given {@code DataChannel} from our list of active channels.
     * <p>
     * Before sending any response on the channel, this method must be invoked
     * to claim ownership of it.
     * This avoids a potential race between the request processing thread in
     * the normal case, and a thread calling {@code cancel()} at env shutdown
     * time.
     *
     * @return true, if the channel is still active (usual case); false
     * otherwise, presumably because the service was shut down.
     */
    synchronized private boolean unregisterChannel(DataChannel dc) {
        return activeChannels.remove(dc);
    }

    @Override
    public Runnable getRunnable(DataChannel dataChannel) {
        return new GroupServiceRunnable(dataChannel, protocol);
    }

    class GroupServiceRunnable extends ExecutingRunnable {
        GroupServiceRunnable(DataChannel dataChannel,
                             RepGroupProtocol protocol) {
            super(dataChannel, protocol, true);
            registerChannel(dataChannel);
        }

        @Override
        protected ResponseMessage getResponse(RequestMessage request)
            throws IOException {

            ResponseMessage rm = protocol.process(GroupService.this, request);

            /*
             * If the channel has already been closed, before we got a chance to
             * produce the response, then just discard the tardy response and
             * return null.
             */
            return unregisterChannel(channel) ? rm : null;
        }

        @Override
        protected void logMessage(String message) {
            LoggerUtils.warning(logger, repNode.getRepImpl(), message);
        }
    }
}
