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
package com.sleepycat.je.rep.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Formatter;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.MasterStateException;
import com.sleepycat.je.rep.MasterTransferFailureException;
import com.sleepycat.je.rep.MemberActiveException;
import com.sleepycat.je.rep.MemberNotFoundException;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicaStateException;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationMutableGroup;
import com.sleepycat.je.rep.ReplicationMutableNode;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.UnknownMasterException;
import com.sleepycat.je.rep.elections.Learner;
import com.sleepycat.je.rep.elections.MasterValue;
import com.sleepycat.je.rep.elections.Protocol;
import com.sleepycat.je.rep.elections.TimebasedProposalGenerator;
import com.sleepycat.je.rep.impl.GroupService;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepGroupProtocol;
import com.sleepycat.je.rep.impl.RepGroupProtocol.Fail;
import com.sleepycat.je.rep.impl.RepGroupProtocol.GroupResponse;
import com.sleepycat.je.rep.impl.RepGroupProtocol.TransferOK;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.impl.TextProtocol.MessageExchange;
import com.sleepycat.je.rep.impl.TextProtocol.OK;
import com.sleepycat.je.rep.impl.TextProtocol.ProtocolError;
import com.sleepycat.je.rep.impl.TextProtocol.RequestMessage;
import com.sleepycat.je.rep.impl.TextProtocol.ResponseMessage;
import com.sleepycat.je.rep.impl.node.NameIdPair;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.ReplicationFormatter;
import com.sleepycat.je.rep.utilint.ServiceDispatcher.ServiceConnectFailedException;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.LoggerUtils;

/**
 * Administrative APIs for use by applications which do not have direct access
 * to a replicated environment. The class supplies methods that can be
 * used to list group members, remove members, update network addresses, edit
 * group member information, and find the current master.
 *
 * Information is found and updated by querying nodes in the group. Because of
 * that, ReplicationGroupAdmin can only obtain information when there is at
 * least one node alive in the replication group.
 *
 * Changes to the group information require that there be a master present and
 * that there is a quorum of replicas to acknowledge the changes.
 * If there are not enough nodes in the group to form a quorum and elect a
 * master, that can be overridden using {@link overrideQuorum(int)}
 */
public class ReplicationGroupAdmin {

    private final String groupName;
    private Set<InetSocketAddress> helperSockets;
    private final Protocol electionsProtocol;
    private final RepGroupProtocol groupProtocol;
    private final Logger logger;
    private final Formatter formatter;
    private final DataChannelFactory channelFactory;

    /**
     * Constructs a group admin object.
     *
     * @param groupName the name of the group to be administered
     * @param helperSockets the sockets on which it can contact helper nodes
     * in the replication group to carry out admin services.
     */
    public ReplicationGroupAdmin(String groupName,
                                 Set<InetSocketAddress> helperSockets) {
        this(groupName, helperSockets,
             ReplicationNetworkConfig.createDefault());
    }

    /**
     * @hidden SSL deferred
     * Constructs a group admin object.
     *
     * @param groupName the name of the group to be administered
     * @param helperSockets the sockets on which it can contact helper nodes
     * in the replication group to carry out admin services.
     * @param repNetConfig a network configuration to use
     */
    public ReplicationGroupAdmin(String groupName,
                                 Set<InetSocketAddress> helperSockets,
                                 ReplicationNetworkConfig repNetConfig) {
        this(groupName, helperSockets,
             initializeFactory(repNetConfig, groupName));
    }

    /**
     * @hidden SSL deferred
     * Constructs a group admin object.
     *
     * @param groupName the name of the group to be administered
     * @param helperSockets the sockets on which it can contact helper nodes
     * in the replication group to carry out admin services.
     * @param channelFactory the factory for channel creation
     */
    public ReplicationGroupAdmin(String groupName,
                                 Set<InetSocketAddress> helperSockets,
                                 DataChannelFactory channelFactory) {
        this.groupName = groupName;
        this.helperSockets = helperSockets;
        this.channelFactory = channelFactory;

        electionsProtocol =
            new Protocol(TimebasedProposalGenerator.getParser(),
                         MasterValue.getParser(),
                         groupName,
                         NameIdPair.NOCHECK,
                         null /* repImpl */,
                         channelFactory);
        groupProtocol =
            new RepGroupProtocol(groupName, NameIdPair.NOCHECK, null,
                                 channelFactory);
        logger = LoggerUtils.getLoggerFixedPrefix
            (getClass(), NameIdPair.NOCHECK.toString());
        formatter = new ReplicationFormatter(NameIdPair.NOCHECK);
    }

    /**
     * Returns the helper sockets being used to contact a replication group
     * member, in order to query for the information.
     *
     * @return the set of helper sockets.
     */
    public Set<InetSocketAddress> getHelperSockets() {
        return helperSockets;
    }

    /**
     * Sets the helper sockets being used to contact a replication group
     * member, in order to query for the information.
     *
     * @param helperSockets the sockets on which it can contact helper nodes
     * in the replication group to carry out admin services.
     */
    public void setHelperSockets(Set<InetSocketAddress> helperSockets) {
        this.helperSockets = helperSockets;
    }

    /**
     * Returns the name of the replication group.
     *
     * @return the group name.
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Returns the socket address associated with the node that's currently
     * the master.
     *
     * @return the socket address associated with the master
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    private InetSocketAddress getMasterSocket()
        throws UnknownMasterException,
               EnvironmentFailureException {

        MasterValue masterValue = Learner.findMaster(electionsProtocol,
                                                     helperSockets,
                                                     logger,
                                                     null,
                                                     formatter);
        return new InetSocketAddress(masterValue.getHostName(),
                                     masterValue.getPort());
    }

    /**
     * Returns the node name associated with the master
     *
     * @return the master node ID
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public String getMasterNodeName()
        throws UnknownMasterException,
               EnvironmentFailureException {
        MasterValue masterValue = Learner.findMaster(electionsProtocol,
                                                     helperSockets,
                                                     logger,
                                                     null,
                                                     formatter);
        return masterValue.getNodeName();
    }

    /**
     * Removes this node from the group, so that it is no longer a member of
     * the group. When removed, it will no longer be able to connect to a
     * master, nor can it participate in elections.Once removed, a node cannot
     * be added again to the group under the same node name.
     * <p>
     * Ideally, the node being removed should be shut down before this call is
     * issued.
     * <p>
     * If the node is an active <code>Replica</code> the master will terminate
     * its connection with the node and will not allow the replica to reconnect
     * with the group, since it's no longer a member of the group. If the node
     * wishes to re-join it should do so with a different node name.
     * <p>
     * An active Master cannot be removed. It must first be shutdown, or
     * transition to the <code>Replica</code> state before it can be removed
     * from the group.
     * <p>
     * {@link NodeType#SECONDARY Secondary} nodes cannot be removed; they
     * automatically leave the group when they are shut down or become
     * disconnected from the master.
     *
     * @param nodeName identifies the node being removed from the group
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws IllegalArgumentException if the type of the node is {@code
     * SECONDARY}
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>nodeName</code> is not a member of the replication group
     *
     * @throws MasterStateException if the member being removed is currently
     * the Master
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     * @see <a href=
     * "{@docRoot}/../ReplicationGuide/utilities.html#node-addremove" target=
     * "_top">Adding and Removing Nodes From the Group</a>
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public void removeMember(String nodeName)
        throws UnknownMasterException,
               MemberNotFoundException,
               MasterStateException,
               EnvironmentFailureException {

        final String masterErrorMessage = "Cannot remove an active master";
        final RequestMessage request =
            groupProtocol.new RemoveMember(nodeName);

        final RepNodeImpl node = checkMember(
            nodeName, masterErrorMessage, /* electableOnly */ false);
        if (node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Cannot remove node with transient ID: " + nodeName);
        }

        doMessageExchange(request, OK.class);
    }

    /**
     * @hidden internal, for use in disaster recovery [#23447]
     *
     * Deletes this node from the group, so that it is no longer a member of
     * the group. When deleted, it will not connect to a master, or participate
     * in elections until the environment is reopened. Unlike removed nodes,
     * deleted nodes are completely removed from the group, so they can be
     * added again to the group under the same node name.
     * <p>
     * The node being deleted must be shut down before this call is issued.
     * <p>
     * If the node is an active <code>Replica</code> the master will terminate
     * its connection with the node.
     * <p>
     * An active Master cannot be deleted. It must first be shutdown, or
     * transition to the <code>Replica</code> state before it can be deleted
     * from the group.
     * <p>
     * {@link NodeType#SECONDARY Secondary} and {@link NodeType#EXTERNAL
     * External} nodes cannot be deleted; they automatically leave the group
     * when they are shut down or become disconnected from the master.
     *
     * @param nodeName identifies the node being deleted from the group
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws MemberActiveException if the type of the node is {@code
     * SECONDARY} or {@code EXTERNAL}, or if the node is active
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>nodeName</code> is not a member of the replication group
     *
     * @throws MasterStateException if the member being deleted is currently
     * the Master
     *
     * @throws ReplicaStateException if the master transferred during the time
     * of the delete operation
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs.
     */
    public void deleteMember(String nodeName)
        throws UnknownMasterException,
               MemberActiveException,
               MemberNotFoundException,
               MasterStateException,
               ReplicaStateException,
               EnvironmentFailureException {

        final String masterErrorMessage = "Cannot delete an active master";
        final RequestMessage request =
            groupProtocol.new DeleteMember(nodeName);

        final RepNodeImpl node = checkMember(
            nodeName, masterErrorMessage, /* electableOnly */ false);
        if (node.getType().hasTransientId()) {
            throw new IllegalArgumentException(
                "Cannot delete node with transient ID: " + nodeName);
        }

        doMessageExchange(request, OK.class);
    }

    /**
     * Returns the current composition of the group from the Master.
     *
     * @return the group description
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     */
    public ReplicationGroup getGroup()
        throws UnknownMasterException,
               EnvironmentFailureException {

        GroupResponse resp = (GroupResponse) doMessageExchange
            (groupProtocol.new GroupRequest(), GroupResponse.class);

        return new ReplicationGroup(resp.getGroup());
    }

    /**
     * Returns the {@link com.sleepycat.je.rep.NodeState state} of a replicated
     * node and <code>state</code> of the application where the node is
     * running in.
     *
     * @param repNode a ReplicationNode includes those information which are
     * needed to connect to the node
     * @param socketConnectTimeout the timeout value for creating a socket
     * connection with the replicated node
     *
     * @return the state of the replicated node
     *
     * @throws IOException if the machine is down or no response is returned
     *
     * @throws ServiceConnectFailedException if can't connect to the service
     * running on the replicated node
     */
    public NodeState getNodeState(ReplicationNode repNode,
                                  int socketConnectTimeout)
        throws IOException, ServiceConnectFailedException {

        DbPing ping = new DbPing(
            repNode, groupName, socketConnectTimeout, channelFactory);

        return ping.getNodeState();
    }

    /**
     * Returns the {@link com.sleepycat.je.rep.NodeState state} of a replicated
     * node and <code>state</code> of the application where the node is
     * running in.  Assumes the helperHosts contains the address of the node.
     *
     * @param nodeName of the node to contact about the state of the node.
     * @param socketConnectTimeout the timeout value for creating a socket
     * connection with the replicated node
     *
     * @return the state of the replicated node
     * @throws Exception if unable to contact the node.
     *
     */
    public NodeState getNodeState(String nodeName,
                    int socketConnectTimeout) throws Exception {
        NodeState state = null;
        Exception exception = null;

        Iterator<InetSocketAddress> addresses = helperSockets.iterator();
        while (addresses.hasNext()) {
            try {
                final DbPing ping = new DbPing(nodeName, groupName,
                                addresses.next(), socketConnectTimeout, null);
                state = ping.getNodeState();
                break;
            } catch (Exception e) {
                exception = e;
            }
        }
        if (state == null && exception != null) {
            throw exception;
        }
        return state;
    }

    /**
     * Used to override the number of nodes needed to elect a master or form
     * quorum.  This function should only be used when there are not enough
     * nodes up or in communication with each other to elect a master or
     * form a quorum.  Use {@link getNodeState(String, int) getNodeState} to
     * get the status of each node in the group.
     * This function sets the replication parameter
     * {@link ReplicationMutableConfig#ELECTABLE_GROUP_SIZE_OVERRIDE ELECTABLE_GROUP_SIZE_OVERRIDE}
     * for each node included in the helperSockets. Specify 0 to clear to
     * override.
     * @throws Exception if unable to connect to one of the helper sockets or
     *  unable to set the override value on one of the sites.
     */
    public void overrideQuorum(int newQuorumSize) throws Exception {
        if (newQuorumSize < 0) {
            throw new IllegalArgumentException(
                "The new quorum size must be 0 or greater.");
        }
        Iterator<InetSocketAddress> addresses = helperSockets.iterator();
        while (addresses.hasNext()) {
            final InetSocketAddress address = addresses.next();
            final MessageExchange me = groupProtocol.new MessageExchange(
                            address,
                            GroupService.SERVICE_NAME,
                            groupProtocol.new OverrideQuorum(newQuorumSize));
            me.run();

            ResponseMessage resp = me.getResponseMessage();

            if (resp == null) {
                if (me.getException() != null) {
                    throw me.getException();
                }
            }

            if (!(resp instanceof OK)) {
                throw getException(resp);
            }
        }
    }

    /**
     * Update the network address for a specified member of the replication
     * group. When updating the address of this target replication node, the
     * node cannot be alive. One common use case is when the replication member
     * must be moved to a new host, possibly because of machine failure.
     * <p>
     * To make a network address change, take these steps:
     * <ol>
     * <li> Shutdown the node that is being updated.
     * <li> Use this method to change the hostname and port of the node.
     * <li> Start the node on the new machine, or at its new port, using the new
     *    hostname/port. If the log files are available at the node, they will
     *    be reused. A network restore operation may need to be initiated by
     *    the application to copy over any needed log files if no log files are
     *    available, or if they have become obsolete.
     * </ol>
     * <p>
     * The address of a {@link NodeType#SECONDARY} node cannot be updated with
     * this method, since nodes must be members but not alive to be updated,
     * and secondary nodes are not members when they are not alive.  To change
     * the address of a secondary node, restart the node with the updated
     * address.
     *
     * @param nodeName the name of the node whose address will be updated.
     * @param newHostName the new host name of the node
     * @param newPort the new port number of the node
     *
     * @throws EnvironmentFailureException if an unexpected, internal or
     * environment-wide failure occurs
     *
     * @throws MasterStateException if the member being updated is currently
     * the master
     *
     * @throws MemberNotFoundException if the node denoted by
     * <code>nodeName</code> is not a member of the replication group
     *
     * @throws ReplicaStateException if the member being updated is currently
     * alive
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @see DbResetRepGroup DbResetRepGroup, which can be used in a
     * related but different use case to copy and move a group.
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public void updateAddress(String nodeName, String newHostName, int newPort)
        throws EnvironmentFailureException,
               MasterStateException,
               MemberNotFoundException,
               ReplicaStateException,
               UnknownMasterException {

        final String masterErrorMessage =
            "Can't update address for the current master.";
        RequestMessage request =
            groupProtocol.new UpdateAddress(nodeName, newHostName, newPort);

        checkMember(nodeName, masterErrorMessage, /* electableOnly */ false);
        doMessageExchange(request, OK.class);
    }

    public String transferMaster(Set<String> nodeNames,
            int timeout,
            TimeUnit timeUnit,
            boolean force) {
        return transferMaster(nodeNames, timeout, timeUnit, force, null);
    }

    /**
     * Transfers the master state from the current master to one of the
     * electable replicas supplied in the argument list.  This method sends a
     * request to the original master to perform the operation.
     *
     * @throws MasterTransferFailureException if the master transfer operation
     * fails
     *
     * @throws UnknownMasterException if the master was not found
     *
     * @throws IllegalArgumentException if {@code nodeNames} contains the name
     * of a node that is not electable
     *
     * @see ReplicatedEnvironment#transferMaster
     */
    public String transferMaster(Set<String> nodeNames,
                                 int timeout,
                                 TimeUnit timeUnit,
                                 boolean force,
                                 String reason)
        throws MasterTransferFailureException,
               UnknownMasterException {

        for (String node : nodeNames) {
            checkMember(node, null, /* electableOnly */ true);
        }
        final String nodeNameList = commaJoin(nodeNames);
        final long timeoutMillis = timeUnit.toMillis(timeout);
        final RequestMessage transferMaster =
            groupProtocol.new TransferMaster(nodeNameList,
                                             timeoutMillis, force, reason);
        TransferOK result =
            (TransferOK)doMessageExchange(transferMaster, TransferOK.class);
        return result.getWinner();
    }

    /**
     * Gets the ReplicationMutableGroup object from the RepGroupDB.
     *
     * @return the ReplicationMutableGroup for the group.
     */
    public ReplicationMutableGroup getMutableGroup() {
        return new ReplicationMutableGroup(getGroup().getRepGroupImpl());
    }

    /**
     * Inserts the group represented by the argument into RepGroupDB.
     *
     * @param group the group to insert
     */
    public void putGroup(ReplicationMutableGroup group)
        throws EnvironmentFailureException, MasterStateException,
        MemberNotFoundException, UnknownMasterException {
        doMessageExchange(
            groupProtocol.new UpdateGroup(group.getGroup()), OK.class);
    }

    /**
     * Gets a ReplicationMutableNode object from the RepGroupDB.
     *
     * @param nodeName name of the node to get.
     * @return the ReplicationMutableNode for the node.
     */
    public ReplicationMutableNode getNode(String nodeName)
        throws UnknownMasterException, EnvironmentFailureException {
        if (nodeName == null) {
            throw new IllegalArgumentException("Node name must be non-null.");
        }
        final RepGroupImpl group = getGroup().getRepGroupImpl();
        RepNodeImpl node = group.getNode(nodeName);

        return new ReplicationMutableNode(node, group, false);
    }

    /**
     * Creates a new ReplicationMutableNode object with the given information.
     *
     * @param nodeName name of the node
     * @param hostName host address of the node
     * @param port     port of the node
     * @param type     type of the node
     * @return the newly created RepNodeImpl object.
     */
    public ReplicationMutableNode createNode(String nodeName, String hostName,
        int port, NodeType type)
        throws UnknownMasterException, EnvironmentFailureException {
        if (nodeName == null || hostName == null || type == null) {
            throw new IllegalArgumentException(
                "Node, hostName, and type must be non null");
        }
        final RepGroupImpl group = getGroup().getRepGroupImpl();
        final NameIdPair pair =
            new NameIdPair(nodeName, group.getNextNodeId());
        final RepNodeImpl node = new RepNodeImpl(pair, type, true, false,
            hostName, port, group.getChangeVersion(),
            JEVersion.CURRENT_VERSION);
        return new ReplicationMutableNode(node, group, true);
    }

    /**
     * Inserts a node into the RepGroupDB.
     *
     * @param node the node to insert
     */
    public void putNode(ReplicationMutableNode node)
        throws EnvironmentFailureException, MasterStateException,
        MemberNotFoundException, UnknownMasterException {
        final RepNodeImpl oldNode = node.getOldNode();
        if ((oldNode != null) &&
            (!oldNode.getName().matches(node.getName()) ||
             (oldNode.getNodeId() != node.getId()))) {
            doMessageExchange(
                groupProtocol.new DeleteMember(oldNode.getName()), OK.class);
            node.clearOldNode();
        }
        doMessageExchange(
            groupProtocol.new AddNode(node.getNode()), OK.class);
    }

    private String commaJoin(Set<String> words) {
        boolean first = true;
        StringBuilder sb = new StringBuilder();
        for (String w : words) {
            if (!first) {
                sb.append(',');
            }
            sb.append(w);
            first = false;
        }
        return sb.toString();
    }

    /*
     * Check that the specified node is an appropriate target. For example,
     * make sure it's a valid node in the group, and it's not the same as
     * the original node.
     */
    private RepNodeImpl checkMember(String nodeName,
                                    String masterErrorMessage,
                                    boolean electableOnly)
        throws MasterStateException,
               MemberNotFoundException {

        final RepGroupImpl group = getGroup().getRepGroupImpl();
        final RepNodeImpl node = group.getNode(nodeName);

        /* Check the membership. */
        if (node == null ||
            /* Creation is not yet acknowledged */
            (!node.isRemoved() && !node.isQuorumAck())) {
            throw new MemberNotFoundException("Node: " + nodeName +
                                              " is not a member of the " +
                                              "group: " + groupName);
        }

        if (electableOnly && !node.getType().isElectable()) {
            throw new IllegalArgumentException("Node: " + nodeName +
                                               " must have node type" +
                                               " ELECTABLE, was " +
                                               node.getType());
        }

        if (node.isRemoved() && node.isQuorumAck()) {
            throw new MemberNotFoundException("Node: " + nodeName +
                                              " is not currently a member " +
                                              "of the group: " + groupName +
                                              ", it has been removed.");
        }

        /* Check if the node itself is the master. */
        if (masterErrorMessage != null) {
            final InetSocketAddress masterAddress = getMasterSocket();
            if (masterAddress.equals(node.getSocketAddress())) {
                throw new MasterStateException(masterErrorMessage);
            }
        }

        return node;
    }

    /* Do a message exchange with the targeted master. */
    private ResponseMessage doMessageExchange(RequestMessage request,
                                              Class<?> respClass)
        throws EnvironmentFailureException,
               MasterStateException,
               MemberNotFoundException,
               UnknownMasterException {

        /* Do the communication. */
        final InetSocketAddress masterAddress = getMasterSocket();
        final MessageExchange me = groupProtocol.new MessageExchange
            (masterAddress, GroupService.SERVICE_NAME, request);
        me.run();

        ResponseMessage resp = me.getResponseMessage();

        if (resp == null) {
            if (me.getException() != null) {
                throw new UnknownMasterException
                    ("Problem communicating with master.", me.getException());
            }

            /*
             * Returning null on success is part of the message protocol, the
             * caller expects it.
             */
            return null;
        }
        if (respClass == null && resp instanceof Fail) {
            throw getException(resp);
        }

        if (respClass != null &&
            !(resp.getClass().getName().equals(respClass.getName()))) {
            throw getException(resp);
        }

        return resp;
    }

    /**
     * Examines the response and generates a meaningful error exception.
     */
    private DatabaseException getException(ResponseMessage resp) {
        if (resp == null) {
            return EnvironmentFailureException.unexpectedState
                ("No response to request");
        }

        if (resp instanceof Fail) {
            Fail fail = (Fail) resp;
            switch (fail.getReason()) {
                case MEMBER_NOT_FOUND:
                    return new MemberNotFoundException(fail.getMessage());
                case IS_MASTER:
                    return new MasterStateException(fail.getMessage());
                case IS_REPLICA:
                    return new ReplicaStateException(fail.getMessage());
                case TRANSFER_FAIL:
                    // TODO: not worth it for now, but it wouldn't be hard to
                    // distinguish IllegalArg. cases here
                    return new MasterTransferFailureException
                        (fail.getMessage());
                default:
                    return EnvironmentFailureException.
                        unexpectedState(fail.getMessage());
            }
        }

        if (resp instanceof ProtocolError) {
            return EnvironmentFailureException.unexpectedState
                (((ProtocolError)resp).getMessage());
        }

        return EnvironmentFailureException.unexpectedState
            ("Response not recognized: " + resp);
    }

    private static DataChannelFactory initializeFactory(
        ReplicationNetworkConfig repNetConfig,
        String logContext) {

        return DataChannelFactoryBuilder.construct(repNetConfig, logContext);
    }
}
