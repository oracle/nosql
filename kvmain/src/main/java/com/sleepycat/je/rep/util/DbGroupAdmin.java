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

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.sleepycat.je.JEVersion;
import com.sleepycat.je.rep.NodeState;
import com.sleepycat.je.rep.NodeType;
import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationMutableConfig;
import com.sleepycat.je.rep.ReplicationMutableGroup;
import com.sleepycat.je.rep.ReplicationMutableNode;
import com.sleepycat.je.rep.ReplicationNetworkConfig;
import com.sleepycat.je.rep.impl.RepGroupImpl;
import com.sleepycat.je.rep.impl.RepNodeImpl;
import com.sleepycat.je.rep.net.DataChannelFactory;
import com.sleepycat.je.rep.utilint.HostPortPair;
import com.sleepycat.je.rep.utilint.net.DataChannelFactoryBuilder;
import com.sleepycat.je.utilint.CmdUtil;
import com.sleepycat.je.utilint.PropUtil;

/**
 * DbGroupAdmin supplies the functionality of the administrative class {@link
 * ReplicationGroupAdmin} in a convenient command line utility. For example, it
 * can be used to display replication group information, or to remove a node
 * from the replication group.
 * <p>
 * Note: This utility does not handle security and authorization. It is left
 * to the user to ensure that the utility is invoked with proper authorization.
 * <p>
 * See {@link DbGroupAdmin#main} for a full description of the command line
 * arguments.
 */
/*
 * SSL deferred
 * See {@link ReplicationConfig} for descriptions of the parameters that
 * control replication service access.
 */
public class DbGroupAdmin {

    enum Command { DUMP, REMOVE, TRANSFER_MASTER, UPDATE_ADDRESS,
        DELETE, NODE, GROUP, CREATE, STATE, OVERRIDE_QUORUM }

    private String groupName;
    private Set<InetSocketAddress> helperSockets;
    private String nodeName;
    private String newHostName;
    private int newPort;
    private int quorumSize;
    private String timeout;
    private boolean forceFlag;
    private DataChannelFactory channelFactory;
    private ReplicationGroupAdmin groupAdmin;
    private final ArrayList<Command> actions = new ArrayList<>();

    private static final String undocumentedUsageString =
        "  -netProps <optional>   # name of a property file containing\n" +
        "                            # properties needed for replication\n" +
        "                            # service access\n";

    private static final String usageString =
        "Usage: " + CmdUtil.getJavaCommand(DbGroupAdmin.class) + "\n" +
        "  -groupName <group name>   # name of replication group\n" +
        "  -helperHosts <host:port>  # identifier for one or more members\n" +
        "                            # of the replication group which can\n"+
        "                            # be contacted for group information,\n"+
        "                            # in this format:\n" +
        "                            # hostname[:port][,hostname[:port]]\n" +
        "  -dumpGroup                # dump group information\n" +
        "  -removeMember <node name> # node to be removed\n" +
        "  -updateAddress <node name> <new host:port>\n" +
        "                            # update the network address for a\n" +
        "                            # specified node.  The node should not\n" +
        "                            # be alive when updating the address\n" +
        "  -transferMaster [-force] <node1,node2,...> <timeout>\n" +
        "                            # transfer master role to one of the\n" +
        "                            # specified nodes.\n" +
        "  -overrideQuorum <size>    # Overrides the quorum size to the\n" +
        "                            # given value for each of the\n" +
        "                            # nodes specified by -helperHosts.\n" +
        "  -getNodeState <node name> # gets the state of the given node.\n" +
        "  -editGroup                # gets the group and enters an edit\n" +
        "                            # mode to edit the group information.\n" +
        "  -editNode <node name>     # gets the node of the given name\n" +
        "                            # and enters an edit mode to edit the\n" +
        "                            # node information.\n" +
        "  -createNode <node name> <host:port>\n" +
        "                            # creates a new node with the given\n" +
        "                            # information and allows further\n" +
        "                            # editing.";

    /* Undocumented options for main()
     *   -netProps &lt;propFile&gt;  # (optional)
     *                               # name of a property file containing
     *                               # properties needed for replication
     *                               # service access
     *   -deleteMember <node name>   # Deletes the node from the group, doesn't
     *                               # just mark it removed
     */

    final static String putUsage =
        "   put                    # makes the node changes permanent\n" +
        "                          # and quits\n";
    final static String idUsage =
        "   id [-force] <id>       # sets the id of the node\n";
    final static String typeUsage =
        "   type [-force] <type>   # sets the type of the node\n";
    final static String quorumUsage =
        "   quorumAck <true/false> # sets whether node is marked as part\n" +
        "                          # of the quorum\n";
    final static String removeUsage =
        "   isRemoved [-force] <true/false> \n" +
        "                          # sets whether node is marked as\n" +
        "                          # removed from group\n";
    final static String addressUsage =
        "   address <host:port>    # sets the network address\n";
    final static String changeUsage =
        "   changeVersion [-force] <version>\n" +
        "                          # sets the version of the node\n";
    final static String jeUsage =
        "   jeVersion [-force] <version>\n" +
        "                          # sets the JE version of the node.\n";
    public final static String nodeUsage = "\n" +
        "   help                   # this message\n" +
        "   print                  # prints the node information\n" +
        putUsage +
        "   quit                   # discards the node changes and quits\n" +
        idUsage +
        typeUsage +
        quorumUsage +
        removeUsage +
        addressUsage +
        changeUsage +
        jeUsage +
        "   See documentation for ReplicationMutableNode for more info.";

    final static String PROMPT = "> ";

    final static String uuidUsage =
        "   uuid <uuid>            # sets the id of the group\n";
    final static String formatUsage =
        "   formatVersion [-force] <version>\n" +
        "                          # sets the group format version\n";
    final static String changeGroupUsage =
        "   changeVersion [-force] <version>\n" +
        "                          # sets the group change version\n";
    final static String sequenceUsage =
        "   nodeIdSequence [-force] <id>\n" +
        "                          # sets the next available node id\n";
    final static String jeGroupUsage =
        "   minJEVersion [-force] <version>\n" +
        "                          # sets the minimum supported JE version\n";
    public final static String groupUsage = "\n" +
        "   help                   # this message\n" +
        "   print                  # prints the group information\n" +
        "   put                    # makes the group changes permanent\n" +
        "                          # and quits\n" +
        "   quit                   # discards the group changes and quits\n" +
        uuidUsage +
        formatUsage +
        changeGroupUsage +
        sequenceUsage +
        jeGroupUsage +
        "   See documentation for ReplicationMutableGroup for more info.";

    /**
     * Usage:
     * <pre>
     * java {com.sleepycat.je.rep.util.DbGroupAdmin |
     *       -jar je-&lt;version&gt;.jar DbGroupAdmin}
     *   -groupName &lt;group name&gt;  # name of replication group
     *   -helperHosts &lt;host:port&gt; # identifier for one or more members
     *                            # of the replication group which can be
     *                            # contacted for group information, in
     *                            # this format:
     *                            # hostname[:port][,hostname[:port]]*
     *   -dumpGroup               # dump group information
     *   -removeMember &lt;node name&gt;# node to be removed
     *   -updateAddress &lt;node name&gt; &lt;new host:port&gt;
     *                            # update the network address for a specified
     *                            # node. The node should not be alive when
     *                            # updating address
     *   -transferMaster [-force] &lt;node1,node2,...&gt; &lt;timeout&gt;
     *                            # transfer master role to one of the
     *                            # specified nodes.
     * </pre>
     */
    public static void main(String... args)
        throws Exception {

        DbGroupAdmin admin = new DbGroupAdmin();
        admin.parseArgs(args);
        admin.run();
    }

    /**
     * Print usage information for this utility.
     *
     * @param msg
     */
    private void printUsage(String msg) {
        if (msg != null) {
            System.out.println(msg);
        }

        System.out.println(usageString);
        System.exit(-1);
    }


    /**
     * Parse the command line parameters.
     *
     * @param argv Input command line parameters.
     */
    private void parseArgs(String argv[]) {
        int argc = 0;
        int nArgs = argv.length;
        String netPropsName = null;

        if (nArgs == 0) {
            printUsage(null);
            System.exit(0);
        }

        while (argc < nArgs) {
            String thisArg = argv[argc++];
            if (thisArg.equals("-groupName")) {
                if (argc < nArgs) {
                    groupName = argv[argc++];
                } else {
                    printUsage("-groupName requires an argument");
                }
            } else if (thisArg.equals("-helperHosts")) {
                if (argc < nArgs) {
                    helperSockets = HostPortPair.getSockets(argv[argc++]);
                } else {
                    printUsage("-helperHosts requires an argument");
                }
            } else if (thisArg.equals("-dumpGroup")) {
                actions.add(Command.DUMP);
            } else if (thisArg.equals("-removeMember")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.REMOVE);
                } else {
                    printUsage("-removeMember requires an argument");
                }
            } else if (thisArg.equals("-updateAddress")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];

                    if (argc < nArgs) {
                        String hostPort = argv[argc++];
                        int index = hostPort.indexOf(":");
                        if (index < 0) {
                            printUsage("Host port pair format must be " +
                                       "<host name>:<port number>");
                        }

                        newHostName = hostPort.substring(0, index);
                        newPort = Integer.parseInt
                            (hostPort.substring(index + 1, hostPort.length()));
                    } else {
                        printUsage("-updateAddress requires a " +
                                   "<host name>:<port number> argument");
                    }

                    actions.add(Command.UPDATE_ADDRESS);
                } else {
                    printUsage
                        ("-updateAddress requires the node name argument");
                }
            } else if (thisArg.equals("-transferMaster")) {

                // TODO: it wouldn't be too hard to allow "-force" as a
                // node name.
                //
                if (argc < nArgs && "-force".equals(argv[argc])) {
                    forceFlag = true;
                    argc++;
                }
                if (argc + 1 < nArgs) {
                    nodeName = argv[argc++];

                    /*
                     * Allow either
                     *     -transferMaster mercury,venus 900 ms
                     * or
                     *     -transferMaster mercury,venus "900 ms"
                     */
                    if (argc + 1 < nArgs && argv[argc + 1].charAt(0) != '-') {
                        timeout = argv[argc] + " " + argv[argc + 1];
                        argc += 2;
                    } else {
                        timeout = argv[argc++];
                    }

                    actions.add(Command.TRANSFER_MASTER);
                } else {
                    printUsage
                        ("-transferMaster requires at least two arguments");
                }
            } else if (thisArg.equals("-netProps")) {
                if (argc < nArgs) {
                    netPropsName = argv[argc++];
                } else {
                    printUsage("-netProps requires an argument");
                }
            } else if (thisArg.equals("-deleteMember")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.DELETE);
                } else {
                    printUsage("-deleteMember requires an argument");
                }
            } else if (thisArg.equals("-getNodeState")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.STATE);
                } else {
                    printUsage("-getNodeState requires the node name");
                }
            } else if (thisArg.equals("-editNode")) {
                if (argc < nArgs) {
                    nodeName = argv[argc++];
                    actions.add(Command.NODE);
                } else {
                    printUsage("-editNode requires the node name");
                }
            } else if (thisArg.equals("-createNode")) {
                if ((argc + 1) < nArgs) {
                    nodeName = argv[argc++];
                    String hostPort = argv[argc++];
                    int index = hostPort.indexOf(":");
                    if (index < 0) {
                        printUsage("Host port pair format must be " +
                                   "<host name>:<port number>");
                    }

                    newHostName = hostPort.substring(0, index);
                    newPort = Integer.parseInt
                        (hostPort.substring(index + 1, hostPort.length()));
                    actions.add(Command.CREATE);
                } else {
                    printUsage(
                        "-createNode requires the node name and address");
                }
            } else if (thisArg.equals("-editGroup")) {
                actions.add(Command.GROUP);
            } else if (thisArg.equals("-overrideQuorum")) {
                if (argc < nArgs) {
                    actions.add(Command.OVERRIDE_QUORUM);
                    quorumSize = Integer.parseInt(argv[argc++]);
                } else {
                    printUsage("-overrideQuorum requires quorum size.");
                }
            } else {
                printUsage(thisArg + " is not a valid argument");
            }
        }

        ReplicationNetworkConfig repNetConfig =
        		ReplicationNetworkConfig.createDefault();
        if (netPropsName != null) {
            try {
                repNetConfig =
                    ReplicationNetworkConfig.create(new File(netPropsName));
            } catch (FileNotFoundException fnfe) {
                printUsage("The net properties file " + netPropsName +
                           " does not exist: " + fnfe.getMessage());
            } catch (IllegalArgumentException iae) {
                printUsage("The net properties file " + netPropsName +
                           " is not valid: " + iae.getMessage());
            }
        }

        this.channelFactory = initializeFactory(repNetConfig, groupName);
    }

    /* Execute commands */
    private void run()
        throws Exception {

        createGroupAdmin();

        if (actions.size() == 0) {
            return;
        }

        for (Command action : actions) {
            switch (action) {

                /* Dump the group information. */
            case DUMP:
                dumpGroup();
                break;

                /* Remove a member. */
            case REMOVE:
                removeMember(nodeName);
                break;

                /* Transfer the current mastership to a specified node. */
            case TRANSFER_MASTER:
                transferMaster(nodeName, timeout);
                break;

                /* Update the network address of a specified node. */
            case UPDATE_ADDRESS:
                updateAddress(nodeName, newHostName, newPort);
                break;

                /* Delete a member */
            case DELETE:
                deleteMember(nodeName);
                break;

                /* Get a node to edit. */
            case NODE:
                getNode(nodeName);
                break;

                /* Create a new node to edit. */
            case CREATE:
                createNode(nodeName, newHostName, newPort);
                break;

                /* Get the group information to edit. */
            case GROUP:
                getGroup();
                break;

                /* Get the node state information. */
            case STATE:
                getNodeState(nodeName);
                break;

            case OVERRIDE_QUORUM:
                overrideQuorum(quorumSize);
                break;

            default:
                throw new AssertionError();
            }
        }
    }

    private DbGroupAdmin() {
    }

    /**
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
    */
    /*
     * SSL deferred
     * This constructor does not support non-default service net properties.
     * See the other constructor forms which allow setting of net properties.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets) {
        this(groupName, helperSockets, (ReplicationNetworkConfig)null);
    }

    /**
     * @hidden SSL deferred
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
     * @param netPropsFile a File containing replication net property
     * settings.  This parameter is ignored if null.
     * @throws FileNotFoundException if the netPropsFile does not exist
     * @throws IllegalArgumentException if the netPropsFile contains
     * invalid settings.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets,
                        File netPropsFile)
        throws FileNotFoundException {

        this(groupName, helperSockets, makeRepNetConfig(netPropsFile));
    }

    /**
     * @hidden SSL deferred
     * Create a DbGroupAdmin instance for programmatic use.
     *
     * @param groupName replication group name
     * @param helperSockets set of host and port pairs for group members which
     * can be queried to obtain group information.
     * @param netConfig replication net configuration - null allowable
     * This parameter is ignored if null.
     * @throws IllegalArgumentException if the netProps contains
     * invalid settings.
     */
    public DbGroupAdmin(String groupName,
                        Set<InetSocketAddress> helperSockets,
                        ReplicationNetworkConfig netConfig) {
        this.groupName = groupName;
        this.helperSockets = helperSockets;
        this.channelFactory = initializeFactory(netConfig, groupName);

        createGroupAdmin();
    }

    /* Create the ReplicationGroupAdmin object. */
    private void createGroupAdmin() {
        if (groupName == null) {
            printUsage("Group name must be specified");
        }

        if ((helperSockets == null) || (helperSockets.size() == 0)) {
            printUsage("Host and ports of helper nodes must be specified");
        }

        groupAdmin = new ReplicationGroupAdmin(
            groupName, helperSockets, channelFactory);
    }

    /**
     * Display group information. Lists all members and the group master.  Can
     * be used when reviewing the <a
     * href="http://www.oracle.com/technetwork/database/berkeleydb/je-faq-096044.html#HAChecklist">group configuration. </a>
     */
    public void dumpGroup() {
        RepGroupImpl repGroupImpl = groupAdmin.getGroup().getRepGroupImpl();
        System.out.println(getFormattedOutput(repGroupImpl));
    }

    /**
     * Remove a node from the replication group. Once removed, a
     * node cannot be added again to the group under the same node name.
     *
     * <p>{@link NodeType#SECONDARY Secondary} nodes cannot be removed; they
     * automatically leave the group when they are shut down or become
     * disconnected from the master.
     *
     * @param name name of the node to be removed
     *
     * @see ReplicationGroupAdmin#removeMember
     */
    /*
     * TODO: EXTERNAL is hidden for now. The doc need updated to include
     * EXTERNAL when it becomes public.
     */
    public void removeMember(String name) {
        if (name == null) {
            printUsage("Node name must be specified");
        }

        groupAdmin.removeMember(name);
    }

    /**
     * @hidden internal, for use in disaster recovery [#23447]
     *
     * Deletes a node from the replication group, which allows the node to be
     * added to the group again under the same name.
     *
     * <p>{@link NodeType#SECONDARY Secondary} and {@link NodeType#EXTERNAL
     * External} nodes cannot be deleted; they automatically leave the group
     * when they are shut down or become disconnected from the master.
     *
     * @param name name of the node to be deleted
     *
     * @see ReplicationGroupAdmin#deleteMember
     */
    public void deleteMember(String name) {
        if (name == null) {
            printUsage("Node name must be specified");
        }

        groupAdmin.deleteMember(name);
    }

    /**
     * Update the network address for a specified node. When updating the
     * address of a node, the node cannot be alive. See {@link
     * ReplicationGroupAdmin#updateAddress} for more information.
     *
     * <p>The address of a {@link NodeType#SECONDARY} node cannot be updated
     * with this method, since nodes must be members but not alive to be
     * updated, and secondary nodes are not members when they are not alive.
     * To change the address of a secondary node, restart the node with the
     * updated address.
     *
     * @param nodeName the name of the node whose address will be updated
     * @param newHostName the new host name of the node
     * @param newPort the new port number of the node
     */
    @SuppressWarnings("hiding")
    public void updateAddress(String nodeName,
                              String newHostName,
                              int newPort) {
        if (nodeName == null || newHostName == null) {
            printUsage("Node name and new host name must be specified");
        }

        if (newPort <= 0) {
            printUsage("Port of the new network address must be specified");
        }

        groupAdmin.updateAddress(nodeName, newHostName, newPort);
    }

    /**
     * Prints the state of the given node.  Assumes the socket address given
     * when calling DbGroupAdmin is the address of the give node.
     */
    public void getNodeState(String name) {
        NodeState state = null;
        try {
            state = groupAdmin.getNodeState(name, 10);
        } catch (Exception e) {
            System.out.println("Error getting node state: " + e.getMessage());
        }
        if (state == null) {
            System.out.println("Unable to get node state.");
            return;
        }
        System.out.println(state.toString());
    }

    /**
     * Overrides the quorum size required to elect a new master or
     * acknowledge updates.  This function should only be used when there are
     * not enough nodes up or in communication with each other to elect a
     * master or form a quorum.  Use -getNodeState to get the state of each
     * node in the group.
     *
     * See {@link ReplicationMutableConfig#ELECTABLE_GROUP_SIZE_OVERRIDE ELECTABLE_GROUP_SIZE_OVERRIDE}
     */
    public void overrideQuorum(int quorumSize) {
        try {
            groupAdmin.overrideQuorum(quorumSize);
        } catch (Exception e) {
            System.out.println(e);
            return;
        }
        System.out.println("Quorum size successfully overridden.");
    }

    /**
     * Transfers the master role from the current master to one of the
     * electable replicas specified in the argument list.
     *
     * @param nodeList comma-separated list of nodes
     * @param timeout in <a href="../../EnvironmentConfig.html#timeDuration">
     *        same form</a> as accepted by duration config params
     *
     * @see ReplicatedEnvironment#transferMaster
     */
    @SuppressWarnings("hiding")
    public void transferMaster(String nodeList,
                               String timeout,
                               String reason) {
        String result =
            groupAdmin.transferMaster(parseNodes(nodeList),
                                      PropUtil.parseDuration(timeout),
                                      TimeUnit.MILLISECONDS,
                                      forceFlag,
                                      reason);
        System.out.println("The new master is: " + result);
    }

    public void transferMaster(String nodeList, String timeout) {
        transferMaster(nodeList, timeout, null);
    }

    private static boolean hasForce(Scanner scanner) {
        if (scanner.hasNext("-force")) {
            scanner.next();
            return true;
        }
        return false;
    }

    private static NodeType getType(String type) {
        try {
            return NodeType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private void editNode(ReplicationMutableNode node) {
        System.out.print("Entering interactive node editing mode:");
        System.out.println(nodeUsage);
        Scanner inputScanner = null;
        Scanner scanner = null;
        System.out.print(PROMPT);
        String line = null;
        try {
            inputScanner = new Scanner(System.in);
            while (true) {
                if (line != null) {
                    System.out.print(PROMPT);
                }
                line = inputScanner.hasNextLine()
                    ? inputScanner.nextLine() : null;
                if (line == null) {
                    return;
                }
                if (scanner != null) {
                    scanner.close();
                }
                scanner = new Scanner(line);
                String command = scanner.hasNext() ? scanner.next() : null;
                if (command == null) {
                    continue;
                }
                if (command.equalsIgnoreCase("print")) {
                    System.out.print(node.toString());
                } else if (command.equalsIgnoreCase("put")) {
                    try {
                        groupAdmin.putNode(node);
                        System.out.println("Put node, quitting.");
                        return;
                    } catch (Exception e) {
                        System.out.println("Error updating node: "
                            + e.getMessage());
                    }
                } else if (command.equalsIgnoreCase("quit")) {
                    System.out.println("Quitting.");
                    return;
                } else if (command.equalsIgnoreCase("id")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getId());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextInt()) {
                        try {
                            int id = scanner.nextInt();
                            node.setId(id, force);
                            System.out.println("Id set to " + id);
                        } catch (Exception e) {
                            System.out.println("Error setting id: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(idUsage);
                    }
                } else if (command.equalsIgnoreCase("type")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getType());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNext()) {
                        NodeType type = getType(scanner.next());
                        if (type == null) {
                            System.out.println(typeUsage);
                        } else {
                            try {
                                node.setType(type, force);
                                System.out.println("Type set to " + type);
                            } catch (Exception e) {
                                System.out.println("Error setting type: "
                                    + e.getMessage());
                            }
                        }
                    } else {
                        System.out.println(typeUsage);
                    }
                } else if (command.equalsIgnoreCase("quorumAck")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getQuorumAck());
                        continue;
                    }
                    if (scanner.hasNextBoolean()) {
                        try {
                            boolean quorum = scanner.nextBoolean();
                            node.setQuorumAck(quorum);
                            System.out.println("Quorum set to " + quorum);
                        } catch (Exception e) {
                            System.out.println("Error setting id: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(quorumUsage);
                    }
                } else if (command.equalsIgnoreCase("isRemoved")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getIsRemoved());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextBoolean()) {
                        try {
                            boolean remove = scanner.nextBoolean();
                            node.setIsRemoved(remove, force);
                            System.out.println("isRemoved set to " + remove);
                        } catch (Exception e) {
                            System.out.println("Error setting isRemoved: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(removeUsage);
                    }
                } else if (command.equalsIgnoreCase("address")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getHostName() + ":"
                            + node.getPort());
                        continue;
                    }
                    if (scanner.hasNext()) {
                        String hostName;
                        int port;
                        String hostPort = scanner.next();
                        try {
                            int index = hostPort.indexOf(":");
                            if (index < 0) {
                                System.out.println(
                                    "Host port pair format must be "
                                    + "<host name>:<port number>");
                                if (scanner.hasNext()) {
                                    scanner.nextLine();
                                }
                                continue;
                            }

                            hostName = hostPort.substring(0, index);
                            port = Integer.parseInt(hostPort.substring(
                                index + 1, hostPort.length()));
                        } catch (Exception e) {
                            System.out.println(addressUsage);
                            if (scanner.hasNext()) {
                                scanner.nextLine();
                            }
                            continue;
                        }
                        try {
                            node.setAddress(hostName, port);
                            System.out.println("Address set to " + hostName
                                + ":" + port);
                        } catch (Exception e) {
                            System.out.println("Error setting address: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(addressUsage);
                    }
                } else if (command.equalsIgnoreCase("changeVersion")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getChangeVersion());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextInt()) {
                        try {
                            int version = scanner.nextInt();
                            node.setChangeVersion(version, force);
                            System.out.println(
                                "Change version set to " + version);
                        } catch (Exception e) {
                            System.out.println("Error setting change version: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(changeUsage);
                    }
                } else if (command.equalsIgnoreCase("jeVersion")) {
                    if (!scanner.hasNext()) {
                        System.out.println(node.getJEVersion());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNext()) {
                        String version = scanner.next();
                        JEVersion jeversion = null;
                        try {
                            jeversion = new JEVersion(version);
                        } catch (Exception e) {
                            System.out.println(
                                "Not a valid JE version format: " + version);
                        }
                        if (jeversion != null) {
                            try {
                                node.setJEVersion(jeversion, force);
                                System.out.println("JE version set to "
                                    + jeversion.toString());
                            } catch (Exception e) {
                                System.out.println("Error setting JE version: "
                                    + e.getMessage());
                            }
                        }
                    } else {
                        System.out.println(jeUsage);
                    }
                } else {
                    System.out.println("Unknown command: " + command);
                    System.out.println(nodeUsage);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (inputScanner != null) {
                inputScanner.close();
            }
        }
    }

    public void getNode(String name) {
        ReplicationMutableNode node = groupAdmin.getNode(name);
        editNode(node);
    }

    public void createNode(String name, String hostName, int port) {
        ReplicationMutableNode node = groupAdmin.createNode(name, hostName,
                        port, NodeType.ELECTABLE);
        editNode(node);
    }

    public void getGroup() {
        ReplicationMutableGroup group = groupAdmin.getMutableGroup();
        System.out.print("Entering interactive group editing mode:");
        System.out.println(groupUsage);
        Scanner inputScanner = null;
        Scanner scanner = null;
        System.out.print(PROMPT);
        String line = null;
        try {
            inputScanner = new Scanner(System.in);
            while (true) {
                if (line != null) {
                    System.out.print(PROMPT);
                }
                line = inputScanner.hasNextLine() ? inputScanner.nextLine()
                                : null;
                if (line == null) {
                    return;
                }
                if (scanner != null) {
                    scanner.close();
                }
                scanner = new Scanner(line);
                String command = scanner.hasNext() ? scanner.next() : null;
                if (command == null) {
                    continue;
                }
                if (command.equalsIgnoreCase("print")) {
                    System.out.println(group.toString());
                } else if (command.equalsIgnoreCase("put")) {
                    try {
                        groupAdmin.putGroup(group);
                        System.out.println("Put group, quitting.");
                        return;
                    } catch (Exception e) {
                        System.out.println("Error updating group: "
                            + e.getMessage());
                    }
                } else if (command.equalsIgnoreCase("quit")) {
                    System.out.println("Quitting.");
                    return;
                } else if (command.equalsIgnoreCase("uuid")) {
                    if (scanner.hasNext()) {
                        try {
                            UUID id = UUID.fromString(scanner.next());
                            group.setUUID(id);
                            System.out.println("UUID set to " + id.toString());
                        } catch (Exception e) {
                            System.out.println("Error setting uuid: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(group.getUUID());
                    }
                } else if (command.equalsIgnoreCase("formatVersion")) {
                    if (!scanner.hasNext()) {
                        System.out.println(group.getFormatVersion());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextInt()) {
                        try {
                            int version = scanner.nextInt();
                            group.setFormatVersion(version, force);
                            System.out.println(
                                "Format version set to " + version);
                        } catch (Exception e) {
                            System.out.println("Error setting format version: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(formatUsage);
                    }
                } else if (command.equalsIgnoreCase("changeVersion")) {
                    if (!scanner.hasNext()) {
                        System.out.println(group.getChangeVersion());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextInt()) {
                        try {
                            int version = scanner.nextInt();
                            group.setChangeVersion(version, force);
                            System.out.println(
                                            "Change version set to " + version);
                        } catch (Exception e) {
                            System.out.println("Error setting change version: "
                                            + e.getMessage());
                        }
                    } else {
                        System.out.println(changeGroupUsage);
                    }
                } else if (command.equalsIgnoreCase("nodeIdSequence")) {
                    if (!scanner.hasNext()) {
                        System.out.println(group.getNodeIdSequence());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNextInt()) {
                        try {
                            int id = scanner.nextInt();
                            group.setNodeIdSequence(id, force);
                            System.out.println("Node id sequence set to " + id);
                        } catch (Exception e) {
                            System.out.println(
                                "Error setting next available node id: "
                                + e.getMessage());
                        }
                    } else {
                        System.out.println(sequenceUsage);
                    }
                } else if (command.equalsIgnoreCase("minJEVersion")) {
                    if (!scanner.hasNext()) {
                        System.out.println(group.getMinimumJEVersion());
                        continue;
                    }
                    boolean force = hasForce(scanner);
                    if (scanner.hasNext()) {
                        String version = scanner.next();
                        JEVersion jeversion = null;
                        try {
                            jeversion = new JEVersion(version);
                            System.out.println("Minimum JE version set to "
                                            + jeversion.toString());
                        } catch (Exception e) {
                            System.out.println("Not a valid JE version format: "
                                            + version);
                        }
                        if (jeversion != null) {
                            try {
                                group.setMinimumJEVersion(jeversion, force);
                            } catch (Exception e) {
                                System.out.println("Error setting JE version: "
                                                + e.getMessage());
                            }
                        }
                    } else {
                        System.out.println(jeUsage);
                    }
                } else if (command.equalsIgnoreCase("help")) {
                    System.out.println(groupUsage);
                } else {
                    System.out.println("Unknown command: " + command);
                    System.out.println(groupUsage);
                }
            }
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (inputScanner != null) {
                inputScanner.close();
            }
        }
    }

    private Set<String> parseNodes(String nodes) {
        if (nodes == null) {
            throw new IllegalArgumentException("node list may not be null");
        }
        StringTokenizer st = new StringTokenizer(nodes, ",");
        Set<String> set = new HashSet<>();
        while (st.hasMoreElements()) {
            set.add(st.nextToken());
        }
        return set;
    }

    /*
     * This method presents group information in a user friendly way. Internal
     * fields are hidden.
     */
    private String getFormattedOutput(RepGroupImpl repGroupImpl) {
        StringBuilder sb = new StringBuilder();

        /* Get the master node name. */
        String masterName = groupAdmin.getMasterNodeName();

        /* Get the electable nodes information. */
        sb.append("\nGroup: " + repGroupImpl.getName() + "\n");
        sb.append("Electable Members:\n");
        Set<RepNodeImpl> nodes = repGroupImpl.getElectableMembers();
        if (nodes.size() == 0) {
            sb.append("    No electable members\n");
        } else {
            for (RepNodeImpl node : nodes) {
                String type =
                    masterName.equals(node.getName()) ? "master, " : "";
                sb.append("    " + node.getName() + " (" + type +
                          node.getHostName() + ":" + node.getPort() + ")\n");
            }
        }

        /* Get information about secondary nodes */
        sb.append("\nSecondary Members:\n");
        nodes = repGroupImpl.getSecondaryMembers();
        if (nodes.isEmpty()) {
            sb.append("    No secondary members\n");
        } else {
            for (final RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ")\n");
            }
        }

        /* Get information about external nodes */
        sb.append("\nExternal Members:\n");
        nodes = repGroupImpl.getExternalMembers();
        if (nodes.isEmpty()) {
            sb.append("    No external members\n");
        } else {
            for (final RepNodeImpl node : nodes) {
                sb.append("    " + node.getName() + " (" + node.getHostName() +
                          ":" + node.getPort() + ")\n");
            }
        }

        return sb.toString();
    }

    private static ReplicationNetworkConfig makeRepNetConfig(File propFile)
        throws FileNotFoundException {

        if (propFile == null) {
            return ReplicationNetworkConfig.createDefault();
        }

        return ReplicationNetworkConfig.create(propFile);
    }

    private static DataChannelFactory initializeFactory(
        ReplicationNetworkConfig repNetConfig,
        String logContext) {

        if (repNetConfig == null) {
            repNetConfig =
                ReplicationNetworkConfig.createDefault();
        }

        return DataChannelFactoryBuilder.construct(repNetConfig, logContext);
    }
}
