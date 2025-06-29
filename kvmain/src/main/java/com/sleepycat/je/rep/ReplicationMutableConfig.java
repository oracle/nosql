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

package com.sleepycat.je.rep;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Properties;

import com.sleepycat.je.config.ConfigParam;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.rep.impl.RepImpl;
import com.sleepycat.je.rep.impl.RepParams;

/**
 * Specifies the attributes that may be changed after a {@link
 * ReplicatedEnvironment} has been created. {@code ReplicationMutableConfig} is
 * a parameter to {@link ReplicatedEnvironment#setMutableConfig} and is
 * returned by {@link ReplicatedEnvironment#getMutableConfig}.
 */
public class ReplicationMutableConfig implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    /*
     * Note: all replicated parameters should start with
     * EnvironmentParams.REP_PARAMS_PREFIX, which is "je.rep.",
     * see SR [#19080].
     */

    /**
     * Boolean flag if set to true, an Arbiter may acknowledge a transaction if
     * a replication node is not available.
     *
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>True</td>
     * </tr>
     * </table>
     */
    public static final String ALLOW_ARBITER_ACK =
            EnvironmentParams.REP_PARAM_PREFIX + "allowArbiterAck";

    /**
     * Identifies the Primary node in a two node group. See the discussion of
     * issues when
     * <a href= "{@docRoot}/../ReplicationGuide/lifecycle.html#twonode">
     * configuring two node groups</a>}
     *
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Boolean</td>
     * <td>Yes</td>
     * <td>False</td>
     * </tr>
     * </table>
     */
    public static final String DESIGNATED_PRIMARY =
        EnvironmentParams.REP_PARAM_PREFIX + "designatedPrimary";

    /**
     * An escape mechanism to modify the way in which the number of electable
     * nodes, and consequently the quorum requirements for elections and commit
     * acknowledgments, is calculated. The override is accomplished by
     * specifying the quorum size via this mutable configuration parameter.
     * <p>
     * When this parameter is set to a non-zero value at a member node, the
     * member will use this value as the electable group size, instead of using
     * the metadata stored in the RepGroup database for its quorum
     * calculations.  This parameter's value should be set to the number of
     * electable nodes known to be available. The default value is zero, which
     * indicates normal operation with the electable group size being
     * calculated from the metadata.
     *<p>
     * Please keep in mind that this is an escape mechanism, only for use in
     * exceptional circumstances, to be used with care. Since JE HA is no
     * longer maintaining quorum requirements automatically, there is the
     * possibility that the simple majority of unavailable nodes could elect
     * their own Master, which would result in a diverging set of changes to
     * the same environment being made by multiple Masters. It is essential to
     * ensure that the problematic nodes are in fact down before making this
     * temporary configuration change.
     *
     * See the discussion in <a href=
     * "{@docRoot}/../ReplicationGuide/election-override.html">Appendix:
     * Managing a Failure of the Majority</a>.
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>0</td>
     * </tr>
     * </table>
     *
     * @see QuorumPolicy
     * @see com.sleepycat.je.Durability.ReplicaAckPolicy
     */
    public static final String ELECTABLE_GROUP_SIZE_OVERRIDE =
        EnvironmentParams.REP_PARAM_PREFIX + "electableGroupSizeOverride";

    /**
     * Override the minimum amount of time an election must take (the default
     * is 1000 milliseconds).  If the election takes less than the given time
     * then the election results will be delayed until the minimum time is
     * reached.
     *
     * <table border="1">
     * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>1000 second</td>
     * </tr>
     * </table>
     *
     */
    public static final String OVERRIDE_MIN_ELECTION_TIME=
        EnvironmentParams.REP_PARAM_PREFIX + "overrideMinElectionTime";

    /**
     * The election priority associated with this node. The election algorithm
     * for choosing a new master will pick the participating node that has the
     * most current set of log files. When there is a tie, the election
     * priority is used as a tie-breaker to select amongst these nodes.
     * <p>
     * A priority of zero is used to ensure that this node is never elected
     * master, even if it has the most up to date log files. Note that the node
     * still votes for a Master and participates in quorum requirements. Please
     * use this option with care, since it means that some node with less
     * current log files could be elected master. As a result, this node would
     * be forced to rollback committed data and must be prepared to handle any
     * {@link RollbackException} exceptions that might be thrown.
     *
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Integer</td>
     * <td>Yes</td>
     * <td>1</td>
     * <td>0</td>
     * <td>Integer.MAX_VALUE</td>
     * </tr>
     * </table>
     *
     * @see RollbackException
     */
    public static final String NODE_PRIORITY =
        EnvironmentParams.REP_PARAM_PREFIX + "node.priority";

    /**
     * The maximum number of <i>most recently used</i> database handles that
     * are kept open during the replay of the replication stream.
     *
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>Int</td>
     * <td>Yes</td>
     * <td>10</td>
     * <td>1</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @since 5.0.38
     */
    public static final String REPLAY_MAX_OPEN_DB_HANDLES =
        EnvironmentParams.REP_PARAM_PREFIX + "replayMaxOpenDbHandles";

    /**
     * The string identifying one or more helper host and port pairs in
     * this format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td><td>Default</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td>String</td>
     * <td>Yes</td>
     * <td>""</td>
     * </tr>
     * </table>
     * @see ReplicationMutableConfig#setHelperHosts
     * @see ReplicationMutableConfig#getHelperHosts
     */
    public static final String HELPER_HOSTS =
        EnvironmentParams.REP_PARAM_PREFIX + "helperHosts";

    /**
     * The maximum amount of time that an inactive database handle is kept open
     * during a replay of the replication stream. Handles that are inactive for
     * more than this time period are automatically closed. Note that this does
     * not impact any handles that may have been opened by the application.
     *
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr><td>Name</td><td>Type</td><td>Mutable</td>
     * <td>Default</td><td>Minimum</td><td>Maximum</td></tr>
     * <tr>
     * <td>{@value}</td>
     * <td><a href="#timeDuration">Duration</a></td>
     * <td>No</td>
     * <td>30 sec</td>
     * <td>1 sec</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="../EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     *
     * @since 5.0.38
     */
    public static final String REPLAY_DB_HANDLE_TIMEOUT =
        EnvironmentParams.REP_PARAM_PREFIX + "replayOpenHandleTimeout";

    /**
     * @hidden
     *
     * For internal use only.
     *
     * The timeout specifies the amount of time that the
     * {@link com.sleepycat.je.rep.util.ReplicationGroupAdmin#transferMaster
     * ReplicationGroupAdmin.transferMastership} command can use to
     * have the specified replica catch up with the original master.
     * <p>
     * If the replica has not successfully caught up with the original
     * master, the call to {@link
     * com.sleepycat.je.rep.util.ReplicationGroupAdmin#transferMaster
     * ReplicationGroupAdmin.transferMastership} will throw an exception.
     * <table border="1">
	 * <caption style="display:none">Information about configuration option</caption>
     * <tr>
     * <td>Name</td>
     * <td>Type</td>
     * <td>Mutable</td>
     * <td>Default</td>
     * <td>Minimum</td>
     * <td>Maximum</td>
     * </tr>
     * <tr>
     * <td>{@value}</td>
     * <td>
     * <a href="../EnvironmentConfig.html#timeDuration">Duration</a>
     * </td>
     * <td>Yes</td>
     * <td>100 s</td>
     * <td>1 s</td>
     * <td>-none-</td>
     * </tr>
     * </table>
     *
     * @see <a href="../EnvironmentConfig.html#timeDuration">Time Duration
     * Properties</a>
     */
    public static final String CATCHUP_MASTER_TIMEOUT =
        EnvironmentParams.REP_PARAM_PREFIX + "catchupMasterTimeout";

    static {

        /*
         * Force loading when a ReplicationConfig is used with strings and
         * an environment has not been created.
         */
        @SuppressWarnings("unused")
        ConfigParam forceLoad = RepParams.GROUP_NAME;
    }

    /**
     * @hidden
     * Storage for replication related properties.
     */
    protected Properties props;

    /* For unit testing only: only ever set false when testing. */
    transient boolean validateParams = true;

    /**
     * Create a ReplicationMutableConfig initialized with the system
     * default settings. Parameter defaults are documented with the string
     * constants in this class.
     */
    public ReplicationMutableConfig() {
        props = new Properties();
    }

    /**
     * Used by ReplicationConfig to support construction from a property file.
     * @param properties Hold replication related properties
     */
    ReplicationMutableConfig(Properties properties, boolean validateParams)
        throws IllegalArgumentException {

        this.validateParams = validateParams;
        validateProperties(properties);
        /* For safety, copy the passed in properties. */
        props = new Properties();
        props.putAll(properties);
    }

    /**
     * Fills in the properties calculated by the environment to the given
     * config object.
     */
    void fillInEnvironmentGeneratedProps(RepImpl repImpl) {
        props.put(RepParams.DESIGNATED_PRIMARY.getName(),
                  Boolean.toString(repImpl.isDesignatedPrimary()));
        props.put(RepParams.NODE_PRIORITY.getName(),
                  Integer.toString(getNodePriority()));
    }

    /**
     * @hidden
     * For internal use only
     */
    public void copyMutablePropsTo(ReplicationMutableConfig toConfig) {

        Properties toProps = toConfig.props;
        Enumeration<?> propNames = props.propertyNames();
        while (propNames.hasMoreElements()) {
            String paramName = (String) propNames.nextElement();
            ConfigParam param =
                EnvironmentParams.SUPPORTED_PARAMS.get(paramName);
            assert param != null;
            if (param.isForReplication() &&
                param.isMutable()) {
                String newVal = props.getProperty(paramName);
                toProps.setProperty(paramName, newVal);
            }
        }
    }

    /**
     * If {@code isPrimary} is true, designate this node as a Primary. This
     * setting only takes effect for electable nodes. The application must
     * ensure that exactly one electable node is designated to be a Primary at
     * any given time. Primary node configuration is only a concern when the
     * group has two electable nodes, and there cannot be a simple
     * majority. See the overview on <a href=
     * "{@docRoot}/../ReplicationGuide/lifecycle.html#twonode">configuring two
     * node groups</a>.
     *
     * @param isPrimary true if this node is to be made the Primary
     *
     * @return this
     */
    public ReplicationMutableConfig setDesignatedPrimary(boolean isPrimary) {
        DbConfigManager.setBooleanVal(props, RepParams.DESIGNATED_PRIMARY,
                                      isPrimary, validateParams);
        return this;
    }

    /**
     * Determines whether this node is the currently designated Primary.  See
     * the overview on <a href=
     * "{@docRoot}/../ReplicationGuide/lifecycle.html#twonode"> issues around
     * two node groups</a>
     * @return true if this node is a Primary, false otherwise.
     */
    public boolean getDesignatedPrimary() {
        return DbConfigManager.getBooleanVal(props,
                                             RepParams.DESIGNATED_PRIMARY);
    }

    /**
     * Returns the value associated with the override. A value of zero means
     * that the number of electable nodes is determined as usual, that is, from
     * the contents of the group metadata.
     *
     * @return the number of electable nodes as specified by the override
     *
     * @see #ELECTABLE_GROUP_SIZE_OVERRIDE
     */
    public int getElectableGroupSizeOverride() {
        return DbConfigManager.
            getIntVal(props, RepParams.ELECTABLE_GROUP_SIZE_OVERRIDE);
    }

    /**
     * Sets the size used to determine the number of electable nodes.
     *
     * @param override the number of electable nodes. A value of zero means
     * that the number of electable nodes is determined as usual, that is, from
     * the contents of the group metadata.
     *
     * @return this
     *
     * @see #ELECTABLE_GROUP_SIZE_OVERRIDE
     */
    public ReplicationMutableConfig
        setElectableGroupSizeOverride(int override) {

        DbConfigManager.
            setIntVal(props, RepParams.ELECTABLE_GROUP_SIZE_OVERRIDE, override,
                      validateParams);
        return this;
    }

    /**
     * Gets the minimum amount of time in milliseconds an election must take.
     * The election will be delayed if it takes less than the minimum amount
     * of time.
     *
     * @see #OVERRIDE_MIN_ELECTION_TIME
     * @return this
     */
    public int getOverrideMinElectionTime() {
        return DbConfigManager.
            getIntVal(props, RepParams.OVERRIDE_MIN_ELECTION_TIME);
    }

    /**
     * Sets the minimum amount of time in milliseconds an election must take.
     *
     * @see #OVERRIDE_MIN_ELECTION_TIME
     *
     * @param override whether to disable the delay or not.
     * @return this
     */
    public ReplicationMutableConfig
        setOverrideMinElectionTime(int override) {

        DbConfigManager.
            setIntVal(props, RepParams.OVERRIDE_MIN_ELECTION_TIME, override,
                      validateParams);
        return this;
    }

    /**
     * Returns the election priority associated with the node.
     *
     * @return the priority for this node
     *
     * @see #NODE_PRIORITY
     */
    public int getNodePriority() {
        return DbConfigManager.getIntVal(props, RepParams.NODE_PRIORITY);
    }

    /**
     * Sets the election priority for the node. The algorithm for choosing a
     * new master will pick the participating node that has the most current
     * set of log files. When there is a tie, the priority is used as a
     * tie-breaker to select amongst these nodes.
     * <p>
     * A priority of zero is used to ensure that a node is never elected
     * master, even if it has the most current set of files. Please use this
     * option with caution, since it means that a node with less current log
     * files could be elected master potentially forcing this node to rollback
     * data that had been committed.
     *
     * @param priority the priority to be associated with the node. It must be
     * zero, or a positive number.
     *
     * @see #NODE_PRIORITY
     */
    public ReplicationMutableConfig setNodePriority(int priority) {
        DbConfigManager.setIntVal(props, RepParams.NODE_PRIORITY, priority,
                                  validateParams);
        return this;
    }

    /**
     * Returns the string identifying one or more helper host and port pairs in
     * this format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     * The port name may be omitted if it's the default port.
     *
     * @return the string representing the host port pairs
     *
     */
    public String getHelperHosts() {
        return DbConfigManager.getVal(props, RepParams.HELPER_HOSTS);
    }

    /**
     * Identify one or more helpers nodes by their host and port pairs in this
     * format:
     * <pre>
     * hostname[:port][,hostname[:port]]*
     * </pre>
     * If the port is omitted, the default port defined by XXX is used.
     *
     * @param hostsAndPorts the string representing the host and port pairs.
     *
     * @return this
     */
    public ReplicationMutableConfig setHelperHosts(String hostsAndPorts) {
        DbConfigManager.setVal
            (props, RepParams.HELPER_HOSTS, hostsAndPorts, validateParams);
        return this;
    }

    /**
     * Set this configuration parameter with this value. Values are validated
     * before setting the parameter.
     *
     * @param paramName the configuration parameter name, one of the String
     * constants in this class
     * @param value the configuration value.
     *
     * @return this;
     *
     * @throws IllegalArgumentException if the paramName or value is invalid.
     */
    public ReplicationMutableConfig setConfigParam(String paramName,
                                                   String value)
        throws IllegalArgumentException {

        DbConfigManager.setConfigParam(props,
                                       paramName,
                                       value,
                                       true,   /* require mutability. */
                                       validateParams,
                                       true,   /* forReplication */
                                       true);  /* verifyForReplication */
        return this;
    }

    /**
     * Return the value for this parameter.
     * @param paramName a valid configuration parameter, one of the String
     * constants in this class.
     * @return the configuration value.
     *
     * @throws IllegalArgumentException if the paramName is invalid.
     */
    public String getConfigParam(String paramName)
        throws IllegalArgumentException {

        return DbConfigManager.getConfigParam(props, paramName);
    }

    /**
     * Validate a property bag passed in a construction time.
     */
    void validateProperties(Properties checkProps)
        throws IllegalArgumentException {

        /* Check that the properties have valid names and values */
        Enumeration<?> propNames = checkProps.propertyNames();
        while (propNames.hasMoreElements()) {
            String name = (String) propNames.nextElement();
            /* Is this a valid property name? */
            if (DbConfigManager.handleRemovedProps(checkProps, name)) {
                continue;
            }
            ConfigParam param =
                EnvironmentParams.SUPPORTED_PARAMS.get(name);
            if (param == null) {
                throw new IllegalArgumentException
                (name + " is not a valid JE environment configuration");
            }
            /* Is this a valid property value? */
            if (validateParams) {
                param.validateValue(checkProps.getProperty(name));
            }
        }
    }

    /**
     * @hidden
     * For internal use only.
     * Access the internal property bag, used during startup.
     */
    public Properties getProps() {
        return props;
    }

    /**
     * List the configuration parameters and values that have been set
     * in this configuration object.
     */
    @Override
    public String toString() {
        return props.toString();
    }

    /**
     * For unit testing only
     */
    void setOverrideValidateParams(boolean validateParams) {
        this.validateParams = validateParams;
    }

    /**
     * @hidden
     * For testing only
     */
    public boolean getValidateParams() {
        return validateParams;
    }

    /**
     * @hidden
     * For internal use only.
     * Overrides Object.clone() to clone all properties, used by this class and
     * ReplicationConfig.
     */
    @Override
    protected Object clone()
        throws CloneNotSupportedException {

        ReplicationMutableConfig copy =
            (ReplicationMutableConfig) super.clone();
        copy.props = (Properties) props.clone();
        return copy;
    }
}
