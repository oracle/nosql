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
package oracle.kv.impl.security;

import java.util.Properties;
import java.util.logging.Logger;

import oracle.kv.impl.admin.param.RMISocketPolicyBuilder;
import oracle.kv.impl.admin.param.RepNetConfigBuilder;
import oracle.kv.impl.admin.param.SecurityParams;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.util.registry.ClearSocketPolicy;
import oracle.kv.impl.util.registry.RMISocketPolicy;

import com.sleepycat.je.rep.ReplicationNetworkConfig;

/**
 * Factory class for generating RMISocketPolicy instances and configuring
 * JE data channels.
 */
public class ClearTransport
    implements RMISocketPolicyBuilder, RepNetConfigBuilder {

    /**
     * Simple constructor, for use by newInstance().
     */
    public ClearTransport() {
    }

    /*
     * RMISocketPolicyBuilder interface methods
     */

    /**
     * Creates an instance of the RMISocketPolicy.
     */
    @Override
    public RMISocketPolicy makeSocketPolicy(SecurityParams securityParams,
                                            ParameterMap map,
                                            Logger logger)
        throws Exception {

        return new ClearSocketPolicy();
    }

    /**
     * Construct a set of properties for client access.
     */
    @Override
    public Properties getClientAccessProperties(SecurityParams sp,
                                                ParameterMap map) {
        return new Properties();
    }

    /*
     * RepNetConfigBuilder interface methods
     */

    @Override
    public Properties makeChannelProperties(SecurityParams sp,
                                            ParameterMap map) {

        final Properties props = new Properties();
        props.setProperty(ReplicationNetworkConfig.CHANNEL_TYPE, "basic");
        return props;
    }
}
