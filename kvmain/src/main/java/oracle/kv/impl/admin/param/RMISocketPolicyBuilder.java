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

package oracle.kv.impl.admin.param;

import java.util.Properties;
import java.util.logging.Logger;

import oracle.kv.impl.util.registry.RMISocketPolicy;
import oracle.kv.impl.param.ParameterMap;

/**
 * Interface used for creating RMISocketPolicy instances.
 */
public interface RMISocketPolicyBuilder {

    /**
     * Make the socket policy.
     */
    RMISocketPolicy makeSocketPolicy(SecurityParams securityParams,
                                     ParameterMap map,
                                     Logger logger)
        throws Exception;

    /**
     * Make a set of properties for client access.
     */
    Properties getClientAccessProperties(SecurityParams securityParams,
                                         ParameterMap map);
}
