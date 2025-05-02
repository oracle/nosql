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

import oracle.kv.impl.param.ParameterMap;

/**
 * This interface defines the bridge mechanism for constructing a
 * JE ReplicationAccessConfig properties from the defined security parameters
 */
public interface RepNetConfigBuilder {
    /**
     * Returns a set of properties related to channel factory creation
     * to be used when constructing a ReplicationConfig.
     */
    public Properties makeChannelProperties(SecurityParams securityParams,
                                            ParameterMap map);
}
