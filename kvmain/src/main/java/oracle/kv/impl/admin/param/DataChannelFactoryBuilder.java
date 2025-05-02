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

import com.sleepycat.je.rep.net.DataChannelFactory;
import oracle.kv.impl.param.ParameterMap;

/**
 * This interface defines the bridge mechanism for constructing a
 * JE DataChannelFactory from the defined security parameters
 */
public interface DataChannelFactoryBuilder {
    public DataChannelFactory makeChannelFactory(SecurityParams securityParams,
                                                 ParameterMap map)
        throws Exception;
}
