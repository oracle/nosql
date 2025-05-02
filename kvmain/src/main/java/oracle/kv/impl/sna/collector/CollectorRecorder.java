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

package oracle.kv.impl.sna.collector;

import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.StorageNodeParams;

/**
 * Implement this interface to store collected monitor stat.
 */
public interface CollectorRecorder {

    public enum MetricType {
        PING, PLAN, RNENV, RNEVENT, RNEXCEPTION, RNOP, RNTABLE, RNJVM,
        RNENDPOINTGROUP, LOGGINGSTAT,
    }

    /**
     * @param type specify the stat type. We support type: ping, plan, rnEnv,
     * rnEvent, rnException.
     * @param stat to be record collected stat message
     */
    public void record(MetricType type, String stat);

    /**
     * update collector recorder according to new GlobalParams and
     * StorageNodeParams.
     */
    public void updateParams(GlobalParams newGlobalParams,
                             StorageNodeParams newSNParams);

    /**
     * close the collector recorder
     */
    public void close();
}
