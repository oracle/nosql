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

package oracle.kv.impl.rep.admin;

import java.io.Serializable;

import oracle.kv.KVVersion;
import oracle.kv.impl.rep.RepNode;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentStats;

/**
 * Diagnostic and status information about a RepNode.
 */
public class RepNodeInfo implements Serializable {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private final EnvironmentConfig envConfig;
    private final EnvironmentStats envStats;

    /* Since R2 patch 1 */
    private final KVVersion version;

    RepNodeInfo(RepNode repNode) {
        version = KVVersion.CURRENT_VERSION;
        Environment env = repNode.getEnv(0L);
        if (env == null) {
            envConfig = null;
            envStats = null;
            return;
        }

        envConfig = env.getConfig();
        envStats = env.getStats(null);
    }

    public EnvironmentConfig getEnvConfig() {
        return envConfig;
    }

    public EnvironmentStats getEnvStats() {
        return envStats;
    }

    /**
     * Gets the RepNode's software version.
     * 
     * @return the RepNode's software version
     */
    public KVVersion getSoftwareVersion() {
        assert version != null;
        return version;
    }

    @Override 
    public String toString() {
        return "Environment Configuration:\n" +  envConfig +
            "\n" + envStats;
    }
}
