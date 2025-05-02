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

package oracle.kv.impl.arb.admin;

import java.io.Serializable;

import com.sleepycat.je.rep.arbiter.ArbiterStats;

import oracle.kv.KVVersion;
import oracle.kv.impl.arb.ArbNode;


/**
 * Diagnostic and status information about a ArbNode.
 */
public class ArbNodeInfo implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final ArbiterStats arbStats;

    private final KVVersion version;

    ArbNodeInfo(ArbNode arbNode) {
        version = KVVersion.CURRENT_VERSION;
        arbStats = arbNode.getStats(false);
    }
    
    ArbNodeInfo(KVVersion version) {
        this.version = version;
        arbStats = null;
    }

    public ArbiterStats getStats() {
        return arbStats;
    }

    /**
     * Gets the ArbNode's software version.
     *
     * @return the ArbNode's software version
     */
    public KVVersion getSoftwareVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "Arbiter Version:\n" +  version +
            "\n" + arbStats;
    }
}
