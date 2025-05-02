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

package oracle.kv.impl.diagnostic;

import java.io.File;
import java.util.Map;

/**
 * This class is used to store the latency, java version and network
 * connectivity status. It is used in environment parameters checking.
 *
 */
public class EnvParams {
    private long latency;
    private JavaVersionVerifier javaVersion;
    private Map<SNAInfo, Boolean> map;
    private File saveFolder;

    public EnvParams(long latency, JavaVersionVerifier javaVersion,
                     Map<SNAInfo, Boolean> map, File saveFolder) {
        this.latency = latency;
        this.javaVersion = javaVersion;
        this.map = map;
        this.saveFolder = saveFolder;
    }

    public long getLatency() {
        return latency;
    }

    public JavaVersionVerifier getJavaVersion() {
        return javaVersion;
    }

    public Map<SNAInfo, Boolean> getNetworkConnectionMap() {
        return map;
    }

    public File getSaveFolder() {
        return saveFolder;
    }
}
