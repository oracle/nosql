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

package oracle.kv.impl.sna;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Container class for information about the storage node.
 */
public class StorageNodeInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Long> storageDirectories = new HashMap<>();

    /**
     * Returns a map of storage directories and sizes configured for the
     * storage node. The key of the map is the directory path. The value is
     * the size. If the size is &lt; 0, there was an error retriving the
     * size information on the SN (see the SN logs for details). The
     * directories are those defined by ParameterState.BOOTSTRAP_MOUNT_POINTS
     * for the SN.
     *
     * @return a map of storage directories and sizes
     */
    public Map<String, Long> getStorageDirectorySizes() {
        return storageDirectories;
    }

    /**
     * Adds storage directory information.
     */
    void addStorageDirectory(String directory, long size) {
        storageDirectories.put(directory, size);
    }
}
