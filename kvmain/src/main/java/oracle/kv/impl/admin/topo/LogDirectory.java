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

package oracle.kv.impl.admin.topo;

import java.util.Objects;

import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.SizeParameter;

/**
 * Description of a log directory. A log directory is a directory path
 * and size. If the path is null the log directory is the root directory
 */
public class LogDirectory implements Comparable<LogDirectory> {

    public static final LogDirectory DEFAULT_LOG_DIR =
            new LogDirectory(null, 0L);

    /* null indicates the root directory (isRoot will return true) */
    private final String path;
    private final long size;

    public LogDirectory(String path, long size) {
        assert size >= 0L;
        this.path = path;
        this.size = size;
    }

    /**
     * Creates a log directory instance from the specified parameter. The
     * parameter type can be STRING or SIZE.
     */
    LogDirectory(Parameter p) {
        this(p.getName(), SizeParameter.getSize(p));
    }

    /**
     * Returns the log directory path string or null.
     */
    public String getPath() {
        return path;
    }

    /**
     * Returns the log directory size.
     */
    public long getSize() {
        return size;
    }

    /**
     * Returns true if this log directory instance represents the root
     * directory.
     */
    boolean isRoot() {
        return path == null;
    }

    /**
     * This will sort on size. Note that this does not return 0 if the sizes
     * are equal. This is to allow for duplicates in sorted sets and maps and
     * to order root and explicitly storage directories. This violates the
     * contract for compareTo() and is inconsistent with equals().
     */
    @Override
    public int compareTo(LogDirectory other) {
        /*
         * If the sizes are equal, and this storage directory represents the
         * root, then sort this lower than the other. This will sort explicit
         * storage directories higher.
         */
        return (size == other.size) ? (isRoot() || !other.isRoot()) ? -1 : 1 :
                                      (size > other.size) ? 1 : -1;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LogDirectory)) {
            return false;
        }
        final LogDirectory other = (LogDirectory)obj;
        if (size != other.size) {
            return false;
        }
        if (path == null) {
            return other.path == null;
        }
        return (other.path == null) ? false : path.equals(other.path);
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 59 * hash + Objects.hashCode(this.path);
        hash = 59 * hash + (int)(this.size ^ (this.size >>> 32));
        return hash;
    }

    @Override
    public String toString() {
        return "LogDirectory[" + path + ", " + size + "]";
    }
}
