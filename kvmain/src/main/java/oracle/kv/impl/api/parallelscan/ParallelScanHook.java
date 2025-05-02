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

package oracle.kv.impl.api.parallelscan;

/**
 * @hidden
 * A hook interface used during ParallelScan processing. This is really for
 * debugging and unit tests.
 */
public interface ParallelScanHook {

    public enum HookType {
        AFTER_PROCESSING_STREAM,
        BEFORE_EXECUTE_REQUEST
    }

    /*
     * This will generally be called within an assert so the return value
     * should be true unless the callback wants an AssertionError thrown.
     */
    public boolean callback(Thread thread, HookType hookType, String info);
}
