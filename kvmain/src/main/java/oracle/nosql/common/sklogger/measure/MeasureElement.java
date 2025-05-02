/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.common.sklogger.measure;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Represents an element of measurement.
 *
 * <p>A measurement result can be obtained for the element with respect to a
 * watcher. The result represents the measurement between the last time the
 * watcher is reset until present.
 */
public interface MeasureElement<R extends JsonSerializable> {

    static final String GLOBAL_WATCHER_NAME = null;

    /**
     * Obtains the measurement result with respect to a watcher. The obtained
     * result represents a measurement based on the period between present time
     * and the last time this method is called with the same {@code
     * watcherName} and the {@code clear} flag set to {@code true}.
     *
     * <p>The {@code clear} flag exists for the purpose of compatibility. The
     * use cases of setting {@code clear} to false are that a measurement
     * result is obtained without interfere a future reading. We expect such
     * obtain attempts should always suffice by creating a different watcher.
     * Hence we are deprecating the flag in the future. The methods without the
     * flag should be used.
     *
     * @param watcherName the watcher name
     * @param clear {@code true} to mark the start of the period of the next
     * result to obtain, will be deprecated in the future
     * @return the result, must be non-null
     * @throws IllegalArgumentException if {@code watcherName} is null
     */
    R obtain(String watcherName, boolean clear);

    default R obtain(String watcherName) {
        return obtain(watcherName, true);
    }

    default R obtain(boolean clear) {
        return obtain(GLOBAL_WATCHER_NAME, clear);
    }

    default R obtain() {
        return obtain(GLOBAL_WATCHER_NAME);
    }
}

