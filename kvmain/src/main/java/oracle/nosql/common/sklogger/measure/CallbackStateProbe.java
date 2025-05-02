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

import java.util.function.Supplier;

import oracle.nosql.common.jss.JsonSerializable;

/**
 * Probes the current system state.
 *
 * <p>Instead of providing observed data like {@link StateProbe}, the caller
 * provides a callback which will be invoked during {@link #obtain}. This
 * class is suitable for background-service instrumentations such as GC state,
 * memory usage, etc.
 *
 * @see StateProbe
 */
public class CallbackStateProbe<R extends JsonSerializable>
    implements MeasureElement<R> {

    private final Supplier<R> resultSupplier;

    public CallbackStateProbe(Supplier<R> resultSupplier) {
        this.resultSupplier = resultSupplier;
    }

    @Override
    public synchronized R obtain(String watcherName, boolean clear) {
        return resultSupplier.get();
    }
}

