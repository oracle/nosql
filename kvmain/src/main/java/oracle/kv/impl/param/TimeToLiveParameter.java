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

package oracle.kv.impl.param;

import java.util.concurrent.TimeUnit;
import oracle.kv.table.TimeToLive;

/**
 * String format is "long Unit"
 * Accepted units are valid TimeUnit strings which represent either days or
 * hours.
 *
 * @see DurationParameter
 */
public class TimeToLiveParameter extends DurationParameter {

    private static final long serialVersionUID = 1L;

    public TimeToLiveParameter(String name, String value) {
        super(name, value);
        /* Will throw IllegalArgumentException if values are bad */
        toTimeToLive();
    }

    public TimeToLiveParameter(String name, TimeUnit unit, long amount) {
        super(name, unit, amount);
        /* Will throw IllegalArgumentException if values are bad */
        toTimeToLive();
    }

    public final TimeToLive toTimeToLive() {
        return TimeToLive.createTimeToLive(getAmount(), getUnit());
    }

    @Override
    public boolean equals(Parameter other) {
        return (other instanceof TimeToLiveParameter) ? super.equals(other) :
                                                        false;
    }
}
