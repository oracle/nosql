/*-
 * Copyright (C) 2002, 2025, Oracle and/or its affiliates. All rights reserved.
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

package com.sleepycat.je.utilint;

/**
 * Stat base class for stats that want to advertise a return value of a
 * particular type.
 *
 * @param <T> The statistic value type
 */
public abstract class StatWithValueType<T> extends Stat<T> {

    /**
     * Creates an instance that registers itself with an owning group.
     *
     * @param group the owning group
     * @param definition the definition of the statistic
     */
    protected StatWithValueType(StatGroup group, StatDefinition definition) {
        super(group, definition);
    }

    /**
     * Creates an instance without registering it with an owning group.
     *
     * @param definition the definition of the statistic
     */
    protected StatWithValueType(StatDefinition definition) {
        super(definition);
    }

    /**
     * Returns the type of the value returned by the {@link #get} method.
     *
     * @return the type of the value returned by get
     */
    public abstract Class<T> getValueType();

    /**
     * Returns the value of the statistic as an instance of the specified type,
     * or {@code null} if the value is a different type.
     *
     * @param <S> the desired value type
     * @param requestedType the class of the desired value type
     * @return the statistic value or {@code null}
     */
    public <S> S getForType(Class<S> requestedType) {
        if (!getValueType().isAssignableFrom(requestedType)) {
            return null;
        }
        return requestedType.cast(get());
    }
}
