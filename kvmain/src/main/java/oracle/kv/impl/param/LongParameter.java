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

public class LongParameter extends Parameter {

    private static final long serialVersionUID = 1L;

    private final long value;

    public LongParameter(String name, long value) {
        super(name);
        this.value = value;
    }

    public LongParameter(String name, String value) {
        this(name, Long.parseLong(value));
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public String asString() {
        return Long.toString(value);
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.LONG;
    }
}
