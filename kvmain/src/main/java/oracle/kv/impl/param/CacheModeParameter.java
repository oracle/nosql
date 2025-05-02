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

import com.sleepycat.je.CacheMode;

public class CacheModeParameter extends Parameter {

    private static final long serialVersionUID = 1L;

    private final CacheMode value;

    public CacheModeParameter(String name, String val) {
        super(name);
        try {
            this.value =
                Enum.valueOf(CacheMode.class,
                             val.toUpperCase(java.util.Locale.ENGLISH));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid CacheMode format: " +
                                               val);
        }
    }

    public CacheModeParameter(String name, CacheMode value) {
        super(name);
        this.value = value;
    }

    public CacheMode asCacheMode() {
        return value;
    }

    @Override
    public Enum<?> asEnum() {
        return value;
    }

    @Override
    public String asString() {
        return value.toString();
    }

    @Override
    public ParameterState.Type getType() {
        return ParameterState.Type.CACHEMODE;
    }
}
