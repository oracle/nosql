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

public class DefaultParameter {
    public Object create(String name, String defaultValue,
                         ParameterState.Type type) {
        return Parameter.createParameter(name, defaultValue, type);
    }

    public static Parameter getDefaultParameter(ParameterState ps) {
        return (Parameter) ps.getDefaultParameter();
    }

    public static Parameter getDefaultParameter(String name) {
        ParameterState state = ParameterState.lookup(name);
        if (state != null) {
            return DefaultParameter.getDefaultParameter(state);
        }
        throw new IllegalArgumentException("No such parameter: " + name);
    }
}
