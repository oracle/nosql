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

package oracle.kv.table;

import java.util.Map;

/**
 * JsonDef is an extension of {@link FieldDef} to define schemaless data modeled
 * as JSON. A field defined as JsonDef can contain MapOfAnyValue, ArrayOfAnyValue,
 * or any of the atomic types.
 *
 * @since 3.0
 */
public interface JsonDef extends FieldDef {

    /**
     * Return a set of all Json fields that are MR_Counter.
     * @return a set of all Json fields that are MR_Counter
     * or null if there is no MR_Counter field.
     * @since 22.1
     */
    default Map<String, Type> allMRCounterFields() {
        return null;
    }
}
