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

package oracle.kv.impl.measurement;

/**
 * The MeasurementType is the metadata for a given measurement type. The
 * definition consists of a name and a description field.  The description
 * field is meant to contain an English, user-understandable explanation of the
 * measurement type.
 *
 * Note that only the id is sent over the wire. This class itself is never
 * transmitted, and is not serializable.
 */
public class MeasurementType {
 
    private final int id;
    private final String name;
    private final String description;

    public MeasurementType(int id,
                           String name, 
                           String description) {
        this.id = id;
        this.name = name;
        this.description = description;
    }

    public int getId() {
        return id;
    }
    
    public String getName() {
        return name;
    }
    
    public String getDescription() {
        return description;
    }
    
    @Override
    public String toString() {
        return name + " : " + description;
    }
}