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

import java.io.Serializable;

/**
 */
public class StackTrace implements Measurement, Serializable {

    private static final long serialVersionUID = 1L;
    
    private final int measurementId;
    private final String stackTrace;

    public StackTrace(int id, String trace) {
        measurementId = id;
        stackTrace = trace;
    }

    @Override
    public int getId() {
        return measurementId;
    }
    
    @Override
    public String toString() {
        return stackTrace;
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getEnd() {
        return 0;
    }
}