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

package oracle.kv.impl.rep.migration.generation;

import oracle.kv.impl.topo.PartitionId;

/**
 * Exception that will be raised when partition metadata manager fails to
 * read or write the JE database due to errors, or experiences irrecoverable
 * errors and must be closed.
 */
public class PartitionMDException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final PartitionGeneration gen;

    private final String dbName;

    PartitionMDException(String dbName,
                         String msg,
                         PartitionGeneration gen,
                         Throwable cause) {
        super(msg, cause);
        this.dbName = dbName;
        this.gen = gen;
    }

    PartitionMDException(String dbName, String msg, Throwable cause) {
        this(dbName, msg, null, cause);
    }

    public PartitionId getPartitionId() {
        return gen.getPartId();
    }


    public PartitionGeneration getGeneration() {
         return gen;
    }

    public String getDbName() {
        return dbName;
    }
}
