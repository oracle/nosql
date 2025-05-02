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

package oracle.kv.impl.admin;

import java.util.logging.Logger;

import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminDatabase.LongKeyDatabase;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.api.table.TableMetadata;

import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

public class TableStore extends AdminStore {

    public static TableStore getReadOnlyInstance(Logger logger,
                                                 Environment repEnv) {
        return new TableStore(logger, repEnv, true);
    }

    private final LongKeyDatabase<TableMetadata> metadataDb;

    TableStore(Logger logger, Environment env, boolean readOnly) {
        super(logger);
        metadataDb = new LongKeyDatabase<>(DB_TYPE.TABLE, logger,
                                           env, readOnly);
    }

    /**
     * Gets the TableMetadata object using the specified transaction.
     */
    public TableMetadata getTableMetadata(Transaction txn) {
        return metadataDb.get(txn, LongKeyDatabase.ZERO_KEY,
                              (txn != null ? LockMode.RMW :
                                             LockMode.DEFAULT),
                              TableMetadata.class);
    }

    /**
     * Persists the specified metadata object with the specified transaction.
     */
    public boolean putTableMetadata(Transaction txn,
                                    TableMetadata md,
                                    boolean noOverwrite) {
        return metadataDb.put(txn, LongKeyDatabase.ZERO_KEY,
                              md, noOverwrite);
    }

    @Override
    public void close() {
        metadataDb.close();
    }
}
