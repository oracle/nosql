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

import oracle.kv.impl.admin.Admin.Memo;
import oracle.kv.impl.admin.AdminDatabase.DB_TYPE;
import oracle.kv.impl.admin.AdminDatabase.LongKeyDatabase;
import oracle.kv.impl.admin.AdminStores.AdminStore;
import oracle.kv.impl.admin.param.Parameters;

import com.sleepycat.je.Environment;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

public class GeneralStore extends AdminStore {

    public static GeneralStore getReadOnlyInstance(Logger logger,
                                                   Environment env) {
        return new GeneralStore(logger, env, true);
    }

    /* For unit tests only */
    public static GeneralStore getReadTestInstance(Environment env) {
        return new GeneralStore(Logger.getLogger(GeneralStore.class.getName()),
                                env, false);
    }
    
    private final LongKeyDatabase<Parameters> parametersDb;
    private final LongKeyDatabase<Memo> memoDb;

    public GeneralStore(Logger logger, Environment env, boolean readOnly) {
        super(logger);
        parametersDb = new LongKeyDatabase<>(DB_TYPE.PARAMETERS, logger,
                                             env, readOnly);
        memoDb = new LongKeyDatabase<>(DB_TYPE.MEMO, logger, env, readOnly);
    }

    public Memo getMemo() {
        return getMemo(null);
    }

    /**
     * Gets Parameters from the store.
     */
    public Parameters getParameters(Transaction txn) {
        return parametersDb.get(txn, LongKeyDatabase.ZERO_KEY,
                                LockMode.DEFAULT, Parameters.class);
    }

    public void putParameters(Transaction txn, Parameters p) {
        parametersDb.put(txn, LongKeyDatabase.ZERO_KEY, p, false);
    }

    Memo getMemo(Transaction txn) {
        return memoDb.get(txn, LongKeyDatabase.ZERO_KEY,
                          LockMode.DEFAULT, Memo.class);
    }

    void putMemo(Transaction txn, Memo memo) {
        memoDb.put(txn, LongKeyDatabase.ZERO_KEY, memo, false);
    }

    @Override
    public void close() {
        parametersDb.close();
        memoDb.close();
    }
}
