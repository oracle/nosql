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

package com.sleepycat.je.rep.util;

import java.io.File;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DbInternal;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.dbi.DbTree;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.rep.vlsn.VLSNBucket;
import com.sleepycat.je.rep.vlsn.VLSNRange;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.Locker;
import com.sleepycat.je.txn.Txn;

/**
 * A utility that reads the VLSNIndex database and dumps the database contents.
 * The database contents includes VLSNRange and VLSNBuckets.
 * <p>
 * This utility is useful when there was any VLSN issues 
 * related to the VLSNIndex, Ghostbuckets, VLSNBuckets, VLSNRange that are 
 * already flushed to the database.
 *
 */

public class VLSNIndexDump {
    final private String envHome;
    private static final String usageString =
        "usage: java -cp je.jar " +
        "com.sleepycat.je.rep.util.VLSNIndexDump\n" +
        " -h <dir>                          # environment home directory\n";

    /**
     * Usage:
     * <pre>
     * java -cp je.jar com.sleepycat.je.rep.util.VLSNIndexDump
     *   -h &lt;dir&gt;                          # environment home directory
     * </pre>
     */

    
    public VLSNIndexDump(final String envHome) {
        this.envHome = envHome;
    }

    private static void printUsage(String msg) {
        System.err.println(msg);
        System.err.println(usageString);
        System.exit(-1);
    }

    public static void main(String[] args) {
        final int nArgs = args.length;
        String envHome = null;
        int argc = 0;

        while (argc < nArgs) {
            String thisArg = args[argc++];
            if (thisArg.equals("-h")) {
                if (argc < nArgs) {
                    envHome = args[argc++];
                } else {
                    printUsage("-h requires an argument");
                }
            }
        }
        if (envHome == null) {
            printUsage("-h requires an argument");
            System.exit(-1);
        }
        
        final VLSNIndexDump vlsnIndexDump = new VLSNIndexDump(envHome);
        vlsnIndexDump.dumpVLSNIndex();
    }

    private void dumpVLSNIndex() {
        final EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setReadOnly(true);
        envConfig.setTransactional(true);
        final Environment env = new Environment(new File(envHome), envConfig);
        final EnvironmentImpl envImpl = DbInternal.getEnvironmentImpl(env);
        Locker locker = Txn.createLocalAutoTxn(envImpl,
            new TransactionConfig());

        DatabaseImpl mappingDbImpl = null;
        try {
            DbTree dbTree = envImpl.getDbTree();
            mappingDbImpl = dbTree.getDb(locker, "_jeVlsnMapDb",
                null /* databaseHandle */, false);
        } finally {
            locker.operationEnd(true);
        }

        locker = BasicLocker.createBasicLocker(envImpl);
        Cursor cursor = DbInternal.makeCursor(mappingDbImpl, locker,
            CursorConfig.DEFAULT);
        try {
            DbInternal.getCursorImpl(cursor).setAllowEviction(false);

            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry data = new DatabaseEntry();

            int count = 0;
            while (cursor.getNext(key, data,
                LockMode.DEFAULT) == OperationStatus.SUCCESS) {

                Long keyValue = LongBinding.entryToLong(key);

                System.out.println("\n key => " + keyValue);
                if (count == 0) {
                    VLSNRange range = VLSNRange.readFromDatabase(data);
                    System.out.println("range =>");
                    System.out.println(range);
                } else {
                    VLSNBucket bucket = VLSNBucket.readFromDatabase(data);
                    System.out.println("bucket =>");
                    bucket.dump(System.out);
                }
                count++;
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }

            if (locker != null) {
                locker.operationEnd(true);
            }
            env.close();
        }
    }
}
