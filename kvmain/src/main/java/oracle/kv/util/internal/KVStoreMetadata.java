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

package oracle.kv.util.internal;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import oracle.kv.impl.rep.MetadataHelper;
import oracle.kv.impl.rep.migration.PartitionMigrationHelper;
import oracle.kv.impl.rep.migration.generation.PartitionGenerationHelper;
import oracle.kv.impl.rep.table.MaintenanceMetadataHelper;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentNotFoundException;

/**
 * KVStoreMetadata is an unadvertised, in-progress utility which can read and
 * display the contents of the RN database for debugging and diagnostic
 * purposes.It must be invoked on the node where the RN environment is hosted.
 *
 * <pre>
 *   KVStoreMetadata &lt;directory where RN database is located&gt;
 * </pre>
 *
 * Note that the RN database is typically in <storagedir>/rgXrnY/env. The je.jar
 * must be in the classpath, as well as the usual kvstore classes.
 *
 * Currently, the utility print below metadata in KVStoreMetadata.json 1) Store
 * version 2) Topology 3) Security 4) Table 5) Partition Migration 6) Partition
 * Generation 7) Secondary Info and Table Maintenance. It would be a good idea
 * to expand this based on future requirement.
 */
public class KVStoreMetadata {
    /**
	 * {@literal
	 * Usage:
	 * java -cp KVHOME/lib/kvstore.jar oracle.kv.util.internal.KVStoreMetadata
	 *  <storagedir>/rgXrnY/env
	 * }
	 */
    public static void main(String[] args) {
        String mdFileName = "KVSMetadataInfo.json";
        Environment env = null;

        int nArgs = args.length;
        if (nArgs == 0) {
            System.out.println("Usage: " + KVStoreMetadata.class.getName()
                    + " <RN Environment Directory>");
            System.out.println(
                    "For example: oracle.kv.util.internal.KVStoreMetadata "
                            + "/scratch/RepData/rg1-rn1/env");
            System.exit(-1);
        }

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setReadOnly(true);
        dbConfig.setCacheMode(CacheMode.EVICT_LN);
        dbConfig.setTransactional(true);

        File envHome = new File(args[0]);
        try {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setReadOnly(true);
            envConfig.setTransactional(true);
            env = new Environment(envHome, envConfig);
        } catch (EnvironmentNotFoundException ex) {
            System.out.println("Please check RN Environtment direcory for"
                    + " log files: " + envHome);
            ex.printStackTrace(System.err);
            System.exit(-1);
        } catch (Exception e) {
            System.out.println("General exception.");
            e.printStackTrace();
        }

        System.out
                .println("Retrieving metadata from RN database in " + envHome);

        try (FileWriter mdWriter = new FileWriter(mdFileName)) {
            MetadataHelper.printVersionMetadata(env, dbConfig, mdWriter);

            MetadataHelper.printTopoMetadata(env, dbConfig, mdWriter);

            MetadataHelper.printSecurityMetadata(env, dbConfig, mdWriter);

            MetadataHelper.printTableMetadata(env, dbConfig, mdWriter);

            PartitionMigrationHelper.printPartitionMigrationMetadata(env,
                    dbConfig, mdWriter);

            PartitionGenerationHelper.printPartitionGenerationMetadata(env,
                    dbConfig, mdWriter);

            MaintenanceMetadataHelper.printSecondaryTableMaintenanceMetadata(
                    env, dbConfig, mdWriter);

            System.out.println("Retrieved Metadata Information from RN" +
                               " databases. Please find metadata information" +
                               " into KVSMetadataInfo.json.");
        } catch (IOException ex) {
            System.out.println("IOException caught in Main.");
            ex.printStackTrace(System.err);
        } catch (Exception e) {
            System.out.println("General Exception caught in Main.");
            e.printStackTrace(System.err);
        } finally {
            if (env != null) {
                env.close();
            }
        }
    }
}
