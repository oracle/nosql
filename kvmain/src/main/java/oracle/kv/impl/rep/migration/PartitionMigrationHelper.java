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

package oracle.kv.impl.rep.migration;

import java.io.FileWriter;
import java.io.IOException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

/**
 * Helper class for exporting partition migration metadata.
 */
public class PartitionMigrationHelper {

    /* Prevent construction */
    private PartitionMigrationHelper() { }

    /**
     * Prints JSON representation of the partition migration metadata.
     */
    public static void printPartitionMigrationMetadata(Environment env,
                                                       DatabaseConfig dbConfig,
                                                       FileWriter mdWriter) {
        try (Database db = PartitionMigrations.openDb(env, dbConfig, null)) {
            PartitionMigrations.fetchRead(db).print(mdWriter);
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.err.println("Exception caught retrieving partition" +
                               " migration metadata.");
            e.printStackTrace();
        }
    }
}
