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

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

/**
 * Helper class for exporting partition generation metadata.
 */
public class PartitionGenerationHelper {
    /* Prevent construction */
    private PartitionGenerationHelper() {
    }

    public static void printPartitionGenerationMetadata(Environment env,
            DatabaseConfig dbConfig, FileWriter mdWriter) {
        try (Database dbGeneration = PartitionGenDBManager.openDb(env, dbConfig,
                null)) {
            List<PartitionGeneration> listPartGen = PartitionGenDBManager
                    .getPartitionGenerationList(dbGeneration);
            if (listPartGen.size() > 0) {
                final ObjectNode jsonPartGenMain = JsonUtils.createObjectNode();
                final ArrayNode partGenArray = jsonPartGenMain
                        .putArray("PartitionsGenerationMetadata");
                for (int i = 0; i < listPartGen.size(); i++) {
                    String jsonStr = JsonUtils.prettyPrint(listPartGen.get(i));
                    ObjectNode jsonPartGen = JsonUtils.parseJsonObject(jsonStr);
                    partGenArray.add(jsonPartGen);
                }
                mdWriter.write(JsonUtils.writeAsJson(jsonPartGenMain, true));
                mdWriter.write(System.getProperty("line.separator"));
            }
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.out.println("Exception caught retrieving partition"
                    + " generation metadata: " + e.getMessage());
        }
    }
}