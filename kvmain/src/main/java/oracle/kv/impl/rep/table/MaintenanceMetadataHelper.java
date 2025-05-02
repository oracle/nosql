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

package oracle.kv.impl.rep.table;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import oracle.kv.impl.rep.migration.generation.PartitionGenDBManager;
import oracle.kv.impl.rep.table.SecondaryInfoMap.DeletedTableInfo;
import oracle.kv.impl.rep.table.SecondaryInfoMap.SecondaryInfo;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

/**
 * Helper class for exporting secondary and table maintenance metadata.
 */
public class MaintenanceMetadataHelper {
    /* Prevent construction */
    private MaintenanceMetadataHelper() {
    }

    /**
     * Prints JSON representation of the secondary and table maintenance
     * metadata.
     */
    public static void printSecondaryTableMaintenanceMetadata(Environment env,
                                                    DatabaseConfig dbConfig,
                                                    FileWriter mdWriter) {
        try(Database dbSecondary =
                TableManager.openInfoDB(env, dbConfig, null)) {
            SecondaryInfoMap infoMap = SecondaryInfoMap.fetch(dbSecondary);

            /* Secondary/indexes info */
            ObjectNode secondaryInfo = JsonUtils.createObjectNode();
            Map<String, SecondaryInfo> secondaryInfoMap = infoMap
                    .getSecondaryMap();
            for (Map.Entry<String, SecondaryInfo> entry : secondaryInfoMap
                    .entrySet()) {
                String jsonFormattedStr = JsonUtils
                        .prettyPrint(entry.getValue());
                ObjectNode jnodeSecInfo = JsonUtils
                        .parseJsonObject(jsonFormattedStr);
                secondaryInfo.put(entry.getKey(), jnodeSecInfo);
            }
            ObjectNode jnodeSecInfoMain = JsonUtils.createObjectNode();
            jnodeSecInfoMain.set("SecondaryMetadata", secondaryInfo);
            mdWriter.write(JsonUtils.writeAsJson(jnodeSecInfoMain, true));
            mdWriter.write(System.getProperty("line.separator"));

            /* Deleted table info */
            ObjectNode delTableInfo = JsonUtils.createObjectNode();
            Map<String, DeletedTableInfo> deletedTableInfoMap = infoMap
                    .getDeletedTableMap();
            if (!deletedTableInfoMap.isEmpty()) {
                for (Map.Entry<String, DeletedTableInfo> entry :
                                            deletedTableInfoMap.entrySet()) {
                    String jsonFormattedStr = JsonUtils
                            .prettyPrint(entry.getValue());
                    ObjectNode jnodeDelTableInfo = JsonUtils
                            .parseJsonObject(jsonFormattedStr);
                    delTableInfo.put(entry.getKey(), jnodeDelTableInfo);
                }
                ObjectNode jnodeTableInfoMain = JsonUtils.createObjectNode();
                jnodeTableInfoMain.set("TableMaintenanceMetadata",
                        delTableInfo);
                mdWriter.write(JsonUtils.writeAsJson(jnodeTableInfoMain, true));
                mdWriter.write(System.getProperty("line.separator"));
            } else {
                System.out.println("No active deletion of table is going on."
                        + " So table maintenance data do not available at this "
                        + "time.");
            }
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.err.println("Exception caught retrieving metadata from "
                    + PartitionGenDBManager.getDBName() + " database.");
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            System.out.println(exceptionAsString);
        }
    }
}
