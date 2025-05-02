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

package oracle.kv.impl.rep;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import oracle.kv.KVVersion;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.rep.table.TableManager;
import oracle.kv.impl.security.RoleInstance;
import oracle.kv.impl.security.metadata.KVStoreUser;
import oracle.kv.impl.security.metadata.SecurityMetadata;
import oracle.kv.impl.topo.Topology;
import oracle.kv.impl.util.TopologyPrinter;
import oracle.nosql.common.json.ArrayNode;
import oracle.nosql.common.json.JsonUtils;
import oracle.nosql.common.json.ObjectNode;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

/**
 * MetadataHelper is implemented to retrieve Version,  Topology, Security,
 * Table metadata from the various databases.
 */
public class MetadataHelper{

    /* Prevent construction */
    private MetadataHelper() {}

    /*
     * Retrieve KVStore Version metadata and store into output file.
     */
    public static void printVersionMetadata(Environment env,
                                            DatabaseConfig dbConfig,
                                            FileWriter mdWriter) {
        try (Database dbVersion = VersionManager.openDb(env, dbConfig)) {
            KVVersion kvVersion = VersionManager.getLocalVersion(dbVersion);

            String jsonVersionInfo = JsonUtils.prettyPrint(kvVersion);
            ObjectNode version = JsonUtils.createObjectNode();
            version = JsonUtils.parseJsonObject(jsonVersionInfo);
            ObjectNode jsonVersion = JsonUtils.createObjectNode();
            jsonVersion.set("VersionMetadata", version);
            mdWriter.write(JsonUtils.writeAsJson(jsonVersion, true));
            mdWriter.write(System.getProperty("line.separator"));
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.out.println("Exception caught retrieving version metadata.");
            e.printStackTrace();
        }
    }

    /*
     * Retrieve topology metadata and store into output file.
     */
    public static void printTopoMetadata(Environment env,
                                         DatabaseConfig dbConfig,
                                         FileWriter mdWriter) {
        try (Database dbTopo = RepNode.openTopoDatabase(env, dbConfig, null)) {
            Topology topo = RepNode.getTopology(dbTopo);
            final ObjectNode topologyJSONOutput = TopologyPrinter
                    .printTopologyJson(topo, null, TopologyPrinter.all, false);

            final ObjectNode jsonTopology = JsonUtils.createObjectNode();
            jsonTopology.set("TopologyMetadata", topologyJSONOutput);
            mdWriter.write(JsonUtils.writeAsJson(jsonTopology, true));
            mdWriter.write(System.getProperty("line.separator"));
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.out
                    .println("Exception caught retrieving topology metadata.");
            e.printStackTrace();
        }
    }

    /*
     * Retrieve security metadata and store into output file.
     */
    public static void printSecurityMetadata(Environment env,
                                             DatabaseConfig dbConfig,
                                             FileWriter mdWriter) {
        try (Database secuirtyDb =
                SecurityMetadataManager.openDb(env, dbConfig, null)) {
            SecurityMetadata secMetaData = SecurityMetadataManager
                    .getSecurityData(secuirtyDb);

            if (secMetaData != null) {
                Collection<KVStoreUser> users = secMetaData.getAllUsers();

                final ObjectNode jsonSecurity = JsonUtils.createObjectNode();
                final ArrayNode userArray = jsonSecurity.putArray("Users");

                for (final KVStoreUser user : users) {
                    ObjectNode jsonSecUser = JsonUtils.parseJsonObject(
                            user.getDescription().detailsAsJSON());
                    userArray.add(jsonSecUser);
                }

                final ArrayNode roleArray = jsonSecurity.putArray("Roles");
                Collection<RoleInstance> roles = secMetaData.getAllRoles();
                for (final RoleInstance role : roles) {
                    ObjectNode jsonSecRole = JsonUtils.parseJsonObject(
                            role.getDescription().detailsAsJSON());
                    roleArray.add(jsonSecRole);
                }

                final ObjectNode jsonSecMain = JsonUtils.createObjectNode();
                jsonSecMain.set("SecurityMetadata", jsonSecurity);

                mdWriter.write(JsonUtils.writeAsJson(jsonSecMain, true));
                mdWriter.write(System.getProperty("line.separator"));
            }
        } catch (DatabaseException | IOException | NullPointerException e) {
            System.out
                    .println("Exception caught retrieving security metadata.");
            e.printStackTrace();
        }
    }

    /*
     * Retrieve table metadata and store into output file.
     */
    public static void printTableMetadata(Environment env,
                                          DatabaseConfig dbConfig,
                                          FileWriter mdWriter) {
        try (Database dbTable = TableManager.openDb(env, dbConfig, null)) {
            final ObjectNode jsonTables = JsonUtils.createObjectNode();
            final ArrayNode tableArray = jsonTables.putArray("tables");

            List<TableImpl> tables = TableManager.getTables(dbTable);
            for (TableImpl table : tables) {
                ObjectNode tableNode = JsonUtils
                        .parseJsonObject(table.toJsonString(true));
                tableArray.add(tableNode);
            }

            final ObjectNode jsonTableMain = JsonUtils.createObjectNode();
            jsonTableMain.set("TableMetadata", jsonTables);
            mdWriter.write(JsonUtils.writeAsJson(jsonTableMain, true));
            mdWriter.write(System.getProperty("line.separator"));
        } catch (DatabaseException | IOException | NullPointerException
                | IllegalStateException e) {
            System.out.println("Exception caught retrieving Table metadata.");
            e.printStackTrace();
        }
    }
}