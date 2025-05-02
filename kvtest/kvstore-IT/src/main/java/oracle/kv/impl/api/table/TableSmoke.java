/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import java.util.Iterator;
import java.util.Map;

import oracle.kv.Direction;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;

/**
 * Users {
 *    id Integer (pk)
 *    firstName String
 *    lastName  String
 *    age       Integer
 * }
 *
 * Phone (child of Users) {
 *    number String (pk)
 *    type Enum (home, work, mobile)
 * }
 *
 * Address (child of Users) {
 *    type Enum (home, work, other) (pk)
 *    street String
 *    city   String
 *    state  (String -- could be enum of states...)
 *    zip    Integer
 * }
 *
 * Email (child of Users) {
 *    address String (pk)
 *    provider String
 * }
 *
 * Providers (top-level) {
 *   name String (pk)
 * }
 */

public class TableSmoke {

    static private void cleanup(KVStore store) {
        Iterator<Key> iter = store.storeKeysIterator
            (Direction.UNORDERED, 10);
        try {
            while (iter.hasNext()) {
                store.delete(iter.next());
            }
        } catch (Exception e) {
            System.err.println("Exception cleaning store: " + e);
        }
    }

    static private void addUser(TableAPI apiImpl, Table table,
                                int id, String first, String last,
                                int age) {
        Row row = table.createRow();
        row.put("id", id);
        row.put("firstName", first);
        row.put("lastName", last);
        row.put("age", age);
        apiImpl.put(row, null, null);
    }

    static private void addAddress(TableAPI apiImpl, Table table,
                                   int id, String type,
                                   String street, String city, String state,
                                   int zip)  {
        Row row = table.createRow();
        row.put("id", id);
        row.putEnum("type", type);
        row.put("street", street);
        row.put("city", city);
        row.put("state", state);
        if (zip != 0) {
            row.put("zip", zip);
        } else {
            row.putNull("zip");
        }
        apiImpl.put(row, null, null);
    }

    public static void main(String[] args) throws Exception {
        KVStore store = KVStoreFactory.getStore
            (new KVStoreConfig("single", "lapcat:13230"));

        cleanup(store);

        TableAPI apiImpl = store.getTableAPI();

        TableImpl userTable = TableBuilder.createTableBuilder("User",
                                                              "Table of Users",
                                                              null)
            .addInteger("id")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .primaryKey("id")
            .shardKey("id")
            .buildTable();

        try {
            TableTestBase.addTable(userTable, true, userTable.getNamespace());
        } catch (Exception e) {
            System.err.println("Exception creating User table: " + e);
        }

        userTable = (TableImpl) apiImpl.getTable("User");

        TableImpl addressTable =
            TableBuilder.createTableBuilder("Address",
                                            "Table of addresses for users",
                                            userTable)
            .addEnum("type",
                     new String[]{"home", "work", "other"}, null, null, null)
            .addString("street")
            .addString("city")
            .addString("state")
            /* make zip nullable */
            .addInteger("zip", null, true, null)
            .primaryKey("type")
            .shardKey("id")
            .buildTable();
        try {
            TableTestBase.addTable(addressTable, true, userTable.getNamespace());
        } catch (Exception e) {
            System.err.println("Exception creating Address table: " + e);
        }

        userTable = (TableImpl) apiImpl.getTable("User");
        addressTable = userTable.getChildTable("Address");

        /*
         * Look at tables
         */
        Map<String, Table> topLevelTables = apiImpl.getTables();
        for (Map.Entry<String, Table> entry : topLevelTables.entrySet()) {
            Table t = entry.getValue();
            System.out.println("Table: " + t.getName());
            Map<String, Table> childTables = t.getChildTables();
            for (Map.Entry<String, Table> child : childTables.entrySet()) {
                Table ct = child.getValue();
                System.out.println("\tChild: " + ct.getName());
            }
        }
        String userTableString = userTable.toJsonString(true);
        System.out.println("User table:\n" + userTableString);
        System.out.println("Address table:\n" +
                           addressTable.toJsonString(true));
        TableImpl tmp =
            TableBuilder.fromJsonString(userTableString, null);
        System.out.println("User table:\n" + tmp.toJsonString(true));


        addUser(apiImpl, userTable, 76, "Joe", "Jones", 38);
        addUser(apiImpl, userTable, 87, "Jane", "Doe", 44);
        addUser(apiImpl, userTable, 56, "Sam", "Spade", 32);

        addAddress(apiImpl, addressTable, 76,
                   "home",
                   "15 Happy Lane",
                   "Whoville", "MT", 12345);
        addAddress(apiImpl, addressTable, 76,
                   "work",
                   "17 Technology Place",
                   "Burlington", "MA", 23456);

        addAddress(apiImpl, addressTable, 87,
                   "home",
                   "123 Four Drive",
                   "Nowhere", "ID", 56789);

        addAddress(apiImpl, addressTable, 56,
                   "home",
                   "75 Sixth Street",
                   "West Nowhere", "ME", 0);

        /*
         * Get...
         */
        PrimaryKey pk = addressTable.createPrimaryKey();
        pk.put("id", 56);
        pk.putEnum("type", "home");
        Row row = apiImpl.get(pk, null);
        System.out.println("Got user 56:\n" + row.toJsonString(true));

        pk = userTable.createPrimaryKey();
        pk.put("id", 76);
        Iterator<Row> rows = apiImpl.tableIterator(pk, null, null);
        System.out.println("Iterate below 76");
        while (rows.hasNext()) {
            Row current = rows.next();
            System.out.println(current.toJsonString(true));
        }

        cleanup(store);

        /*
         * Create/parse a complex table.  This includes a top-level record
         * as well as a record embedded in a map, to test some oddities.
         */
        TableImpl complexTable =
            TableBuilder.createTableBuilder("Complex",
                                            "Complex table example",
                                            null)
            .addInteger("id")
            .addEnum("workday",
                     new String[]{"monday", "tuesday",
                                  "wednesday", "thursday", "friday"},
                     null, null, null)
            .addField("likes", TableBuilder.createArrayBuilder()
                      .addString(null).build())
            .addField("this_is_a_map", TableBuilder.createMapBuilder()
                      .addInteger(null).build())
            .addField("nested_record",
                      TableBuilder.createRecordBuilder("nested_record")
                      .addString("name")
                      .addInteger("age", "age", null, 12)
                      /* use Long vs Date for comparison */
                      .addLong("birthday").build())
            .addField("map_of_record", TableBuilder.createMapBuilder()
                      .addField(TableBuilder.createRecordBuilder("in_map")
                                .addInteger("foo").build()).build())
            .primaryKey("id")
            .shardKey("id")
            .buildTable();
        try {
            TableTestBase.addTable(complexTable, true, userTable.getNamespace());
        } catch (Exception e) {
            System.err.println("Exception creating Address table: " + e);
        }

        complexTable = (TableImpl) apiImpl.getTable("Complex");
        System.out.println(complexTable.toJsonString(true));
    }
}
