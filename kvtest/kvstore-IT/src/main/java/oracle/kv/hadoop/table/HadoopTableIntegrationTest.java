/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.hadoop.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVSecurityConstants;
import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.ParamConstant;
import oracle.kv.PasswordCredentials;
import oracle.kv.RequestTimeoutException;
import oracle.kv.TestBase;
import oracle.kv.hadoop.table.TableInputFormatBase.TopologyLocatorWrapper;
import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.admin.plan.Plan;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.security.PasswordStore;
import oracle.kv.impl.security.filestore.FileStoreManager;
import oracle.kv.impl.security.util.SecurityUtils;
import oracle.kv.impl.security.wallet.WalletManager;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.util.ExternalDataSourceUtils;
import oracle.kv.impl.util.PortFinder;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.client.ClientLoggerUtils;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableIteratorOptions;
import oracle.kv.util.CreateStore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test that verifies the functionality of the various interfaces and
 * classes that support integration of Hadoop and the KVStore Table API.
 */
public class HadoopTableIntegrationTest extends TestBase {

    private static final Class<?> THIS_CLASS =
        HadoopTableIntegrationTest.class;
    private static final String THIS_CLASS_NAME = THIS_CLASS.getSimpleName();
    private static final String STORE_NAME_PREFIX =
        THIS_CLASS_NAME + "-tablestore-";
    private static Logger staticLogger =
        ClientLoggerUtils.getLogger(THIS_CLASS, THIS_CLASS_NAME);

    private static final int START_PORT = 14837;
    private static final int N_VEHICLE_TABLE_ROWS = 79;
    private static final int N_TIMESTAMP_TABLE_ROWS = 10;
    private static final int N_NUMBER_TABLE_ROWS = 10;

    static CreateStore createStore;
    static PortFinder[] portFinders;
    static int createStoreCount;
    static TableAPI tableApi;
    static KVStore store;

    /* Tables for the different unit tests implemented in this class. */
    static TableImpl vehicleTable;
    static TableImpl upperFieldsTable;
    static TableImpl nullFieldsTable;
    static TableImpl timestampTable;
    static TableImpl numberTable;
    static TableImpl jsonTable;

    /* Constants for generating vehicleTable rows with random content. */
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String[] TYPES = {"auto", "truck", "suv"};
    private static final String[] MAKES = {"Ford", "GM", "Chrysler"};

    private static final String[] MODELS_FORD_AUTO =
        {"Focus", "Taurus", "Fiesta", "Edge"};
    private static final String[] MODELS_FORD_TRUCK = {"F150", "F250", "F350"};
    private static final String[] MODELS_FORD_SUV =
        {"Escape", "Expedition", "Explorer"};

    private static final String[] MODELS_GM_AUTO =
        {"Camaro", "Corvette", "Impala", "Malibu"};
    private static final String[] MODELS_GM_TRUCK =
        {"Sierra", "Silverado1500", "Silverado2500"};
    private static final String[] MODELS_GM_SUV =
        {"Tahoe", "Equinox", "Blazer"};

    private static final String[] MODELS_CHRYSLER_AUTO =
        {"Sebring", "Imperial", "Lebaron", "PTCruiser"};
    private static final String[] MODELS_CHRYSLER_TRUCK =
        {"Ram1500", "Ram2500", "Ram3500"};
    private static final String[] MODELS_CHRYSLER_SUV =
        {"Aspen", "Pacifica", "Journey"};

    private static final String[] CLASSES =
        {"4WheelDrive", "AllWheelDrive", "FrontWheelDrive", "RearWheelDrive"};

    private static final String[] COLORS =
        {"red", "blue", "green", "yellow", "white", "black"};

    private static final String[] DUP_CLASS_SUFFIXES =
        {"4cylinder", "6cylinder", "hybrid", "diesel"};

    private static final Map<PrimaryKey, Row> EXPECTED_VEHICLE_TABLE =
        new HashMap<PrimaryKey, Row>();

    private static final Map<PrimaryKey, Row> EXPECTED_TIMESTAMP_TABLE =
        new HashMap<PrimaryKey, Row>();

    private static final Map<PrimaryKey, Row> EXPECTED_NUMBER_TABLE =
        new HashMap<PrimaryKey, Row>();

    /* Constants for creating the upperFieldsTable. */
    private static final String[] ALL_UPPER_FIELD_NAMES = {"AAAA", "BBBB"};

    /* Constants for creating the nullFieldsTable. */
    private static final String[] NULL_FIELD_NAMES =
        {"field_one", "field_two", "field_three"};

    /* Constants for creating the jsonTable. */

    private static final String DQ = "\""; /* Macro for double quote. */

    /*
     * Each of the 3 elements of the JSON_SENATOR_INFO array initialized here
     * is a json document that is inserted as a row in the test table used
     * to verify that json data types can be processed.
     */
    public static final String[] JSON_SENATOR_INFO =
    {
"{" +
    DQ + "description" + DQ + ": " +
      DQ + "Junior Senator for Wisconsin" + DQ + ", " +
    DQ + "party" + DQ + ": " + DQ + "Independent" + DQ + ", " +
    DQ + "congress_numbers" + DQ + ": [ " +
        "113, " +
        "114, " +
        "115" +
    " ], " +
    DQ + "state" + DQ + ": " + DQ + "WI" + DQ + ", " +
    DQ + "district" + DQ + ": " + DQ + "Wisconsin 3-rd" + DQ + ", " +
    DQ + "startdate" + DQ + ": " + DQ + "2013-01-03T03:02:01.123" + DQ + ", " +
    DQ + "enddate" + DQ + ": " + DQ + "2021-01-03T01:02:03.123456789" + DQ +
        ", " +
    DQ + "seniority" + DQ + ": " + "29" + ", " +
    DQ + "current" + DQ + ": true, " +
    DQ + "duties" + DQ + ": { " +
        DQ + "comittee" + DQ + ": [ " +
            DQ + "Intelligence" + DQ + ", " +
            DQ + "Judiciary" + DQ + ", " +
            DQ + "Appropriations" + DQ +
        " ], " +
        DQ + "caucus" + DQ + ": [ " +
            DQ + "Congressional Progressive" + DQ + ", " +
            DQ + "Afterschool" + DQ +
        " ]" +
    " }, " +
    DQ + "personal" + DQ + ": { " +
        DQ + "firstname" + DQ + ": " + DQ + "Tammy" + DQ + ", " +
        DQ + "lastname" + DQ + ": " + DQ + "Baldwin" + DQ + ", " +
        DQ + "birthday" + DQ + ": " + DQ + "1962-02-11" + DQ + ", " +
        DQ + "social_media" + DQ + ": { " +
            DQ + "website" + DQ + ": " +
            DQ + "https://www.baldwin.senate.gov" + DQ + ", " +
            DQ + "rss_url" + DQ + ": " +
              DQ + "http://www.baldwin.senate.gov/rss/feeds/?type=all" + DQ +
              ", " +
            DQ + "twitterid" + DQ + ": " + DQ + "SenatorBaldwin" + DQ +
        " }, " +
        DQ + "address" + DQ + ": { " +
            DQ + "home" + DQ + ": { " +
                DQ + "number" + DQ + ": " + DQ + "23315" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Wallbury Court" + DQ + ", " +
                DQ + "apt" + DQ + ": " + DQ + "17" + DQ + ", " +
                DQ + "city" + DQ + ": " + DQ + "Madison" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "WI" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "53779" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "608-742-8331" + DQ +
            " }, " +
            DQ + "work" + DQ + ": { " +
                DQ + "number" + DQ + ": " +
                  DQ + "Hart Senate Office Building" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Second Street" + DQ + ", " +
                DQ + "apt" + DQ + ": " + DQ + "709" + DQ + ", " +
                DQ + "city" + DQ + ": " + DQ + "Washington" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "DC" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "20510" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "202-224-5653" + DQ +
            " } " +
        " }, " +
        DQ + "cspanid" + DQ + ": 57884" +
    " }, " +
    DQ + "contrib" + DQ + ": 11991435.37" +
" }",

"{" +
    DQ + "description" + DQ + ": " +
      DQ + "Senior Senator for Ohio" + DQ + ", " +
    DQ + "party" + DQ + ": " + DQ + "Democrat" + DQ + ", " +
    DQ + "congress_numbers" + DQ + ": [ " +
        "213, " +
        "214, " +
        "215" +
    " ], " +
    DQ + "state" + DQ + ": " + DQ + "OH" + DQ + ", " +
    DQ + "district" + DQ + ": " + DQ + "Ohio 7-th" + DQ + ", " +
    DQ + "startdate" + DQ + ": " + DQ + "2010-01-03T05:04:09.456" + DQ + ", " +
    DQ + "enddate" + DQ + ": " + DQ + "2020-01-05T03:01:02.567812349" + DQ +
        ", " +
    DQ + "seniority" + DQ + ": " + "37" + ", " +
    DQ + "current" + DQ + ": true, " +
    DQ + "duties" + DQ + ": { " +
        DQ + "comittee" + DQ + ": [ " +
            DQ + "Ways and Means" + DQ + ", " +
            DQ + "Judiciary" + DQ + ", " +
            DQ + "Democratic Steering" + DQ +
        " ], " +
        DQ + "caucus" + DQ + ": [ " +
            DQ + "Congressional Automotive" + DQ + ", " +
            DQ + "Human Rights" + DQ + ", " +
            DQ + "Steel Industry" + DQ +
        " ]" +
    " }, " +
    DQ + "personal" + DQ + ": { " +
        DQ + "firstname" + DQ + ": " + DQ + "Sherrod" + DQ + ", " +
        DQ + "lastname" + DQ + ": " + DQ + "Brown" + DQ + ", " +
        DQ + "birthday" + DQ + ": " + DQ + "1952-11-09" + DQ + ", " +
        DQ + "social_media" + DQ + ": { " +
            DQ + "website" + DQ + ": " +
            DQ + "https://www.brown.senate.gov" + DQ + ", " +
            DQ + "rss_url" + DQ + ": " +
             DQ + "http://www.brown.senate.gov/rss/feeds/?type=all&amp;" + DQ +
             ", " +
            DQ + "twitterid" + DQ + ": " + DQ + "SenSherrodBrown" + DQ +
        " }, " +
        DQ + "address" + DQ + ": { " +
            DQ + "home" + DQ + ": { " +
                DQ + "number" + DQ + ": " + DQ + "9155-ext" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Vaughan" + DQ + ", " +
                DQ + "apt" + DQ + ": " + "null" + ", " +
                DQ + "city" + DQ + ": " + DQ + "Columbus" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "OH" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "43221" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "901-234-3774" + DQ +
            " }, " +
            DQ + "work" + DQ + ": { " +
                DQ + "number" + DQ + ": " +
                  DQ + "Hart Senate Office Building" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Second Street" + DQ + ", " +
                DQ + "apt" + DQ + ": " + DQ + "355" + DQ + ", " +
                DQ + "city" + DQ + ": " + DQ + "Washington" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "DC" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "20001" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "202-224-2315" + DQ +
            " } " +
        " }, " +
        DQ + "cspanid" + DQ + ": 5051" +
    " }, " +
    DQ + "contrib" + DQ + ": 2571354.93" +
" }",

"{ " +
    DQ + "description" + DQ + ": " +
      DQ + "Senior Senator for Maine" + DQ + ", " +
    DQ + "party" + DQ + ": " + DQ + "Independent" + DQ + ", " +
    DQ + "congress_numbers" + DQ + ": [ " +
        "313, " +
        "314, " +
        "315" +
    " ], " +
    DQ + "state" + DQ + ": " + DQ + "MD" + DQ + ", " +
    DQ + "district" + DQ + ": " + DQ + "Maine 19-th" + DQ + ", " +
    DQ + "startdate" + DQ + ": " + DQ + "2013-01-03T12:00:00.789" + DQ + ", " +
    DQ + "enddate" + DQ + ": " + DQ + "2022-01-03T11:59:59.918273645" + DQ +
        ", " +
    DQ + "seniority" + DQ + ": " + "51" + ", " +
    DQ + "current" + DQ + ": true, " +
    DQ + "duties" + DQ + ": { " +
        DQ + "comittee" + DQ + ": [ " +
            DQ + "Oversight" + DQ + ", " +
            DQ + "Judiciary" + DQ +
        " ], " +
        DQ + "caucus" + DQ + ": [ " +
            DQ + "Environmental" + DQ + ", " +
            DQ + "Human Rights" + DQ + ", " +
            DQ + "Main Tourism" + DQ +
        " ]" +
    " }, " +
    DQ + "personal" + DQ + ": { " +
        DQ + "firstname" + DQ + ": " + DQ + "Angus" + DQ + ", " +
        DQ + "lastname" + DQ + ": " + DQ + "King" + DQ + ", " +
        DQ + "birthday" + DQ + ": " + DQ + "1944-03-31" + DQ + ", " +
        DQ + "social_media" + DQ + ": { " +
            DQ + "website" + DQ + ": " +
            DQ + "https://www.king.senate.gov" + DQ + ", " +
            DQ + "rss_url" + DQ + ": " +
              DQ + "http://www.king.senate.gov/rss/feeds/?type=all&amp;" + DQ +
              ", " +
            DQ + "twitterid" + DQ + ": " + DQ + "SenAngusKing" + DQ +
        " }, " +
        DQ + "address" + DQ + ": { " +
            DQ + "home" + DQ + ": { " +
                DQ + "number" + DQ + ": " + DQ + "26B" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Swan Lane" + DQ + ", " +
                DQ + "apt" + DQ + ": " + DQ + "23" + DQ + ", " +
                DQ + "city" + DQ + ": " + DQ + "Portland" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "ME" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "21332" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "310-432-4775" + DQ +
            " }, " +
            DQ + "work" + DQ + ": { " +
                DQ + "number" + DQ + ": " +
                  DQ + "Hart Senate Office Building" + DQ + ", " +
                DQ + "street" + DQ + ": " + DQ + "Second Street" + DQ + ", " +
                DQ + "apt" + DQ + ": " + DQ + "133" + DQ + ", " +
                DQ + "city" + DQ + ": " + DQ + "Washington" + DQ + ", " +
                DQ + "state" + DQ + ": " + DQ + "DC" + DQ + ", " +
                DQ + "zipcode" + DQ + ": " + DQ + "20001" + DQ + ", " +
                DQ + "phone" + DQ + ": " + DQ + "202-224-5344" + DQ +
            " } " +
        " }, " +
        DQ + "cspanid" + DQ + ": 37413" +
    " }, " +
    DQ + "contrib" + DQ + ": 2571354.93" +
" }"
    };

    private static final Map<PrimaryKey, Row> EXPECTED_JSON_TABLE =
        new HashMap<PrimaryKey, Row>();

    private static final int N_PARTITION_SETS = 6;
    private static final int N_SHARDS = 3;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        staticLogger.info("staticSetUp: start store");
        TestUtils.clearTestDirectory();
        TestStatus.setManyRNs(true);
        startStore();

        staticLogger.finest("staticSetUp: get the TableAPI to the store");
        store = KVStoreFactory.getStore(createKVConfig(createStore));
        tableApi = store.getTableAPI();

        /*
         * A. Create & populate vehicleTable once; shared by multiple tests.
         *
         * 1. Use TableBuilder to create the table with the desired fields
         * 2. Use the CLI Admin to add the table just created to the store
         * 3. From TableAPI, get a ref to the table just created from the store
         * 4. Populate the fields of the table just created with test data
         */

        /* 1. Create the vehicleTable with the desired fields and types. */
        TableBuilder tblBldr = TableBuilder.createTableBuilder("vehicleTable");

        /* Add the table fields */
        tblBldr.addString("type");
        tblBldr.addString("make");
        tblBldr.addString("model");
        tblBldr.addString("class");
        tblBldr.addString("color");
        tblBldr.addDouble("price");
        tblBldr.addInteger("count");
        tblBldr.addNumber("serialnumber");
        tblBldr.addTimestamp("creationdate", 6);

        /* Assign multiple fields as primary key and shard key */
        tblBldr.primaryKey("type", "make", "model", "class", "color");
        tblBldr.shardKey("type", "make", "model");

        vehicleTable = tblBldr.buildTable();
        String tblName = vehicleTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" + vehicleTable.getPrimaryKey() +
                            ", shardKey = " + vehicleTable.getShardKey() +
                            "]");

        /* 2. Add the just created table to the store. */
        final CommandServiceAPI cliAdmin = createStore.getAdmin();

        int planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (vehicleTable.getParent() != null ?
              vehicleTable.getParent().getFullName() : null),
             vehicleTable.getFieldMap(),
             vehicleTable.getPrimaryKey(),
             vehicleTable.getPrimaryKeySizes(),
             vehicleTable.getShardKey(),
             null, // ttl
             null, // limits
             vehicleTable.isR2compatible(),
             vehicleTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        Plan.State state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "]" +
                          "successfully added to store");

        /* 3. Retrieve a reference to the table just created. */
        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        vehicleTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");
        populateVehicleTable(N_VEHICLE_TABLE_ROWS);
        displayRows(vehicleTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");

        /* B. Create 2nd table for testing field names & types (SR23978). */
        tblBldr = TableBuilder.createTableBuilder("UPPERFIELDSTABLE");

        /* Add the table fields */
        tblBldr.addInteger(ALL_UPPER_FIELD_NAMES[0]);
        tblBldr.addString(ALL_UPPER_FIELD_NAMES[1]);
        tblBldr.primaryKey(ALL_UPPER_FIELD_NAMES[0]);
        upperFieldsTable = tblBldr.buildTable();
        tblName = upperFieldsTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" +
                            upperFieldsTable.getPrimaryKey() + "]");

        planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (upperFieldsTable.getParent() != null ?
              upperFieldsTable.getParent().getFullName() : null),
             upperFieldsTable.getFieldMap(),
             upperFieldsTable.getPrimaryKey(),
             upperFieldsTable.getPrimaryKeySizes(),
             upperFieldsTable.getShardKey(),
             null, // ttl
             null, // limits
             upperFieldsTable.isR2compatible(),
             upperFieldsTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "] " +
                          "successfully added to store");

        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        upperFieldsTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");

        Row row = upperFieldsTable.createRow();
        row.put(ALL_UPPER_FIELD_NAMES[0], 1);
        row.put(ALL_UPPER_FIELD_NAMES[1], "ALL_UPPER_FIELDS_IN_TABLE");
        tableApi.putIfAbsent(row, null, null);

        displayRows(upperFieldsTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");


        /* C. Create 3rd table for testing null field values (SR24021). */
        tblBldr = TableBuilder.createTableBuilder("NULLFIELDSTABLE");

        /* Add the table fields */
        tblBldr.addString(NULL_FIELD_NAMES[0]);
        tblBldr.addString(NULL_FIELD_NAMES[1]);
        tblBldr.addString(NULL_FIELD_NAMES[2]);
        tblBldr.primaryKey(NULL_FIELD_NAMES[0]);
        nullFieldsTable = tblBldr.buildTable();
        tblName = nullFieldsTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" + nullFieldsTable.getPrimaryKey() +
                            "]");

        planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (nullFieldsTable.getParent() != null ?
              nullFieldsTable.getParent().getFullName() : null),
             nullFieldsTable.getFieldMap(),
             nullFieldsTable.getPrimaryKey(),
             nullFieldsTable.getPrimaryKeySizes(),
             nullFieldsTable.getShardKey(),
             null, // ttl
             null, // limits
             nullFieldsTable.isR2compatible(),
             nullFieldsTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "] " +
                          "successfully added to store");

        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        nullFieldsTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");

        /* Row 00: all rows with non-null field values. */
        row = nullFieldsTable.createRow();
        row.put(NULL_FIELD_NAMES[0], "ROW00_field_one_value_NOT_NULL");
        row.put(NULL_FIELD_NAMES[1], "ROW00_field_two_value_NOT_NULL");
        row.put(NULL_FIELD_NAMES[2], "ROW00_field_three_value_NOT_NULL");
        tableApi.putIfAbsent(row, null, null);

        /* Row 01: only field_one and field_three are non-null. */
        row = nullFieldsTable.createRow();
        row.put(NULL_FIELD_NAMES[0], "ROW01_field_one_value_NOT_NULL");
        row.put(NULL_FIELD_NAMES[2], "ROW01_field_three_value_NOT_NULL");
        tableApi.putIfAbsent(row, null, null);

        /* Row 02: all rows with non-null field values. */
        row = nullFieldsTable.createRow();
        row.put(NULL_FIELD_NAMES[0], "ROW02_field_one_value_NOT_NULL");
        row.put(NULL_FIELD_NAMES[1], "ROW02_field_two_value_NOT_NULL");
        row.put(NULL_FIELD_NAMES[2], "ROW02_field_three_value_NOT_NULL");
        tableApi.putIfAbsent(row, null, null);

        displayRows(nullFieldsTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");

        /* D. Create table for testing fields with Timestamp type (SR25802). */

        /* 1. Create the timestampTable with the desired fields and types. */
        tblBldr = TableBuilder.createTableBuilder("timestampTable");

        /* Add the table fields */
        tblBldr.addInteger("id");
        tblBldr.addTimestamp("timestampField", 9);

        /* Assign the id field as primary key. */
        tblBldr.primaryKey("id");

        timestampTable = tblBldr.buildTable();
        tblName = timestampTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" + timestampTable.getPrimaryKey() +
                            ", shardKey = " + timestampTable.getShardKey() +
                            "]");

        /* 2. Add the just created table to the store. */
        planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (timestampTable.getParent() != null ?
              timestampTable.getParent().getFullName() : null),
             timestampTable.getFieldMap(),
             timestampTable.getPrimaryKey(),
             timestampTable.getPrimaryKeySizes(),
             timestampTable.getShardKey(),
             null, // ttl
             null, // limits
             timestampTable.isR2compatible(),
             timestampTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "]" +
                          " successfully added to store");

        /* 3. Retrieve a reference to the table just created. */
        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        timestampTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");
        populateTimestampTable(N_TIMESTAMP_TABLE_ROWS);
        displayRows(timestampTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");

        /* E. Create table for testing fields with Number type (SR25802). */

        /* 1. Create the numberTable with the desired fields and types. */
        tblBldr = TableBuilder.createTableBuilder("numberTable");

        /* Add the table fields */
        tblBldr.addInteger("id");
        tblBldr.addNumber("intNumberField");
        tblBldr.addNumber("longNumberField");
        tblBldr.addNumber("floatNumberField");
        tblBldr.addNumber("doubleNumberField");
        tblBldr.addNumber("bigdecimalNumberField");

        /* Assign the id field as primary key. */
        tblBldr.primaryKey("id");

        numberTable = tblBldr.buildTable();
        tblName = numberTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" + numberTable.getPrimaryKey() +
                            ", shardKey = " + numberTable.getShardKey() + "]");

        /* 2. Add the just created table to the store. */
        planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (numberTable.getParent() != null ?
              numberTable.getParent().getFullName() : null),
             numberTable.getFieldMap(),
             numberTable.getPrimaryKey(),
             numberTable.getPrimaryKeySizes(),
             numberTable.getShardKey(),
             null, // ttl
             null, // limits
             numberTable.isR2compatible(),
             numberTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "] " +
                          "successfully added to store");

        /* 3. Retrieve a reference to the table just created. */
        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        numberTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");
        populateNumberTable(N_NUMBER_TABLE_ROWS);
        displayRows(numberTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");

        /* F. Create table for testing fields with JSON type (SR25802). */

        /* 1. Create the jsonTable with the desired fields and types. */
        tblBldr = TableBuilder.createTableBuilder("jsonTable");

        /* Add the table fields; just an id field and a field of type json. */
        tblBldr.addInteger("id");
        tblBldr.addJson("jsonField", "senator info in json format");

        /* Assign the id field as primary key. */
        tblBldr.primaryKey("id");

        jsonTable = tblBldr.buildTable();
        tblName = jsonTable.getName();

        staticLogger.finest("staticSetUp: Built table [name=" + tblName +
                            ", primaryKey=" + jsonTable.getPrimaryKey() +
                            ", shardKey = " + jsonTable.getShardKey() + "]");

        /* 2. Add the just created table to the store. */
        planId = cliAdmin.createAddTablePlan
            ("AddTable", null, tblName,
             (jsonTable.getParent() != null ?
              jsonTable.getParent().getFullName() : null),
             jsonTable.getFieldMap(),
             jsonTable.getPrimaryKey(),
             jsonTable.getPrimaryKeySizes(),
             jsonTable.getShardKey(),
             null, // ttl
             null, // limits
             jsonTable.isR2compatible(),
             jsonTable.getSchemaId(),
             false, null, // JSON collection fields
             null); // description

        cliAdmin.approvePlan(planId);
        cliAdmin.executePlan(planId, false);

        /* Verify the plan succeeded. */
        staticLogger.fine(
            "staticSetUp: waiting for AddTablePlan to complete ...");
        state = cliAdmin.awaitPlan(planId, 0, null);
        assertTrue(state == Plan.State.SUCCEEDED);

        staticLogger.info("staticSetUp: table [" + tblName + "] " +
                          "successfully added to store");

        /* 3. Retrieve a reference to the table just created. */
        staticLogger.fine("staticSetUp: retrieve the table just added to " +
                          "the store [" + tblName + "]");
        jsonTable = (TableImpl) tableApi.getTable(tblName);

        /* Populate the table just created with test data. */
        staticLogger.fine("staticSetUp: populating table with test data ...");
        populateJsonTable(JSON_SENATOR_INFO.length);

        displayRows(jsonTable);
        staticLogger.info("staticSetUp: " + tblName + " populated with data");
    }

    @AfterClass
    public static void staticTearDown() throws Exception {

        /*
         * The tests are now run by default using the async protocol
         * rather than synchronous RMI for remote communication. When
         * the async protocol is used, and the tests are configured
         * to log output, then the call to CreateStore.shutdown will log
         * multiple stack traces referencing a TemporaryDialogException
         * from the RefreshRepNodeStateThread via the async communication
         * mechanism. Although the test passes, the appearance of all
         * those stack traces can be alarming and/or confusing. As a
         * result, this method checks whether or not async communication
         * is being used and, if yes, it disables logging in the root
         * logger right before calling CreateStore.shutdown, and
         * re-enables logging before exiting.
         *
         * Note that if you wish to have this test use RMI, then the
         * the following properties should be set when executing this
         * test:
         *    -Doracle.kv.async.server=false
         *    -Doracle.kv.async=false
         *    -Doracle.kv.jvm.extraargs=
         *       '-Doracle.kv.async.server=false;-Doracle.kv.async=false'
         */
        final boolean asyncClient = Boolean.parseBoolean(
            System.getProperty("oracle.kv.async", "true"));
        final boolean asyncServer = Boolean.parseBoolean(
            System.getProperty("oracle.kv.async.server", "true"));

        if (!asyncClient && !asyncServer) {

            if (store != null) {
                store.close();
                staticLogger.info("staticTearDown: store closed");
            }

            if (createStore != null) {
                createStore.shutdown();
                staticLogger.info("staticTearDown: createStore shutdown");
            }

        } else {

            final Logger rootLogger = Logger.getLogger("");
            try {
                staticLogger.finest("staticTearDown: Disable root logger");
                rootLogger.setLevel(java.util.logging.Level.OFF);

                if (store != null) {
                    store.close();
                    staticLogger.info("staticTearDown: store closed");
                }

                if (createStore != null) {
                    createStore.shutdown();
                }

            } finally {
                rootLogger.setLevel(java.util.logging.Level.ALL);
                staticLogger.finest("staticTearDown: root logger re-enabled");
                staticLogger.info("staticTearDown: createStore shutdown");
            }
        }
    }

    /**
     * The TestBase parent class for this class defines a protected method
     * named clearTestDirectory that calls TestUtils.clearTestDirectory to
     * remove all of the directory/file artifacts created when the store
     * used by this test class is created in the staticSetUp method. That
     * protected method is called in the setUp method of the parent class
     * before the execution of each of the test case methods of this class.
     * This means that after the store is created once (and only once) in
     * the staticSetUp method of this class, the log files written by that
     * store are removed before the execution of each of the test cases;
     * which will now (as of JE 7.2.2 that includes a new external deletion
     * mechanism for .jdb files) result in failure caused by an exception
     * characterized by a LOG_UNEXPECTED_FILE_DELETION.
     *
     * To avoid the sort of failure described above, the clearTestDirectory
     * method from the parent class is overridden here so that the store's
     * log files are only removed once, in the staticSetUp method before the
     * store is started; not removed prior to the execution of each test
     * case.
     */
    @Override
    protected void clearTestDirectory() {
        /* NO-OP */
    }

    @Override
    protected Logger getLogger() {
        return staticLogger;
    }

    private static void startStore()
        throws Exception {

        /*
         * Make a local 3x3 store.  This exercises the metadata
         * distribution code better than a 1x1 store.
         *
         * Cannot have subclasses override parameters because this is done in
         * static context.  Need another mechanism for that if subclasses need
         * alternative configuration.
         */
        createStoreCount++;
        createStore = new CreateStore(STORE_NAME_PREFIX + createStoreCount,
                                      START_PORT,
                                      3, /* n SNs */
                                      3, /* rf */
                                      10, /* n partitions */
                                      2, /* capacity per SN */
                                      2 * CreateStore.MB_PER_SN,
                                      false, /* use threads is false */
                                      null);
        setPolicies(createStore);
        createStore.start();
        staticLogger.info("store started [" + STORE_NAME_PREFIX +
                          createStoreCount + ":" + START_PORT + "]");
        portFinders = createStore.getPortFinders();

    }

    static KVStoreConfig createKVConfig(CreateStore cs) {
        final KVStoreConfig config = cs.createKVConfig();
        config.setDurability
            (new Durability(Durability.SyncPolicy.SYNC,
                            Durability.SyncPolicy.SYNC,
                            Durability.ReplicaAckPolicy.SIMPLE_MAJORITY));
        return config;
    }

    static void setPolicies(CreateStore cstore) {
        final ParameterMap map = new ParameterMap();
        map.setParameter(ParameterState.AP_CHECK_ADD_INDEX, "1 s");
        cstore.setPolicyMap(map);
    }

    static int countStoreRecords() {
        final Iterator<Key> iter =
            store.storeKeysIterator(Direction.UNORDERED, 500);
        int count = 0;
        while (iter.hasNext()) {
            iter.next();
            ++count;
        }
        return count;
    }

    static void removeAllData() {
        final Iterator<Key> iter =
                  store.storeKeysIterator(Direction.UNORDERED, 10);
        try {
            while (iter.hasNext()) {
                store.delete(iter.next());
            }
        } catch (Exception e) {
            System.err.println("Exception cleaning store: " + e);
        }
    }

    /*
     * Note on the test cases in this test class:
     *
     * The TableInputFormat class encapsulates configuration information that
     * is needed when running a MapReduce job initiated from the command line.
     * The configuration of the TableInputFormat class is initialized using
     * either the static setter methods on TableInputFormat (typically invoked by
     * the MapReduce job's driver program), or through corresponding properties
     * set on an instance of <code>org.apache.hadoop.conf.Configuration</code>
     * (set through Java system properties). Therefore, to support both
     * configuration models, some of the test cases in this test class use
     * the static setter methods to initialize the TableInputFormat class
     * employed by a given test case, and some initialize the class using a
     * Hadoop Configuration.
     * <p>
     * Note also that for each item specified in the configuration of the
     * TableInputFormat employed by a given test case, there are corresponding
     * items referenced in each <code>TableInputSplit</code> returned by
     * that instance of TableInputFormat. Where the values of the items
     * reflected in each such split should be equal to the values of the
     * corresponding items in the associated TableInputFormat; except, of
     * course, for each split's partition information, which is not configured
     * via properties or static setter methods, but is determined based on
     * the topology of the store used by this test.
     */

    /**
     * Verifies that for a TableInputFormat class that is configured to use
     * the store employed by this test class, the InputSplit instances
     * returned by that TableInputFormat via the getSplits method will each
     * be configured with the expected values.
     */
    @Test
    public void testTableInputSplitConfiguration() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, JobContext, and TableInputFormat with
         * the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(vehicleTable.getName());
        final JobContext jobContext = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);

        getSplitsVerifyConfiguration(jobContext, tblInputFormat);
    }

    /**
     * Verifies that the serialization/deserialization mechanism provided
     * by the TableInputSplit class and employed by Hadoop MapReduce, operates
     * as expected. That is, when the values produced by the InputSplit
     * serialization mechanism are used by the InputSplit deserialization
     * mechanism to reconstruct a new InputSplit, this test case verifies
     * that the new InputSplit and the original input Split are equivalent.
     */
    @Test
    public void testTableInputSplitSerializationDeserialization()
        throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Only need a staticly configured TableInputFormat; can use null for
         * JobContext in getSplits method.
         */
        final TableInputFormat tblInputFormat =
            getConfiguredTableInputFormat(
                getBasicHadoopConfiguration(vehicleTable.getName()));

        verifyFieldSerialization(tblInputFormat.getSplits(null));
    }

    /**
     * Verifies that for each split returned by the TableInputFormat class,
     * the associated TableRecordReader returns a subset of the records
     * currently in the table with which the store was initialized; and
     * the union of all the records returned from each split's corresponding
     * RecordReader equals all of the records in that table.
     */
    @Test
    public void testTableRecordReader() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, TaskAttemptContext, and TableInputFormat,
         * all with the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(vehicleTable.getName());
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);
        final List<InputSplit> splits = tblInputFormat.getSplits(ctx);

        /*
         * If this test is run using a secure store, then copy the security
         * file and trust file to the classpath location.
         *
         * The Hadoop KV split implementation reads the security login
         * file and trust file from the classpath. Users need to create an
         * additional jar that contains the security file and trust file
         * and upload it through the "-libjars" option for the hadoop
         * command. Then the jar with security file and trust file will
         * be distributed to each split. So each split will read the
         * files from the classpath. This behavior is documented. This unit
         * test should share the same pattern as the source implementation
         * to access the security information. As a result, the security
         * file and trust file are copied to the classpath location here.
         */
        if (SECURITY_ENABLE) {

            final ClassLoader cl =
                TableRecordReaderBase.class.getClassLoader();
            final String basePath = cl.getResource(".").getFile();
            final TableInputSplit sp = (TableInputSplit) splits.get(0);
            File srcFile = createStore.getDefaultUserLoginFile();
            File destFile = new File(basePath, sp.getSecurityLogin());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            srcFile = createStore.getTrustStore();
            destFile = new File(basePath, sp.getSecurityTrust());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
        }

        /*
         * Loop through each split, retrieve the spit's RecordReader, use
         * that RecordReader to retrieve all of the records (if any) in the
         * the test table that are associated with that split, and verify
         * that those records are all contained in the expected set of
         * records.
         *
         * Then verify that the union of all records retrieved via the
         * various RecordReaders equals the set of expected records by
         * verifying that the total number of records retrieved via
         * RecordReaders equals the number of records in the test table.
         */
        int i = 0;
        int nRows = 0;
        for (InputSplit inputSplit : splits) {

            final TableInputSplit split = (TableInputSplit) inputSplit;
            logger.finest("split[" + i + "] = " + split);
            final RecordReader<PrimaryKey, Row> recRdr =
                tblInputFormat.createRecordReader(split, ctx);

            int j = 0;
            while (recRdr.nextKeyValue()) {
                final PrimaryKey key = recRdr.getCurrentKey();
                final Row row = recRdr.getCurrentValue();
                logger.finest("row[" + j + "]: key = " + key +
                              ", row = " + row);
                assertTrue(EXPECTED_VEHICLE_TABLE.containsKey(
                                                  recRdr.getCurrentKey()));
                assertTrue(EXPECTED_VEHICLE_TABLE.containsValue(
                                                  recRdr.getCurrentValue()));
                j++;
                nRows++;
            }
            i++;
        }

        /*
         * Reaching this point means that the set of all rows returned by
         * each split's RecordReader is a subset of the set of expected rows.
         * To verify that the set of expected rows is no greater than
         * the set of rows returned by the RecorderReader, and thus the
         * sets are equal, verify that the sets are the same size.
         */
        assertEquals(nRows, EXPECTED_VEHICLE_TABLE.size());
    }

    /**
     * Verifies that for each split returned by the TableInputFormat class,
     * the associated TableRecordReader returns the expected rows with
     * fields of Timestamp type from the table. See SR25802.
     */
    @Test
    public void testTimestampTableRecordReader() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, TaskAttemptContext, and TableInputFormat,
         * all with the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(timestampTable.getName());
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);
        final List<InputSplit> splits = tblInputFormat.getSplits(ctx);

        /*
         * If this test is run using a secure store, then copy the security
         * file and trust file to the classpath location. For more info,
         * see the comment in testTableRecordReader.
         */
        if (SECURITY_ENABLE) {

            final ClassLoader cl =
                TableRecordReaderBase.class.getClassLoader();
            final String basePath = cl.getResource(".").getFile();
            final TableInputSplit sp = (TableInputSplit) splits.get(0);
            File srcFile = createStore.getDefaultUserLoginFile();
            File destFile = new File(basePath, sp.getSecurityLogin());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            srcFile = createStore.getTrustStore();
            destFile = new File(basePath, sp.getSecurityTrust());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
        }

        /*
         * Loop through each split, retrieve the spit's RecordReader,
         * use that RecordReader to retrieve all of the records in the
         * timestampTable that are associated with that split, and
         * verify that each record is contained in the expected set
         * of records.
         */
        int i = 0;
        int nRows = 0;
        for (InputSplit inputSplit : splits) {
            final TableInputSplit split = (TableInputSplit) inputSplit;
            logger.finest("split[" + i + "] = " + split);
            final RecordReader<PrimaryKey, Row> recRdr =
                tblInputFormat.createRecordReader(split, ctx);

            int j = 0;
            while (recRdr.nextKeyValue()) {
                final PrimaryKey key = recRdr.getCurrentKey();
                final Row row = recRdr.getCurrentValue();
                logger.finest("row[" + j + "]: key = " + key +
                              ", row = " + row);
                assertTrue(EXPECTED_TIMESTAMP_TABLE.containsKey(
                                                  recRdr.getCurrentKey()));
                assertTrue(EXPECTED_TIMESTAMP_TABLE.containsValue(
                                                  recRdr.getCurrentValue()));
                j++;
                nRows++;
            }
            i++;
        }

        /*
         * Reaching this point means that the set of all rows returned by
         * each split's RecordReader is a subset of the set of expected rows.
         * To verify that the set of expected rows is no greater than
         * the set of rows returned by the RecorderReader, and thus the
         * sets are equal, verify that the sets are the same size.
         */
        assertEquals(nRows, EXPECTED_TIMESTAMP_TABLE.size());
    }

    /**
     * Verifies that for each split returned by the TableInputFormat class,
     * the associated TableRecordReader returns the expected rows with
     * fields of Number type from the table. See SR25802.
     */
    @Test
    public void testNumberTableRecordReader() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, TaskAttemptContext, and TableInputFormat,
         * all with the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(numberTable.getName());
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);
        final List<InputSplit> splits = tblInputFormat.getSplits(ctx);

        /*
         * If this test is run using a secure store, then copy the security
         * file and trust file to the classpath location. For more info,
         * see the comment in testTableRecordReader.
         */
        if (SECURITY_ENABLE) {

            final ClassLoader cl =
                TableRecordReaderBase.class.getClassLoader();
            final String basePath = cl.getResource(".").getFile();
            final TableInputSplit sp = (TableInputSplit) splits.get(0);
            File srcFile = createStore.getDefaultUserLoginFile();
            File destFile = new File(basePath, sp.getSecurityLogin());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            srcFile = createStore.getTrustStore();
            destFile = new File(basePath, sp.getSecurityTrust());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
        }

        /*
         * Loop through each split, retrieve the spit's RecordReader, use
         * that RecordReader to retrieve all of the records in the numberTable
         * that are associated with that split, and verify that each record
         * is contained in the expected set of records.
         */
        int i = 0;
        int nRows = 0;
        for (InputSplit inputSplit : splits) {
            final TableInputSplit split = (TableInputSplit) inputSplit;
            logger.finest("split[" + i + "] = " + split);
            final RecordReader<PrimaryKey, Row> recRdr =
                tblInputFormat.createRecordReader(split, ctx);

            int j = 0;
            while (recRdr.nextKeyValue()) {
                final PrimaryKey key = recRdr.getCurrentKey();
                final Row row = recRdr.getCurrentValue();
                logger.finest("row[" + j + "]: key = " + key +
                              ", row = " + row);
                assertTrue(EXPECTED_NUMBER_TABLE.containsKey(
                                                  recRdr.getCurrentKey()));
                assertTrue(EXPECTED_NUMBER_TABLE.containsValue(
                                                  recRdr.getCurrentValue()));
                j++;
                nRows++;
            }
            i++;
        }

        /*
         * Reaching this point means that the set of all rows returned by
         * each split's RecordReader is a subset of the set of expected rows.
         * To verify that the set of expected rows is no greater than
         * the set of rows returned by the RecorderReader, and thus the
         * sets are equal, verify that the sets are the same size.
         */
        assertEquals(nRows, EXPECTED_NUMBER_TABLE.size());
    }

    /**
     * Verifies that for each split returned by the TableInputFormat class,
     * the associated TableRecordReader returns the expected JSON rows
     * from the table. See SR25802.
     */
    @Test
    public void testJsonTableRecordReader() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, TaskAttemptContext, and TableInputFormat,
         * all with the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(jsonTable.getName());
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);
        final List<InputSplit> splits = tblInputFormat.getSplits(ctx);

        /*
         * If this test is run using a secure store, then copy the security
         * file and trust file to the classpath location. For more info,
         * see the comment in testTableRecordReader.
         */
        if (SECURITY_ENABLE) {

            final ClassLoader cl =
                TableRecordReaderBase.class.getClassLoader();
            final String basePath = cl.getResource(".").getFile();
            final TableInputSplit sp = (TableInputSplit) splits.get(0);
            File srcFile = createStore.getDefaultUserLoginFile();
            File destFile = new File(basePath, sp.getSecurityLogin());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            srcFile = createStore.getTrustStore();
            destFile = new File(basePath, sp.getSecurityTrust());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
        }

        /*
         * Loop through each split, retrieve the spit's RecordReader, use
         * that RecordReader to retrieve all of the records in the jsonTable
         * that are associated with that split, and verify that each record
         * is contained in the expected set of records.
         */
        int i = 0;
        int nRows = 0;
        for (InputSplit inputSplit : splits) {
            final TableInputSplit split = (TableInputSplit) inputSplit;
            logger.finest("split[" + i + "] = " + split);
            final RecordReader<PrimaryKey, Row> recRdr =
                tblInputFormat.createRecordReader(split, ctx);

            int j = 0;
            while (recRdr.nextKeyValue()) {
                final PrimaryKey key = recRdr.getCurrentKey();
                final Row row = recRdr.getCurrentValue();
                logger.finest("row[" + j + "]: key = " + key +
                              ", row = " + row);
                assertTrue(EXPECTED_JSON_TABLE.containsKey(
                                                  recRdr.getCurrentKey()));
                assertTrue(EXPECTED_JSON_TABLE.containsValue(
                                                  recRdr.getCurrentValue()));
                j++;
                nRows++;
            }
            i++;
        }

        /*
         * Reaching this point means that the set of all rows returned by
         * each split's RecordReader is a subset of the set of expected rows.
         * To verify that the set of expected rows is no greater than
         * the set of rows returned by the RecorderReader, and thus the
         * sets are equal, verify that the sets are the same size.
         */
        assertEquals(nRows, EXPECTED_JSON_TABLE.size());
    }

    /**
     * Regression test for SR25870. Prior to addressing the issue documented
     * in SR25870, if the RecordReader encountered exceptions due to network
     * issues or other types failures, the RecordReader would simply catch,
     * log, and swallow the exception, and then silently exit the record
     * retrieval process. When an incomplete set of results is returned by
     * the RecordReader with no reason indicated, it can be confusing and
     * hard to debug. The fix (changeset 4579) was to change the nextKeyValue
     * method of the oracle.kv.hadoop.table.TableRecordReaderBase class to
     * propagate exceptions encountered during record retrieval as an
     * IOException, to help users better diagnose such failures.
     *
     * This test verifies that the RecordReader no longer swallows exceptions
     * when failure occurs during record retrieval.
     */
    @Test
    public void testTableRecordReaderExceptionPropagation() throws Exception {

        logger.fine("\n\n--- ENTERED " + testName.getMethodName() + " ---\n");

        /*
         * Need a Configuration, TaskAttemptContext, and TableInputFormat,
         * all with the same basic configuration.
         */
        final Configuration conf =
            getBasicHadoopConfiguration(vehicleTable.getName());
        final TableTaskAttemptContext ctx = new TableTaskAttemptContext(conf);
        final TableInputFormat tblInputFormat =
                                   getConfiguredTableInputFormat(conf);
        final List<InputSplit> splits = tblInputFormat.getSplits(ctx);

        /*
         * If this test is run using a secure store, then copy the security
         * file and trust file to the classpath location. For more info,
         * see the comment in testTableRecordReader.
         */
        if (SECURITY_ENABLE) {

            final ClassLoader cl =
                TableRecordReaderBase.class.getClassLoader();
            final String basePath = cl.getResource(".").getFile();
            final TableInputSplit sp = (TableInputSplit) splits.get(0);
            File srcFile = createStore.getDefaultUserLoginFile();
            File destFile = new File(basePath, sp.getSecurityLogin());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
            srcFile = createStore.getTrustStore();
            destFile = new File(basePath, sp.getSecurityTrust());
            SecurityUtils.copyOwnerWriteFile(srcFile, destFile);
        }

        /*
         * This particular test injects a RequestTimeoutException to
         * verify that the RecordReader handles rather than swallows
         * the exception. Unless logging is disabled when the failure
         * is injected, even though the test passes, a stack trace
         * referencing the RequestTimeoutException will be displayed
         * in the test output. This can be alarming and/or confusing
         * when analyzing test output. Thus, to prevent confusion,
         * the RecordReader's logger is disabled prior to initiating
         * the record retrieval; and re-enables the logger before
         * exiting this test.
         */
        final org.apache.log4j.Logger recRdrLogger =
             org.apache.log4j.Logger.getLogger(
                "oracle.kv.hadoop.table.TableRecordReaderBase");
        final org.apache.log4j.Level recRdrLevel = recRdrLogger.getLevel();
        recRdrLogger.setLevel(org.apache.log4j.Level.OFF);

        /*
         * Begin record retrieval in the RecordReader by looping through
         * each split initialized above and retrieving the spit's
         * RecordReader. For each split, inject a RequestTimeoutException
         * during record retrieval by setting the hook defined in the
         * TableRecordBase class, and verify that the RecordReader
         * propagates (does not swallow) the RequestTimeoutException.
         */
        try {
            for (InputSplit inputSplit : splits) {
                final TableInputSplit split = (TableInputSplit) inputSplit;
                final TableRecordReader recRdr =
                    (TableRecordReader) tblInputFormat.createRecordReader(
                                                           split, ctx);
                /* Set RTE during the iteration */
                recRdr.setIterFailHook(new TestHook<RecordValue>() {
                    @Override
                    public void doHook(RecordValue rv) {
                        throw new RequestTimeoutException(
                            5000, "generated test exception", null, false);
                    }
                });
                while (recRdr.nextKeyValue()) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }

            /*
             * Reaching this point means that the RecordReader must have
             * swallowed the RequestTimeoutException that was injected
             * into the RecordReader's record retrieval; thus, assert
             * test failure.
             */
            fail("No exception. Expected a RequestTimeoutException " +
                 "wrapped in an IOException");

        } catch (IOException e) {

            /*
             * Verify the RecordReader received and propagated the
             * RequestTimeoutExcepiont as expected.
             */
            assert (e.getCause() instanceof RequestTimeoutException);

        } finally {

            /* Re-enable the RecordReader's logger. */
            recRdrLogger.setLevel(recRdrLevel);
        }
    }


    /* Convenience methods employed by the test cases of this class. */

    /*
     * Convenience method that uses the given JobContext to retrieve the
     * splits from the given TableInputFormat and then verifies that the
     * configuration information that should be equivalent on each returned
     * split is indeed equivalent. This method may be invoked by different
     * test cases as a sanity check, prior to performing the actual test.
     */
    private void getSplitsVerifyConfiguration(final JobContext jobContext,
                                              final TableInputFormat inputFmt)
                     throws Exception  {

        /*
         * The property values contained in the given JobContext specify the
         * the config values that should be equivalent on each split. Here
         * those values are retrieved once, and then used in the comparisons
         * performed when iterating over the set of splits returned by the
         * given TableInputFormat (below).
         */
        final Configuration conf = jobContext.getConfiguration();

        String[] helperHostsArray = null;
        final String helperHostsStr =
            conf.get(ParamConstant.KVSTORE_NODES.getName());
        if (helperHostsStr != null) {
            helperHostsArray = helperHostsStr.trim().split(",");
        }
        final List<String> helperHosts = Arrays.asList(helperHostsArray);

        final String storeName =
            conf.get(ParamConstant.KVSTORE_NAME.getName());

        final String tblName = conf.get(ParamConstant.TABLE_NAME.getName());

        long timeout = 0L;
        final TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
        final String timeoutStr = conf.get(ParamConstant.TIMEOUT.getName());
        if (timeoutStr != null) {
            timeout = ExternalDataSourceUtils.parseTimeout(timeoutStr);
        }

        int maxRequests = 0;
        final String maxRequestsStr =
            conf.get(ParamConstant.MAX_REQUESTS.getName());
        if (maxRequestsStr != null) {
            maxRequests = Integer.parseInt(maxRequestsStr);
        }

        int batchSize = 0;
        final String batchSizeStr =
            conf.get(ParamConstant.BATCH_SIZE.getName());
        if (batchSizeStr != null) {
            batchSize = Integer.parseInt(batchSizeStr);
        }

        int maxBatches = 0;
        final String maxBatchesStr =
            conf.get(ParamConstant.MAX_BATCHES.getName());
        if (maxBatchesStr != null) {
            maxBatches = Integer.parseInt(maxBatchesStr);
        }

        final boolean secureStore = KVSecurityUtil.usingSecureStore(conf);

        /*
         * Verification: get the splits and iterate, verifying each split
         * contains the expected values.
         */
        final List<InputSplit> splits = inputFmt.getSplits(jobContext);

        int i = 0;
        for (InputSplit inputSplit : splits) {
            final TableInputSplit split = (TableInputSplit) inputSplit;

            logger.fine("split[" + i + "] = " + split);

            /*
             * Only verify kvHelperHosts when the given split contains
             * a non-zero number of partition sets. If a split was generated
             * without partition sets, then it will not have helper host info.
             */
            final List<Set<Integer>> splitPartitionSets =
                                             split.getPartitionSets();
            if (splitPartitionSets.size() > 0) {
                final List<String> splitHelperHosts =
                    Arrays.asList(split.getKVHelperHosts());
                logger.finest("Configuration: helperHosts = " + helperHosts +
                              ", split: helperHosts = " + splitHelperHosts);
                assertEquals(helperHosts.size(), splitHelperHosts.size());
                for (int j = 0; j < splitHelperHosts.size(); j += 1) {
                    assertTrue(helperHosts.contains(splitHelperHosts.get(j)));
                    assertTrue(splitHelperHosts.contains(helperHosts.get(j)));
                }
            }

            /* Verify the kvStore field */
            logger.finest("Configuration: storeName = " + storeName +
                          ", split: storeName = " + split.getKVStoreName());
            assertEquals(split.getKVStoreName(), storeName);

            /* Verify the tableName field */
            logger.finest("Configuration: tableName = " + tblName +
                          ", split: tableName = " + split.getTableName());
            assertEquals(split.getTableName(), tblName);

            /* Verify the timeout & timeoutUnit fields */
            logger.finest("Configuration: timeout = " + timeout +
                          ", split: timeout = " + split.getTimeout());
            assertEquals(split.getTimeout(), timeout);

            logger.finest("Configuration: timeoutUnit = " + timeoutUnit +
                          ", split: timeoutUnit = " + split.getTimeoutUnit());
            assertEquals(split.getTimeoutUnit(), timeoutUnit);

            /* Verify the maxRequests field */
            logger.finest("Configuration: maxRequests = " + maxRequests +
                          ", split: maxRequests = " + split.getMaxRequests());
            assertEquals(split.getMaxRequests(), maxRequests);

            /* Verify the batchSize field */
            logger.finest("Configuration: batchSize = " + batchSize +
                          ", split: batchSize = " + split.getBatchSize());
            assertEquals(split.getBatchSize(), batchSize);

            /* Verify the maxBatches field */
            logger.finest("Configuration: maxBatches = " + maxBatches +
                          ", split: maxBatches = " + split.getMaxBatches());
            assertEquals(split.getMaxBatches(), maxBatches);

            if (secureStore) {
                /* Verify the loginFlnm field */
                logger.finest("Configuration: loginFlnm = " +
                              getName(
                                  KVSecurityUtil.getLoginFlnm()) +
                              "split: loginFlnm = " +
                              split.getSecurityLogin());

                /*
                 * The login file name in split contains file name only.
                 * The LoginFlnm in KVSecurityUtils contains file path, other
                 * cases need to use file path in KVSecurityUtils, so we strip
                 * out the file name here for current test case expectation.
                 */
                assertEquals(split.getSecurityLogin(),
                    getName(KVSecurityUtil.getLoginFlnm()));

                /* Verify the contents of the passwordCredentials field */
                final PasswordCredentials passwordCredentials =
                    KVSecurityUtil.getPasswordCredentials();

                logger.finest("Configuration: userName = " +
                              passwordCredentials.getUsername() +
                              ", split: username = " +
                              split.getSecurityCredentials().getUsername());
                logger.finest("Configuration: password = " +
                              String.valueOf(
                                  passwordCredentials.getPassword()) +
                              ", split: password = " +
                              String.valueOf(
                              split.getSecurityCredentials().getPassword()));

                assertEquals(split.getSecurityCredentials().getUsername(),
                             passwordCredentials.getUsername());
                assertArrayEquals(
                    split.getSecurityCredentials().getPassword(),
                    passwordCredentials.getPassword());

                /* Verify the trustFlnm field */
                logger.finest("Configuration: trustFlnm = " +
                              getName(
                                  KVSecurityUtil.getTrustFlnm()) +
                              "split: trustFlnm = " +
                              split.getSecurityTrust());

                /*
                 * The trust file name in split contains file name only.
                 * The TrustFlnm in KVSecurityUtils contains file path, other
                 * cases need to use file path in KVSecurityUtils, so we strip
                 * out the file name here for current test case expectation.
                 */
                assertEquals(split.getSecurityTrust(),
                    getName(KVSecurityUtil.getTrustFlnm()));
            }

            i++;
        }
    }

    /*
     * Convenience method that, for each InputSpit in the given List that
     * has a non-empty set of partition sets, verifies that the mechanism
     * used by Hadoop MapReduce for serializing the contents of a given
     * split (the split's write method) behaves as expected; by producing
     * output that can be used by MapReduce's deserialization mechanism
     * (the split's readField method) to reconstruct a split equivalent
     * to the original, serialized split.
     */
    private void verifyFieldSerialization(final List<InputSplit> splits)
                     throws Exception  {
        int i = 0;
        for (InputSplit split : splits) {
            final TableInputSplit split1 = (TableInputSplit) split;

            logger.fine("split1[" + i + "] = " + split1);

            final List<Set<Integer>> split1PartitionSets =
                                         split1.getPartitionSets();

            if (split1PartitionSets.size() == 0) {
                logger.fine("partition sets for split1[" + i + "]: NONE");
            } else {
                logger.fine("partition sets for split1[" + i + "]:" +
                            split1PartitionSets);

                /* Serialize the contents of split1 (from getSplits) */
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(baos);
                split1.write(oos);
                oos.close();

                /* Deserialize by reading the serialized data into split2 */
                final ByteArrayInputStream bais =
                    new ByteArrayInputStream(baos.toByteArray());
                final ObjectInputStream ois = new ObjectInputStream(bais);
                final TableInputSplit split2 = new TableInputSplit();
                split2.readFields(ois);

                /* Verify the serialization/deserialization for each field */

                /* The locations field */
                final String[] split1Locs = split1.getLocations();
                final int split1NLocs = split1Locs.length;
                final String[] split2Locs = split2.getLocations();
                final int split2NLocs = split2Locs.length;
                logger.finest("split1: nLocs = " + split1NLocs + ", locs = " +
                              Arrays.asList(split1Locs) + ", split2: " +
                              "nLocs = " + split2NLocs + ", locs = " +
                              Arrays.asList(split1Locs));

                for (int j = 0; j < split1NLocs; j += 1) {
                    assertEquals(split1Locs[j], split2Locs[j]);
                }

                /* The kvHelperHosts field */
                final String[] split1HelperHosts = split1.getKVHelperHosts();
                final int split1NHosts = split1HelperHosts.length;
                final String[] split2HelperHosts = split2.getKVHelperHosts();
                final int split2NHosts = split2HelperHosts.length;
                logger.finest("split1: nHelperHosts = " + split1NHosts +
                              ", helperHosts = " +
                              Arrays.asList(split1HelperHosts) +
                              ", split2: nHelperHosts = " + split2NHosts +
                              ", helperHosts = " +
                              Arrays.asList(split2HelperHosts));

                for (int j = 0; j < split1NHosts; j += 1) {
                    assertEquals(split1HelperHosts[j], split2HelperHosts[j]);
                }

                /* The kvStore field */
                logger.finest("split1: storeName = " +
                              split1.getKVStoreName() + ", split2: " +
                              "storeName = " + split2.getKVStoreName());
                assertEquals(split1.getKVStoreName(), split2.getKVStoreName());

                /* The tableName field */
                logger.finest("split1: tableName = " + split1.getTableName() +
                              ", split2: tableName = " +
                              split2.getTableName());
                assertEquals(split1.getTableName(), split2.getTableName());

                /* The primaryKeyProperty field */
                logger.finest("split1: primaryKeyProperty = " +
                              split1.getPrimaryKeyProperty() +
                              ", split2: primaryKeyProperty = " +
                              split2.getPrimaryKeyProperty());
                assertEquals(split1.getPrimaryKeyProperty(),
                             split2.getPrimaryKeyProperty());

                /* The fieldRangeProperty field (for MultiRowOptions) */
                logger.finest("split1: fieldRangeProperty = " +
                              split1.getFieldRangeProperty() + ", split2: " +
                              "fieldRangeProperty = " +
                              split2.getFieldRangeProperty());
                assertEquals(split1.getFieldRangeProperty(),
                             split2.getFieldRangeProperty());

                /* The direction field */
                logger.finest("split1: direction = " + split1.getDirection() +
                              ", split2: direction = " +
                              split2.getDirection());
                assertEquals(split1.getDirection(), split2.getDirection());

                /* The consistency field */
                logger.finest("split1: consistency = " +
                            split1.getConsistency() + ", split2: " +
                            "consistency = " + split2.getConsistency());
                assertEquals(split1.getConsistency(), split2.getConsistency());

                /* The timeout & timeoutUnit fields */
                logger.finest("split1: timeout = " + split1.getTimeout() +
                              ", split2: timeout = " + split2.getTimeout());
                assertEquals(split1.getTimeout(), split2.getTimeout());

                logger.finest("split1: timeoutUnit = " +
                              split1.getTimeoutUnit() + ", split2: " +
                              "timeoutUnit = " + split2.getTimeoutUnit());
                assertEquals(split1.getTimeoutUnit(), split2.getTimeoutUnit());

                /* The maxRequests field */
                logger.finest("split1: maxRequests = " +
                              split1.getMaxRequests() + ", split2: " +
                              "maxRequests = " + split2.getMaxRequests());
                assertEquals(split1.getMaxRequests(), split2.getMaxRequests());

                /* The batchSize field */
                logger.finest("split1: batchSize = " + split1.getBatchSize() +
                              ", split2: batchSize = " +
                              split2.getBatchSize());
                assertEquals(split1.getBatchSize(), split2.getBatchSize());

                /* The maxBatches field */
                logger.finest("split1: maxBatches = " +
                              split1.getMaxBatches() + ", split2: " +
                              "maxBatches = " + split2.getMaxBatches());
                assertEquals(split1.getMaxBatches(), split2.getMaxBatches());

                /* The partitionSets field */
                final List<Set<Integer>> split2PartitionSets =
                                             split2.getPartitionSets();
                logger.finest("split1: nPartitionSets = " +
                              split1PartitionSets.size() + ", split2: " +
                              "nPartitionSets = " +
                              split2PartitionSets.size());
                logger.finest("split1: partitionSets = " +
                              split1PartitionSets + ", split2: " +
                              "partitionSets = " + split2PartitionSets);
                assertEquals(split1PartitionSets, split2PartitionSets);

                /* The serverLoginFlnm field */
                logger.finest("split1: serverLoginFlnm = " +
                              split1.getSecurityLogin() + ", split2: " +
                             "serverLoginFlnm = " +
                             split2.getSecurityLogin());
                assertEquals(split1.getSecurityLogin(),
                             split2.getSecurityLogin());

                /* The passwordCredentials field */
                final PasswordCredentials passwordCredentials1 =
                                            split1.getSecurityCredentials();
                final PasswordCredentials passwordCredentials2 =
                                            split2.getSecurityCredentials();

                if (passwordCredentials1 != null &&
                    passwordCredentials2 != null) {

                    logger.finest("split1: userName = " +
                        split1.getSecurityCredentials().getUsername() +
                        ", split2: username = " +
                        split2.getSecurityCredentials().getUsername());
                    logger.finest("split1: password = " +
                        String.valueOf(
                            split1.getSecurityCredentials().getPassword()) +
                        ", split2: password = " +
                        String.valueOf(
                            split2.getSecurityCredentials().getPassword()));
                        assertEquals(
                            split1.getSecurityCredentials().getUsername(),
                            split2.getSecurityCredentials().getUsername());
                        assertArrayEquals(
                            split1.getSecurityCredentials().getPassword(),
                            split2.getSecurityCredentials().getPassword());

                } else if (passwordCredentials1 != null &&
                           passwordCredentials2 == null) {

                    fail("non-null PasswordCredentials from split1, but " +
                         "null PasswordCredentials from split2");

                } else if (passwordCredentials1 == null &&
                           passwordCredentials2 != null) {

                    fail("null PasswordCredentials from split1, but " +
                         "non-null PasswordCredentials from split2");
                }

                /* The trustFlnm field */
                logger.finest("split1: trustFlnm = " +
                              split1.getSecurityTrust() + ", split2: " +
                             "trustFlnm = " +
                             split2.getSecurityTrust());
                assertEquals(split1.getSecurityTrust(),
                             split2.getSecurityTrust());
            }
            i++;
        }
    }

    /*
     * Convenience method that creates an instance of the Hadoop Configuration
     * class and populates it with the basic information associated with the
     * store employed by this test class; which is needed by most/all test
     * cases when constructing an instance of TaskAttemptContext for
     * the various Hadoop methods that are used by the test cases in this
     * class.
     */
    public static Configuration getBasicHadoopConfiguration(
                                    final String tableName) throws Exception {

        final Configuration conf = new Configuration();

        /*
         * Some unit tests in this package (not in this class) do not need
         * to start a concrete, non-mocked store; in which case the call
         * to CreateStore.start() made in the staticSetUp() method of this
         * class is not called. For cases in which a concrete store is
         * not started, this method still needs to use CreateStore to
         * initialize the necessary items needed by the Hadoop integration
         * test classes that call this method to obtain a Configuration.
         */
        if (createStore == null) {
            final String testStoreName = "test-store-name";
            final int startPort = 13390;
            final int numStorageNodes = 3;
            final int replicationFactor = 3;
            final int numPartitions = 10;
            final int capacity = 1;
            createStore = new CreateStore(
                              testStoreName, startPort, numStorageNodes,
                              replicationFactor, numPartitions, capacity);
            createStore.initStorageNodes();
        }
        if (portFinders == null) {
            portFinders = createStore.getPortFinders();
        }
        final String[] helperHosts = new String[portFinders.length];
        for (int i = 0; i < portFinders.length; i++) {
            helperHosts[i] = createStore.getHostname() + ":" +
                                 portFinders[i].getRegistryPort();
        }
        final StringBuilder helperHostsBuf = new StringBuilder(helperHosts[0]);
        for (int i = 1; i < helperHosts.length; i++) {
            helperHostsBuf.append("," + helperHosts[i]);
        }
        conf.set(ParamConstant.KVSTORE_NODES.getName(),
                 helperHostsBuf.toString());

        final String storeName = createStore.getStoreName();
        conf.set(ParamConstant.KVSTORE_NAME.getName(), storeName);

        conf.set(ParamConstant.TABLE_NAME.getName(), tableName);

        final int batchSize = 0;
        final String batchSizeStr = String.valueOf(batchSize);
        conf.set(ParamConstant.BATCH_SIZE.getName(), batchSizeStr);

        final long timeout = 0;
        final String timeoutStr = String.valueOf(timeout);
        conf.set(ParamConstant.TIMEOUT.getName(), timeoutStr);
        createStore.setSecInHadoopConfiguration(conf);

        return conf;
    }

    /*
     * Convenience method that creates an instance of TableInputFormat
     * and configures it with the basic information contained in the given
     * Hadoop Configuration; which is needed by most/all test cases when
     * verifying the classes used to support the Hadoop integration
     * with the KVStore Table API.
     *
     * Note that the TopologyLocatorWrapper parameter is used support
     * mocking some of the behavior in the TableInputFormatBase class;
     * where the static method TopologyLocator.get() is invoked to
     * 'discover' the topology of the store with which the TableInputFormat
     * is communicating. For tests where an actual store is not started,
     * mocking the behavior of TopologyLocator is necessary. Unfortunately,
     * the EasyMock framework does not allow static methods to be mocked.
     * As a result, TableInputFormatBase provides the nested class
     * TopologyLocatorWrapper, which provides a non-static form of the
     * get() method that acts as a simple passthrough to its counterpart
     * on TopologyLocator.
     *
     * For tests that do not need to mock the behavior of the TableInputFormat
     * that is returned by this method, the version of this method that
     * takes only a singel Configuration parameter should be used; in
     * which case, a concrete, non-mocked version of the get() method will
     * be employed.
     */
    public static TableInputFormat getConfiguredTableInputFormat(
                                       Configuration conf) throws Exception {

        return getConfiguredTableInputFormat(conf, null);
    }

    public static TableInputFormat getConfiguredTableInputFormat(
                                   Configuration conf,
                                   TopologyLocatorWrapper topologyLocator)
                                       throws Exception {

        final TableInputFormat tableInputFormat =
            (topologyLocator == null ?
               new TableInputFormat() : new TableInputFormat(topologyLocator));

        String[] helperHosts = null;
        final String helperHostsStr =
            conf.get(ParamConstant.KVSTORE_NODES.getName());
        if (helperHostsStr != null) {
            helperHosts = helperHostsStr.trim().split(",");
        }
        TableInputFormat.setKVHelperHosts(helperHosts);

        final String storeName = conf.get(
                                     ParamConstant.KVSTORE_NAME.getName());
        TableInputFormat.setKVStoreName(storeName);

        final String tableName = conf.get(ParamConstant.TABLE_NAME.getName());
        TableInputFormat.setTableName(tableName);

        final String maxRequestsStr = conf.get(
                                        ParamConstant.MAX_REQUESTS.getName());
        int maxRequests = 0;
        if (maxRequestsStr != null) {
            maxRequests = Integer.parseInt(maxRequestsStr);
        }
        TableInputFormat.setMaxRequests(maxRequests);

        final String batchSizeStr = conf.get(
                                        ParamConstant.BATCH_SIZE.getName());
        int batchSize = 0;
        if (batchSizeStr != null) {
            batchSize = Integer.parseInt(batchSizeStr);
        }
        TableInputFormat.setBatchSize(batchSize);

        int maxBatches = 0;
        final String maxBatchesStr = conf.get(
                                         ParamConstant.MAX_BATCHES.getName());
        if (maxBatchesStr != null) {
            maxBatches = Integer.parseInt(maxBatchesStr);
        }
        TableInputFormat.setMaxBatches(maxBatches);

        if (KVSecurityUtil.usingSecureStore(conf)) {
            TableInputFormat.setKVSecurity(
                KVSecurityUtil.getLoginFlnm(),
                KVSecurityUtil.getPasswordCredentials(),
                KVSecurityUtil.getTrustFlnm());
        }

        long timeout = 0L;
        final String timeoutStr = conf.get(ParamConstant.TIMEOUT.getName());
        if (timeoutStr != null) {
            timeout = Long.parseLong(timeoutStr);
        }
        TableInputFormat.setTimeout(timeout);

        TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
        final String timeoutUnitStr =
            conf.get(ParamConstant.TIMEOUT_UNIT.getName());
        if (timeoutUnitStr != null) {
            timeoutUnit = TimeUnit.valueOf(timeoutUnitStr);
        }
        TableInputFormat.setTimeoutUnit(timeoutUnit);

        return tableInputFormat;
    }

    /*
     * Adds the given number of rows to the vehicleTable created during setup.
     */
    static void populateVehicleTable(int nOps) {
        for (long i = 0; i < nOps; i++) {
            addRowToVehicleTable();
        }
    }

    /*
     * Adds the given number of rows to the timestampTable created in setup.
     */
    static void populateTimestampTable(int nOps) {
        for (int i = 0; i < nOps; i++) {
            addRowToTimestampTable(i);
        }
    }

    /*
     * Adds the given number of rows to the numberTable created during setup.
     */
    static void populateNumberTable(int nOps) {
        for (int i = 0; i < nOps; i++) {
            addRowToNumberTable(i);
        }
    }

    /*
     * Adds the given number of rows to the jsonTable created during setup.
     */
    static void populateJsonTable(int nOps) {
        for (int i = 0; i < nOps; i++) {
            addRowToJsonTable(i);
        }
    }

    /*
     * Adds a single row to the test table named 'vehicleTable' in the store.
     * The content of the row that is added by this method should be unique
     * relative to the other rows in the table.
     */
    static void addRowToVehicleTable() {

        final int typeIndx = SECURE_RANDOM.nextInt(TYPES.length);
        final int makeIndx = SECURE_RANDOM.nextInt(MAKES.length);
        final int classIndx = SECURE_RANDOM.nextInt(CLASSES.length);
        final int colorIndx = SECURE_RANDOM.nextInt(COLORS.length);

        final String type  = TYPES[typeIndx];
        final String make  = MAKES[makeIndx];
        String vClass = CLASSES[classIndx];
        String color = COLORS[colorIndx];

        String[] models = MODELS_FORD_AUTO;
        float priceMult = 2.0f;

        if ("suv".equals(type)) {
            priceMult = 4.0f;
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_SUV;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_SUV;
            } else {
                /* Default to make "Ford" */
                models = MODELS_FORD_SUV;
            }
        } else if ("truck".equals(type)) {
            priceMult = 3.0f;
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_TRUCK;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_TRUCK;
            } else {
                /* Default to make "Ford" */
                models = MODELS_FORD_TRUCK;
            }
        } else {
            /* Default to type "auto" */
            if ("Chrysler".equals(make)) {
                models = MODELS_CHRYSLER_AUTO;
            } else if ("GM".equals(make)) {
                models = MODELS_GM_AUTO;
            }
        }
        final int modelIndx = SECURE_RANDOM.nextInt(models.length);
        final String model = models[modelIndx];

        final float basePrice = 10371.59f;
        final float deltaPrice = SECURE_RANDOM.nextFloat();
        final double price = (priceMult * basePrice) + deltaPrice;

        final int count = SECURE_RANDOM.nextInt(100);

        final BigDecimal serialnumber =
            new BigDecimal(SECURE_RANDOM.nextDouble());

        final int[] timestampComponents = {
            1955 + SECURE_RANDOM.nextInt(65),    /* random year, 1955-2020 */
            1 + SECURE_RANDOM.nextInt(12),       /* random month */
            1 + SECURE_RANDOM.nextInt(28),       /* random day, 1-28 */
            SECURE_RANDOM.nextInt(24),           /* random hour, 0-23 */
            SECURE_RANDOM.nextInt(60),           /* random minute, 0-59 */
            SECURE_RANDOM.nextInt(60),           /* random second, 0-59 */
            SECURE_RANDOM.nextInt(1000000000) }; /* random nanosecond */

        final Timestamp creationdate =
            TimestampUtils.createTimestamp(timestampComponents);

        final Row row = vehicleTable.createRow();

        row.put("type", type);
        row.put("make", make);
        row.put("model", model);
        row.put("class", vClass);
        row.put("color", color);
        row.put("price", price);
        row.put("count", count);
        row.putNumber("serialnumber", serialnumber);
        row.put("creationdate", creationdate);

        /* Row may exist. If so, then change it slightly to avoid overwrite. */
        final PrimaryKey dupKey = row.createPrimaryKey();
        final Row dupRow = tableApi.get(dupKey, null);
        if (dupRow != null) {
            final int indx0 = SECURE_RANDOM.nextInt(DUP_CLASS_SUFFIXES.length);
            final int indx1 = SECURE_RANDOM.nextInt(COLORS.length);
            final String class2nd = DUP_CLASS_SUFFIXES[indx0];
            final String color2nd = COLORS[indx1];
            vClass = vClass + "-" + class2nd;
            color = color + "-on-" + color2nd;

            row.put("class", vClass);
            row.put("color", color);
        }
        EXPECTED_VEHICLE_TABLE.put(row.createPrimaryKey(), row);
        tableApi.putIfAbsent(row, null, null);
    }

    /*
     * Adds a single row to the test table named 'timestampTable' in the store.
     */
    static void addRowToTimestampTable(final int i) {

        final int[] timestampComponents = {
            1970 + SECURE_RANDOM.nextInt(65),    /* random year, 1970-2035 */
            1 + SECURE_RANDOM.nextInt(12),       /* random month */
            1 + SECURE_RANDOM.nextInt(28),       /* random day, 1-28 */
            SECURE_RANDOM.nextInt(24),           /* random hour, 0-23 */
            SECURE_RANDOM.nextInt(60),           /* random minute, 0-59 */
            SECURE_RANDOM.nextInt(60),           /* random second, 0-59 */
            SECURE_RANDOM.nextInt(1000000000) }; /* random nanosecond */

        final Timestamp timestampValue =
            TimestampUtils.createTimestamp(timestampComponents);

        final Row row = timestampTable.createRow();

        row.put("id", i);
        row.put("timestampField", timestampValue);

        EXPECTED_TIMESTAMP_TABLE.put(row.createPrimaryKey(), row);

        tableApi.putIfAbsent(row, null, null);
    }

    /*
     * Adds a single row to the test table named 'numberTable' in the store.
     */
    static void addRowToNumberTable(final int i) {

        final int intNumber = SECURE_RANDOM.nextInt();
        final long longNumber = SECURE_RANDOM.nextLong();
        final float floatNumber = SECURE_RANDOM.nextFloat();
        final double doubleNumber = SECURE_RANDOM.nextDouble();
        final BigDecimal bigdecimalNumber =
            new BigDecimal(SECURE_RANDOM.nextDouble());

        final Row row = numberTable.createRow();

        row.put("id", i);
        row.putNumber("intNumberField", intNumber);
        row.putNumber("longNumberField", longNumber);
        row.putNumber("floatNumberField", floatNumber);
        row.putNumber("doubleNumberField", doubleNumber);
        row.putNumber("bigdecimalNumberField", bigdecimalNumber);

        EXPECTED_NUMBER_TABLE.put(row.createPrimaryKey(), row);

        tableApi.putIfAbsent(row, null, null);
    }

    /*
     * Adds a single row to the test table named 'jsonTable' in the store.
     */
    static void addRowToJsonTable(final int i) {

        final Row row = jsonTable.createRow();

        row.put("id", i);
        row.putJson("jsonField", JSON_SENATOR_INFO[i]);

        EXPECTED_JSON_TABLE.put(row.createPrimaryKey(), row);

        tableApi.putIfAbsent(row, null, null);
    }

    /* Convenience method that can be used for debugging a given test case. */
    static void displayRows(Table tbl) {

        final TableIterator<Row> itr =
            tableApi.tableIterator(tbl.createPrimaryKey(), null, null);
        while (itr.hasNext()) {
            staticLogger.finest((itr.next()).toString());
        }
        itr.close();
    }

    /**
     * Convenience utility class for setting up for security.
     */
    public static final class KVSecurityUtil {

        private static String loginFlnm;
        private static PasswordCredentials passwordCredentials;
        private static String trustFlnm;

        /**
         * If the test is configured to work with a secure store, then
         * this method caches the artifacts necessary to interact with
         * that secure store, and returns true; otherwise, returns false.
         */
        public static boolean usingSecureStore(Configuration conf)
                                  throws IOException {

            loginFlnm = conf.get(KVSecurityConstants.SECURITY_FILE_PROPERTY);
            if (loginFlnm == null) {
                return false;
            }

            trustFlnm =
                conf.get(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
            if (trustFlnm == null) {
                return false;
            }

            final String userName =
                conf.get(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
            if (userName == null) {
                return false;
            }

            /* Determine if wallet or password file and get file/dir name. */
            boolean usingWallet = false;
            String credentialsLoc =
                conf.get(KVSecurityConstants.AUTH_PWDFILE_PROPERTY);
            if (credentialsLoc == null) {
                credentialsLoc =
                    conf.get(KVSecurityConstants.AUTH_WALLET_PROPERTY);
                if (credentialsLoc != null) {
                    usingWallet = true;
                }
            }
            if (credentialsLoc == null) {
                return false;
            }

            /*
             * Retrieve the user password (from either the password file or
             * wallet directory) and use that password, along with the
             * username (retrieved above) to construct the PasswordCredentials.
             */
            PasswordStore fileStore = null;
            final File credentialsFd = new File(credentialsLoc);

            if (usingWallet) { /* Retrieve the password from the wallet. */

                staticLogger.info("USING WALLET DIRECTORY [" + credentialsFd +
                                  "]");
                final WalletManager storeMgr = new WalletManager();
                fileStore = storeMgr.getStoreHandle(credentialsFd);

            } else { /* Retrieve the password from the password file. */

                staticLogger.info("USING PASSWORD FILE [" + credentialsFd +
                                  "]");
                final FileStoreManager storeMgr = new FileStoreManager();
                fileStore = storeMgr.getStoreHandle(credentialsFd);
            }

            fileStore.open(null);
            final Collection<String> secretAliases =
                                         fileStore.getSecretAliases();
            final Iterator<String> aliasItr = secretAliases.iterator();
            final char[] userPassword = (aliasItr.hasNext() ?
                                fileStore.getSecret(aliasItr.next()) : null);
            passwordCredentials =
                new PasswordCredentials(userName, userPassword);

            return true;
        }

        public static String getLoginFlnm() {
            return loginFlnm;
        }

        public static oracle.kv.PasswordCredentials getPasswordCredentials() {
            return passwordCredentials;
        }

        public static String getTrustFlnm() {
            return trustFlnm;
        }
    }

    /**
     * Utility method that creates and returns an instance of TableInputSplit
     * that is initialized with an example state; including (non-existent)
     * security-related state.
     */
    public static TableInputSplit createTableInputSplit(
                                      final JobConf jobConf) {
        return createTableInputSplit(
                   jobConf, TableInputSplit.QUERY_BY_PRIMARY_ALL_PARTITIONS);
    }
    public static TableInputSplit createTableInputSplit(final JobConf jobConf,
                                                        final int queryBy) {
        TableInputSplit split = new TableInputSplit();

        final String locStr =
            jobConf.get(ParamConstant.KVHADOOP_NODES.getName());

        split.setLocations(locStr.trim().split(","));

        final String helperHostStr =
            jobConf.get(ParamConstant.KVSTORE_NODES.getName());
        split = split.setKVHelperHosts(helperHostStr.trim().split(","));

        split = split.setKVStoreName(
            jobConf.get(ParamConstant.KVSTORE_NAME.getName()));

        split = split.setTableName(
            jobConf.get(ParamConstant.TABLE_NAME.getName()));

        split = split.setPrimaryKeyProperty(jobConf.get(
            ParamConstant.PRIMARY_KEY.getName()));

        split = split.setDirection(Direction.UNORDERED);

        final String consistencyStr =
            jobConf.get(ParamConstant.CONSISTENCY.getName());
        if (consistencyStr != null) {
            final Consistency consistency =
                ExternalDataSourceUtils.parseConsistency(consistencyStr);
            split = split.setConsistency(consistency);
        }

        final String timeoutStr =
            jobConf.get(ParamConstant.TIMEOUT.getName());
        if (timeoutStr != null) {
            final long timeout =
                ExternalDataSourceUtils.parseTimeout(timeoutStr);
            split = split.setTimeout(timeout);
        }

        TimeUnit timeoutUnit = TimeUnit.MILLISECONDS;
        final String timeoutUnitStr =
            jobConf.get(ParamConstant.TIMEOUT_UNIT.getName());
        if (timeoutUnitStr != null) {
            timeoutUnit = TimeUnit.valueOf(timeoutUnitStr);
        }
        split = split.setTimeoutUnit(timeoutUnit);

        final String maxRequestsStr =
            jobConf.get(ParamConstant.MAX_REQUESTS.getName());
        if (maxRequestsStr != null) {
            try {
                final int maxRequests =  Integer.parseInt(maxRequestsStr);
                split = split.setMaxRequests(maxRequests);
            } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
            }/* CHECKSTYLE:ON */
        }

        final String batchSizeStr =
            jobConf.get(ParamConstant.BATCH_SIZE.getName());
        if (batchSizeStr != null) {
            try {
                final int batchSize =  Integer.parseInt(batchSizeStr);
                split = split.setBatchSize(batchSize);
            } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
            }/* CHECKSTYLE:ON */
        }

        final String maxBatchesStr =
            jobConf.get(ParamConstant.MAX_BATCHES.getName());
        if (maxBatchesStr != null) {
            try {
                final int maxBatches =  Integer.parseInt(maxBatchesStr);
                split = split.setMaxBatches(maxBatches);
            } catch (NumberFormatException e) /* CHECKSTYLE:OFF */ {
            }/* CHECKSTYLE:ON */
        }

        final String loginFile =
            jobConf.get(KVSecurityConstants.SECURITY_FILE_PROPERTY);
        final String trustFile =
            jobConf.get(KVSecurityConstants.SSL_TRUSTSTORE_FILE_PROPERTY);
        final String userName =
            jobConf.get(KVSecurityConstants.AUTH_USERNAME_PROPERTY);
        if (loginFile != null && trustFile != null && userName != null) {
            final char[] testPswd = "NoSql00__123456".toCharArray();
            final PasswordCredentials pswdCreds =
                new PasswordCredentials(userName, testPswd);
            split = split.setKVStoreSecurity(loginFile, pswdCreds, trustFile);
        }

        final int nPartitions = 60;
        final int nPartitionsPerSet = nPartitions / N_PARTITION_SETS;
        final List<Set<Integer>> partitionSets =
            new ArrayList<Set<Integer>>();
        int partitionNumber = nPartitions;
        for (int i = 0; i < N_PARTITION_SETS; i++) {
            final Set<Integer> partitionSet = new HashSet<Integer>();
            for (int j = 0; j < nPartitionsPerSet; j++) {
                partitionSet.add(Integer.valueOf(partitionNumber--));
            }
            partitionSets.add(partitionSet);
        }
        split = split.setPartitionSets(partitionSets);

        final Set<RepGroupId> shardSet = new HashSet<RepGroupId>();
        for (int i = 1; i <= N_SHARDS; i++) {
            shardSet.add(new RepGroupId(i));
        }
        split = split.setShardSet(shardSet);

        /* Default to splitting on partition sets. */
        return split.setQueryInfo(queryBy, null);
    }

    public static TableIteratorOptions createTableIteratorOptions(
                                      final TableInputSplit split) {
        if (split == null) {
            return null;
        }
        return new TableIteratorOptions(split.getDirection(),
                                        split.getConsistency(),
                                        split.getTimeout(),
                                        split.getTimeoutUnit(),
                                        split.getMaxRequests(),
                                        split.getBatchSize());
    }

    private static String getName(String fileName) {
        if (fileName == null) {
            return null;
        }

        final int lastPos = fileName.lastIndexOf("/");
        return fileName.substring(lastPos + 1);
    }
}
