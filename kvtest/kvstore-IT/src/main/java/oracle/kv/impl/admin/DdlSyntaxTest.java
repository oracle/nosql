/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.admin;

import static oracle.kv.impl.api.table.TableTestBase.makeIndexList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.TestBase;
import oracle.kv.impl.admin.DdlHandler.DdlOperation;
import oracle.kv.impl.admin.TableDdlOperation.CreateIndex;
import oracle.kv.impl.api.table.DDLGenerator;
import oracle.kv.impl.api.table.FieldDefFactory;
import oracle.kv.impl.api.table.FieldDefImpl;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.IndexImpl.AnnotatedField;
import oracle.kv.impl.api.table.MapBuilder;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RegionMapper;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.SequenceDefImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableBuilderBase;
import oracle.kv.impl.api.table.TableEvolver;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TableMetadataHelper;
import oracle.kv.impl.query.compiler.CompilerAPI;
import oracle.kv.impl.query.compiler.Translator.IdentityDefHelper;
import oracle.kv.query.PrepareCallback;
import oracle.kv.query.PrepareCallback.QueryOperation;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Table;
import oracle.kv.table.TimeToLive;
import oracle.kv.table.FieldDef.Type;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases that operate transiently on TableDdl.  The general pattern is to
 * parse a DDL statement and validate that the results are as expected.  No
 * store or persistence is required.  In the case where indexes and child tables
 * are added to existing tables, or tables are altered (evolved) the tests use
 * a transient TableMetadata instance to mimic what an admin might have.
 */
public class DdlSyntaxTest extends TestBase {

    TableMetadata metadata;

    /*
     * Shared tables
     */
    final static String userTableStatement =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING, age INTEGER," +
        "primary key (id))";
    final static String addressTableStatement =
        "CREATE TABLE Users.address" +
        "(type ENUM(home, work, other)," +
        "streetName STRING, city STRING, streetNumber INTEGER, zip INTEGER," +
        "primary key (type))";

    /*
     * CREATE TABLE
     */
    final static String simpleUserTable =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING, primary key (id))";
    final static String simpleChildTable =
        "CREATE TABLE Users.address" +
        "(type INTEGER, streetName STRING, city STRING, primary key (type))";
    final static String tableWithConstraints =
        "CREATE TABLE Constraints" +
        "(" +
        "ival INTEGER,"+
        "lval LONG," +
        "fval FLOAT," +
        "dval DOUBLE DEFAULT 78.5," +
        "sval STRING DEFAULT \"xyz\" ," +
        "day ENUM (monday, tuesday, wednesday)," +
        "bval BINARY," +
        "fixedbval BINARY(5)," +
        "tsval0 TIMESTAMP(0) default 1000, " +
        "tsval3 TIMESTAMP(3) default \"1970-01-01T00:00:00\", " +
        "tsval9 TIMESTAMP(9) default \"-6383-01-01\", " +
        "PRIMARY KEY (SHARD(ival), lval, fval)" +
        ")";

    final static String shardTable =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING," +
        "primary key (shard(id),firstName, lastName))";
    final static String badChildWithShard =
        "CREATE TABLE Users.address" +
        "(type INTEGER, streetName STRING, city STRING, " +
        "primary key (shard(type), city))";
    final static String pkeyIsShardTable =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING," +
        "primary key (shard(id)))";
    final static String pkeyIsShardTable1 =
        "CREATE TABLE Users" +
        "(id INTEGER, firstName STRING, lastName STRING," +
        "primary key (shard(id, firstName)))";
    final static String pkeyBadShardTable =
        "CREATE TABLE Users" +
        "(id INTEGER, primary key ())";
    final static String pkeyBadShardTable1 =
        "CREATE TABLE Users" +
        "(id INTEGER, primary key (shard(id), id))";
    /* make sure trailing white space is OK */
    static final String createWhitespace =
        "CREATE TABLE ws(id INTEGER, PRIMARY KEY(id))   \r\n\t";
    /* range in array value */
    static final String rangeInArray =
        "CREATE TABLE range(" +
        "id INTEGER," +
        "PRIMARY KEY(id), " +
        "ary ARRAY(INTEGER))";
    /* binary/fixed type with default value and not null constraint */
    static final String withBinaryAndFixed =
        "CREATE TABLE withBinaryAndFixed (" +
                "id INTEGER, " +
                "b BINARY DEFAULT 'Tk9ORQ==', " +
                "bn BINARY NOT NULL DEFAULT 'Tk9UTk9ORQ==', " +
                "fb BINARY(10) DEFAULT 'Tk9ORTEyMzQ1Ng=='," +
                "fbn BINARY(10) NOT NULL DEFAULT 'Tk9UTk9ORTEyMw=='," +
                "PRIMARY KEY(id))";

    /*
     * DROP TABLE
     */
    static final String dropAddress = "DROP TABLE users.address";
    static final String dropUsers = "DROP TABLE IF EXISTS users";
    static final String[] dropTableStatements = {dropAddress, dropUsers};

    /*
     * CREATE INDEX
     */
    static final String createCityIndex =
        "CREATE INDEX city ON users.address (city) COMMENT \"city index\"";
    static final String createLastNameIndex =
        "CREATE INDEX lastName ON users(lastName)";
    static final String createIfNotExists =
        "CREATE INDEX IF NOT EXISTS foo ON bar(x)";
    static final String createMapIndex1 =
        "CREATE INDEX map1 ON users(addresses.keys(), addresses.values().city)";
    static final String createMapIndex2 =
        "CREATE INDEX map1 ON users(addresses.city)";

    static final String[] createIndexStatements =
    {createCityIndex, createLastNameIndex, createIfNotExists,
     createMapIndex1, createMapIndex2};

    /*
     * CREATE FULLTEXT INDEX
     */
    static final String createAddressTextIndex =
        "CREATE FULLTEXT INDEX addresses ON users.address (city, streetName " +
        "{\"type\" : \"string\", \"ignore_above\": 80 }, " +
        "type {\"type\" : \"integer\", \"analyzer\": \"numerical\"}) " +
        "ES_SHARDS = 3 ES_REPLICAS = 1 " + " OVERRIDE " +
        "COMMENT \"addresses\"";

    /*
     * Above statement should produce these fields. Order of JSON pairs
     * is different from in the statement.
     */
    static final AnnotatedField[] createAddressTextIndexFields = {
        new AnnotatedField("city", null),
        new AnnotatedField("streetName",
                           "{\"ignore_above\":80,\"type\":\"string\"}"),
        new AnnotatedField("type",
                           "{\"analyzer\":\"numerical\",\"type\":\"integer\"}")
    };

    /*
     * DROP INDEX
     */
    static final String dropCityIndex =
        "DROP INDEX city ON users.address";
    static final String dropLastNameIndex =
        "DROP INDEX lastName ON users";
    static final String dropIfExists =
        "DROP INDEX IF EXISTS foo ON bar";
    static final String dropWithOverride =
        "DROP INDEX foo ON bar OVERRIDE";
    static final String[] dropIndexStatements =
    {dropCityIndex, dropLastNameIndex, dropIfExists, dropWithOverride};

    /*
     * ALTER TABLE
     */
    static final String alterAddFloat = "ALTER TABLE users (ADD f FLOAT)";
    static final String alterAddIdentityAllways = "ALTER TABLE users (ADD i1 " +
        "INTEGER  GENERATED ALWAYS AS IDENTITY)";
    static final String alterAddIdentityByDef = "ALTER TABLE users (ADD i2 " +
        "INTEGER  GENERATED BY DEFAULT AS IDENTITY)";
    static final String alterAddIdentityOnNull = "ALTER TABLE users (ADD i3 " +
        "INTEGER  GENERATED BY DEFAULT ON NULL AS IDENTITY)";
    static final String alterDropMultiple =
        "ALTER TABLE users.address (DROP streetNumber, DROP city)";
    static final String alterDropAge = "ALTER TABLE users (drop age)";

    /*
     * DESCRIBE
     */
    static final String describeUsers = "DESCRIBE TABLE users";
    static final String describeAddress = "DESCRIBE TABLE users.address";
    static final String describeAddressAsJson =
        "DESCRIBE AS JSON TABLE users.address";
    static final String describeField =
        "DESCRIBE TABLE users (firstName, lastName)";
    static final String describeTables = "SHOW TABLES";
    static final String describeTablesAsJson = "SHOW AS JSON TABLES";
    static final String describeIndex =
        "DESCRIBE INDEX LastName ON users";
    static final String describeIndexAsJson =
        "DESCRIBE AS JSON INDEX City ON users.address";

    static final String[] describeStatements = {
        describeUsers, describeAddress, describeAddressAsJson,
        describeField, describeTables, describeTablesAsJson,
        describeIndex, describeIndexAsJson
    };

    /*
     * COMMENT
     */
    static final String comment1 =
        "CREATE TABLE foo1 COMMENT \"fooTable\" (id INTEGER, PRIMARY KEY(id))";
    static final String comment2 =
        "CREATE TABLE foo2(id INTEGER COMMENT \"ok\", PRIMARY KEY(id))";
    static final String comment3 =
        "CREATE TABLE foo3(id INTEGER, full_name RECORD(name STRING COMMENT " +
        "\"first name\", last STRING) COMMENT \"a record\"," +
        " PRIMARY KEY (id))";
    static final String comment4 =
        "CREATE TABLE foo4(id INTEGER COMMENT \"ok\"," +
        "PRIMARY KEY(id))";
    static final String comment5 =
        "CREATE TABLE foo5 COMMENT \"table comment\" "+
         "(id INTEGER, full_name RECORD(name STRING COMMENT \"first name\", " +
        "last STRING," +
        "nested RECORD(i INTEGER) COMMENT \"nested\") COMMENT \"a record\"," +
        " PRIMARY KEY (id))";

    /*
     * Incorrect Syntax statements.  These all result in ParseException.
     */
    /* no table definition */
    static final String badSyntax1 = "CREATE TABLE foo";
    /* bad type */
    static final String badSyntax2 =
        "CREATE TABLE foo1(id INT, PRIMARY KEY(id))";
    /* bad field name */
    static final String badSyntax3 =
        "CREATE TABLE foo2(6d INTEGER, PRIMARY KEY(6d))";
    /* bad table name */
    static final String badSyntax4 =
        "CREATE TABLE _foo3(id INTEGER, PRIMARY KEY (id))";
    /* bad child table name */
    static final String badSyntax5 =
        "CREATE TABLE users.8foo4(name STRING, PRIMARY KEY (name))";
    /* trailing junk in statement */
    static final String badSyntax6 =
        "CREATE TABLE foo5(id INTEGER, PRIMARY KEY (id))x";
    /* missing closing ")" */
    static final String badSyntax7 = "CREATE INDEX i ON foo(a,b";
    /* bad enumeration name */
    static final String badSyntax8 =
        "CREATE TABLE foo6(e1 ENUM(1), PRIMARY KEY (e1))";
    /* bad enumeration name */
    static final String badSyntax9 =
        "CREATE TABLE foo7(e1 ENUM(a, a1, _5), PRIMARY KEY (e1))";
    /* bad default value. It should be an ID, not STRING */
    static final String badSyntax10 =
        "CREATE TABLE foo8(e1 ENUM(a, a1) DEFAULT \"a\", PRIMARY KEY(e1))";
    /* bad default value for string, should be STRING */
    static final String badSyntax11 =
        "CREATE TABLE foo9(name STRING DEFAULT bad, PRIMARY KEY (name))";
    /* index missing field list */
    static final String badSyntax12 = "CREATE INDEX i ON foo";
    /* bad comment -- should be STRING */
    static final String badSyntax14 =
        "CREATE TABLE foo10(id INTEGER, COMMENT bad, PRIMARY KEY(id))";
    /* bad comment -- missing STRING */
    static final String badSyntax15 =
        "CREATE TABLE foo11(id INTEGER COMMENT, PRIMARY KEY(id))";
    /* bad comment -- 2 comments for table comment */
    static final String badSyntax16 =
        "CREATE TABLE foo12(id INTEGER, COMMENT \"x\" " +
        "COMMENT \"y\", PRIMARY KEY(id))";
    /* bad comment -- 2 comments for field comment */
    static final String badSyntax17 =
        "CREATE TABLE foo13(id INTEGER COMMENT \"x\" " +
        "COMMENT \"y\", PRIMARY KEY(id))";
    /* 2 primary keys */
    static final String badSyntax18 =
        "CREATE TABLE foo14(id INTEGER, PRIMARY KEY(id), PRIMARY KEY(id))";
    /* invalid use of elementof[] (empty) */
    static final String badSyntax19 =
        "CREATE INDEX idx on foo ([])";
    /* default value in array not allowed */
    static final String badSyntax20 =
        "CREATE TABLE range(id INTEGER, PRIMARY KEY(id), " +
        "ary ARRAY(INTEGER DEFAULT 10))";
    static final String badSyntax21 =
        "CREATE TABLE range(id INTEGER DEFAULT 7 GENERATED ALWAYS AS " +
            "IDENTITY, name STRING, PRIMARY KEY(id))";
    static final String badSyntax22 =
        "CREATE TABLE range(id INTEGER GENERATED BY DEFAULT AS " +
            "IDENTITY DEFAULT 5, name STRING, PRIMARY KEY(id))";
    static final String badSyntax23 =
        "CREATE TABLE range(id INTEGER GENERATED BY DEFAULT ON NULL AS " +
            "IDENTITY DEFAULT 5, name STRING, PRIMARY KEY(id))";
    static final String badSyntax24 =
        "ALTER TABLE users (ADD i1 INTEGER DEFAULT 10 GENERATED ALWAYS AS " +
            "IDENTITY )";
    static final String badSyntax25 =
        "ALTER TABLE users (ADD i1 INTEGER GENERATED ALWAYS AS " +
            "IDENTITY DEFAULT 20 )";

    /*
     * Incorrect semantics statements.  These are detected by table
     * construction after the parse and result in a DdlException that is
     * not a ParseException.
     */
    /* no parent table */
    static final String badTable1 =
        "CREATE TABLE foo.bar(id INTEGER, PRIMARY KEY (id))";
    /* primary key is a nonexistent field*/
    static final String badTable2 =
        "CREATE TABLE foo(name STRING, PRIMARY KEY (id))";
    /* primary key contains a nonexistent field */
    static final String badTable3 =
        "CREATE TABLE foo(name STRING, PRIMARY KEY (name, id))";
    /* primary key has the same field twice */
    static final String badTable4 =
        "CREATE TABLE foo(name STRING, last STRING, " +
        "PRIMARY KEY (SHARD(name), last, name))";
    /* alter a table that doesn't exist */
    static final String badTable6 = "ALTER TABLE foo(ADD id INTEGER)";
    /* bad default value for enumeration, not in enum */
    static final String badTable7 =
        "CREATE TABLE foo(e1 ENUM(a, a1) DEFAULT z, PRIMARY KEY(e1))";
    /* duplicated primary key field from parent table */
    static final String badTable8 =
        "CREATE TABLE Users.foo" +
        "(eid INTEGER, street STRING, city STRING," +
        "primary key (id, eid))";
    /* same as above, but change the order of the key */
    static final String badTable9 =
        "CREATE TABLE Users.foo" +
        "(eid INTEGER, street STRING, city STRING," +
        "primary key (eid, id))";

    /*
     * Statements that put a size constraint on a primary key field.
     */
    static final String pkeySizeParent =
        "CREATE TABLE UsersWithSize (id INTEGER, name STRING, " +
        "PRIMARY KEY(id(2)))";

    static final String pkeySize1 =
        "CREATE TABLE foo (id INTEGER, PRIMARY KEY(id(3)))";
    static final String pkeySize2 =
        "CREATE TABLE foo (id INTEGER, id1 INTEGER, " +
        "PRIMARY KEY(id(3),id1(1)))";
    static final String pkeySize3 =
        "CREATE TABLE foo (id INTEGER, id1 INTEGER, " +
        "PRIMARY KEY(shard(id),id1(1)))";
    static final String pkeySize4 =
        "CREATE TABLE foo (id INTEGER, id1 INTEGER, " +
        "PRIMARY KEY(shard(id(1)),id1))";
    static final String pkeyChildSize1 =
        "CREATE TABLE Users.foo (id1 INTEGER, id2 INTEGER, " +
        "PRIMARY KEY(id1, id2(4)))";
    static final String pkeyChildSize2 =
        "CREATE TABLE UsersWithSize.foo (id1 INTEGER, id2 INTEGER, " +
        "PRIMARY KEY(id1, id2(4)))";

    /* size 5 is ignored because it's the max */
    static final String pkeySizeIgnored =
        "CREATE TABLE foo (id INTEGER, PRIMARY KEY(id(5)))";

    /* Failure conditions */
    /* bad size */
    static final String badPkeySize1 =
        "CREATE TABLE foo (id INTEGER, PRIMARY KEY(id(6)))";
    static final String badPkeyType1 =
        "CREATE TABLE foo (id STRING, PRIMARY KEY(id(3)))";

    static final String[] primaryKeySizeStatements = {
        pkeySize1, pkeySize2, pkeySize3, pkeySize4,
        pkeyChildSize1, pkeyChildSize2
    };

    static final String[] badPrimaryKeySizeStatements = {
        badPkeySize1, badPkeyType1
    };

    /*
     * Statements that should fail because of syntax problems
     */
    static final String[] badSyntaxStatements = {
        badSyntax1, badSyntax2, badSyntax3, badSyntax4, badSyntax5,
        badSyntax6,
        badSyntax7, badSyntax8, badSyntax9, badSyntax10, badSyntax11,
        badSyntax12, badSyntax14, badSyntax15, badSyntax16,
        badSyntax17, badSyntax18, badSyntax19, badSyntax20, badSyntax21,
        badSyntax22, badSyntax23, badSyntax24, badSyntax25
    };

    /*
     * Statements with correct syntax, but fail because of table semantic rules
     */
    static final String[] badTableStatements = {
        badTable1, badTable2, badTable3, badTable4, badTable6,
        badTable7, badTable8, badTable9
    };

    /*
     * Statements with COMMENTs
     */
    static final String[] commentStatements = {
        comment1, comment2, comment3, comment4, comment5
    };

    /**
     * These next functions will override those from the TestBase class, which
     * are not needed.
     */
    @Override
    @Before
    public void setUp()
        throws Exception {

        resetMetadata();
    }

    @Override
    @After
    public void tearDown()
        throws Exception {

        /* nothing yet */
    }

    @Test
    public void testCreateTable()
        throws Exception {

        /*
         * Create a simple, transient table.  Deliberately use field names
         * that don't match the DDL statement in terms of case to make sure that
         * case-insensitivity works.
         */
        TableImpl userTable = TableBuilder.createTableBuilder("users")
            .addInteger("id")
            .addString("firstname")
            .addString("lastname")
            .primaryKey("id")
            .buildTable();

        DdlHandler ddl = parse(simpleUserTable);
        TableImpl ddlTable = ddl.getTable();
        assertTablesEqual(ddlTable, userTable);

        /*
         * Try a parent-child relationship
         */
        userTable = addToMetadata(userTable);

        TableImpl addressTable =
            TableBuilder.createTableBuilder(null, "address", null,
                                            userTable, metadata.getRegionMapper())
            .addInteger("type")
            .addString("streetname")
            .addString("city")
            .primaryKey("type")
            .buildTable();
        ddl = parse(simpleChildTable);
        ddlTable = ddl.getTable();
        assertTablesEqual(ddlTable, addressTable);

        /*
         * Table with interesting constraints
         */
        ddl = parse(tableWithConstraints);

        if (!ddl.getSuccess()) {
            System.out.println(ddl.getException());
            assertTrue(false);
        }

        ddlTable = ddl.getTable();

        ddl = parse(shardTable);
        assertTrue(ddl.getSuccess());
        ddlTable = ddl.getTable();

        ddl = parse(createWhitespace);
        assertTrue(ddl.getSuccess());
        ddlTable = ddl.getTable();

        ddl = parse(rangeInArray);

        if (!ddl.getSuccess()) {
            System.out.println(ddl.getException());
            assertTrue(false);
        }

        ddlTable = ddl.getTable();

        ddl = parse(badChildWithShard);
        assertFalse(ddl.getSuccess());

        /*
         * Cases where the user redundantly specifies a shard key equal to the
         * primary key.
         */
        ddl = parse(pkeyIsShardTable);
        assertTrue(ddl.getSuccess());
        ddl = parse(pkeyIsShardTable1);
        assertTrue(ddl.getSuccess());

        /* try a couple of bad primary key definitions */
        ddl = parse(pkeyBadShardTable);
        assertFalse(ddl.getSuccess());
        ddl = parse(pkeyBadShardTable1);
        assertFalse(ddl.getSuccess());

        /*
         * create a table contains binary and fixed binary types with default
         * value/not null constraint
         */
        ddl = parse(withBinaryAndFixed);
        assertTrue(ddl.getSuccess());
        TableImpl table = ddl.getTable();
        RowImpl row = table.createRow();
        row.put("id", 1);
        row.addMissingFields();

        for (String field : row.getFieldNames()) {
            if (field.equals("id")) {
                continue;
            }

            FieldValue fv = row.get(field);
            FieldDef fdef = row.getDefinition().getFieldDef(field);
            assertFalse(fv == null || fv.isNull());

            if (fv.isBinary()) {
                assertTrue(fdef.isBinary());
            } else if (fv.isFixedBinary()) {
                assertTrue(fdef.isFixedBinary());
                assertTrue(fdef.asFixedBinary().getSize() ==
                           fv.asFixedBinary().get().length);
            }

            FieldValue defVal = table.getDefaultValue(field);
            assertTrue(defVal != null);
            assertTrue(fv.equals(defVal));

            if (field.indexOf("n") > 0) {
                assertFalse(table.isNullable(field));
            } else {
                assertTrue(table.isNullable(field));
            }
        }
    }

    @Test
    public void testCreateTableWithTTL()
        throws Exception {
        testTTLClause("42 hours", 42, TimeUnit.HOURS);
        testTTLClause("42 days",  42, TimeUnit.DAYS);
        testTTLClause("42 h",     42, TimeUnit.HOURS);
        testTTLClause("42 d",     42, TimeUnit.DAYS);

        badTTLClause("42 seconds");
        badTTLClause("42hours");
        badTTLClause("42hurs");
        badTTLClause("42day");
        badTTLClause("42daysrs");
     }

    @Test
    public void testAlterTableWithTTL()
        throws Exception {

        addTable(userTableStatement);
        DdlHandler ddl = parse(userTableStatement);
        Table table = ddl.getTable();
        Assert.assertNotNull(table);

        ddl = parse("ALTER TABLE Users USING TTL 20 hours");
        table = ddl.getTable();
        Assert.assertNotNull(table);
        Assert.assertEquals(TimeToLive.ofHours(20), table.getDefaultTTL());
    }

    @Test
    public void testAlterTableWithInvalidTTLValueRaisesError()
        throws Exception {

        addTable(userTableStatement);
        DdlHandler ddl = parse(userTableStatement);
        Table table = ddl.getTable();
        Assert.assertNotNull(table);
        ddl = parse("ALTER TABLE Users USING TTL 2555555555 hours");
        Assert.assertNotNull(ddl.getErrorMessage());
        Assert.assertTrue(ddl.getErrorMessage().indexOf("TTL value") != -1);
    }

    @Test
    public void testAlterTableWithInvalidTTLUnitRaisesError()
        throws Exception {

        addTable(userTableStatement);
        DdlHandler ddl = parse(userTableStatement);
        Table table = ddl.getTable();
        Assert.assertNotNull(table);
        ddl = parse("ALTER TABLE Users USING TTL 25 seconds");
        Assert.assertNotNull(ddl.getErrorMessage());
        Assert.assertTrue(ddl.getErrorMessage().indexOf("TTL Unit") != -1);
    }



    /**
     * Parse a CREATE TABLE... statement with USING TTL... clause.
     *
     * @param ttlString  clause to parse.
     * @param expectedTTL expected duration after parse
     * @param expectedUnit expected TimeUnit after parse
     */
    private void testTTLClause(String ttlString, int expectedTTL,
                               TimeUnit expectedUnit) {
        String stmt = "CREATE TABLE Users (id INTEGER, primary key (id)) "
                + "USING TTL " + ttlString;
        DdlHandler ddl = parse(stmt);
        assertTrue(ddl.getErrorMessage(), ddl.getSuccess());
        Table table = ddl.getTable();
        assertTrue(table.getDefaultTTL().getValue() == expectedTTL);
        assertTrue(expectedUnit.equals(table.getDefaultTTL().getUnit()));
    }

    private void badTTLClause(String clause) {
        String stmt = "CREATE TABLE Users (id INTEGER, primary key (id)) "
                + "USING TTL " + clause;
        DdlHandler ddl = parse(stmt);
        assertTrue("Expected to fail parsing TTL "+ clause,  !ddl.getSuccess());
    }


    @Test
    public void testDropTable()
        throws Exception {

        /*
         * NOTE: parsing of drop table statements is syntax only and does
         * no validation of the table and/or index fields.
         */
        for (String statement : dropTableStatements) {
            DdlHandler ddl = parse(statement);
            assertTrue(ddl.isTableDrop());
        }
    }

    @Test
    public void testCreateIndex()
        throws Exception {

        /*
         * NOTE: parsing of create index statements is syntax only and does
         * no validation of the table and/or index fields.
         */
        for (String statement : createIndexStatements) {
            DdlHandler ddl = parse(statement);
            createIndexAsserts(ddl);
        }
    }

    @Test
    public void testCreateTextIndex()
        throws Exception {

        DdlHandler ddl = parse(createAddressTextIndex);

        createIndexAsserts(ddl);

        /* In addition, verify fields were parsed correctly. */
        DdlOperation ddlop = ddl.getDdlOp();
        assertTrue(ddlop.getClass().equals(CreateIndex.class));
        CreateIndex ci = (CreateIndex) ddlop;

        List<AnnotatedField> fields =
            Arrays.asList(createAddressTextIndexFields);

        assertTrue(ci.compareAnnotatedFields(fields));
    }

    private void createIndexAsserts(DdlHandler ddl) {
        assertTrue(ddl.isIndexAdd());
    }

    @Test
    public void testDropIndex()
        throws Exception {

        /*
         * NOTE: parsing of drop index statements is syntax only and does
         * no validation of the table and/or index fields.
         */
        for (String statement : dropIndexStatements) {
            DdlHandler ddl = parse(statement);
            dropIndexAsserts(ddl);
        }
    }

    private void dropIndexAsserts(DdlHandler ddl) {
        assertTrue(ddl.isIndexDrop());
    }

    @Test
    public void testAlterTable()
        throws Exception {

        addTable(userTableStatement);
        addTable(addressTableStatement);

        DdlHandler ddl = parse(alterAddFloat);
        assertNotNull(ddl.getTable());

        ddl = parse(alterDropAge);
        assertNotNull(ddl.getTable());
        assertNull(ddl.getTable().getField("age"));

        /*
         * Remove multiple fields from address table.  Assert starting state.
         */
        TableImpl addressTable = getTable("users.address");
        assertNotNull(addressTable.getField("streetNumber"));
        assertNotNull(addressTable.getField("city"));

        ddl = parse(alterDropMultiple);
        assertNotNull(ddl.getTable());
        assertNull(ddl.getTable().getField("streetNumber"));
        assertNull(ddl.getTable().getField("city"));
    }

    @Test
    public void testAlterTableIdentity()
        throws Exception {

        addTable(userTableStatement);
        addTable(addressTableStatement);

        DdlHandler ddl = parse(alterAddIdentityAllways);
        assertNotNull(ddl.getTable());

        ddl = parse(alterAddIdentityByDef);
        assertNotNull(ddl.getTable());

        ddl = parse(alterAddIdentityOnNull);
        assertNotNull(ddl.getTable());

    }

    @Test
    public void testDescribe()
        throws Exception {

        addTable(userTableStatement);
        addTable(addressTableStatement);

        for (String statement : describeStatements) {
            DdlHandler ddl = parse(statement);
            assertTrue(statement + ": " +
                       ddl.getErrorMessage(),
                       (ddl.isDescribe() | ddl.isShow()));
        }

        /*
         * SHOW TABLES is the alternative.
         */
        DdlHandler ddl = parse("SHOW TABLES");
        assertTrue(ddl.isShow());
        assertNull(ddl.getTable());
    }

    /**
     * This test is fairly manual in that it doesn't currently validate the
     * resulting tables.  Doing so would cause it to expand because it would not
     * be possible to use looping code without more framework.
     */
    @Test
    public void testComment()
        throws Exception {

        DdlHandler ddl;
        for (String statement : commentStatements) {
            ddl = parse(statement);
            assertNotNull(statement, ddl.getTable());
        }

        /*
         * Test table comment, comment3 has one.
         */
        ddl = parse(comment5);
        TableImpl table = ddl.getTable();
        assertTrue("Comment mismatch",
                   "table comment".equals(table.getDescription()));
    }

    /**
     * Tests bad statements and related error handling.
     * Distinguish between parse errors and those which are based on table
     * semantics.  This is done by looking at the exceptions thrown.
     */
    @Test
    public void testBadStatements()
        throws Exception {

        addTable(userTableStatement);

        for (String statement : badSyntaxStatements) {
            DdlHandler ddl = parse(statement);
            assertFalse(statement, ddl.getSuccess());
        }

        for (String statement : badTableStatements) {
            DdlHandler ddl = parse(statement);
            assertFalse(statement, ddl.getSuccess());
        }
    }

    @Test
    public void testPrimaryKeySize()
        throws Exception {

        /*
         * These are used to test parent/child tables
         */
        addTable(userTableStatement);
        addTable(pkeySizeParent);

        for (String statement : primaryKeySizeStatements) {
            DdlHandler ddl = parse(statement);
            assertTrue(ddl.getTable().getPrimaryKeySizes() != null);
        }

        for (String statement : badPrimaryKeySizeStatements) {
            DdlHandler ddl = parse(statement);
            assertFalse(statement, ddl.getSuccess());
        }

        /* assert that a size of 5 is ignored */
        DdlHandler ddl = parse(pkeySizeIgnored);
        assertTrue(ddl.getTable().getPrimaryKeySizes() == null);
    }

    /**
     * Tests for DDLGenerator class. Compares the TableImpl built by
     * TableBuilder.createTableBuilder(). buildTable() and the one created by
     * DDLGenerator class.
     */
    @Test
    public void testDDLGenerator()
        throws Exception {

        /*
         * Build a simple table
         */
        TableImpl tableBuilt1 = TableBuilder.createTableBuilder("table1")
                .addInteger("id")
                .addString("firstname")
                .addString("lastname")
                .primaryKey("id")
                .buildTable();

        assertDdlEquality(tableBuilt1);

        /*
         * Add table1 to table metadata
         */
        addToMetadata(tableBuilt1);

        /*
         * Create table table2 - child table of table1
         */
        TableImpl tableBuilt2 =
            TableBuilder.createTableBuilder(null, "table2", null,
                                            tableBuilt1,
                                            metadata.getRegionMapper())
            .addInteger("type")
            .addString("city")
            .addString("streetname")
            .primaryKey("type")
            .buildTable();

        assertDdlEquality(tableBuilt2);

        /*
         * Create table table3 with default and not null value for age(Integer)
         * firstname(String) and types(Enum) fields
         */
        TableImpl tableBuilt3 = TableBuilder.createTableBuilder("table3")
            .addInteger("id")
            .addInteger("age", null, false, 20)
            .addString("firstname", null, false, "na")
            .addEnum("types", new String[]{"home", "work"}, null, false, "home")
            .primaryKey("id")
            .buildTable();

        assertDdlEquality(tableBuilt3);

        /*
         * Create a complex table:
         *
         * CREATE TABLE complex (
         * COMMENT "Table Description"
         * id INTEGER,
         * PRIMARY KEY (id),
         * nestedMap MAP(RECORD( m MAP(FLOAT), a ARRAY(RECORD(age INTEGER)))),
         * address RECORD (street INTEGER, streetName STRING, city STRING, \
         *                 zip INTEGER COMMENT "zip comment"),
         * friends MAP (STRING),
         * floatArray ARRAY (FLOAT),
         * aFixedBinary BINARY(5),
         * days ENUM(mon, tue, wed, thur, fri, sat, sun) NOT NULL DEFAULT tue
         *           COMMENT "Enum Values"
         * )
         * USING TTL 5 HOURS
         */
        String[] enumValues = {"mon", "tue", "wed", "thu", "fri"};
        FieldDef arrayDef1 = TableBuilder.createArrayBuilder()
                .addFloat().build();
        FieldDef mapDef1 = TableBuilder.createMapBuilder()
                .addString().build();

        FieldDef mapDef2 = TableBuilder.createMapBuilder()
                .addFloat().build();
        FieldDef recordDef1 = TableBuilder.createRecordBuilder("rec1")
                .addInteger("age").build();
        FieldDef arrayDef2 = TableBuilder.createArrayBuilder()
                .addField(recordDef1).build();
        FieldDef recordDef2 = TableBuilder.createRecordBuilder("rec2")
                .addField("m", mapDef2)
                .addField("a", arrayDef2)
                .build();
        FieldDef mapDef3 = TableBuilder.createMapBuilder()
                .addField(recordDef2)
                .build();
        FieldDef recordDef3 = TableBuilder.createRecordBuilder("rec3")
                .addInteger("street")
                .addString("streetName")
                .addString("city")
                .addInteger("zip", "zip comment", true, null)
                .build();

        TimeToLive ttl = TimeToLive.ofHours(5);

        TableBuilder tableBuilder = (TableBuilder) TableBuilder
            .createTableBuilder(null, "table4", "Table Description",
                                null, metadata.getRegionMapper())
                .addInteger("id")
                .primaryKey("id")
                .addFixedBinary("aFixedBinary", 5)
                .addEnum("days", enumValues, "Enum Values", false, "tue")
                .addField("floatArray", arrayDef1)
                .addField("friends", mapDef1)
                .addField("nestedMap", mapDef3)
                .addField("address", recordDef3);

        tableBuilder.setDefaultTTL(ttl);
        TableImpl tableBuilt4 = tableBuilder.buildTable();
        assertDdlEquality(tableBuilt4);

        /*
         * Create table table5 with primary key size for id1 4
         */
        TableImpl tableBuilt5 = TableBuilder.createTableBuilder("table5")
                .addInteger("id1")
                .addString("id2")
                .primaryKey("id1")
                .primaryKeySize("id1", 4)
                .buildTable();

        assertDdlEquality(tableBuilt5);

        /*
         * Create table table6 with primary key size for id1 4, id3 5
         * (should default to 0 i.e. no size restriction) and id4 with no size
         * restriction
         */
        TableImpl tableBuilt6 = TableBuilder.createTableBuilder("table6")
                .addInteger("id1")
                .addString("id2")
                .addInteger("id3")
                .addInteger("id5")
                .primaryKey("id1", "id3", "id5")
                .primaryKeySize("id1", 4)
                .primaryKeySize("id3", 5)
                .buildTable();

        assertDdlEquality(tableBuilt6);

        /*
         * Add table6 to table metadata
         */
        addToMetadata(tableBuilt6);

        /*
         * Create table table7 which is a child table of table6. Primary key
         * size of id6 and id8 is 2.
         */
        TableImpl tableBuilt7 = TableBuilder
            .createTableBuilder(null, "table7", null, tableBuilt6,
                                metadata.getRegionMapper())
                .addInteger("id6")
                .addString("id7")
                .addInteger("id8")
                .primaryKey("id6", "id8")
                .primaryKeySize("id6", 2)
                .primaryKeySize("id8", 2)
                .buildTable();

        assertDdlEquality(tableBuilt7);

        /*
         * Build a table with Timestamp and Map of enums.
         */
        Timestamp timestampDef = new Timestamp(0);
        /* map {Enum} */
        MapBuilder ab = TableBuilder.createMapBuilder();
        MapBuilder mapBuilderEnum = (MapBuilder)ab.addEnum("enumField",
                                                           enumValues, null);
        TableImpl tableBuilt8 = TableBuilder.createTableBuilder("table8")
                .addInteger("id")
                .addString("firstname")
                .addString("lastname")
                .addTimestamp("timestampField", 9, null, null, timestampDef)
                .addField("mapFieldEnum", mapBuilderEnum.build())
                .primaryKey("id")
                .buildTable();

        assertDdlEquality(tableBuilt8);
    }

    /**
     * Tests the Text and Secondary indexes created by DDLGenerator
     */
    @Test
    public void testIndexDDLGenerator() {

        /*
         * Test1: Text Index with no properties and annotations
         */
        TableImpl tableBuilt1 = TableBuilder.createTableBuilder("table1")
                .addInteger("id1")
                .addString("id2")
                .primaryKey("id1")
                .buildTable();

        List<String> indexFields = new ArrayList<String>();
        indexFields.add("id1");
        indexFields.add("id2");
        String indexName = "TextIndex";
        String indexDescription = "TextIndexDescription";

        tableBuilt1.addIndex(new IndexImpl(indexName, tableBuilt1, indexFields,
                                           null, true, false,
                                           new HashMap<String, String>(),
                                           new HashMap<String, String>(),
                                           indexDescription));

        assertDdlEquality(tableBuilt1);
        DDLGenerator ddlGenerator = new DDLGenerator(tableBuilt1, null);

        List<String> textIndexDDLs = ddlGenerator.getAllIndexDDL();
        assertTrue(textIndexDDLs.size() == 1);

        DdlHandler ddlHandler = parse(textIndexDDLs.get(0));
        assertTrue(ddlHandler.isIndexAdd());

        /*
         * Test2: Text Index with properties and annotations
         */
        TableImpl tableBuilt2 = TableBuilder.createTableBuilder("table2")
                .addInteger("id1")
                .addString("id2")
                .primaryKey("id1")
                .buildTable();

        HashMap<String, String> annotations = new HashMap<String, String>();
        String annotation = "{\"type\" : \"integer\"}";
        annotations.put("id1", annotation);
        annotations.put("id2", annotation);

        HashMap<String, String> properties = new HashMap<String, String>();
        properties.put("ES_SHARDS", "3");
        properties.put("ES_REPLICAS", "1");

        tableBuilt2.addIndex(new IndexImpl(indexName, tableBuilt2, indexFields,
                                           null, true, false,
                                           annotations, properties,
                                           indexDescription));

        assertDdlEquality(tableBuilt2);
        ddlGenerator = new DDLGenerator(tableBuilt2, null);

        textIndexDDLs = ddlGenerator.getAllIndexDDL();
        assertTrue(textIndexDDLs.size() == 1);

        ddlHandler = parse(textIndexDDLs.get(0));
        assertTrue(ddlHandler.isIndexAdd());

        /*
         * Test3: Secondary Indexes -- simple and complex
         */
        TableImpl tableBuilt3 = TableBuilder.createTableBuilder("table3")
            .addInteger("id1")
            .addString("id2")
            .addField("arrayOfString", TableBuilder.createArrayBuilder()
                      .addString().build())
            .addField("mapOfInt", TableBuilder.createMapBuilder()
                      .addInteger().build())
            .primaryKey("id1")
            .buildTable();

        tableBuilt3.addIndex(new IndexImpl(indexName, tableBuilt3, indexFields,
            null, indexDescription));

        tableBuilt3.addIndex(new IndexImpl("arrayIndex", tableBuilt3,
                                           makeIndexList("arrayOfString[]"),
                                           null, null));
        tableBuilt3.addIndex(new IndexImpl("mapKeyIndex", tableBuilt3,
                                           makeIndexList("mapOfInt.keys()"),
                                           null, null));
        tableBuilt3.addIndex(new IndexImpl("mapValueIndex", tableBuilt3,
                                           makeIndexList("mapOfInt.values()"),
                                           null, null));
        tableBuilt3.addIndex(new IndexImpl("mapKeyValueIndex", tableBuilt3,
                                           makeIndexList("mapOfInt.keys()",
                                                         "mapOfInt.values()"),
                                           null, null));

        assertDdlEquality(tableBuilt3);
        ddlGenerator = new DDLGenerator(tableBuilt3, null);

        List<String> secondaryIndexDDLs = ddlGenerator.getAllIndexDDL();
        assertTrue(secondaryIndexDDLs.size() == 5);

        for (int i = 0; i < 5; i++) {
            ddlHandler = parse(secondaryIndexDDLs.get(i));
            assertTrue(ddlHandler.getSuccess());
            assertTrue(ddlHandler.isIndexAdd());
        }

        /*
         * Test3: Secondary Indexes -- with no nulls | with unique key per rows
         */
        TableImpl tableBuilt4 = TableBuilder.createTableBuilder("table4")
                .addInteger("id")
                .addString("name")
                .addString("age")
                .addString("phone")
                .primaryKey("id")
                .buildTable();

        tableBuilt4.addIndex(new IndexImpl("idxAge",
                                           tableBuilt4,
                                           makeIndexList("age"),
                                           null,
                                           true, /* indexNulls */
                                           false, /* isUnique */
                                           null));

        tableBuilt4.addIndex(new IndexImpl("idxName",
                                           tableBuilt4,
                                           makeIndexList("name"),
                                           null,
                                           false, /* indexNulls */
                                           false, /* isUnique */
                                           null));

        tableBuilt4.addIndex(new IndexImpl("idxPhone",
                                           tableBuilt4,
                                           makeIndexList("phone"),
                                           null,
                                           false, /* indexNulls */
                                           true, /* isUnique */
                                           null));

        assertDdlEquality(tableBuilt4);
        ddlGenerator = new DDLGenerator(tableBuilt4, null);

        secondaryIndexDDLs = ddlGenerator.getAllIndexDDL();
        assertTrue(secondaryIndexDDLs.size() == 3);

        String idxDdl = secondaryIndexDDLs.get(0);
        assertTrue(idxDdl.contains("idxAge"));
        assertFalse(idxDdl.contains("WITH NO NULLS"));
        assertFalse(idxDdl.contains("WITH UNIQUE KEYS PER ROW"));

        idxDdl = secondaryIndexDDLs.get(1);
        assertTrue(idxDdl.contains("idxName"));
        assertTrue(idxDdl.contains("WITH NO NULLS"));
        assertFalse(idxDdl.contains("WITH UNIQUE KEYS PER ROW"));

        idxDdl = secondaryIndexDDLs.get(2);
        assertTrue(idxDdl.contains("idxPhone"));
        assertTrue(idxDdl.contains("WITH NO NULLS"));
        assertTrue(idxDdl.contains("WITH UNIQUE KEYS PER ROW"));
    }

    /**
     * Generates alter table (add field | drop field) ddl
     */
    @Test
    public void testDDLGenAlter() {
        TableImpl oldT = TableBuilder.createTableBuilder("test")
                            .addInteger("id")
                            .addString("name")
                            .addInteger("age")
                            .addString("desc")
                            .primaryKey("id")
                            .buildTable();

        final Timestamp now = new Timestamp(System.currentTimeMillis());

        DDLGenerator gen;
        TableEvolver te;
        String[] ddls;
        TableImpl newT;
        List<TableImpl> tables = new ArrayList<TableImpl>();

        FieldDef education = TableBuilder.createRecordBuilder("education")
                .addTimestamp("start", 0)
                .addTimestamp("end", 0)
                .addString("school", null, false, "n/a")
                .addEnum("degree",
                         new String[] {"BS", "MS", "PHD"},
                         null,
                         false,
                         "BS")
                .build();

        /*
         * ALTER TABLE test(ADD birthDate TIMESTAMP(0),
         *                  ADD login TIMESTAMP(3) NOT NULL
         *                      DEFAULT '<current-time> ',
         *                  ADD key BINARY(20),
         *                  ADD vip BOOLEAN,
         *                  ADD degree ENUM(BS, MS, PHD) NOT NULL DEFAULT BS,
         *                  ADD info JSON)
         */
        te = TableEvolver.createTableEvolver(oldT);
        te.addTimestamp("birthDate", 0)
          .addTimestamp("login", 3, null, false, now)
          .addFixedBinary("key", 20)
          .addBoolean("vip")
          .addEnum("degree", new String[] {"BS", "MS", "PHD"}, null, false, "BS")
          .addJson("info", null);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /* ALTER TABLE test(DROP age, DROP key) */
        te = TableEvolver.createTableEvolver(newT);
        te.removeField("Age");
        te.removeField("Key");
        newT = te.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(ADD loginTime TIMESTAMP(3),
         *                  ADD education RECORD(start TIMESTAMP(0),
         *                  end TIMESTAMP(0),
         *                  school STRING NOT NULL DEFAULT "n/a",
         *                  degree ENUM(BS, MS, PHD) NOT NULL DEFAULT BS),
         *                  DROP login,
         *                  DROP degree)
         */
        te = TableEvolver.createTableEvolver(newT);
        te.addTimestamp("loginTime", 3);
        te.removeField("login");
        te.addField("education", education);
        te.removeField("degree");
        newT = te.evolveTable();
        tables.add(newT.clone());

        for (TableImpl table : tables) {
            gen = new DDLGenerator(oldT, false, null);
            ddls = gen.genAlterDdl(table);
            assertEquals(1, ddls.length);

            validateAlterDdl(ddls[0], oldT, table);
            oldT = table;
        }
    }

    /**
     * Generates alter table (add <nested-field> | drop <nested-field>) ddl
     */
    @Test
    public void testDDLGenAlterNestedField() {
        TableImpl oldT = TableBuilder.createTableBuilder("test")
                            .addInteger("id")
                            .addString("name")
                            .primaryKey("id")
                            .buildTable();

        DDLGenerator gen;
        TableEvolver te;
        String[] ddls;
        TableImpl newT;
        List<TableImpl> tables = new ArrayList<TableImpl>();

        FieldDef addr = TableBuilder.createRecordBuilder("address")
                              .addString("addrLine")
                              .addString("city")
                              .addString("zipcode")
                              .addString("country")
                              .build();

        FieldDef arrAddr = TableBuilder.createArrayBuilder()
                                .addField(addr)
                                .build();

        FieldDef education = TableBuilder.createRecordBuilder("education")
                                .addTimestamp("start", 0)
                                .addTimestamp("end", 0)
                                .addString("school", null, false, "n/a")
                                .addEnum("degree",
                                         new String[] {"BS", "MS", "PHD"},
                                         null, false, "BS")
                                .addField("addresses", arrAddr)
                                .build();

        FieldDef arrRec = TableBuilder.createArrayBuilder()
                            .addField(education)
                            .build();
        FieldDef mapRec = TableBuilder.createMapBuilder()
                            .addField(education)
                            .build();

        te = TableEvolver.createTableEvolver(oldT);
        te.addField("rec", education)
          .addField("arrRec", arrRec)
          .addField("mapRec", mapRec);
        newT = te.evolveTable();
        tables.add(newT.clone());

        te = TableEvolver.createTableEvolver(newT);
        te.addString("REC.COUNTRY");
        te.removeField("REC.END");

        te.addString("arrRec[].country");
        te.removeField("arrRec[].end");
        te.addString("arrRec[].addresses[].addrLine2");
        te.removeField("arrRec[].addresses[].country");

        te.addString("mapRec.values().country");
        te.removeField("mapRec.values().end");
        te.addString("mapRec.values().addresses[].addrLine2");
        te.removeField("mapRec.values().addresses[].country");
        newT = te.evolveTable();
        tables.add(newT.clone());

        te = TableEvolver.createTableEvolver(newT);
        te.removeField("rec.addresses");
        te.removeField("arrRec[].addresses");
        te.removeField("mapRec.values().addresses");
        te.addField("addresses", arrAddr);
        newT = te.evolveTable();
        tables.add(newT.clone());

        for (TableImpl table : tables) {
            gen = new DDLGenerator(oldT, false, null);
            ddls = gen.genAlterDdl(table);
            assertEquals(1, ddls.length);

            validateAlterDdl(ddls[0], oldT, table);
            oldT = table;
        }
    }

    /**
     * Generate ddl for table which has identity column
     */
    @Test
    public void testDDLGenIdentity() {
        testDDLGenIdentityInternal(true);
        testDDLGenIdentityInternal(false);
    }

    public void testDDLGenIdentityInternal(boolean includeIdentityInfo) {
        List<TableImpl> tables = new ArrayList<TableImpl>();

        TableBuilderBase tb;
        FieldDefImpl fdef;

        /* default sequence definition */
        IdentityDefHelper idh0 = new IdentityDefHelper();

        /* customized sequence definition */
        IdentityDefHelper idh1 = new IdentityDefHelper();
        idh1.setStart("5");
        idh1.setIncrement("10");
        idh1.setMax("105");
        idh1.setCache("3");
        idh1.setCycle(true);

        /* INTEGER type */
        fdef = FieldDefImpl.Constants.integerDef;
        tb = TableBuilder.createTableBuilder("test")
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.setIdentity("id", true, false, new SequenceDefImpl(fdef, idh0));
        tables.add(tb.buildTable().clone());

        tb.setIdentity("id", false, false, new SequenceDefImpl(fdef, idh1));
        tables.add(tb.buildTable().clone());

        /* LONG type */
        fdef = FieldDefImpl.Constants.longDef;
        tb = TableBuilder.createTableBuilder("test")
                .addLong("id")
                .addString("name")
                .primaryKey("id");
        tb.setIdentity("id", true, false, new SequenceDefImpl(fdef, idh0));
        tables.add(tb.buildTable().clone());

        tb.setIdentity("id", false, true, new SequenceDefImpl(fdef, idh1));
        tables.add(tb.buildTable().clone());

        /* NUMBER type */
        fdef = FieldDefImpl.Constants.numberDef;
        tb = TableBuilder.createTableBuilder("test")
                .addNumber("id")
                .addString("name")
                .primaryKey("id");
        tb.setIdentity("id", true, false, new SequenceDefImpl(fdef, idh0));
        tables.add(tb.buildTable().clone());

        tb.setIdentity("id", false, false, new SequenceDefImpl(fdef, idh1));
        tables.add(tb.buildTable().clone());

        for (TableImpl t : tables) {
            TableImpl t1 = parseTable(getDdl(t, includeIdentityInfo));
            if (includeIdentityInfo) {
                assertEquals(t, t1);
                assertEquals(t.getIdentitySequenceDef(),
                             t1.getIdentitySequenceDef());
            } else {
                assertNull(t1.getIdentityColumnInfo());
                assertEquals(t.getFieldMap(), t1.getFieldMap());
            }
        }
    }

    @Test
    public void testDDLGenMRCounter() {
        String localRegion = "r0";
        metadata.setLocalRegionName(localRegion);

        Map<String, Type> mrcounters = new HashMap<String, Type>();
        mrcounters.put("i", Type.INTEGER);
        mrcounters.put("a.l", Type.LONG);
        mrcounters.put("\"_b\".\"_n\"", Type.NUMBER);

        TableBuilderBase tb = TableBuilder
                .createTableBuilder(null, "test", null, null,
                                    metadata.getRegionMapper())
                .addInteger("id")
                .addField("i", FieldDefImpl.getCRDTDef(Type.INTEGER))
                .addField("l", FieldDefImpl.getCRDTDef(Type.LONG))
                .addField("n", FieldDefImpl.getCRDTDef(Type.NUMBER))
                .addField("j", FieldDefFactory.createJsonDef(mrcounters))
                .primaryKey("id");
        tb.addRegion(localRegion);

        TableImpl t  = tb.buildTable();
        assertDdlEquality(t, metadata.getRegionMapper());
    }

    /**
     * Generates alter table identity field:
     *   modify identity field
     *   drop identity field
     *   add identity field
     */
    @Test
    public void testDDLGenAlterIdentity() {

        List<TableImpl> tables = new ArrayList<TableImpl>();
        SequenceDefImpl seqDef;
        TableBuilder tb;
        TableImpl newT;
        TableEvolver te;
        String[] ddls;

        tb = (TableBuilder)TableBuilder.createTableBuilder("test")
                            .addLong("id")
                            .addString("name")
                            .primaryKey("id");
        TableImpl oldT = tb.buildTable();

        /*
         * ALTER TABLE test(MODIFY id GENERATED ALWAYS AS IDENTITY
         *                  (START WITH 1
         *                   INCREMENT BY 1
         *                   MAXVALUE 9223372036854775807
         *                   MINVALUE -9223372036854775808
         *                   CACHE 1000))
         */
        seqDef = new SequenceDefImpl((FieldDefImpl)tb.getField("id"),
                                     new IdentityDefHelper());
        te = TableEvolver.createTableEvolver(oldT);
        te.setIdentity("id", true, false, seqDef);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(MODIFY id GENERATED ALWAYS AS IDENTITY
         *                  ( START WITH 0
         *                    INCREMENT BY 2
         *                    MAXVALUE 100
         *                    MINVALUE -9223372036854775808
         *                    CACHE 5 CYCLE ))
         */
        IdentityDefHelper idh = new IdentityDefHelper();
        idh.setStart("0");
        idh.setIncrement("2");
        idh.setMax("100");
        idh.setCache("5");
        idh.setCycle(true);

        seqDef = new SequenceDefImpl((FieldDefImpl)tb.getField("id"), idh);
        te = TableEvolver.createTableEvolver(newT);
        te.setIdentity("id", true, false, seqDef);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(MODIFY id GENERATED ALWAYS AS IDENTITY
         *                  ( START WITH 1
         *                    INCREMENT BY -2
         *                    MAXVALUE 9223372036854775807
         *                    MINVALUE -100
         *                    CACHE 10))
         */
        idh = new IdentityDefHelper();
        idh.setIncrement("-2");
        idh.setMin("-100");
        idh.setCache("10");
        idh.setCycle(false);
        seqDef = new SequenceDefImpl((FieldDefImpl)te.getField("id"), idh);

        te = TableEvolver.createTableEvolver(newT);
        te.setIdentity("id", true, false, seqDef);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(MODIFY id GENERATED BY DEFAULT ON NULL AS IDENTITY
         *                  ( START WITH 1
         *                    INCREMENT BY -2
         *                    MAXVALUE 9223372036854775807
         *                    MINVALUE -100 CACHE 10))
         */
        te = TableEvolver.createTableEvolver(newT);
        te.setIdentity("id", false, true, seqDef);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /* ALTER TABLE test(MODIFY id DROP IDENTITY) */
        te = TableEvolver.createTableEvolver(newT);
        te.setIdentity(null, false, false, null);
        newT = te.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(ADD uid INTEGER GENERATED ALWAYS AS IDENTITY
         *                  ( START WITH 1
         *                    INCREMENT BY 1
         *                    MAXVALUE 2147483647
         *                    MINVALUE -2147483648 CACHE 1000))
         */
        te = TableEvolver.createTableEvolver(newT);
        te.addInteger("uid");
        te.setIdentity("uid", true, false,
                       new SequenceDefImpl((FieldDefImpl)te.getField("uid"),
                                           new IdentityDefHelper()));
        newT = te.evolveTable();
        tables.add(newT.clone());

        DDLGenerator gen;
        for (TableImpl table : tables) {
            gen = new DDLGenerator(oldT, false, null);
            ddls = gen.genAlterDdl(table);
            assertEquals(1, ddls.length);

            validateAlterDdl(ddls[0], oldT, table);
            oldT = table;
        }
    }

    /**
     * Test alter table with UUID field
     */
    @Test
    public void testDDLGenAlterUUID() {
        TableImpl oldT = TableBuilder.createTableBuilder("test")
                            .addInteger("id")
                            .addString("name")
                            .primaryKey("id")
                            .buildTable();

        DDLGenerator gen;
        TableEvolver evolver;
        String[] ddls;
        TableImpl newT;
        List<TableImpl> tables = new ArrayList<TableImpl>();

        /* ALTER TABLE test(ADD uuid1 STRING AS UUID) */
        evolver = TableEvolver.createTableEvolver(oldT);
        evolver.addField("uuid1", FieldDefImpl.Constants.uuidStringDef);
        newT = evolver.evolveTable();
        tables.add(newT.clone());

        /*
         * ALTER TABLE test(ADD uuid2 STRING AS UUID GENERATED BY DEFAULT,
         *                  DROP uuid1)
         */
        evolver = TableEvolver.createTableEvolver(newT);
        evolver.removeField("uuid1");
        evolver.addField("uuid2", FieldDefImpl.Constants.defaultUuidStrDef);
        newT = evolver.evolveTable();
        tables.add(newT.clone());

        for (TableImpl table : tables) {
            gen = new DDLGenerator(oldT, false, null);
            ddls = gen.genAlterDdl(table);
            assertEquals(1, ddls.length);

            validateAlterDdl(ddls[0], oldT, table);
            oldT = table;
        }
    }

    /**
     * Test alter table to modify ttl
     */
    @Test
    public void testDDLGenAlterTTL() {
        TableImpl oldT = TableBuilder.createTableBuilder("test")
                            .addInteger("id")
                            .addString("name")
                            .primaryKey("id")
                            .buildTable();

        DDLGenerator gen;
        TableEvolver te;
        String[] ddls;
        TableImpl newT;
        List<TableImpl> tables = new ArrayList<TableImpl>();

        /* ALTER TABLE test USING TTL 30 DAYS */
        te = TableEvolver.createTableEvolver(oldT);
        te.setDefaultTTL(TimeToLive.ofDays(30));
        newT = te.evolveTable();
        tables.add(newT.clone());

        /* ALTER TABLE test USING TTL 365 DAYS */
        te = TableEvolver.createTableEvolver(newT);
        te.setDefaultTTL(TimeToLive.ofDays(365));
        newT = te.evolveTable();
        tables.add(newT.clone());

        /* ALTER TABLE test USING TTL 0 DAYS */
        te = TableEvolver.createTableEvolver(newT);
        te.setDefaultTTL(TimeToLive.DO_NOT_EXPIRE);
        newT = te.evolveTable();
        tables.add(newT.clone());

        for (TableImpl table : tables) {
            gen = new DDLGenerator(oldT, false, null);
            ddls = gen.genAlterDdl(table);
            assertEquals(1, ddls.length);

            validateAlterDdl(ddls[0], oldT, table);
            oldT = table;
        }

        /*
         * 2 ddls:
         *  ALTER TABLE test(ADD Age INTEGER)
         *   ALTER TABLE test USING TTL 3 DAYS
         */
        tables.clear();
        te = TableEvolver.createTableEvolver(newT);
        te.addInteger("Age");
        newT = te.evolveTable();
        tables.add(newT.clone());

        te.setDefaultTTL(TimeToLive.ofDays(3));
        newT = te.evolveTable();
        tables.add(newT.clone());

        gen = new DDLGenerator(oldT, false, null);
        ddls = gen.genAlterDdl(newT);
        assertEquals(2, ddls.length);

        int i = 0;
        for (String ddl : ddls) {
            newT = tables.get(i++);
            validateAlterDdl(ddl, oldT, newT);
            oldT = newT;
        }
    }

    /**
     * Test alter table add/drop regions
     */
    @Test
    public void testDDLGenAlterRegions() {
        metadata.setLocalRegionName("iad");
        metadata.createRegion("phx");
        metadata.createRegion("sjc");

        RegionMapper regionMapper = metadata.getRegionMapper();

        TableImpl oldT;
        TableImpl newT;
        TableBuilder tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.addRegion("iad");
        oldT = tb.buildTable();

        /* ALTER TABLE test ADD REGIONS phx, sjc */
        tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.addRegion("phx");
        tb.addRegion("sjc");
        newT = tb.buildTable();

        DDLGenerator gen;
        String[] ddls;
        String expDdl;

        gen = new DDLGenerator(oldT, false, regionMapper);
        ddls = gen.genAlterDdl(newT);
        assertEquals(1, ddls.length);
        validateAlterDdl(ddls[0], oldT, newT);

        /* ALTER TABLE test DROP REGIONS sjc */
        oldT = newT;
        tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.addRegion("phx");
        newT = tb.buildTable();

        gen = new DDLGenerator(oldT, false, regionMapper);
        ddls = gen.genAlterDdl(newT);
        assertEquals(1, ddls.length);
        expDdl = "ALTER TABLE " + oldT.getFullName() + " DROP REGIONS sjc";
        assertEquals(ddls[0], expDdl);

        /*
         * 2 alter table ddls generated:
         *  ALTER TABLE test ADD REGIONS sjc
         *  ALTER TABLE test DROP REGIONS phx
         */
        oldT = newT;
        tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.addRegion("sjc");
        newT = tb.buildTable();

        gen = new DDLGenerator(oldT, false, regionMapper);
        ddls = gen.genAlterDdl(newT);
        assertEquals(2, ddls.length);
        expDdl = "ALTER TABLE " + oldT.getFullName() + " ADD REGIONS sjc";
        assertEquals(ddls[0], expDdl);
        expDdl = "ALTER TABLE " + oldT.getFullName() + " DROP REGIONS phx";
        assertEquals(ddls[1], expDdl);
    }

    /**
     * Test alter generate multiple ddls
     */
    @Test
    public void testDDLGenAlterMultipleDdls() {
        metadata.setLocalRegionName("iad");
        metadata.createRegion("phx");
        metadata.createRegion("sjc");

        RegionMapper regionMapper = metadata.getRegionMapper();

        TableImpl oldT;
        TableImpl newT;
        TableBuilder tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addString("name")
                .primaryKey("id");
        tb.addRegion("iad");
        oldT = tb.buildTable();

        /*
         * 3 ddls generated:
         *  ALTER TABLE test(ADD info JSON, DROP name)
         *  ALTER TABLE test USING TTL 30 DAYS
         *  ALTER TABLE test ADD REGIONS phx, sjc
         */
        tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null, regionMapper)
                .addInteger("id")
                .addJson("info", null)
                .primaryKey("id");
        tb.setDefaultTTL(TimeToLive.ofDays(30));
        tb.addRegion("phx");
        tb.addRegion("sjc");
        newT = tb.buildTable();

        String[] expDdls = new String[] {
            "ALTER TABLE test(ADD info JSON, DROP name)",
            "ALTER TABLE test USING TTL 30 DAYS",
            "ALTER TABLE test ADD REGIONS phx, sjc"
        };

        DDLGenerator gen = new DDLGenerator(oldT, false, regionMapper);
        String[] ddls = gen.genAlterDdl(newT);
        assertEquals(expDdls.length, ddls.length);
        int i = 0;
        TableImpl t = oldT;
        for (String ddl : ddls) {
            assertEquals(expDdls[i++], ddl);
            t = validateAlterDdl(ddl, t, null);
        }
        assertEquals(getDdl(newT), getDdl(t));
    }

    /**
     * Error cases in generating alter table ddls
     */
    @Test
    public void testDDLGenAlterErrors() {
        TableImpl oldT = TableBuilder.createTableBuilder("test")
                .addInteger("k1")
                .addString("k2")
                .addString("name")
                .shardKey("k1")
                .primaryKey("k1", "k2")
                .buildTable();

        /* Not same table */
        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("test1")
                        .addInteger("k1")
                        .addString("k2")
                        .addString("name")
                        .shardKey("k1")
                        .primaryKey("k1", "k2")
                        .buildTable());

        /*
         * Different primary key fields
         */

        /* different shard key */
        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("Test")
                            .addInteger("k1")
                            .addString("k2")
                            .addString("name")
                            .shardKey("k1", "k2")
                            .primaryKey("k1", "k2")
                            .buildTable());

        /* one more primary key field */
        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("Test")
                            .addInteger("k1")
                            .addString("k2")
                            .addString("k3")
                            .addString("name")
                            .shardKey("k1")
                            .primaryKey("k1", "k2", "k3")
                            .buildTable());

        /* primary key field "k2" type is different: String -> Integer */
        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("Test")
                            .addInteger("k1")
                            .addInteger("k2")
                            .addString("name")
                            .shardKey("k1")
                            .primaryKey("k1", "k2")
                            .buildTable());

        /*
         * The original table is NOT multi-region table, the new table must
         * NOT be multi-region table either
         */
        metadata.setLocalRegionName("iad");
        TableBuilder tb = (TableBuilder)TableBuilder
                .createTableBuilder(null, "test", null, null,
                                    metadata.getRegionMapper())
                .addInteger("k1")
                .addString("k2")
                .addString("name")
                .shardKey("k1")
                .primaryKey("k1", "k2");
        tb.addRegion("iad");
        genAlterDdlFail(oldT, tb.buildTable());

        /* Field type changed */
        oldT = TableBuilder.createTableBuilder("Test")
                .addInteger("id")
                .addString("name")
                .addTimestamp("ts", 3)
                .addField("rec",
                          TableBuilder.createRecordBuilder("addr")
                              .addString("city")
                              .build())
                .addField("arr",
                          TableBuilder.createArrayBuilder()
                              .addString()
                              .build())
                .primaryKey("id")
                .buildTable();

        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("Test")
                            .addInteger("id")
                            .addString("name")
                            .addTimestamp("ts", 6)
                            .primaryKey("id")
                            .buildTable());

        genAlterDdlFail(oldT,
                        TableBuilder.createTableBuilder("Test")
                            .addInteger("id")
                            .addString("name")
                            .addJson("rec", null)
                            .primaryKey("id")
                            .buildTable());

    }

    private void genAlterDdlFail(TableImpl oldT, TableImpl newT) {
        DDLGenerator gen = new DDLGenerator(oldT, false);
        try {
            gen.genAlterDdl(newT);
            fail("Expect to fail but not");
        } catch (IllegalArgumentException iae) {
            /* expected */
        }
    }

    private TableImpl validateAlterDdl(String alterTableDdl,
                                       TableImpl oldT,
                                       TableImpl newT) {
        roundTrip(oldT);
        if (newT != null) {
            roundTrip(newT);
        }

        PrepareCB info = new PrepareCB();
        TestMDHelper mdh = new TestMDHelper(oldT, metadata.getRegionMapper());

        CompilerAPI.compile(alterTableDdl.toCharArray(),
                            null /* ExecuteOptions */, mdh, null, null, info);
        assertEquals(QueryOperation.ALTER_TABLE, info.getOperation());

        TableImpl table = info.getNewTable();
        assertNotNull(table);

        if (newT != null) {
            assertEquals("[validateDdl] exp: " + newT.toJsonString(false) +
                         "\nact: " + table.toJsonString(false), newT, table);
            assertEquals(newT.getIdentitySequenceDef(),
                         table.getIdentitySequenceDef());
        }
        return table;
    }

    private void roundTrip(TableImpl t) {
        String ddl = getDdl(t);
        TableImpl t1 = parseTable(ddl);
        String ddl1 = getDdl(t1);
        assertEquals("[RoundTrip-getDdl]exp=" + ddl + ",act=", ddl, ddl1);
    }

    private TableImpl parseTable(String ddl) {
        PrepareCB info = new PrepareCB();
        TestMDHelper mdh = new TestMDHelper(null, metadata.getRegionMapper());
        CompilerAPI.compile(ddl.toCharArray(), null /* ExecuteOptions */, mdh,
                            null, null, info);
        return info.getNewTable();
    }

    private String getDdl(TableImpl table) {
        return getDdl(table, true);
    }

    private String getDdl(TableImpl table, boolean includeIdentityInfo) {
        DDLGenerator gen = new DDLGenerator(table, false,
                                            metadata.getRegionMapper(),
                                            includeIdentityInfo);
        return gen.getDDL();
    }

    private static class TestMDHelper implements TableMetadataHelper {

        private final TableImpl table;
        private final RegionMapper regionMapper;

        TestMDHelper(TableImpl table, RegionMapper regionMapper) {
            this.table = table;
            this.regionMapper = regionMapper;
        }

        @Override
        public TableImpl getTable(String namespace, String tableName) {
            String qfname = NameUtils.makeQualifiedName(namespace, tableName);
            if (table != null &&
                table.getFullNamespaceName().equalsIgnoreCase(qfname)) {
                return table;
            }
            return null;
        }

        @Override
        public TableImpl getTable(String namespace,
                                  String[] tablePath,
                                  int cost) {
            return getTable(namespace, makeTableName(tablePath));
        }

        private String makeTableName(String[] tablePath) {
            StringBuilder sb = new StringBuilder();
            for (String path : tablePath) {
                if (sb.length() > 0) {
                    sb.append(".");
                }
                sb.append(path);
            }
            return sb.toString();
        }

        @Override
        public RegionMapper getRegionMapper() {
            return regionMapper;
        }
    }

    private static class PrepareCB implements PrepareCallback {

        private TableImpl newTable;
        private QueryOperation queryOperation;

        @Override
        public void queryOperation(QueryOperation op) {
            queryOperation = op;
        }

        public QueryOperation getOperation() {
            return queryOperation;
        }

        @Override
        public void newTable(Table table) {
            newTable = (TableImpl)table;
        }

        public TableImpl getNewTable() {
            return newTable;
        }
    }

    /*
     * Test utilities
     */

    /**
     * Parse the DDL statement. This is encapsulated in case the interface to
     * DdlHandler needs to change.
     */
    private DdlHandler parse(String statement) {
        return new DdlHandler(statement,
                              null, /* namespace */
                              metadata);
    }

    private static void assertTablesEqual(TableImpl t1, TableImpl t2) {
        assertTrue(t1.fieldsEqual(t2));
    }

    /**
     * Resets (implicitly clearing) the TableMetadata.  No need to synchronize,
     * a test cases are single-threaded.
     */
    private void resetMetadata() {
        metadata = new TableMetadata(false);
    }

    private TableImpl addToMetadata(TableImpl table) {
        return metadata.addTable(table.getInternalNamespace(),
                                 table.getName(),
                                 table.getParentName(),
                                 table.getPrimaryKey(),
                                 table.getPrimaryKeySizes(),
                                 table.getShardKey(),
                                 table.getFieldMap(),
                                 null, /* TTL */
                                 null, /* limits */
                                 false, 0, null, null /* owner */);
    }

    private TableImpl getTable(String tableName) {
        return metadata.getTable(null, tableName);
    }

    private TableImpl addTable(String statement) {
        DdlHandler ddl = parse(statement);
        assertNotNull(ddl.getTable());
        return addToMetadata(ddl.getTable());
    }

    private void assertDdlEquality(TableImpl table) {
        assertDdlEquality(table, null);
    }

    private void assertDdlEquality(TableImpl table, RegionMapper mapper) {
        DDLGenerator generator = new DDLGenerator(table, mapper);
        DdlHandler ddl = parse(generator.getDDL());
        assertNotNull(ddl.getTable());
        assertTablesEqual(table, ddl.getTable());
    }
}
