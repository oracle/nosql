/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static oracle.kv.impl.api.table.TableTestBase.makeTextIndexList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.KVStore;
import oracle.kv.KVStoreFactory;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.TestBase;
import oracle.kv.Value;
import oracle.kv.impl.admin.param.GlobalParams;
import oracle.kv.impl.admin.param.RepNodeParams;
import oracle.kv.impl.admin.param.StorageNodeParams;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.impl.map.HashKeyToPartitionMap;
import oracle.kv.impl.metadata.Metadata;
import oracle.kv.impl.param.LoadParameters;
import oracle.kv.impl.rep.RepNode;
import oracle.kv.impl.test.TestStatus;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.topo.RepGroup;
import oracle.kv.impl.topo.RepGroupId;
import oracle.kv.impl.topo.RepNodeId;
import oracle.kv.impl.topo.StorageNodeId;
import oracle.kv.impl.util.FileNames;
import oracle.kv.impl.util.KVRepTestConfig;
import oracle.kv.impl.util.SerializationUtil;
import oracle.kv.impl.util.TestUtils;
import oracle.kv.impl.util.VersionUtil;

import com.sleepycat.je.rep.ReplicatedEnvironment;
import com.sleepycat.je.rep.ReplicationGroup;
import com.sleepycat.je.rep.ReplicationNode;
import com.sleepycat.je.rep.subscription.SubscriptionConfig;

import org.junit.Assume;
import org.junit.BeforeClass;

public abstract class TextIndexFeederTestBase extends TestBase {

    static final int TEST_POLL_INTERVAL_MS = 5000;
    static final int TEST_POLL_TIMEOUT_MS = 120000;

    /* test data parameters */
    protected final String testStoreName = "test_kv_store";
    protected final String keyPrefix = "TextSearchUnitTest_Key_";
    protected final String valPrefix = "TextSearchUnitTest_Value_";
    protected final String home = "./subhome/";
    protected final String tifNodeName = "test-subscriber-node";
    protected final String tifNodeHostPort = "localhost:6001";

    /* environment config parameters, single shard with RF=3  */
    protected static int REP_Factor = 3;
    protected static int NUM_SN = 1;
    protected static int NUM_DC = 1;
    protected int numPartitions = 4;
    protected KVRepTestConfig config;
    protected Map<Key, Value> testData;
    protected Map<PartitionId, List<Key>> testDataByPartition;

    /* default data size */
    protected int numRecords = 1024;

    protected static final TableImpl userTableProto =
        TableBuilder.createTableBuilder("User")
        .addInteger("id")
        .addString("firstName")
        .addString("lastName")
        .addInteger("age")
        .addTimestamp("birthDate", 0)
        .addTimestamp("startDate", 3)
        .addTimestamp("endDate", 9)
        .primaryKey("id")
        .shardKey("id")
        .buildTable();

    protected static final TableImpl jokeTableProto =
        TableBuilder.createTableBuilder("Joke")
        .addInteger("id")
        .addString("category") /* Riddle, shaggy dog, dirty, elephant, etc. */
        .addString("text")
        .addFloat("humorQuotient")
        .addTimestamp("originDate", 0)
        .addTimestamp("firstUseDate", 3)
        .addTimestamp("lastUseDate", 9)
        .primaryKey("id")
        .buildTable();

    /*
     * For different tables, each defined with two fields:
     *     1. a field named "id", of type INTEGER,
     *     2. a field named "jsonField", of type JSON.
     *
     * Each row of the basic 'scalar' json table contains a field of type
     * JSON that consists of 6 scalar 'name:value' pairs with the names
     * and corresponding types shown below.
     *
     * Each row of the more complex 'senators' json table contains a field
     * whose value is a complex JSON document that consists of objects
     * consisting of embedded arrays, objects, and scalar types; containing
     * information about members of the US senate.
     */
    protected static final String JSON_ID_FIELD_NAME = "id";
    protected static final String JSON_FIELD_NAME = "jsonData";

    protected static final String JSON_STRING_DEFAULT =
        "{\"type\" : " + "\"string\"" + "}";
    protected static final String JSON_STRING_ENGLISH =
        "{\"type\" : " + "\"string\"" +
        "," +
        " \"analyzer" + "\": " + "\"english\"" +
        "}";
    protected static final String JSON_STRING_STANDARD =
        "{\"type\" : " + "\"string\"" +
        "," +
        " \"analyzer" + "\": " + "\"standard\"" +
        "}";
    protected static final String JSON_INTEGER =
        "{\"type\" : " + "\"integer\"" + "}";
    protected static final String JSON_LONG =
        "{\"type\" : " + "\"long\"" + "}";
    protected static final String JSON_DOUBLE =
        "{\"type\" : " + "\"double\"" + "}";
    protected static final String JSON_FLOAT =
        "{\"type\" : " + "\"float\"" + "}";
    protected static final String JSON_BOOLEAN =
        "{\"type\" : " + "\"boolean\"" + "}";
    protected static final String JSON_TIMESTAMP =
        "{\"type\" : " + "\"date\"" + "}";
    protected static final String JSON_TIMESTAMP_MILLIS =
        "{\"type\" : " + "\"date\"" +
        "," +
        " \"precision" + "\": " + "\"millis\"" +
        "}";
    protected static final String JSON_TIMESTAMP_NANOS =
        "{\"type\" : " + "\"date\"" +
        "," +
        " \"precision" + "\": " + "\"nanos\"" +
        "}";

    protected static final String JSON_FIELD_INTEGER_NAME = "jsonFieldInteger";
    protected static final String JSON_FIELD_LONG_NAME    = "jsonFieldLong";
    protected static final String JSON_FIELD_DOUBLE_NAME  = "jsonFieldDouble";
    protected static final String JSON_FIELD_NUMBER_NAME  = "jsonFieldNumber";
    protected static final String JSON_FIELD_STRING_NAME  = "jsonFieldString";
    protected static final String JSON_FIELD_BOOLEAN_NAME = "jsonFieldBoolean";
    protected static final String JSON_FIELD_DATE_NAME    = "jsonFieldDate";

    protected static final String JSON_TABLE_NAME_SCALAR = "jsonTableScalar";
    protected static final String JSON_INDEX_NAME_SCALAR = "jsonIndexScalar";
    protected static TableImpl jsonTableProtoScalar;

    protected static final String JSON_TABLE_NAME_SENATORS =
                                      "jsonTableSenators";
    protected static final String JSON_INDEX_NAME_SENATORS =
                                      "jsonIndexSenators";

    /* These two indexes have same content but in different order
     * "jsonIndex1" has personal.lastname, startdate and
     * "jsonIndex2" has startdate, personal.lastname
     */
    protected static final String JSON_INDEX_NAME_SENATORS_ORDER =
            "jsonIndex1";
    protected static final String JSON_INDEX_NAME_SENATORS_OPP_ORDER =
            "jsonIndex2";
    /* index that one of its field has wrong path */
    protected static final String JSON_INDEX_NAME_SENATORS_WRONG_PATH =
            "jsonIndex3";
    /* index that one of its field has wrong case */
    protected static final String JSON_INDEX_NAME_SENATORS_WRONG_CASE =
            "jsonIndex4";
    /* index that the type of the field defined in fts is inconsistent
     * with its real type
     */
    protected static final String JSON_INDEX_NAME_SENATORS_INCONS_TYPE =
            "jsonIndex5";

    protected static TableImpl jsonTableProtoSenators;

    protected TableMetadata metadata;
    protected TableImpl userTable;
    protected TableImpl jokeTable;
    protected TableImpl jsonTableScalar;
    protected TableImpl jsonTableSenators;

    /* Initialize the names of all possible 'collapsed' fields used
     * in the senators document; which are needed for generating
     * the elasticsearch mapping, populating the json column in the
     * table, indexing the corresponding document in elasticsearch,
     * and verifying that the documents were correctly indexed.
     */
    protected static String caucus = JSON_FIELD_NAME + "." + "caucus";
    protected static String congressNumbers =
        JSON_FIELD_NAME + "." + "congress_numbers";
    protected static String current = JSON_FIELD_NAME + "." + "current";
    protected static String description =
        JSON_FIELD_NAME + "." + "description";
    protected static String district = JSON_FIELD_NAME + "." + "district";
    protected static String endDate = JSON_FIELD_NAME + "." + "enddate";

    protected static String extraAddress =
        JSON_FIELD_NAME + "." + "extra" + "." + "address";
    protected static String extraContactForm =
        JSON_FIELD_NAME + "." + "extra" + "." + "contact_form";
    protected static String extraFax =
        JSON_FIELD_NAME + "." + "extra" + "." + "fax";
    protected static String extraOffice =
        JSON_FIELD_NAME + "." + "extra" + "." + "office";
    protected static String extraRssUrl =
        JSON_FIELD_NAME + "." + "extra" + "." + "rss_url";

    protected static String leadershipTitle =
        JSON_FIELD_NAME + "." + "leadership_title";
    protected static String party = JSON_FIELD_NAME + "." + "party";

    protected static String personBioGuideId =
        JSON_FIELD_NAME + "." + "person" + "." + "bioguideid";
    protected static String personBirthday =
        JSON_FIELD_NAME + "." + "person" + "." + "birthday";
    protected static String personCspanId =
        JSON_FIELD_NAME + "." + "person" + "." + "cspanid";
    protected static String personFirstname =
        JSON_FIELD_NAME + "." + "person" + "." + "firstname";
    protected static String personGender =
        JSON_FIELD_NAME + "." + "person" + "." + "gender";
    protected static String personGenderLabel =
        JSON_FIELD_NAME + "." + "person" + "." + "gender_label";
    protected static String personLastname =
        JSON_FIELD_NAME + "." + "person" + "." + "lastname";
    protected static String personLink =
        JSON_FIELD_NAME + "." + "person" + "." + "link";
    protected static String personMiddlename =
        JSON_FIELD_NAME + "." + "person" + "." + "middlename";
    protected static String personName =
        JSON_FIELD_NAME + "." + "person" + "." + "name";
    protected static String personNameMod =
        JSON_FIELD_NAME + "." + "person" + "." + "namemod";
    protected static String personNickname =
        JSON_FIELD_NAME + "." + "person" + "." + "nickname";
    protected static String personOdId =
        JSON_FIELD_NAME + "." + "person" + "." + "odid";
    protected static String personPvsId =
        JSON_FIELD_NAME + "." + "person" + "." + "pvsid";
    protected static String personSortname =
        JSON_FIELD_NAME + "." + "person" + "." + "sortname";
    protected static String personTwitterId =
        JSON_FIELD_NAME + "." + "person" + "." + "twitterid";
    protected static String personYoutubeId =
        JSON_FIELD_NAME + "." + "person" + "." + "youtubeid";

    protected static String phone = JSON_FIELD_NAME + "." + "phone";
    protected static String roleType = JSON_FIELD_NAME + "." + "role_type";
    protected static String roleTypeLabel =
        JSON_FIELD_NAME + "." + "role_type_label";
    protected static String senatorClass =
        JSON_FIELD_NAME + "." + "senator_class";
    protected static String senatorClassLabel =
        JSON_FIELD_NAME + "." + "senator_class_label";
    protected static String rank = JSON_FIELD_NAME + "." + "rank";
    protected static String startDate = JSON_FIELD_NAME + "." + "startdate";
    protected static String state = JSON_FIELD_NAME + "." + "state";
    protected static String title = JSON_FIELD_NAME + "." + "title";
    protected static String titleLong =
        JSON_FIELD_NAME + "." + "title_long";
    protected static String website = JSON_FIELD_NAME + "." + "website";

    protected static String congressNumbersWrongPath =
            JSON_FIELD_NAME + "." + "personal" + "." + "congress_numbers";
    protected static String congressNumbersWrongCase =
            JSON_FIELD_NAME + "." + "CONGRESS_numbers";

    protected static class Joke {
        private final String type;
        private final String text;
        private final float humorQuotient;
        private final Timestamp originDate;
        private final Timestamp firstUseDate;
        private final Timestamp lastUseDate;
        private String key; /* The pk for this row, determined later */

        public Joke(String type, String text, float humorQuotient,
                    final Timestamp originDate, final Timestamp firstUseDate,
                    final Timestamp lastUseDate) {
            this.type = type;
            this.text = text;
            this.originDate = originDate;
            this.firstUseDate = firstUseDate;
            this.lastUseDate = lastUseDate;
            this.humorQuotient = humorQuotient;
        }

        public void setKey(String k) {
            key = k;
        }

        public String getKey() {
            return key;
        }

        public String getText() {
            return text;
        }

        public String getCategory() {
            return type;
        }

        public float getHumorQuotient() {
            return humorQuotient;
        }

        public Timestamp getOriginDate() {
            return originDate;
        }

        public Timestamp getFirstUseDate() {
            return firstUseDate;
        }

        public Timestamp getLastUseDate() {
            return lastUseDate;
        }
    }

    protected Joke[] realJokes;

    @BeforeClass
    public static void staticSetUp() throws Exception {
        Assume.assumeFalse(
            "FTS Unit tests are not currently compatible with security",
            SECURITY_ENABLE);
        Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
        Assume.assumeTrue("Skipping test suite due to missing http port",
            System.getProperty("es.http.port") != null);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        TestStatus.setManyRNs(true);

        /* Elasticsearch logging is noisy by default on stdout; just disable it
         * unless debugging.
         */
        final org.apache.log4j.Logger log4jRoot =
            org.apache.log4j.Logger.getRootLogger();
        log4jRoot.removeAllAppenders();
        log4jRoot.addAppender
            (org.apache.log4j.varia.NullAppender.getNullAppender());
        metadata = new TableMetadata(true);

        userTable = metadata.addTable(userTableProto.getInternalNamespace(),
                                      userTableProto.getName(),
                                      userTableProto.getParentName(),
                                      userTableProto.getPrimaryKey(),
                                      userTableProto.getPrimaryKeySizes(),
                                      userTableProto.getShardKey(),
                                      userTableProto.getFieldMap(),
                                      null,
                                      null, /*beforeImageTTL*/
                                      null, false, 0, null, null);

        jokeTable = metadata.addTable(jokeTableProto.getInternalNamespace(),
                                      jokeTableProto.getName(),
                                      jokeTableProto.getParentName(),
                                      jokeTableProto.getPrimaryKey(),
                                      jokeTableProto.getPrimaryKeySizes(),
                                      jokeTableProto.getShardKey(),
                                      jokeTableProto.getFieldMap(),
                                      null,
                                      null, /*beforeImageTTL*/
                                      null, false, 0, null, null);
        metadata.addTextIndex
            (null, "FirstNameIndex", userTable.getFullName(),
             makeTextIndexList
             (new IndexImpl.AnnotatedField("FirstName", JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField("birthDate", JSON_TIMESTAMP),
              new IndexImpl.AnnotatedField("startDate", JSON_TIMESTAMP_MILLIS),
              new IndexImpl.AnnotatedField("endDate", JSON_TIMESTAMP_NANOS)
             ),
             null,
             "user index");

        metadata.addTextIndex
            (null, "JokeIndex", jokeTable.getFullName(),
             makeTextIndexList
             (new IndexImpl.AnnotatedField("category", JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField("text", JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField("originDate", JSON_TIMESTAMP),
              new IndexImpl.AnnotatedField(
                  "firstUseDate", JSON_TIMESTAMP_MILLIS),
              new IndexImpl.AnnotatedField(
                  "lastUseDate", JSON_TIMESTAMP_NANOS)
             ),
             null,
             "joke index");

        realJokes = populateJokesArray();

        createJsonIndexScalar();

        /*
         * config will be initialized in those subclass tests which need
         * KVRepTestConfig() to create a test env. It should also be cleared
         * in teardown() in subclasses.
         *
         * Some tests do not use KVRepTestConfig to create test env. These
         * tests would create their own test env differently and leave the
         * config null;
         */
        config = null;
    }

    @Override
    public void tearDown() throws Exception {
    }

    /**
     * prepare a test env, and create, load and verify test data.
     */
    protected void prepareTestEnv() {
        prepareTestEnv(true);

        logger.fine(topo2String());
    }

    protected void prepareTestEnv(boolean loadTestData) {

        config.startRepNodeServices();
        if (loadTestData) {
            createTestData();
            loadTestData();
            verifyTestData();
        }

        logger.info("Test environment created successfully," +
                    "test data loaded: " + loadTestData);
    }

    /* create a subscription configuration for test */
    protected SubscriptionConfig buildConfig(ReplicatedEnvironment masterEnv)
        throws Exception {
        /* constants and parameters used in test */


        final File subHome =
            new File(TestUtils.getTestDir().getAbsolutePath() +
                     File.separator + home);
        if (!subHome.exists()) {
            if (subHome.mkdir()) {
                logger.info("create test dir " + subHome.getAbsolutePath());
            } else {
                fail("unable to create test dir, fail the test");
            }
        }

        final String feederNode = masterEnv.getNodeName();
        final ReplicationGroup group = masterEnv.getGroup();
        final ReplicationNode member = group.getMember(feederNode);
        final String feederHostPort = "localhost:" + member.getPort();
        final String groupName = group.getName();
        final UUID uuid = group.getRepGroupImpl().getUUID();

        logger.info("Feeder host port " + feederHostPort +
                    " in replication group " + groupName +
                    " (group uuid: " + uuid + ")");

        final SubscriptionConfig conf =
            new SubscriptionConfig(tifNodeName,
                                   subHome.getAbsolutePath(),
                                   tifNodeHostPort,
                                   feederHostPort,
                                   groupName, uuid);

        logger.info("build config: " + conf);
        return conf;
    }

    /* create test data and store them by partition */
    protected void createTestData() {
        testData = new HashMap<>(numRecords);
        testDataByPartition = new HashMap<>();
        final HashKeyToPartitionMap partitionMap =
            new HashKeyToPartitionMap(numPartitions);

        for (int i = 0; i < numRecords; i++) {
            final String keyStr = keyPrefix + Integer.toString(i);
            final String valueStr = valPrefix + Integer.toString(i * i);
            final Key key = Key.createKey(keyStr);
            final Value value = Value.createValue(valueStr.getBytes());
            testData.put(key, value);

            final PartitionId pid =
                partitionMap.getPartitionId(key.toByteArray());

            List<Key> keyList;
            if (!testDataByPartition.containsKey(pid)) {
                keyList = new ArrayList<>();
            } else {
                keyList = testDataByPartition.get(pid);
            }
            keyList.add(key);
            testDataByPartition.put(pid, keyList);
        }

        logger.info("Test data with " + testData.size() +
                    " keys created for " + testDataByPartition.size() +
                    " partitions.");
    }

    /* write test data into store */
    protected void loadTestData() {
        final KVStore kvs = KVStoreFactory.getStore(config.getKVSConfig());
        for (Map.Entry<Key, Value> entry : testData.entrySet()) {
            final Key k = entry.getKey();
            final Value v = entry.getValue();
            kvs.put(k, v, null, Durability.COMMIT_SYNC, 0, null);
        }
        logger.info("Test data with " + testData.size() + " keys loaded.");
    }

    /* read test data from the store and verify */
    protected void verifyTestData() {
        final KVStore store = KVStoreFactory.getStore(config.getKVSConfig());
        int counter = 0;

        final Iterator<KeyValueVersion> iterator =
            store.storeIterator(Direction.UNORDERED, 1);
        while (iterator.hasNext()) {
            final KeyValueVersion kvv = iterator.next();
            final Key k = kvv.getKey();
            final Value v = kvv.getValue();
            verifyKV(k, v);
            counter++;
        }
        assertEquals("number of records mismatch!", numRecords, counter);

        logger.info("All test data verified.");
    }

    /* verify k/v are in test data generated */
    protected void verifyKV(Key k, Value v) {
        assertTrue("Unexpected key", testData.containsKey(k));
        final Value expectedValue = testData.get(k);
        assertArrayEquals("Value mismatch!",
                          expectedValue.toByteArray(),
                          v.toByteArray());
    }

    /* verify checkpoint state */
    protected void verifyCheckpointState(CheckpointState expectedState,
                                         CheckpointState fetchedState) {
        if (!expectedState.equals(fetchedState)) {
            fail("Mismatched checkpoint state, expected: " + expectedState +
                 ", but actual: " + fetchedState);
        }
    }

    /* update metadata in RN */
    protected static boolean updateMetadata(RepNode rn, TableMetadata md) {
        return rn.updateMetadata(clone(md));
    }

    /* Clone the  MD to simulate remote call to the RN */
    private static Metadata<?> clone(Metadata<?> md) {
        return SerializationUtil.getObject(SerializationUtil.getBytes(md),
                                           md.getClass());
    }

    /* dump topology from a RN */
    protected String topo2String() {
        final RepNode rn = config.getRN(new RepNodeId(1, 1));
        final StringBuilder builder = new StringBuilder();
        builder.append("====== start topology dump ======").append("\n");
        for (RepGroupId id : rn.getTopology().getRepGroupIds()) {
            builder.append("rep group: ").append(
                                              id.getGroupName()).append("\n");

            final RepGroup group = rn.getTopology().get(id);
            for (oracle.kv.impl.topo.RepNode node : group.getRepNodes()) {
                builder.append("\trep node: ").append(node.toString());
            }
        }

        builder.append("partitions on node ")
               .append(rn.getRepNodeId().getFullName())
               .append(": ")
               .append(dumpPartitionSet(rn.getPartitions()))
               .append("\n")
               .append("====== done topology dump ======");

        return builder.toString();
    }

    /* create real sample jokes */
    protected Joke[] populateJokesArray() {
        return new Joke[]{
            new Joke("elephant",
                     "Q: Why did the elephant paint its fingernails red? " +
                     "A: So it could hide in the strawberry patch.",
                     0.1f,
                     TimestampUtils.parseString("1953-02-17T01:02:03"),
                     TimestampUtils.parseString("1973-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "2017-12-31T11:59:59.546789321")),
            new Joke("knock knock",
                     "Knock knock. Who's there? Euripides. " +
                     "Euripides who? " +
                     "Euripides trousers, Eumenides " +
                     "trousers!",
                     0.95f,
                     TimestampUtils.parseString("1963-02-17T01:02:03"),
                     TimestampUtils.parseString("1983-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "2001-12-31T11:59:59.546789321")),
            new Joke("self-referential",
                     "Is it solipsistic in here, or is it just me?",
                     0.4f,
                     TimestampUtils.parseString("1973-02-17T01:02:03"),
                     TimestampUtils.parseString("1983-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "1998-12-31T11:59:59.546789321")),
            new Joke("grammar",
                     "I know a man with a wooden leg named Smith. " +
                     "Really, what was the name of his " +
                     "other leg?",
                     0.23f,
                     TimestampUtils.parseString("1955-02-17T01:02:03"),
                     TimestampUtils.parseString("1975-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "2015-12-31T11:59:59.546789321")),
            new Joke("pun",
                     "You can tune a piano but you can't tuna " +
                     "fish, unless you play bass.",
                     0.9333f,
                     TimestampUtils.parseString("1960-02-17T01:02:03"),
                     TimestampUtils.parseString("1970-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "2000-12-31T11:59:59.546789321")),
            new Joke("meta",
                     "Why is six afraid of seven?  Because seven " +
                     "is a prime and primes can be " +
                     "intimidating.",
                     0.5f,
                     TimestampUtils.parseString("1991-02-17T01:02:03"),
                     TimestampUtils.parseString("1992-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "1993-12-31T11:59:59.546789321")),
            new Joke("anti",
                     "What did the lawyer say to the other " +
                     "lawyer? We are both lawyers.",
                     0.001f,
                     TimestampUtils.parseString("1967-02-17T01:02:03"),
                     TimestampUtils.parseString("1969-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "1970-12-31T11:59:59.546789321")),
            new Joke("dad",
                     "I hear that guy who invented Lifesavers " +
                     "made a mint.",
                     0.333f,
                     TimestampUtils.parseString("2016-02-17T01:02:03"),
                     TimestampUtils.parseString("2017-06-04T08:01:01.123"),
                     TimestampUtils.parseString(
                         "2017-12-31T11:59:59.546789321"))
        };
    }

    /* convert a joke to a row in joke table */
    protected RowImpl makeJokeRow(TableImpl table, Joke joke, int which) {
        final RowImpl row = table.createRow();
        row.put("id", which);
        row.put("category", joke.type);
        row.put("text", joke.text);
        row.put("humorQuotient", joke.humorQuotient);
        row.put("originDate", joke.getOriginDate());
        row.put("firstUseDate", joke.getFirstUseDate());
        row.put("lastUseDate", joke.getLastUseDate());
        return row;
    }

    /* dump a set of partition ids */
    private String dumpPartitionSet(Set<PartitionId> parts) {
        final StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (PartitionId pid : parts) {
            builder.append(pid.getPartitionId()).append(" ");
        }
        builder.append("]");
        return builder.toString();
    }

    /**
     * Polling an object until a condition is met or timeout.
     */
    public abstract class PollCondition {
        private final long checkPeriodMs;
        private final long timeoutMs;

        public PollCondition(long checkPeriodMs,
                             long timeoutMs) {
            super();
            assert checkPeriodMs <= timeoutMs;
            this.checkPeriodMs = checkPeriodMs;
            this.timeoutMs = timeoutMs;
        }

        protected abstract boolean condition();

        public boolean await() {

            if (condition()) {
                return true;
            }

            final long timeLimit = System.currentTimeMillis() + timeoutMs;
            do {
                try {
                    Thread.sleep(checkPeriodMs);
                } catch (InterruptedException e) {
                    return false;
                }
                if (condition()) {
                    return true;
                }
            } while (System.currentTimeMillis() < timeLimit);

            return false;
        }
    }

    /**
     * Create an SNA config file, so that the TextIndexFeederManager will
     * have the parameters that it needs to connect to the ES cluster.
     */
    protected void createSnaConfig(String clusterMembers, String clusterName) {
        final GlobalParams gp = config.getGlobalParams();
        /* There's only the one SN in these tests. */
        final StorageNodeId snid = new StorageNodeId(1);
        final StorageNodeParams snp = config.getStorageNodeParams(snid);
        snp.setSearchClusterMembers(clusterMembers);
        snp.setSearchClusterName(clusterName);

        final LoadParameters lp = new LoadParameters();
        lp.addMap(gp.getMap());
        lp.addMap(snp.getMap());

        /* There are three repnodes in some of these tests. */
        for (int i = 1; i <= 3; i++) {
            final RepNodeId rnid = new RepNodeId(1, i);
            final RepNodeParams rnp = config.getRepNodeParams(rnid);
            lp.addMap(rnp.getMap());
        }

        lp.saveParameters(FileNames.getSNAConfigFile(snp.getRootDirPath(),
                                                     gp.getKVStoreName(),
                                                     snid));
    }

    private void createJsonIndexScalar() {

        /*
         * Create and add a table with two columns; where one column is an
         * integer whose value represents the primary key of the given row,
         * and the other column contains a basic json document with scalar
         * field definitions.
         */
        jsonTableProtoScalar =
            TableBuilder.createTableBuilder(JSON_TABLE_NAME_SCALAR)
                             .addInteger(JSON_ID_FIELD_NAME)
                             .addJson(JSON_FIELD_NAME, null)
                             .primaryKey(JSON_ID_FIELD_NAME)
                             .buildTable();

        jsonTableScalar = metadata.addTable(
                             jsonTableProtoScalar.getInternalNamespace(),
                             jsonTableProtoScalar.getName(),
                             jsonTableProtoScalar.getParentName(),
                             jsonTableProtoScalar.getPrimaryKey(),
                             jsonTableProtoScalar.getPrimaryKeySizes(),
                             jsonTableProtoScalar.getShardKey(),
                             jsonTableProtoScalar.getFieldMap(),
                             null,
                             null, /*beforeImageTTL*/
                             null, false, 0, null, null);
        /*
         * Add a full text index for the json column of the table created
         * above.
         *
         * NOTE: an analyzer is specified for only the field of type string;
         * otherwise an exception will be thrown from elasticsearch
         * indicating a problem with the mapping, producing a message
         * of the form:
         *
         * 'ESHttpClient.executeAsync: failed response - status code = 400
         *  [errorStatus = BAD_REQUEST, errorType = mapper_parsing_exception,
         *  errorReason = Mapping definition for [jsonField/jsonFieldInteger]
         *  has unsupported parameters:  [analyzer : english]] from
         *  request = PUT /ondb.kvtest-oracle.kv.impl.tif.textindexfeedertest-
         *  testjsonindex.jsontable.jsonindex/_mapping/text_index_mapping
         *  HTTP/1.1'
         */
        metadata.addTextIndex
             (null,
              JSON_INDEX_NAME_SCALAR,
              jsonTableScalar.getFullName(),
              makeTextIndexList
                  (new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_STRING_NAME,
                        JSON_STRING_ENGLISH),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_INTEGER_NAME,
                        JSON_INTEGER),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_LONG_NAME,
                        JSON_LONG),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_NUMBER_NAME,
                        JSON_LONG),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_DOUBLE_NAME,
                        JSON_DOUBLE),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_BOOLEAN_NAME,
                        JSON_BOOLEAN),
                   new IndexImpl.AnnotatedField
                       (JSON_FIELD_NAME + "." + JSON_FIELD_DATE_NAME,
                        JSON_TIMESTAMP_MILLIS)
                  ),
                  null,
                  "json text index - scalar");
    }

    protected void createJsonIndexSenators() {

        /*
         * Create and add a table with two columns; where one column is an
         * integer whose value represents the primary key of the given row,
         * and the other column contains a json document with complex mix
         * elements.
         */
        jsonTableProtoSenators =
            TableBuilder.createTableBuilder(JSON_TABLE_NAME_SENATORS)
                             .addInteger(JSON_ID_FIELD_NAME)
                             .addJson(JSON_FIELD_NAME, null)
                             .primaryKey(JSON_ID_FIELD_NAME)
                             .buildTable();

        jsonTableSenators = metadata.addTable(
                                jsonTableProtoSenators.getInternalNamespace(),
                                jsonTableProtoSenators.getName(),
                                jsonTableProtoSenators.getParentName(),
                                jsonTableProtoSenators.getPrimaryKey(),
                                jsonTableProtoSenators.getPrimaryKeySizes(),
                                jsonTableProtoSenators.getShardKey(),
                                jsonTableProtoSenators.getFieldMap(),
                                null,
                                null, /*beforeImageTTL*/
                                null, false, 0, null, null);
        /*
         * Add a full text index for the json column of the table of
         * senator rows created above. If you don't want to index
         * every field of the document, then comment out the call
         * below and un-comment the call that follows; that creates
         * a smaller mapping.
         */
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(caucus, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(congressNumbers, JSON_INTEGER),
              new IndexImpl.AnnotatedField(current, JSON_BOOLEAN),
              new IndexImpl.AnnotatedField(description, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(district, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  endDate, JSON_TIMESTAMP_NANOS),
              new IndexImpl.AnnotatedField(extraAddress, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  extraContactForm, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(extraFax, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(extraOffice, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(extraRssUrl, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  leadershipTitle, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(party, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personBioGuideId, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personBirthday, JSON_TIMESTAMP),
              new IndexImpl.AnnotatedField(personCspanId, JSON_INTEGER),
              new IndexImpl.AnnotatedField(
                  personFirstname, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personGender, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personGenderLabel, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personLastname, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personLink, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personMiddlename, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personName, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personNameMod, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personNickname, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personOdId, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(personPvsId, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personSortname, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personTwitterId, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  personYoutubeId, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(phone, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(roleType, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  roleTypeLabel, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  senatorClass, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  senatorClassLabel, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(rank, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(
                  startDate, JSON_TIMESTAMP_MILLIS),
              new IndexImpl.AnnotatedField(state, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(title, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(titleLong, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(website, JSON_STRING_ENGLISH)
            ),
            null,
            "json text index - senators");

/* **********************************************************
        // Uncomment this for a smaller mapping
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
               new IndexImpl.AnnotatedField(congressNumbers, JSON_INTEGER),
               new IndexImpl.AnnotatedField(description, JSON_STRING_ENGLISH),
               new IndexImpl.AnnotatedField(extraAddress, JSON_STRING_ENGLISH),
               new IndexImpl.AnnotatedField(party, JSON_STRING_ENGLISH),
               new IndexImpl.AnnotatedField(personCspanId, JSON_INTEGER),
               new IndexImpl.AnnotatedField(personGender, JSON_STRING_ENGLISH),
               new IndexImpl.AnnotatedField(website, JSON_STRING_ENGLISH)
            ),
            null,
            "json text index - senators");
***************************************************** */
    }

    protected void createJsonIndexSenatorsForBehaviorsTest() {

        /*
         * Create and add a table with two columns; where one column is an
         * integer whose value represents the primary key of the given row,
         * and the other column contains a json document with complex mix
         * elements.
         */
        jsonTableProtoSenators =
            TableBuilder.createTableBuilder(JSON_TABLE_NAME_SENATORS)
                             .addInteger(JSON_ID_FIELD_NAME)
                             .addJson(JSON_FIELD_NAME, null)
                             .primaryKey(JSON_ID_FIELD_NAME)
                             .buildTable();

        jsonTableSenators = metadata.addTable(
                                jsonTableProtoSenators.getInternalNamespace(),
                                jsonTableProtoSenators.getName(),
                                jsonTableProtoSenators.getParentName(),
                                jsonTableProtoSenators.getPrimaryKey(),
                                jsonTableProtoSenators.getPrimaryKeySizes(),
                                jsonTableProtoSenators.getShardKey(),
                                jsonTableProtoSenators.getFieldMap(),
                                null,
                                null, /*beforeImageTTL*/
                                null, false, 0, null, null);
        /*
         * Add a full text index for the json column of the table of
         * senator rows created above. And then create another full text index
         * that has same content but different order. Both of them should be
         * created successfully.
         */
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS_ORDER,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(congressNumbers, JSON_INTEGER),
              new IndexImpl.AnnotatedField(description, JSON_STRING_ENGLISH)
            ),
            null,
            "json text index - jsonindex1");
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS_OPP_ORDER,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(description, JSON_STRING_ENGLISH),
              new IndexImpl.AnnotatedField(congressNumbers, JSON_INTEGER)
            ),
            null,
            "json text index - jsonindex2 has same fields but opposite order" +
            " to jsonindex1");

        /* Add a full text index for the json column of the table of
         * senator rows created above, one field of this index uses a wrong
         * json path. Expect it can be created successfully.
         */
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS_WRONG_PATH,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(congressNumbersWrongPath,
                                           JSON_INTEGER)
            ),
            null,
            "json text index - jsonindex3 with field that has wrong path");
        /* Add a full text index for the json column of the table of
         * senator rows created above, one field of this index has wrong cases.
         * Expect it can be created successfully.
         */
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS_WRONG_CASE,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(congressNumbersWrongCase,
                                           JSON_INTEGER)
            ),
            null,
            "json text index - jsonindex4 with field that has wrong case");
        /* Add a full text index for the json column of the table of
         * senator rows created above, one field of this index has wrong cases.
         * Expect it can be created successfully.
         */
        metadata.addTextIndex(
            null,
            JSON_INDEX_NAME_SENATORS_INCONS_TYPE,
            jsonTableSenators.getFullName(),
            makeTextIndexList(
              new IndexImpl.AnnotatedField(description, JSON_INTEGER)
            ),
            null,
            "json text index - jsonindex5 with inconsistent type");
    }
}
