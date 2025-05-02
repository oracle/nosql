/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.tif.esclient.esResponse.GetResponse;
import oracle.kv.impl.tif.esclient.esResponse.IndexDocumentResponse;
import oracle.kv.impl.tif.esclient.jsonContent.ESJsonBuilder;
import oracle.kv.impl.topo.PartitionId;
import oracle.kv.impl.util.VersionUtil;

import com.fasterxml.jackson.core.JsonGenerator;

import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests to test operations to Elastic Search.
 */
public class ElasticsearchHandlerTest extends TextIndexFeederTestBase {

    private int esHttpPort;
    private static final String esClusterName = "tif-test-es-cluster";
    private ElasticsearchHandler esHandler;

    final String esTestIndexKey = "es-index-text-key";
    private String esIndexName;

    @BeforeClass
    public static void staticSetUp() throws Exception {
         Assume.assumeTrue(
            "FTS is currently incompatible with Java Versons < 11 ",
            VersionUtil.getJavaMajorVersion() >= 11);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        System.setProperty("es.path.home", System.getProperty("testdestdir"));
        esHttpPort = Integer.parseInt(System.getProperty("es.http.port"));

        final String esMembers = "localhost:" + esHttpPort;

        esHandler =
            ElasticsearchHandler.newInstance(esClusterName, esMembers, false,
                                             null, 5000, logger);
        /* enable ensure commit for testing */
        esHandler.enableEnsureCommit();
        esIndexName =
            TextIndexFeeder.deriveESIndexName(kvstoreName, null, "esTestTable",
                                              "esTestIndex");
    }

    @Override
    public void tearDown() throws Exception {

        if (esHandler != null) {
            esHandler.close();
        }


        super.tearDown();
    }

    /**
     * Test ES index operations.
     */
    @Test
    public void testESIndexOperations() throws Exception {

        esHandler.createESIndex(esIndexName);
        if (!esHandler.existESIndex(esIndexName)) {
            fail("fail to create test es index " + esIndexName);
        }

        esHandler.deleteESIndex(esIndexName);
        if (esHandler.existESIndex(esIndexName)) {
            fail("fail to delete test es index " + esIndexName);
        }
    }

    /**
     * Test data operations like get and put/indexing to ES index.
     */
    @Test
    public void testESMappingOperations() throws Exception {

        final JsonGenerator mappingSpec =
            buildTestMappingSpec(esIndexName, false);
        esHandler.createESIndex(esIndexName);
        assertTrue("Expect existent index " + esIndexName,
                   esHandler.existESIndex(esIndexName));

        esHandler.createESIndexMapping(esIndexName, mappingSpec);
        assertTrue("Expect existent index mapping ",
                   esHandler.existESIndexMapping(esIndexName));

        esHandler.createESIndexMapping(esIndexName, mappingSpec);
        
        esHandler.deleteESIndex(esIndexName);
        /* mapping and index must be gone */
        assertFalse("Expect non-existent index mapping ",
                    esHandler.existESIndexMapping(esIndexName));
        assertFalse("expect non-existent es index " + esIndexName,
                    esHandler.existESIndex(esIndexName));
    }

    /**
     * Test data operations like get and put/indexing to ES index.
     */
    @Test
    public void testESIndexDataOperations() throws Exception {

        final JsonGenerator mappingSpec =
            buildTestMappingSpec(esIndexName, false);

        esHandler.createESIndex(esIndexName);
        assertTrue("Expect existent index " + esIndexName,
                   esHandler.existESIndex(esIndexName));

        esHandler.createESIndexMapping(esIndexName, mappingSpec);
        assertTrue("Expect existent index mapping ",
                   esHandler.existESIndexMapping(esIndexName));

        /* create a test checkpoint state */
        final CheckpointState curState = createCkptState();
        logger.info("checkpoint state to send to ES:\n" + curState);
        /* make a put op from state and send to ES index */
        esHandler.index(makePutOp(curState, esIndexName, esTestIndexKey));

        /* read it back from ES index */
        final GetResponse response =
            esHandler.get(esIndexName, esTestIndexKey);
        /* doc must exist */
        assertTrue("expect existent doc with key " + esTestIndexKey,
                   response.isFound());

        final Map<String, Object> map = response.sourceAsMap();
        assertTrue("expect map with >1 entries", (map.size() > 1));
        final CheckpointState fetchedState = new CheckpointState(map);
        logger.info("checkpoint state received from ES:\n" + fetchedState);
        verifyCheckpointState(curState, fetchedState);

        /* delete it */
        esHandler.del(esIndexName, esTestIndexKey);

        /* read it back from ES index */
        final GetResponse response1 =
            esHandler.get(esIndexName, esTestIndexKey);
        /* doc must be gone */
        assertFalse("expect non-existent doc with key " + esTestIndexKey,
                    response1.isFound());

        /* clean up */
        esHandler.deleteESIndex(esIndexName);
        /* mapping and index must be gone */
        assertFalse("Expect non-existent index mapping ",
                    esHandler.existESIndexMapping(esIndexName));
        assertFalse("expect non-existent es index " + esIndexName,
                    esHandler.existESIndex(esIndexName));
    }

    /**
     * Test to send rows from joke table to ES for indexing. Verify by
     * comparing the document sent to ES and read from ES.
     *
     * @throws Exception
     */
    @Test
    public void testESIndexRows() throws Exception {

        final IndexImpl index =
            (IndexImpl) jokeTable.getTextIndex("JokeIndex");
        final JsonGenerator mapSpec =
            ElasticsearchHandler.generateMapping(index);

        logger.info("Dump map spec for Joke table:\n" + mapSpec);

        esHandler.createESIndex(esIndexName);
        esHandler.createESIndexMapping(esIndexName, mapSpec);

        for (int i = 0; i < realJokes.length; i++) {
            final RowImpl row = makeJokeRow(jokeTable, realJokes[i], i);
            /* Save the row's key in the data source array */
            realJokes[i].setKey(TableKey.createKey(jokeTable, row, false)
                                        .getKey().toString());

            final IndexOperation document =
                ElasticsearchHandler.makePutOperation(index, esIndexName, row);

            final IndexDocumentResponse response = esHandler.index(document);
            assert (response.isCreated());
        }

        /* Read the data back from ES and verify it. */
        for (Joke joke : realJokes) {
            final GetResponse getResponse =
                esHandler.get(esIndexName, joke.getKey());

            assertTrue("joke must exist in ES index", getResponse.isFound());
            assertEquals(joke.getText(),
                         getResponse.sourceAsMap().get("text"));
        }
    }

    /**
     * Test that we are able to fetch a list of ES indexs that belong to a
     * given kvstore.
     *
     * @throws Exception
     */
    @Test
    public void testGetAllESIndexesInStore() throws Exception {

        /* create an ES index */
        final IndexImpl index =
            (IndexImpl) jokeTable.getTextIndex("JokeIndex");
        final JsonGenerator mapSpec =
            ElasticsearchHandler.generateMapping(index);
        logger.info("Dump map spec for Joke table:\n" + mapSpec);
        esHandler.createESIndex(esIndexName, new HashMap<String, String>());
        esHandler.createESIndexMapping(esIndexName, mapSpec);

        /* create another ES index under the same kvstore name */
        final String randomESIndexName =
            TextIndexFeeder.deriveESIndexName(kvstoreName, null, "randomTable",
                                              "randomIndex");
        esHandler.createESIndex(randomESIndexName,
                                new HashMap<String, String>());

        /* create another ES index under a different kvstore name */
        final String randomESIndexName2 =
            TextIndexFeeder.deriveESIndexName("randomKVStore", null,
                                              "randomTable", "randomIndex");
        esHandler.createESIndex(randomESIndexName2,
                                new HashMap<String, String>());

        /*
         * we have created 3 ES indices, two of them comes with the kvstore
         * name, now verify we can fetch these two, while skipping the last
         * that is under a different kvstore name.
         */

        /* get all ES indices with kvstore name */
        final Set<String> allESIndices =
            esHandler.getAllESIndexNamesInStore(kvstoreName);

        /* expect two ES indices */
        assertEquals(2, allESIndices.size());

        /* returned index name should belong to the store */
        final String prefix = TextIndexFeeder.deriveESIndexPrefix(kvstoreName);
        for (String indexName : allESIndices) {
            assertTrue("Index name should start with prefix " + prefix +
                    ", index name: " + indexName,
                       indexName.startsWith(prefix));
        }

    }

    /* build a test mapping spec for ES index mapping for checkpoint */
    private JsonGenerator
            buildTestMappingSpec(String indexName, boolean indexVLSN) {

        try {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();

            final ESJsonBuilder mapping = new ESJsonBuilder(baos);

            mapping.startStructure().field("dynamic", "false").startStructure(
                                                                 "properties");

            final Set<String> allFields =
                new CheckpointState().getFieldsNameValue().keySet();

            for (String key : allFields) {
                if (!indexVLSN && key.contains("VLSN")) {
                    continue;
                }

                mapping.startStructure(key).field("index", "false")
                       .field("type", "text").endStructure();
            }

            mapping.endStructure(); // properties end
            mapping.endStructure(); // mapping ends
            logger.info("mapping spec created: " + mapping);

            return mapping.jsonGenarator();
        } catch (IOException e) {
            throw new IllegalStateException("Unable to serialize ES mapping " +
                                            "for checkpoint index " +
                                            indexName, e);
        }
    }

    /* create a put op for ES index from ckpt state */
    private IndexOperation makePutOp(CheckpointState checkpointState,
                                     String indexName,
                                     String checkpointKey) {

        try {
            /* create a document with all checkpoint field */
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ESJsonBuilder document = new ESJsonBuilder(baos);
            document.startStructure();

            for (Map.Entry<String, String> entry :
                     checkpointState.getFieldsNameValue().entrySet()) {

                final String k = entry.getKey();
                final String v = entry.getValue();

                document.field(k, v);
            }

            document.endStructure();
            /* create an input op from document and key */
            return new IndexOperation(indexName, checkpointKey,
                                      document.byteArray(),
                                      IndexOperation.OperationType.PUT);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to serialize ES document " +
                    "for checkpoint: " + toString());
        }
    }

    /* create a test checkpoint state */
    private CheckpointState createCkptState() {
        final CheckpointState checkpointState = new CheckpointState();
        checkpointState.setCheckpointTimeStamp(System.currentTimeMillis());
        checkpointState.setCheckpointVLSN(100);
        checkpointState.setGroupName("test-rg");
        checkpointState.setSrcRepNode("test-source-rn");
        final Set<PartitionId> comp = new HashSet<>();
        comp.add(new PartitionId(1));
        checkpointState.setCompleteTransParts(comp);

        return checkpointState;
    }

}
