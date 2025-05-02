/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.tif;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableKey;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.api.table.TimestampUtils;
import oracle.kv.table.Index;

import org.junit.Test;

/**
 * Unit tests to test translation utility functions.
 */
public class TranslationTest extends TextIndexFeederTestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testRowDeserialization() {

        /* create a row from joke table */
        RowImpl row = makeJokeRow(jokeTable, realJokes[0], 0);
        logger.finest("testRowDeserialization: row = " + row);
        byte[] keyBytes = row.getPrimaryKey(false).toByteArray();
        byte[] valBytes = row.createValue().toByteArray();
        RowImpl row2 = deserializeRow(metadata, "JokeIndex", keyBytes,
                                      valBytes);
        logger.finest("testRowDeserialization: row2 = " + row2);
        assert (row2 != null);
        assertEquals(row, row2);
        assertEquals(row2.getTable(), jokeTable);

        /* for a text index uninterested to the row, expect null */
        row2 = deserializeRow(metadata, "FirstNameIndex", keyBytes, valBytes);
        assertEquals(null, row2);

        /* create another row from user table */
        row = userTable.createRow();
        row.put("id", 1);
        row.put("firstName", "Otto");
        row.put("lastName", "Parts");
        row.put("age", 17);
        row.put("birthdate",
                TimestampUtils.parseString("1953-09-15T01:02:03"));
        row.put("startdate",
                TimestampUtils.parseString("1973-06-04T08:01:01.123"));
        row.put("enddate",
                TimestampUtils.parseString("2017-12-31T11:59:59.546789321"));
        logger.finest("testRowDeserialization: row = " + row);
        keyBytes = row.getPrimaryKey(false).toByteArray();
        valBytes = row.createValue().toByteArray();
        row2 = deserializeRow(metadata, "FirstNameIndex", keyBytes, valBytes);
        assert (row2 != null);
        assertEquals(row, row2);
        assertEquals(userTable.getFullName(), row2.getTable().getFullName());

        /* for a text index uninterested to the row, expect null */
        row2 = deserializeRow(metadata, "JokeIndex", keyBytes, valBytes);
        assertEquals(null, row2);

        /*
         * after we drop the text index,  we should not be able to
         * deserialize the row
         */
        final int numTextIndices = metadata.getTextIndexes().size();
        metadata.dropIndex(null, "FirstNameIndex", userTable.getFullName());
        assertEquals(numTextIndices - 1, metadata.getTextIndexes().size());
        /* unable to deserialize row since text index has been dropped */
        row2 = deserializeRow(metadata, "FirstNameIndex", keyBytes, valBytes);
        assertNull(row2);
    }

    @Test
    public void testMappingGeneration() {
        final IndexImpl indexImpl =
            (IndexImpl) jokeTable.getTextIndex("JokeIndex");

        final String mapSpec =
            ElasticsearchHandler.constructMapping(indexImpl);
        logger.info("mapping spec created:\n" + mapSpec);
        final String expected = "\"analyzer\":\"english\"";
        assertTrue(mapSpec.contains(expected));
    }

    @Test
    public void testIndexableDocumentGeneration() {

        IndexOperation op;
        String expectedPkPath;
        final String indexName = "testESIndex";
        final String textIndexName = "JokeIndex";
        final int jokeID = 7;

        final RowImpl row = makeJokeRow(jokeTable, realJokes[jokeID], jokeID);
        final IndexImpl indexImpl =
            (IndexImpl) jokeTable.getTextIndex(textIndexName);

        /* Verify put operation generation */
        op = ElasticsearchHandler.makePutOperation(indexImpl, indexName, row);
        assertEquals(indexName, op.getESIndexName());
        expectedPkPath =
                TableKey.createKey(row.getTable(), row, false)
                        .getKey()
                        .toString();
        assertEquals(expectedPkPath, op.getPkPath());

        /* Verify del operation generation */
        op = ElasticsearchHandler.makeDeleteOperation(indexImpl, indexName,
                                                      row);
        assertEquals(indexName, op.getESIndexName());
        expectedPkPath =
                TableKey.createKey(row.getTable(), row, false)
                        .getKey()
                        .toString();
        assertEquals(expectedPkPath, op.getPkPath());
    }


    /**
     * For test only. Takes a key/value pair and returns a RowImpl iff the key
     * belongs to a table on which a text index is defined.
     */
    private RowImpl deserializeRow(TableMetadata tableMetadata,
                                   String indexName,
                                   byte[] keyBytes,
                                   byte[] valueBytes) {

        IndexImpl tiImpl = null;
        /* get the text index from table metadata */
        for (Index ti : tableMetadata.getTextIndexes()) {
            if (ti.getName().compareToIgnoreCase(indexName) == 0) {
                tiImpl = (IndexImpl) ti;
                break;
            }
        }

        if (tiImpl != null) {
            return tiImpl.deserializeRow(keyBytes, valueBytes);
        }

        return null;
    }
}
