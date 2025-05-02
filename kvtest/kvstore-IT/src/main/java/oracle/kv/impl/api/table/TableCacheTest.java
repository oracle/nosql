/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import oracle.kv.impl.util.PollCondition;

import org.junit.Test;

/*
 * comment out until cache issue is fixed to quiet mvn warnings.
 * import org.junit.Test;
 */

/**
 * Test the table cache.
 */
public class TableCacheTest {

    private static final int INITIAL_CAPACITY = 10;
    private static final long NO_EXPIRE = 0L;
    private static final long TEST_ENTRY_LIFETIME_MS = 10L;

    private static long ID = 1L;
    private static int SEQ_NUM = 100;

    @Test
    public void testTableCache() {
        final TableCache cache = new TableCache(INITIAL_CAPACITY, NO_EXPIRE);

        /* -- check basic functions -- */

        assertEquals("Wrong capacity", INITIAL_CAPACITY, cache.getCapacity());

        checkNumEntries(cache, 0);

        TableImpl t1 = makeTable("foo", "bar");
        cache.put(t1);
        checkNumEntries(cache, 1);

        /* Insert same table */
        TableImpl ret = cache.put(t1);
        assertTrue("Expected null but returned " + ret, ret == null);
        checkNumEntries(cache, 1);

        TableImpl t2 = t1.clone();
        setSeqNum(t2);

        /* Insert newer table */
        ret = cache.put(t2);
        assertTrue("Expected table but returned null", ret != null);
        checkNumEntries(cache, 1);

        ret = cache.get(t2.getFullNamespaceName());
        assertEquals("Wrong seq number",
                     t2.getSequenceNumber(), ret.getSequenceNumber());

        /* Case insensitive */
        ret = cache.get(t2.getFullNamespaceName().toUpperCase());
        assertEquals("Wrong seq number",
                     t2.getSequenceNumber(), ret.getSequenceNumber());

        /* Attempt to insert older table */
        ret = cache.put(t1);
        assertTrue("Expected null but returned " + ret, ret == null);

        ret = cache.get(t2.getFullNamespaceName());
        assertEquals("Wrong seq number",
                     t2.getSequenceNumber(), ret.getSequenceNumber());

        /* Get by ID */
        ret = cache.get(t2.getId());
        assertTrue("Expected table but returned null", ret != null);
        assertEquals("Wrong seq number",
                     t2.getSequenceNumber(), ret.getSequenceNumber());

        /* Same name, no namespace */
        cache.put(makeTable(null, "bar"));
        checkNumEntries(cache, 2);

        /* Same name, different namespace */
        cache.put(makeTable("Different", "bar"));
        checkNumEntries(cache, 3);

        cache.clear();
        checkNumEntries(cache, 0);

        /* -- Child tables -- */

        t1 = makeTable("foo", "bar");
        final int origSeqNum = t1.getSequenceNumber();

        cache.put(t1);

        TableImpl t1Copy = t1.clone();    /* Prevent modifying the cached copy */

        TableImpl c1 = makeChildTable("child1", t1Copy);
        assertNotEquals("Parent table seq should have changed",
                        origSeqNum, t1Copy.getSequenceNumber());
        assertEquals("Parent and child should have same seq number",
                     t1Copy.getSequenceNumber(), c1.getSequenceNumber());

        TableImpl fromCache = cache.get(t1Copy.getId());
        assertEquals("Cache should contain original table",
                     t1.getSequenceNumber(), fromCache.getSequenceNumber());


        TableImpl c2 = makeChildTable("child2", c1);

        /* Insert the middle child */
        cache.put(c1);
        checkNumEntries(cache, 1);

        fromCache = cache.get(t1Copy.getId());
        assertEquals("Cache should contain updated parent table",
                     t1Copy.getSequenceNumber(), fromCache.getSequenceNumber());

        fromCache = cache.get(c1.getId());
        assertNotNull("Cache should gotten middle child table", fromCache);
        assertEquals("Cache should gotten middle child table",
                     c1.getId(), fromCache.getId());

        fromCache = cache.get(c2.getId());
        assertNotNull("Cache should gotten child table", fromCache);
        assertEquals("Cache should gotten child table",
                     c2.getId(), fromCache.getId());

        /* Get by names */
        fromCache = cache.get(c1.getFullNamespaceName());
        assertEquals("Cache should gotten child table",
                     c1.getId(), fromCache.getId());
        fromCache = cache.get(c2.getFullNamespaceName());
        assertEquals("Cache should gotten child table",
                     c2.getId(), fromCache.getId());

        cache.clear();

        /* -- test invalidation -- */

        cache.put(t2);

        /* Invalide with lower seq num, should do nothing */
        cache.validate(t2.getId(), t2.getSequenceNumber()-1);
        checkNumEntries(cache, 1);

        /* Invalide with same seq num, should do nothing */
        cache.validate(t2.getId(), t2.getSequenceNumber());
        checkNumEntries(cache, 1);

        /* Invalide with higher seq num, should remove it */
        cache.validate(t2.getId(), t2.getSequenceNumber()+1);
        checkNumEntries(cache, 0);

        cache.put(t2);
        checkNumEntries(cache, 1);
        cache.remove(t2.getFullNamespaceName());
        checkNumEntries(cache, 0);

        /* -- test capacity -- */

        for (int i = 0; i < INITIAL_CAPACITY*2; i++) {
            cache.put(makeTable("foo", "bar"+i));
        }
        checkNumEntries(cache, INITIAL_CAPACITY);

        /* Increasing should not change the entries */
        cache.setCapacity(INITIAL_CAPACITY*2);
        checkNumEntries(cache, INITIAL_CAPACITY);

        for (int i = 0; i < INITIAL_CAPACITY*2; i++) {
            cache.put(makeTable("foo", "barII"+i));
        }
        checkNumEntries(cache, INITIAL_CAPACITY*2);

        /* Decreasing should clear the cache */
        cache.setCapacity(INITIAL_CAPACITY);
        checkNumEntries(cache, 0);

        /* -- test expiration -- */

        cache.setLifetime(TEST_ENTRY_LIFETIME_MS);
        assertEquals("Wrong lifetime",
                     TEST_ENTRY_LIFETIME_MS, cache.getLifetime());

        for (int i = 0; i < INITIAL_CAPACITY*2; i++) {
            cache.put(makeTable("foo", "bar"+i));
        }
        checkNumEntries(cache, INITIAL_CAPACITY);

        assertTrue("Failed to expire entries",
                   new PollCondition((int)TEST_ENTRY_LIFETIME_MS,
                                         TEST_ENTRY_LIFETIME_MS*5) {
                        @Override
                        protected boolean condition() {
                            if (cache.getAllValues().isEmpty()) {
                                return true;
                            }
                            /* Get of each entry will remove it if expired */
                            for (TableImpl t : cache.getAllValues()) {
                                cache.get(t.getId());
                            }
                            return false;
                        }
                    }.await());
        /* This will also check that the id cache is cleared */
        checkNumEntries(cache, 0);
    }

    /* Checks the expected number of entries in the cache */
    private void checkNumEntries(TableCache cache, int expectedSize) {
        assertEquals("Wrong number of entries",
                     expectedSize, cache.getAllValues().size());
        /**
         * If the cache is empty, we should expect the ID map to be empty
         */
        if (expectedSize == 0) {
            assertEquals("ID map should be empty", 0, cache.getIdMapSize());
        }
    }

    /* Sets the sequence number of the table to the next seq number */
    private TableImpl setSeqNum(TableImpl target) {
        target.setSequenceNumber(SEQ_NUM++);
        return target;
    }

    /*
     * Make a table with the specified name. The ID and sequence numbers will
     * be set.
     */
    private TableImpl makeTable(String namespace, String name) {
        final TableImpl table = TableBuilder.createTableBuilder(namespace, name)
                .addInteger("id")
                .addString("firstName")
                .addString("lastName")
                .primaryKey("id")
                .shardKey("id")
                .buildTable();
        table.setId(ID++);
        return setSeqNum(table);
    }

    private TableImpl makeChildTable(String name,
                                     TableImpl parent) {
        final TableImpl child =
                    TableBuilder.createTableBuilder(name, null, parent)
                .addString(name)
                .primaryKey(name)
                .buildTable();
        parent.addChild(child);
        child.setId(ID++);
        return setSeqNum(child);
    }
}
