/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.nosql.proxy;

import static oracle.nosql.proxy.protocol.Protocol.SERVICE_UNAVAILABLE;
import static oracle.nosql.proxy.protocol.Protocol.TABLE_NOT_FOUND;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.table.TableAPIImpl;
import oracle.nosql.common.contextlogger.LogContext;
import oracle.nosql.common.sklogger.SkLogger;
import oracle.nosql.proxy.util.TestBase;
import oracle.nosql.proxy.util.TableCache;
import org.junit.Test;

public class TableCacheTest extends TestBase {
    /*
     * Creates a cache where entries expire after 1 second of inactivity,
     * and are refreshed every 250ms
     */
    private static long expirationMs = 1000L;
    private static long refreshMs = 250L;

    private void checkEntryRefresh(TestCache cache,
                                   String namespace,
                                   String tableName) {
        TableCache.TableEntry entry =
            cache.get(namespace, tableName, null);
        assertNotNull(entry);
        long minRefTime = System.currentTimeMillis();
        /* entry should have been refreshed within the last */
        /* refreshMs, with a bit of room for thread scheduling */
        minRefTime -= (refreshMs + 60);
        assertTrue(entry.getLastRefresh() > minRefTime);
    }

    private void checkRefreshLess(long sTime, TableCache.TableEntry entry) {
        assertNotNull(entry);
        if (entry.getLastRefresh() >= sTime) {
            fail("Expected refresh time less than " + sTime + ", got " +
                 entry.getLastRefresh());
        }
    }

    @Test
    public void testCache() throws Exception {

        TestCache cache = new TestCache(expirationMs, refreshMs);

        cache.getTableEntry("ns", "mytable", null);
        Thread.sleep(50);
        cache.getTableEntry("ns", "mytable1", null);
        Thread.sleep(50);
        cache.getTableEntry(null, "mytable", null);
        Thread.sleep(50);
        cache.getTableEntry(null, "mytable1", null);

        /* were they all cached? */
        assertEquals(4, cache.getCacheSize());

        /* let refresh happen at least once */
        Thread.sleep(refreshMs);

        /* check that they all still exist */
        assertEquals(4, cache.getCacheSize());

        /* check that all have been refreshed */
        checkEntryRefresh(cache, null, "mytable");
        checkEntryRefresh(cache, null, "mytable1");
        checkEntryRefresh(cache, "ns", "mytable");
        checkEntryRefresh(cache, "ns", "mytable1");

        /* get all entries again */
        cache.getTableEntry("ns", "mytable", null);
        cache.getTableEntry("ns", "mytable1", null);
        cache.getTableEntry(null, "mytable", null);
        cache.getTableEntry(null, "mytable1", null);

        /*
         * The above accesses will reset the inactivity time for each.
         * .5 seconds and verify none have been removed (expired).
         */
        Thread.sleep(500);
        assertEquals(4, cache.getCacheSize());

        /*
         * get the first/last entry again
         */
        cache.getTableEntry("ns", "mytable", null);
        cache.getTableEntry(null, "mytable1", null);

        /*
         * wait till two should have been removed
         */
        Thread.sleep(600);
        assertEquals(2, cache.getCacheSize());

        /* check that the two not removed have recently been refreshed */
        checkEntryRefresh(cache, "ns", "mytable");
        checkEntryRefresh(cache, null, "mytable1");

        /* remove one entry */
        cache.flushEntry("ns", "mytable");
        assertEquals(1, cache.getCacheSize());

        /* do activity on the last entry */
        cache.getTableEntry(null, "mytable1", null);

        /* wait a bit longer */
        Thread.sleep(300);

        /* verify it's refreshed */
        checkEntryRefresh(cache, null, "mytable1");
        cache.shutDown();
    }

    @Test
    public void testServiceUnavailable() throws Exception {

        TestCache cache = new TestCache(expirationMs, refreshMs);

        cache.getTableEntry("ns", "mytable", null);
        /* simulate service down/unavailable: verify record not removed */
        cache.throwSU = true;
        /* wait long enough to have refreshed entry */
        Thread.sleep(refreshMs + 60);
        /* verify record still exists */
        assertNotNull(cache.get("ns", "mytable", null));
        /* delete entry */
        cache.flushEntry("ns", "mytable");
        /* verify trying to get entry throws SU exception */
        try {
            cache.getTableEntry("ns", "mytable", null);
            fail("Should have thrown RequestException");
        } catch (RequestException re) {
            assertEquals(re.getErrorCode(), SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            fail("Got unexpected exception: " + e);
        }
        /* check normal mode still works */
        cache.throwSU = false;
        assertNotNull(cache.getTableEntry("ns", "mytable", null));
        cache.shutDown();
    }

    @Test
    public void testTableNotFound() throws Exception {

        TestCache cache = new TestCache(expirationMs, refreshMs);

        cache.getTableEntry("ns", "mytable", null);
        /* simulate table not found: verify record removed */
        cache.throwNF = true;
        /* wait long enough to have refreshed entry */
        Thread.sleep(refreshMs + 60);
        /* verify record still removed */
        assertNull(cache.get("ns", "mytable", null));
        /* verify trying to get entry throws NF exception */
        try {
            cache.getTableEntry("ns", "mytable", null);
            fail("Should have thrown RequestException");
        } catch (RequestException re) {
            assertEquals(re.getErrorCode(), TABLE_NOT_FOUND);
        } catch (Exception e) {
            fail("Got unexpected exception: " + e);
        }
        /* check normal mode still works */
        cache.throwNF = false;
        assertNotNull(cache.getTableEntry("ns", "mytable", null));
        cache.shutDown();
    }

    @Test
    public void testUnknownException() throws Exception {

        TestCache cache = new TestCache(expirationMs, refreshMs);

        cache.getTableEntry("ns", "mytable", null);
        /* simulate unknown exception: verify record removed */
        cache.throwUnknown = true;
        /* wait long enough to have refreshed entry */
        Thread.sleep(refreshMs + 60);
        /* verify record removed */
        assertNull(cache.get("ns", "mytable", null));
        /* verify trying to get entry throws unknown exception */
        try {
            cache.getTableEntry("ns", "mytable", null);
            fail("Should have thrown Exception");
        } catch (RequestException re) {
            fail("Got unexpected exception: " + re);
        } catch (Exception e) {
            /* success */
        }
        /* check normal mode still works */
        cache.throwUnknown = false;
        assertNotNull(cache.getTableEntry("ns", "mytable", null));
        cache.shutDown();
    }

    @Test
    public void testValidActivity() throws Exception {

        TestCache cache = new TestCache(expirationMs, refreshMs);
        cache.getTableEntry("ns", "mytable", null);
        /*
         * verify that entry stays in cache as long as we keep
         * accessing it within the inactivity time
         */
        for (int x=0; x<10; x++) {
            /* entry should exist without fetching again */
            TableCache.TableEntry entry =
                cache.get("ns", "mytable", null);
            assertNotNull(entry);
            /* simulate "activity" */
            entry.setLastUsed(System.currentTimeMillis());
            Thread.sleep(300);
        }

        cache.shutDown();
    }

    @Test
    public void testNoRefresh() throws Exception {
        /*
         * Test expiration without refresh
         */
        TestCache cache = new TestCache(500L, 0L);
        cache.getTableEntry("ns", "mytable", null);
        cache.getTableEntry("ns", "mytable1", null);
        cache.getTableEntry(null, "mytable", null);
        cache.getTableEntry(null, "mytable1", null);
        long sTime = System.currentTimeMillis() + 1;

        Thread.sleep(400);
        /* no entries should have been refreshed */
        checkRefreshLess(sTime, cache.get("ns", "mytable", null));
        checkRefreshLess(sTime, cache.get("ns", "mytable1", null));
        checkRefreshLess(sTime, cache.get(null, "mytable", null));
        checkRefreshLess(sTime, cache.get(null, "mytable1", null));

        Thread.sleep(300);
        /* cache should be empty after expiration */
        assertEquals(0, cache.getCacheSize());
        cache.shutDown();
    }

    @Test
    public void testBadParams() throws Exception {
        /* test bad constructor parameters */
        try {
            new TestCache(1L, 1L);
            fail("should have thrown");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("expiration"));
        }
        try {
            new TestCache(1L, -1L);
            fail("should have thrown");
        } catch (IllegalArgumentException iae) {
            assertTrue(iae.getMessage().contains("refresh"));
        }
    }

    public class TestCache extends TableCache {

        /* for specific tests */
        public boolean throwSU = false;
        public boolean throwNF = false;
        public boolean throwUnknown = false;

        public TestCache(long expirationMs, long refreshMs) {
            /* create cache with 50ms thread interval */
            super(new SkLogger("oracle.nosql.proxy", "TestCache"),
                  expirationMs, refreshMs, 50L);
        }

        @Override
        protected TableEntry getTable(String namespace,
                                      String tableName,
                                      String nsname,
                                      LogContext lc) {
            if (throwSU) {
                throw new RequestException(SERVICE_UNAVAILABLE, "unavailable");
            }
            if (throwNF) {
                throw new RequestException(TABLE_NOT_FOUND, "table not found");
            }
            if (throwUnknown) {
                throw new RuntimeException("unknown error");
            }
            return new TestTableEntry(namespace, tableName, nsname);
        }

        @Override
        public void shutDown() {
            super.shutDown();
        }

        @Override
        public KVStoreImpl getStoreByName(String storeName) {
            return null;
        }

        private class TestTableEntry extends TableEntry {

            private TestTableEntry(String namespace,
                                   String tableName,
                                   String nsname) {
                super(null);
            }

            @Override
            public KVStoreImpl getStore() {
                return null;
            }

            @Override
            public TableAPIImpl getTableAPI() {
                return null;
            }

            @Override
            public String getStoreName() {
                return "nostore";
            }

            @Override
            public RequestLimits getRequestLimits() {
                return null;
            }
        }
    }
}
