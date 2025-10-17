/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 */

package oracle.nosql.proxy;

import static oracle.nosql.driver.util.TimestampUtil.createTimestamp;
import static oracle.nosql.driver.util.TimestampUtil.getNanoSeconds;
import static oracle.nosql.driver.util.TimestampUtil.getSeconds;
import static oracle.nosql.driver.util.TimestampUtil.parseString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Timestamp;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;

import org.junit.Test;

/**
 * Test put/get/delete on a table that contains Timestamp field.
 */
public class TimestampTest extends ProxyTestBase {
    private final String tableName = "testTimestamp";

    private final String createTableDDL =
        "create table if not exists " + tableName + "(" +
            "ts0 timestamp(0), " +
            "ts1 timestamp(1), " +
            "ts2 timestamp(2), " +
            "ts3 timestamp(3), " +
            "ts4 timestamp(4), " +
            "ts5 timestamp(5), " +
            "ts6 timestamp(6), " +
            "ts7 timestamp(7), " +
            "ts8 timestamp(8), " +
            "ts9 timestamp(9), " +
            "primary key(shard(ts0), ts9))";

    private final Timestamp epoc = new Timestamp(0);
    private final Timestamp max = parseString("9999-12-31T23:59:59.999999999");
    private final Timestamp min = parseString("-6383-01-01");

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tableOperation(handle, createTableDDL,
                       new TableLimits(20000, 20000, 50),
                       TableResult.State.ACTIVE, 10000);
    }

    @Test
    public void testPutGetDelete() {

        /* A timestamp  */
        String text = "2017-07-13T16:48:05.123456789";
        Timestamp ts = parseString(text);
        doPutGetDeleteTest(ts);

        /* A timestamp with negative year */
        text = "-117-07-13T01:02:52.987654321";
        ts = parseString(text);
        doPutGetDeleteTest(ts);

        /* Use the epoc value: 1970-01-01 */
        doPutGetDeleteTest(epoc);

        /* 1970-01-01T00:00:00.999999999 */
        ts = (Timestamp)epoc.clone();
        ts.setNanos(999999999);
        doPutGetDeleteTest(ts);

        /* Use the minimum Timestamp value */
        doPutGetDeleteTest(min);

        /* Use the maximum Timestamp value */
        doPutGetDeleteTest(max);

        /* Timestamp value is less than minimum value, put should fail. */
        long seconds = getSeconds(min);
        seconds--;
        ts = createTimestamp(seconds, 999999999);
        doPutGetDeleteTest(ts, false);

        /* Timestamp value is greater than minimum value, put should fail */
        seconds = getSeconds(max);
        seconds++;
        ts = createTimestamp(seconds, 0);
        doPutGetDeleteTest(ts, false);
    }

    private void doPutGetDeleteTest(Timestamp ts) {
        doPutGetDeleteTest(ts, true);
    }

    private void doPutGetDeleteTest(Timestamp ts, boolean putShouldSucceed) {
        /* Put a row */
        MapValue value = new MapValue()
            .put("ts0", ts)
            .put("ts1", ts)
            .put("ts2", ts)
            .put("ts3", ts)
            .put("ts4", ts)
            .put("ts5", ts)
            .put("ts6", ts)
            .put("ts7", ts)
            .put("ts8", ts)
            .put("ts9", ts);

        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName(tableName);
        try {
            PutResult putRes = handle.put(putRequest);
            if (putShouldSucceed) {
                assertNotNull("Put failed", putRes.getVersion());
                assertWriteKB(putRes);
            } else {
                fail("Put should fail but not");
            }
        } catch (Exception ex) {
            if (putShouldSucceed) {
                fail("Put should succeed but fail");
            }
            return;
        }

        /* Get the row */
        MapValue key = new MapValue()
            .put("ts0", ts)
            .put("ts9", ts);
        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName(tableName);
        GetResult getRes = handle.get(getRequest);
        value = getRes.getValue();
        assertNotNull("Get failed", value);
        assertReadKB(getRes);

        Timestamp val = value.get("ts0").getTimestamp();
        assertTrue("Wrong value of ts0", val.compareTo(roundUp(ts, 0)) == 0);

        val = value.get("ts1").getTimestamp();
        assertTrue("Wrong value of ts1", val.compareTo(roundUp(ts, 1)) == 0);

        val = value.get("ts2").getTimestamp();
        assertTrue("Wrong value of ts2", val.compareTo(roundUp(ts, 2)) == 0);

        val = value.get("ts3").getTimestamp();
        assertTrue("Wrong value of ts3", val.compareTo(roundUp(ts, 3)) == 0);

        val = value.get("ts4").getTimestamp();
        assertTrue("Wrong value of ts4", val.compareTo(roundUp(ts, 4)) == 0);

        val = value.get("ts5").getTimestamp();
        assertTrue("Wrong value of ts5", val.compareTo(roundUp(ts, 5)) == 0);

        val = value.get("ts6").getTimestamp();
        assertTrue("Wrong value of ts6", val.compareTo(roundUp(ts, 6)) == 0);

        val = value.get("ts7").getTimestamp();
        assertTrue("Wrong value of ts7", val.compareTo(roundUp(ts, 7)) == 0);

        val = value.get("ts8").getTimestamp();
        assertTrue("Wrong value of ts8", val.compareTo(roundUp(ts, 8)) == 0);

        val = value.get("ts9").getTimestamp();
        assertTrue("Wrong value of ts9", val.compareTo(roundUp(ts, 9)) == 0);

        /* Delete the row */
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName(tableName);
        DeleteResult delRes = handle.delete(delRequest);
        assertTrue("Delete failed", delRes.getSuccess());
    }

    /**
     * Rounds the fractional second of Timestamp according to the specified
     * precision.
     */
    private Timestamp roundUp(Timestamp ts, int precision) {
        if (precision == 9 || ts.getNanos() == 0) {
            return ts;
        }

        long seconds = getSeconds(ts);
        int nanos = getNanoSeconds(ts);
        double base = Math.pow(10, (9 - precision));
        nanos = (int)(Math.round(nanos / base) * base);
        if (nanos == (int)Math.pow(10, 9)) {
            seconds++;
            nanos = 0;
        }
        Timestamp ts1 = createTimestamp(seconds, nanos);
        if (ts1.compareTo(max) > 0 ) {
            ts1 = (Timestamp)max.clone();
            nanos = (int)((int)(ts.getNanos() / base) * base);
            ts1.setNanos((int)((int)(ts.getNanos() / base) * base));
        }
        return ts1;
    }
}
