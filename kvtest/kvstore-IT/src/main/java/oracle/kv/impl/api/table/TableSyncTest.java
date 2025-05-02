/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import oracle.kv.Version;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.Row;
import oracle.kv.table.Table;

import org.junit.Test;

/**
 * This test exercises a synchronization bug [#25552] in TableImpl that was
 * caused by a refactor of the table code for the 4.2 release that introduced
 * per-version transient state. It is kept to prevent something similar from
 * happening in the future.
 */
public class TableSyncTest extends TableTestBase {

    private final String tableName = "testTable";
    private final Random rand = new Random();

    @Test
    public void test() throws Exception {
        int numRows = 5000;

        Table table = createTableAndGet();

        loadRows(table, numRows);
        Table tableNew = evolveTable();

        /* The number of exercise threads. */
        int nThreads = 20;
        List<Thread> tasks = new ArrayList<Thread>();
        for (int i = 0; i < nThreads; i++) {
            tasks.add(new ExerciseThread(tableNew, 0, numRows));
        }
        for (Thread t : tasks) {
            t.start();
        }
        for (Thread t : tasks) {
            t.join();
        }
    }

    private Table evolveTable() {
        Table table = getTable(tableName);
        String[] fieldsToAdd = new String[] {"s1", "i1", "l1", "f1"};

        for (String field : fieldsToAdd) {
            TableEvolver evolver =
                 TableEvolver.createTableEvolver(table);
            if (field.startsWith("s")) {
                evolver.addString(field);
            } else if (field.startsWith("i")) {
                evolver.addInteger(field);
            } else if (field.startsWith("l")) {
                evolver.addLong(field);
            } else if (field.startsWith("f")) {
                evolver.addFloat(field);
            }
            evolver.evolveTable();
            table = evolveAndGet(evolver);
        }
        return table;
    }

    private class ExerciseThread extends Thread {
        private final Table table;
        private final int from;
        private final int num;

        ExerciseThread(Table table, int from, int num) {
            this.table = table;
            this.from = from;
            this.num = num;
        }

        @Override
        public void run() {
            int i = 0;
            for (i = from; i < num; i++) {
                PrimaryKey key = table.createPrimaryKey();
                key.put("id", (long)i);
                try {
                    Row row = tableImpl.get(key, null);
                    assertTrue(row != null);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    fail(this + " failed :" + ex);
                }
            }
        }
    }

    private Table createTableAndGet() {
        String ddl = "create table if not exists " + tableName + "(" +
                         "id long, " +
                         "seed long, " +
                         "i integer, " +
                         "l long," +
                         "d double, " +
                         "f float, " +
                         "b boolean, " +
                         "s string, " +
                         "bi binary, " +
                         "fbi binary(128), " +
                         "e enum(sint, int, long, float, double)," +
                         "ar array(string), " +
                         "primary key(id))";
        executeDdl(ddl, true);
        Table table = getTable(tableName);
        assertTrue(table != null);
        return table;
    }

    private void loadRows(Table table, int numRows) {
        for (int i = 0; i < numRows; i++) {
            Row row = getRandomRow(table, i, rand.nextLong());
            Version v = tableImpl.put(row, null, null);
            assertTrue (v != null);
        }
    }

    private Row getRandomRow(Table table, long index, long seed) {
        Row row = table.createRow();
        row.put("id", index);
        row.put("seed", seed);
        Random random = new Random(seed);
        for(String field: table.getFields()) {
            if (field.equals("id") || field.equals("seed")) {
                continue;
            }
            if (field.equals("cid")) {
                row.put("cid", index);
                continue;
            }
            FieldDef def = table.getField(field);
            if (def.isArray()) {
                def = def.asArray().getElement();
                ArrayValue array = row.putArray(field);
                FieldValue value = getFieldValue(random, def);
                array.add(value);
                row.put(field, array);
            } else {
                row.put(field, getFieldValue(random, def));
            }
        }
        return row;
    }

    private FieldValue getFieldValue(Random random, FieldDef def) {
        switch (def.getType()) {
        case INTEGER:
            return def.createInteger(random.nextInt());
        case LONG:
            return def.createLong(random.nextLong());
        case FLOAT:
            return def.createFloat(random.nextFloat());
        case DOUBLE:
            return def.createDouble(random.nextDouble());
        case STRING:
            double randomLen = random.nextDouble();
            if (randomLen >= 0 && randomLen <= 0.2) {
                return def.createString(
                    randomString(random, random.nextInt(40)));
            }
            return def.createString(
                    randomString(random, random.nextInt(20)));
        case BOOLEAN:
            return def.createBoolean(random.nextBoolean());
        case BINARY:
            return def.createBinary(
                getRandomBytes(random, random.nextInt(20)));
        case FIXED_BINARY:
            return def.createFixedBinary(
                getRandomBytes(random, def.asFixedBinary().getSize()));
        case ENUM:
            String[] values = def.asEnum().getValues();
            return def.createEnum(values[random.nextInt(values.length)]);
        default:
            throw new IllegalStateException("Unsupported type:" +
                def.getType());
        }
    }

    /* Generate a String with specified length. */
    private String randomString(Random random, int len) {
        final String AB =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder(len);
        for( int i = 0; i < len; i++ ) {
             sb.append(AB.charAt(random.nextInt(AB.length())));
         }
        return sb.toString();
    }

    /* Generate a Binary value with specified length. */
    private byte[] getRandomBytes(Random random, int len) {
        byte[] data = new byte[len];
        random.nextBytes(data);
        return data;
    }
}
