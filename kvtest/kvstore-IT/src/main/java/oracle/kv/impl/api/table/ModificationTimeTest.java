/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.DurabilityException;
import oracle.kv.FaultException;
import oracle.kv.Version;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.ReadOptions;
import oracle.kv.table.ReturnRow;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableOpExecutionException;
import oracle.kv.table.TableOperation;
import oracle.kv.table.TableOperationFactory;
import oracle.kv.table.TableOperationResult;

import org.junit.Test;

public class ModificationTimeTest extends TableTestBase {
    String tableDDL =
        "create table testTable(" +
        "id INTEGER," +
        "desc STRING," +
        "primary key (id))";

    @Test
    public void testModificationTimeForReturnedRows() {
        executeDdl(tableDDL);

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("testTable");

        Row row1 = table.createRow();
        row1.put("desc", "smith");
        row1.put("id", 1);
        api.put(row1, null, null);

        Row row2 = table.createRow();
        row2.put("desc", "Anna");
        row2.put("id", 2);
        api.put(row2, null, null);

        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);

        ReadOptions options = new ReadOptions(Consistency.ABSOLUTE, 1000,
                                     TimeUnit.MILLISECONDS);
        Row result = api.get(pk, options);
        assertEquals(row1.getLastModificationTime(),
                     result.getLastModificationTime());

        List<Row> list = api.multiGet(pk, null, options);
        assertEquals(list.get(0).getLastModificationTime(),
                     result.getLastModificationTime());

        TableIterator<Row> interator = api.tableIterator(pk, null, null);
        assertEquals(interator.next().getLastModificationTime(),
                     result.getLastModificationTime());

    }

    @Test
    public void testPrevRowModificationTime()
        throws DurabilityException, FaultException, TableOpExecutionException {
        executeDdl(tableDDL);

        TableAPI api = store.getTableAPI();
        Table table = api.getTable("testTable");
        ReturnRow returnRow = table.createReturnRow(ReturnRow.Choice.ALL);

        Row row1 = table.createRow();
        row1.put("desc", "smith");
        row1.put("id", 1);
        api.put(row1, null, null);

        Row row2 = table.createRow();
        row2.put("desc", "Anna");
        row2.put("id", 1);
        Version version1 = api.put(row2, returnRow, null);

        assertEquals(row1.getLastModificationTime(),
                     returnRow.getLastModificationTime());

        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        Row row3 = table.createRow();
        row3.put("desc", "Lily");
        row3.put("id", 1);
        api.putIfAbsent(row3, returnRow, null);
        assertEquals(row2.getLastModificationTime(),
                     returnRow.getLastModificationTime());

        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        api.putIfPresent(row3, returnRow, null);
        assertEquals(row2.getLastModificationTime(),
                     returnRow.getLastModificationTime());

        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        Row row4 = table.createRow();
        row4.put("desc", "Tom");
        row4.put("id", 1);
        api.putIfVersion(row4, version1, returnRow, null);
        assertEquals(row3.getLastModificationTime(),
                     returnRow.getLastModificationTime());

        api.put(row4, null, null);
        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        PrimaryKey pk = table.createPrimaryKey();
        pk.put("id", 1);
        api.delete(pk, returnRow, null);
        assertEquals(returnRow.getLastModificationTime(),
                     row4.getLastModificationTime());

        Row row5 = table.createRow();
        row5.put("desc", "Serena");
        row5.put("id", 1);
        api.put(row5, null, null);
        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        api.deleteIfVersion(pk, version1, returnRow, null);
        assertEquals(returnRow.getLastModificationTime(),
                     row5.getLastModificationTime());

        api.put(row5, null, null);
        Row row6 = table.createRow();
        row6.put("desc", "Nate");
        row6.put("id", 1);
        returnRow = table.createReturnRow(ReturnRow.Choice.ALL);
        TableOperationFactory factory = api.getTableOperationFactory();
        List<TableOperation> ops = new ArrayList<TableOperation>();
        ops.add(factory.createPut(row6, ReturnRow.Choice.ALL, true));
        List<TableOperationResult> results = api.execute(ops, null);
        assertEquals(results.get(0).getPreviousRow().getLastModificationTime(),
                     row5.getLastModificationTime());


    }
}
