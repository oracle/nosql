/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.table;

import static org.junit.Assert.fail;

import oracle.kv.StoreIteratorException;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableIterator;

import org.junit.Test;

public class RestartTest extends TableTestBase {

    @Test
    public void test23236()
        throws Exception {

        TableImpl userTable = buildUserTable();
        scanTable(userTable);
        restartStore();
        userTable = getTable("User");
        scanTable(userTable);
    }

    private void scanTable(Table table) {
        TableIterator<Row> iter = tableImpl.tableIterator(
            table.createPrimaryKey(), null, null);
        try {
            while (iter.hasNext()) {
                iter.next();
            }
        } catch (StoreIteratorException sie) {
            fail("Exception on retrieve: " + sie);
        } finally {
            iter.close();
        }
    }

    private TableImpl buildUserTable()
        throws Exception {

        TableImpl table = TableBuilder.createTableBuilder("User")
            .addString("firstName")
            .addString("lastName")
            .addInteger("age")
            .addString("address")
            .addBinary("binary")
            .primaryKey("lastName", "firstName")
            .shardKey("lastName")
            .buildTable();

        table = addTable(table, true);
        return table;
    }
}
