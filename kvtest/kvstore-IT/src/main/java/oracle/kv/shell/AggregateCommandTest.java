/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.shell;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;

import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.TableBuilder;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.shell.AggregateCommand.AggregateTableSub;
import oracle.kv.table.Row;
import oracle.kv.table.TableAPI;

import org.junit.Test;

/* Test case coverage for AggregateCommand. */
public class AggregateCommandTest extends DataCLITestBase {

    @Test
    public void testAggregateTableSubGetCommandSyntax()
        throws Exception {

        final String expectedResult = AggregateTableSub.COMMAND_SYNTAX;
        assertEquals(expectedResult,
            new AggregateTableSub().getCommandSyntax());
    }

    @Test
    public void testAggregateCommandSubGetCommandDescription()
        throws Exception {

        final String expectedResult = AggregateTableSub.COMMAND_DESCRIPTION;
        assertEquals(expectedResult,
            new AggregateTableSub().getCommandDescription());
    }

    @Test
    public void testAggregateTableSubExecuteBadArgs()
        throws Exception {

        final AggregateTableSub aggTableSub = new AggregateTableSub();
        String[] cmds = new String[]{AggregateTableSub.COMMAND_NAME};

        /* Check unknown argument. */
        CommandShell shell = connectToStore();
        doExecuteUnknownArg(aggTableSub, cmds, shell);

        /* Check flag requires an argument. */
        String[] flagRequiredArgs = {
            AggregateTableSub.SUM_FLAG,
            AggregateTableSub.AVG_FLAG,
            AggregateTableSub.JSON_FLAG
        };
        doExecuteFlagRequiredArg(aggTableSub, cmds, flagRequiredArgs, shell);

        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.FIELD_FLAG, "field1"
        };
        flagRequiredArgs = new String[] {
            AggregateTableSub.VALUE_FLAG,
            AggregateTableSub.START_FLAG,
            AggregateTableSub.END_FLAG,
        };
        doExecuteFlagRequiredArg(aggTableSub, cmds, flagRequiredArgs, shell);

        /* Check missing required argument. */
        cmds = new String[]{AggregateTableSub.COMMAND_NAME};
        HashMap<String, String> argsMap = new LinkedHashMap<String, String>();
        argsMap.put(AggregateTableSub.TABLE_FLAG, "mytable");
        argsMap.put(AggregateTableSub.COUNT_FLAG,
                    AggregateTableSub.COUNT_FLAG);
        argsMap.put(AggregateTableSub.SUM_FLAG, "f1");
        argsMap.put(AggregateTableSub.AVG_FLAG, "f1");

        /* Missing -count, -sum and -avg. */
        String[] requiredArgs = new String[] {
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG,
            AggregateTableSub.AVG_FLAG
        };
        doExecuteRequiredArgs(aggTableSub, cmds, argsMap, requiredArgs,
            AggregateTableSub.COUNT_FLAG + " or " +
                AggregateTableSub.SUM_FLAG + " or " +
                AggregateTableSub.AVG_FLAG, shell);

        requiredArgs = new String[] {
            AggregateTableSub.TABLE_FLAG
        };
        /* Missing -table */
        doExecuteRequiredArgs(aggTableSub, cmds, argsMap, requiredArgs,
            requiredArgs[0], shell);

        argsMap.put(AggregateTableSub.FIELD_FLAG, "f1");
        argsMap.put(AggregateTableSub.VALUE_FLAG, "value1");
        argsMap.put(AggregateTableSub.START_FLAG, "s2");
        argsMap.put(AggregateTableSub.END_FLAG, "e2");

        /* -field f1 but missing -value */
        requiredArgs = new String[] {
            AggregateTableSub.VALUE_FLAG,
            AggregateTableSub.START_FLAG,
            AggregateTableSub.END_FLAG,
        };
        doExecuteRequiredArgs(aggTableSub, cmds, argsMap, requiredArgs,
            requiredArgs[0] + " or " + requiredArgs[1] +
            " | " + requiredArgs[2], shell);
    }

    @Test
    public void testAggregateTableSubExecuteHelp()
        throws Exception {

        final AggregateTableSub aggCmd = new AggregateTableSub();
        final String[] cmds = new String[]{AggregateTableSub.COMMAND_NAME};

        CommandShell shell = connectToStore();
        doExecuteHelp(aggCmd, cmds, shell);
    }

    private void aggregateTableSubExecuteTest(String namespace)
        throws Exception {

        final AggregateTableSub aggCmd = new AggregateTableSub();
        CommandShell shell = connectToStore();

        final String orderTable = "orders";
        final String nsOrderTable =
            NameUtils.makeQualifiedName(namespace, orderTable);
        TableImpl table = addOrderInfoTable(namespace, orderTable);
        loadOrderInfoRecordsTable(100, false, false, 0, table);
        final String idxQuantityTax = "idx0";
        table = addOrderInfoIndex(table, idxQuantityTax,
                                  new String[]{"quantity", "tax"});

        /* Case0: agg table -count -table [ns:]TABLE_NOT_EXISTS */
        String[] cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.TABLE_FLAG,
            NameUtils.makeQualifiedName(namespace, "TABLE_NOT_EXISTS"),
        };
        String expectedMsg = "TABLE_NOT_EXISTS";
        doExecuteShellException(aggCmd, cmds, expectedMsg, shell);

        /* Case1: agg table -count -name [ns:]orders */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
        };
        expectedMsg = "Row count: 100";
        doExecuteCheckRet(aggCmd, cmds, expectedMsg, shell);

        /* Case2: agg table -name [ns:]orders -index idx0 -count */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.COUNT_FLAG,
        };
        expectedMsg = "Row count: 100";
        doExecuteCheckRet(aggCmd, cmds, expectedMsg, shell);

        /* Case3: agg table -name [ns:]orders -sum FIELD_NOT_EXIST */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.SUM_FLAG, "FIELD_NOT_EXIST"
        };
        expectedMsg = "Field does not exist in table: FIELD_NOT_EXIST";
        doExecuteShellArgumentException(aggCmd, cmds, shell);

        /* Case4: agg table -name [ns:]orders -sum order */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.SUM_FLAG, "order"
        };
        expectedMsg = "order: No numerical value";
        doExecuteCheckRet(aggCmd, cmds, expectedMsg, shell);

        /* Case5: agg table -name [ns:]orders -index idx0 -avg prod */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.AVG_FLAG, "prod"
        };
        expectedMsg = "prod: No numerical value";
        doExecuteCheckRet(aggCmd, cmds, expectedMsg, shell);

        /* Case6: agg table -name [ns:]orders -avg tax,quantity */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.AVG_FLAG, "tax,quantity"
        };
        String[] expectedMsgs = new String[]{
            "tax(100 values): 25.25",
            "quantity(100 values): 50.50"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /* Case7: agg table -name [ns:]orders -sum tax,quantity,total */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.SUM_FLAG, "tax,quantity,total"
        };
        expectedMsgs = new String[]{
            "tax(100 values): 2525.00",
            "quantity(100 values): 5050",
            "total(100 values): 5050000",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case8:
         * agg table -name [ns:]orders -count
         *           -sum price,tax,quantity,total
         *           -avg price,tax,quantity,total
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total"
        };
        expectedMsgs = new String[]{
            "Row count: 100",
            "Sum:",
            "price(100 values): 507525.00",
            "tax(100 values): 2525.00",
            "quantity(100 values): 5050",
            "total(100 values): 5050000",
            "Average:",
            "price(100 values): 5075.25",
            "tax(100 values): 25.25",
            "quantity(100 values): 50.50",
            "total(100 values): 50500.00"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case9:
         * agg table -name [ns:]orders -index idx0
         *           -count
         *           -sum price,tax,quantity,total
         *           -avg price,tax,quantity,total
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "total,quantity,tax,price,"
        };
        expectedMsgs = new String[]{
            "Row count: 100",
            "Sum:",
            "price(100 values): 507525.00",
            "tax(100 values): 2525.00",
            "quantity(100 values): 5050",
            "total(100 values): 5050000",
            "Average:",
            "price(100 values): 5075.25",
            "tax(100 values): 25.25",
            "quantity(100 values): 50.50",
            "total(100 values): 50500.00"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case10:
         * agg table -name [ns:]orders -index idx0
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -avg price,tax,quantity,total
         *           -sum total,quantity,tax,price
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "total,quantity,tax,price,"
        };
        expectedMsgs = new String[]{
            "Row count: 100",
            "Sum:",
            "price(100 values): 507525.00",
            "tax(100 values): 2525.00",
            "quantity(100 values): 5050",
            "total(100 values): 5050000",
            "Average:",
            "total(100 values): 50500.00",
            "quantity(100 values): 50.50",
            "tax(100 values): 25.25",
            "price(100 values): 5075.25"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case11:
         * agg table -name [ns:]orders
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -field order -value order1
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total,",
            AggregateTableSub.FIELD_FLAG, "order",
            AggregateTableSub.VALUE_FLAG, "order1",
        };
        expectedMsgs = new String[]{
            "Row count: 10",
            "Sum:",
            "price(10 values): 15577.50",
            "tax(10 values): 77.50",
            "quantity(10 values): 155",
            "total(10 values): 155000",
            "Average:",
            "price(10 values): 1557.75",
            "tax(10 values): 7.75",
            "quantity(10 values): 15.50",
            "total(10 values): 15500.00"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case12:
         * agg table -name [ns:]orders
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -field order -value order1
         *           -field prod -start prod0 -end prod4
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total,",
            AggregateTableSub.FIELD_FLAG, "order",
            AggregateTableSub.VALUE_FLAG, "order1",
            AggregateTableSub.FIELD_FLAG, "prod",
            AggregateTableSub.START_FLAG, "prod0",
            AggregateTableSub.END_FLAG, "prod4",
        };
        expectedMsgs = new String[]{
            "Row count: 5",
            "Sum:",
            "price(5 values): 6532.50",
            "tax(5 values): 32.50",
            "quantity(5 values): 65",
            "total(5 values): 65000",
            "Average:",
            "price(5 values): 1306.50",
            "tax(5 values): 6.50",
            "quantity(5 values): 13.00",
            "total(5 values): 13000.00"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case13:
         * agg table -name [ns:]orders
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -json '{"order":"order1", "prod":"prod5"}'
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total",
            AggregateTableSub.JSON_FLAG,
            "{\"order\":\"order1\", \"prod\":\"prod5\"}"
        };
        expectedMsgs = new String[]{
            "Row count: 1",
            "Sum:",
            "price(1 value): 1608.00",
            "tax(1 value): 8.00",
            "quantity(1 value): 16",
            "total(1 value): 16000",
            "Average:",
            "total(1 value): 16000.00",
            "quantity(1 value): 16.00",
            "tax(1 value): 8.00",
            "price(1 value): 1608.00"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case14:
         * agg table -name [ns:]orders
         *           -count
         *           -json '{"order":"order1"}'
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.JSON_FLAG, "{\"order\":\"order1\"}"
        };
        expectedMsgs = new String[]{
            "Row count: 10"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case15:
         * agg table -name [ns:]orders
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -json '{"order":"order1", "prod":"prod100"}'
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total",
            AggregateTableSub.JSON_FLAG,
            "{\"order\":\"order1\", \"prod\":\"prod100\"}"
        };
        expectedMsgs = new String[]{
            "Row count: 0",
            "Sum:",
            "price: No numerical value",
            "tax: No numerical value",
            "quantity: No numerical value",
            "total: No numerical value",
            "Average:",
            "price: No numerical value",
            "tax: No numerical value",
            "quantity: No numerical value",
            "total: No numerical value",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case16:
         * agg table -name [ns:]orders -index idx0
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -field quantity -start 50
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total",
            AggregateTableSub.FIELD_FLAG, "quantity",
            AggregateTableSub.START_FLAG, "50",
        };
        expectedMsgs = new String[]{
            "Row count: 51",
            "Sum:",
            "price(51 values): 384412.50",
            "tax(51 values): 1912.50",
            "quantity(51 values): 3825",
            "total(51 values): 3825000",
            "Average:",
            "price(51 values): 7537.50",
            "tax(51 values): 37.50",
            "quantity(51 values): 75.00",
            "total(51 values): 75000.00",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case17:
         * agg table -name [ns:]orders -index idx0
         *           -count
         *           -avg total,quantity,tax,price
         *           -sum price,tax,quantity,total
         *           -field quantity -value 50
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "price,tax,quantity,total",
            AggregateTableSub.AVG_FLAG, "price,tax,quantity,total",
            AggregateTableSub.FIELD_FLAG, "quantity",
            AggregateTableSub.VALUE_FLAG, "50",
        };
        expectedMsgs = new String[]{
            "Row count: 1",
            "Sum:",
            "price(1 value): 5025.00",
            "tax(1 value): 25.00",
            "quantity(1 value): 50",
            "total(1 value): 50000",
            "Average:",
            "price(1 value): 5025.00",
            "tax(1 value): 25.00",
            "quantity(1 value): 50.00",
            "total(1 value): 50000.00",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case18:
         * agg table -name [ns:]orders -index idx0
         *           -count
         *           -field quantity -value 50
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, nsOrderTable,
            AggregateTableSub.INDEX_FLAG, idxQuantityTax,
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.FIELD_FLAG, "quantity",
            AggregateTableSub.VALUE_FLAG, "50",
        };
        expectedMsgs = new String[]{
            "Row count: 1"
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);


        /**
         * Case19: test null value
         * agg table -name [ns:]TabTestNull
         *           -count
         *           -sum intF,longF,floatF,doubleF
         *           -avg intF,longF,floatF,doubleF
         */
        int nRows = 10;
        int nNullRows = 5;
        TableImpl t1 = addTestNullTable(namespace);
        loadTestNullTable(t1, nRows, nNullRows);
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, t1.getFullNamespaceName(),
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "intF,longF,floatF,doubleF",
            AggregateTableSub.AVG_FLAG, "intF,longF,floatF,doubleF",
        };
        expectedMsgs = new String[]{
            "Row count: " + nRows,
            "Sum:",
            "intF(" + nNullRows + " values): 10",
            "longF(" + nNullRows + " values): 10",
            "floatF(" + nNullRows + " values): 10.00",
            "doubleF(" + nNullRows + " values): 10.00",
            "Average:",
            "intF(" + nNullRows + " values): 2.00",
            "longF(" + nNullRows + " values): 2.00",
            "floatF(" + nNullRows + " values): 2.00",
            "doubleF(" + nNullRows + " values): 2.00",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /**
         * Case20: test null value
         * agg table -name [ns:]TabTestNull
         *           -count
         *           -sum intF,longF,floatF,doubleF
         *           -avg intF,longF,floatF,doubleF
         *           -field id -start 0 -end 4
         */
        cmds = new String[] {
            AggregateTableSub.COMMAND_NAME,
            AggregateTableSub.TABLE_FLAG, t1.getFullNamespaceName(),
            AggregateTableSub.COUNT_FLAG,
            AggregateTableSub.SUM_FLAG, "intF,longF,floatF,doubleF",
            AggregateTableSub.AVG_FLAG, "intF,longF,floatF,doubleF",
            AggregateTableSub.FIELD_FLAG, "id",
            AggregateTableSub.START_FLAG, "0",
            AggregateTableSub.END_FLAG, String.valueOf(nNullRows - 1)
        };
        expectedMsgs = new String[]{
            "Row count: " + nNullRows,
            "Sum:",
            "intF: No numerical value",
            "longF: No numerical value",
            "floatF: No numerical value",
            "doubleF: No numerical value",
            "Average:",
            "intF: No numerical value",
            "longF: No numerical value",
            "floatF: No numerical value",
            "doubleF: No numerical value",
        };
        doExecuteCheckRet(aggCmd, cmds, expectedMsgs, shell);

        /* clean up */
        removeTable(namespace, table.getFullName(), true, commandService);
        removeTable(namespace, t1.getFullName(), true, commandService);
    }

    @Test
    public void testAggregateTableSubExecute() throws Exception {
        /* tables without namespace */
        aggregateTableSubExecuteTest(null);

        /* tables in user defined namespace */
        final String ns = "ns";
        createNamespace(ns);
        aggregateTableSubExecuteTest(ns);
        dropNamespace(ns);

        /* tables in system default namespace */
        aggregateTableSubExecuteTest(TableAPI.SYSDEFAULT_NAMESPACE_NAME);
    }

    private static class OrderInfo {
        private String id;
        private int quantity;
        private double price;
        private float tax;
        private long total;
        private String comments;

        OrderInfo(int index) {
            this.id = "prod" + index;
            this.quantity = index;
            this.price = index * 100.5;
            this.tax = (float)(index * 0.5);
            this.total = index * 1000;
            this.comments = "comments";
        }

        OrderInfo(boolean maxValue, int index) {
            if (maxValue) {
                this.id = "prod" + index;
                this.quantity = Integer.MAX_VALUE;
                this.price = Double.MAX_VALUE;
                this.tax = Float.MAX_VALUE;
                this.total = Long.MAX_VALUE;
                this.comments = "comments";
            } else {
                this.id = "prod" + index;
                this.quantity = Integer.MIN_VALUE;
                this.price = Double.MIN_VALUE;
                this.tax = Float.MIN_VALUE;
                this.total = Long.MIN_VALUE;
                this.comments = "comments";
            }
        }
    }

    private void loadOrderInfoRecordsTable(int num, boolean nagtive,
            boolean max, int start, TableImpl table) {
        for (int i = start; i < start + num; i++) {
            int index = i + 1;
            if (nagtive) {
                index *= -1;
            }
            OrderInfo order = null;
            if (!max) {
                order = new OrderInfo(index);
            } else {
                order = new OrderInfo((max & !nagtive), index);
            }

            String orderkey = "order" + String.valueOf(i/10);
            String prodkey = "prod" + String.valueOf(i%10);
            try {
                final Row row = table.createRow();
                row.put("order", orderkey);
                row.put("prod", prodkey);
                row.put("id", order.id);
                row.put("quantity", order.quantity);
                row.put("price", order.price);
                row.put("tax", order.tax);
                row.put("total", order.total);
                row.put("comments", order.comments);
                tableImpl.put(row, null, null);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
            }
        }
    }

    private TableImpl addOrderInfoTable(String namespace, String tableName)
        throws Exception {
        /* Exercise the createTableBuilder() variants w/ and w/o namespace */
        TableBuilder tb = namespace == null ?
            TableBuilder.createTableBuilder(tableName) :
            TableBuilder.createTableBuilder(namespace, tableName);

        tb.addString("order", null, false, "")
            .addString("prod", null, false, "")
            .addString("id", null, false, "")
            .addInteger("quantity", null, false, 0)
            .addDouble("price", null, false, 0.0)
            .addFloat("tax", null, false, Float.valueOf("0.0"))
            .addLong("total", null, false, Long.valueOf("0"))
            .addString("comments", null, false, "")
            .primaryKey(new String[]{"order", "prod"})
            .shardKey(new String[]{"order"});
       addTable(tb, true);
       /* Exercise the getTable() variants w/ and w/o namespace */
       return namespace == null ?
           getTable(tableName) : getTable(namespace, tableName);
    }

    private TableImpl addOrderInfoIndex(TableImpl table,
                                        String idxName,
                                        String[] fields)
        throws Exception {
        addIndex(table, idxName, fields, true);
        return getTable(table.getNamespace(), table.getFullName());
    }

    private TableImpl addTestNullTable(String namespace)
        throws Exception {

        String tableName = "TabTestNull";
        /* Exercise the createTableBuilder() variants w/ and w/o namespace */
        TableBuilder tb = namespace == null ?
            TableBuilder.createTableBuilder(tableName) :
            TableBuilder.createTableBuilder(namespace, tableName);

        tb.addInteger("id")
            .addInteger("intF")
            .addLong("longF")
            .addFloat("floatF")
            .addDouble("doubleF")
            .primaryKey(new String[]{"id"});
       addTable(tb, true);
       /* Exercise the getTable() variants w/ and w/o namespace */
       return namespace == null ?
           getTable(tableName) : getTable(namespace, tableName);
    }

    private void loadTestNullTable(TableImpl table, int nRows, int nNullRows) {

        for (int i = 0; i < nNullRows; i++) {
            Row row = (Row)table.createRow().put("id", i);
            tableImpl.put(row, null, null);
        }
        for (int i = 0; i < nRows - nNullRows; i++) {
            Row row = (Row)table.createRow()
                        .put("id", i + nNullRows)
                        .put("intF", i)
                        .put("longF", ((Integer)i).longValue())
                        .put("floatF", ((Integer)i).floatValue())
                        .put("doubleF", ((Integer)i).doubleValue());
            tableImpl.put(row, null, null);
        }
    }
}
