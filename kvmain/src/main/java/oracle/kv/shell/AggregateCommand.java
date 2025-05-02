/*-
 * Copyright (C) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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

package oracle.kv.shell;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import oracle.kv.FaultException;
import oracle.kv.StoreIteratorException;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

/**
 * Aggregate command, a simple data aggregation command to count, sum, or
 * average numeric fields that match the input filter condition and are of
 * the appropriate type.
 */
public class AggregateCommand extends CommandWithSubs {

    final static String COMMAND_OVERVIEW =
        "The Aggregate command encapsulates commands that performs simple " +
        "data" + eol + "aggregation operations on numeric fields of values " +
        "from a store or rows" + eol + "from a table.";

    private static final
        List<? extends SubCommand> subs =
                   Arrays.asList(new AggregateTableSub());

    public AggregateCommand() {
        super(subs, "aggregate", 3, 2);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return COMMAND_OVERVIEW;
    }

    /**
     * Base abstract class for AggregateSubCommands.  This class extracts
     * the generic flags "-count", "-sum" and "-avg" from the command line
     * and includes some common methods for sub commands.
     *
     * The extending classes should implements the abstract method exec().
     */
    abstract static class AggregateSubCommand extends SubCommand {

        final static String COUNT_FLAG = "-count";
        final static String COUNT_FLAG_DESC = COUNT_FLAG;
        final static String SUM_FLAG = "-sum";
        final static String SUM_FLAG_DESC = SUM_FLAG +  " <field[,field]+>";
        final static String AVG_FLAG = "-avg";
        final static String AVG_FLAG_DESC = AVG_FLAG +  " <field[,field]+>";
        final static String START_FLAG = "-start";
        final static String END_FLAG = "-end";

        static final String genericFlags =
            "[" + COUNT_FLAG_DESC + "] [" + SUM_FLAG_DESC + "] " +
            "[" + AVG_FLAG_DESC + "]";

        boolean doCounting;
        List<String> sumFields;
        List<String> avgFields;
        AggResult result;

        AggregateSubCommand(String name, int prefixLength) {
            super(name, prefixLength);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            doCounting = false;
            sumFields = new ArrayList<String>();
            avgFields = new ArrayList<String>();
            result = new AggResult();
            exec(args, shell, result);
            return genStatsSummary(result);
        }

        abstract void exec(String[] args, Shell shell, AggResult aggResult)
            throws ShellException;

        int checkGenericArg(Shell shell, String arg, String[] args, int i)
            throws ShellException {

            int rval = i;
            if (COUNT_FLAG.equals(arg)) {
                doCounting = true;
            } else if (SUM_FLAG.equals(arg)) {
                String str = Shell.nextArg(args, rval++, this);
                String[] fields = str.split(",");
                for (String field: fields) {
                    if (!sumFields.contains(field)) {
                        sumFields.add(field);
                    }
                }
            } else if (AVG_FLAG.equals(arg)) {
                String str = Shell.nextArg(args, rval++, this);
                String[] fields = str.split(",");
                for (String field: fields) {
                    if (!avgFields.contains(field)) {
                        avgFields.add(field);
                    }
                }
            } else {
                shell.unknownArgument(arg, this);
            }
            return rval;
        }

        List<String> getAggFields() {
            if (sumFields.isEmpty() && avgFields.isEmpty()) {
                return null;
            }
            final List<String> fields = new ArrayList<String>();
            if (!sumFields.isEmpty()) {
                for (String field: sumFields) {
                    if (!fields.contains(field)) {
                        fields.add(field);
                    }
                }
            }
            if (!avgFields.isEmpty()) {
                for (String field: avgFields) {
                    if (!fields.contains(field)) {
                        fields.add(field);
                    }
                }
            }
            return fields;
        }

        void validateAggArgs(Shell shell)
            throws ShellException {

            if (!doCounting && sumFields.isEmpty() && avgFields.isEmpty()) {
                shell.requiredArg(COUNT_FLAG + " or " + SUM_FLAG +
                    " or " + AVG_FLAG, this);
            }
        }

        /* Generate output result. */
        String genStatsSummary(AggResult aggResult) {
            StringBuilder sb = new StringBuilder();
            long count = aggResult.getCount();
            if (doCounting) {
                sb.append("Row count: ");
                sb.append(count);
                if (sumFields.isEmpty() && avgFields.isEmpty()) {
                    return sb.toString();
                }
            }

            Formatter fmt = new Formatter(sb);
            if (!sumFields.isEmpty()) {
                sb.append(eol);
                sb.append("Sum:");
                for (String field: sumFields) {
                    if (count == 0) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }
                    Number sum = aggResult.getSum(field);
                    if (sum == null) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }
                    String fieldInfo = getFieldInfo(aggResult, field);
                    if (sum instanceof Double) {
                        if (((Double)sum).isInfinite()) {
                            fmt.format(eolt + "%s: numeric overflow",
                                       fieldInfo);
                            continue;
                        }
                        fmt.format(eolt + "%s: %.2f", fieldInfo, sum);
                    } else {
                        fmt.format(eolt + "%s: %d", fieldInfo, sum);
                    }
                }
            }
            if (!avgFields.isEmpty()) {
                sb.append(eol);
                sb.append("Average:");
                for (String field: avgFields) {
                    if (count == 0) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    }

                    Double avg = aggResult.getAvg(field);
                    String fieldInfo = getFieldInfo(aggResult, field);
                    if (avg == null) {
                        fmt.format(eolt + "%s: No numerical value", field);
                        continue;
                    } else if (avg.isInfinite()) {
                        fmt.format(eolt + "%s: numeric overflow", fieldInfo);
                        continue;
                    }
                    fmt.format(eolt + "%s: %.2f", fieldInfo, avg.doubleValue());
                }
            }

            fmt.close();
            return sb.toString();
        }

        private String getFieldInfo(AggResult aggResult, String field) {
            int cnt = aggResult.getCount(field);
            return field + "(" + cnt + ((cnt > 1) ? " values" : " value") + ")";
        }

        /**
         * A class used to tally the fields value to calculate aggregated
         * sum or average value.
         */
        static class AggResult {

            private long count;
            private final HashMap<String, CalcSum<?>> sums;

            AggResult() {
                sums = new HashMap<String, CalcSum<?>>();
                count = 0;
            }

            void tallyCount() {
                count++;
            }

            long getCount() {
                return count;
            }

            Number getSum(String field) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    return null;
                }
                return sum.getValue();
            }

            int getCount(String field) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    return 0;
                }
                return sum.getCount();
            }

            Double getAvg(String field) {
                Number sum = getSum(field);
                int cnt = getCount(field);
                if (sum == null || cnt == 0) {
                    return null;
                }
                return sum.doubleValue()/cnt;
            }

            void tallyInt(String field, Integer value) {
                tallyLong(field, value.longValue());
            }

            void tallyLong(String field, Long value) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    sum = new CalcSumLong(value);
                    sums.put(field, sum);
                    return;
                }
                if (sum instanceof CalcSumLong) {
                    try {
                        ((CalcSumLong)sum).doSum(value);
                    } catch (ArithmeticException ae) {
                        sum = new CalcSumDouble(sum);
                        ((CalcSumDouble)sum).doSum(value);
                        sums.put(field, sum);
                    }
                } else {
                    ((CalcSumDouble)sum).doSum(value);
                }
            }

            void tallyFloat(String field, Float value) {
                tallyDouble(field, value.doubleValue());
            }

            void tallyDouble(String field, Double value) {
                CalcSum<?> sum = sums.get(field);
                if (sum == null) {
                    sum = new CalcSumDouble(value);
                    sums.put(field, sum);
                } else {
                    ((CalcSumDouble)sum).doSum(value);
                }
            }
        }

        /**
         * Abstract CalcSum class is to calculates a sum generically
         * and count the values, the extending classes should implements
         * the methods: zero(), add(Number v) and valueOf(Number v).
         */
        static abstract class CalcSum<T extends Number> {
            private int count;
            private T sum;

            CalcSum() {
                sum = zero();
                count = 0;
            }

            CalcSum(Number value) {
                sum = valueOf(value);
                count = 1;
            }

            CalcSum(CalcSum<?> cs) {
                sum = valueOf(cs.getValue());
                this.count = cs.getCount();
            }

            void doSum(Number value) {
                sum = add(sum, value);
                count++;
            }

            T getValue() {
                return sum;
            }

            int getCount() {
                return count;
            }

            abstract T zero();
            abstract T valueOf(Number v);
            abstract T add(T v1, Number v2);
        }

        /**
         * CalsSumLong extends CalcSum to represent a sum operator, whose
         * result is Long value.
         */
        static class CalcSumLong extends CalcSum<Long> {

            CalcSumLong(Number value) {
                super(value);
            }

            CalcSumLong(CalcSum<?> sum) {
                super(sum);
            }

            @Override
            Long zero() {
                return 0L;
            }

            @Override
            Long valueOf(Number v) {
                return v.longValue();
            }

            @Override
            Long add(Long v1, Number v2) {
                return longAddAndCheck(v1.longValue(), v2.longValue());
            }

            /* Add two long integers, checking for overflow. */
            private long longAddAndCheck(long a, long b) {
                if (a > b) {
                    /* use symmetry to reduce boundary cases */
                    return longAddAndCheck(b, a);
                }
                /* assert a <= b */
                if (a < 0) {
                    if (b < 0) {
                        /* check for negative overflow */
                        if (Long.MIN_VALUE - b <= a) {
                            return a + b;
                        }
                        throw new ArithmeticException("Add failed: underflow");
                    }
                    /* Opposite sign addition is always safe */
                    return a + b;
                }
                /* check for positive overflow */
                if (a <= Long.MAX_VALUE - b) {
                    return a + b;
                }
                throw new ArithmeticException("Add failed: overflow");
            }
        }

        /**
         * CalsSumDouble extends CalcSum to represent a sum operator, whose
         * result is Double value.
         */
        static class CalcSumDouble extends CalcSum<Double> {

            CalcSumDouble(Number value) {
                super(value);
            }

            CalcSumDouble(CalcSum<?> sum) {
                super(sum);
            }

            @Override
            Double zero() {
                return 0.0d;
            }

            @Override
            Double valueOf(Number v) {
                return v.doubleValue();
            }

            @Override
            Double add(Double v1, Number v2) {
                return v1.doubleValue() + v2.doubleValue();
            }
        }
    }

    /**
     * Performs simple data aggregation operations on numeric fields of
     * a table.
     */
    static class AggregateTableSub extends AggregateSubCommand {
        final static String COMMAND_NAME = "table";
        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String INDEX_FLAG = "-index";
        final static String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG = "-value";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String START_FLAG_DESC = START_FLAG + " <value>";
        final static String END_FLAG_DESC = END_FLAG + " <value>";
        final static String JSON_FLAG = "-json";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";

        final static String COMMAND_SYNTAX =
            "aggregate " + COMMAND_NAME + " " + TABLE_FLAG_DESC + eolt +
            genericFlags + eolt +
            "[" + INDEX_FLAG_DESC + "] [" + FIELD_FLAG_DESC + " " +
            VALUE_FLAG_DESC + "]+" + eolt +
            "[" + FIELD_FLAG_DESC + " [" + START_FLAG_DESC + "] [" +
            END_FLAG_DESC + "]]" + eolt +
            "[" + JSON_FLAG_DESC + "]";

        final static String COMMAND_DESCRIPTION =
            "Performs simple data aggregation operations on numeric fields " +
            "of a table." + eolt +
            COUNT_FLAG + " returns the count of matching records." + eolt +
            SUM_FLAG + " returns the sum of the values of matching " +
            "fields." + eolt +
            AVG_FLAG + " returns the average of the values of matching " +
            "fields." + eolt +
            FIELD_FLAG + " and " + VALUE_FLAG + " pairs are used to " +
            "used to specify fields of the" + eolt + "primary key or " +
            "index key used for the operation.  If no fields are" + eolt +
            "specified an iteration of the entire table or index is " +
            "performed." + eolt +
            FIELD_FLAG + "," + START_FLAG + " and " + END_FLAG + " flags " +
            "can be used to define a value range for" + eolt +
            "the last field specified." +  eolt +
            JSON_FLAG + " indicates that the key field values are in " +
            "JSON format.";

        public AggregateTableSub() {
            super(COMMAND_NAME, 3);
        }

        @Override
        void exec(String[] args, Shell shell, AggResult aggResult)
            throws ShellException {

            Shell.checkHelp(args, this);

            String namespace = null;
            String tableName = null;
            HashMap<String, String> mapVals = new HashMap<String, String>();
            String frFieldName = null;
            String rgStart = null;
            String rgEnd = null;
            String indexName = null;
            String jsonString = null;

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (TABLE_FLAG.equals(arg)) {
                    tableName = Shell.nextArg(args, i++, this);
                } else if (FIELD_FLAG.equals(arg)) {
                    String fname = Shell.nextArg(args, i++, this);
                    if (++i < args.length) {
                        arg = args[i];
                        if (VALUE_FLAG.equals(arg)) {
                            String fVal = Shell.nextArg(args, i++, this);
                            mapVals.put(fname, fVal);
                        } else {
                            while (i < args.length) {
                                arg = args[i];
                                if (START_FLAG.equals(arg)) {
                                    rgStart = Shell.nextArg(args, i++, this);
                                } else if (END_FLAG.equals(arg)) {
                                    rgEnd = Shell.nextArg(args, i++, this);
                                } else {
                                    break;
                                }
                                i++;
                            }
                            if (rgStart == null && rgEnd == null) {
                                invalidArgument(arg + ", " +
                                    VALUE_FLAG + " or " +
                                    START_FLAG + " | " + END_FLAG +
                                    " is required");
                            }
                            frFieldName = fname;
                            i--;
                        }
                    } else {
                        shell.requiredArg(VALUE_FLAG + " or " +
                            START_FLAG + " | " + END_FLAG, this);
                    }
                } else if (INDEX_FLAG.equals(arg)) {
                    indexName = Shell.nextArg(args, i++, this);
                } else if (JSON_FLAG.equals(arg)) {
                    jsonString = Shell.nextArg(args, i++, this);
                } else {
                    i = checkGenericArg(shell, arg, args, i);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }
            validateAggArgs(shell);

            final CommandShell cmdShell = (CommandShell) shell;
            final TableAPI tableImpl = cmdShell.getStore().getTableAPI();

            namespace = NameUtils.getNamespaceFromQualifiedName(tableName);
            String fullName = NameUtils.getFullNameFromQualifiedName(tableName);

            if (namespace == null) {
                namespace = cmdShell.getNamespace();
            }
            final Table table = CommandUtils.findTable(tableImpl,
                                                       namespace,
                                                       fullName);

            List<String> fields = getAggFields();
            if (fields != null) {
                for (String field: fields) {
                    if (table.getField(field) == null) {
                        invalidArgument("Field does not exist in " +
                                "table: " + field);
                    }
                }
            }
            /**
             * Create the key object.
             *  - Create the key from JSON string if it is specified.
             *  - Create key and set field values accordingly.
             */
            RecordValue key = null;
            if (jsonString != null) {
                key = CommandUtils.createKeyFromJson(table, indexName,
                                                     jsonString);
            } else {
                if (indexName == null) {
                    key = table.createPrimaryKey();
                } else {
                    key = CommandUtils.findIndex(table, indexName)
                          .createIndexKey();
                }
                /* Set field values to key. */
                for (Map.Entry<String, String> entry: mapVals.entrySet()) {
                    CommandUtils.putIndexKeyValues(key, entry.getKey(),
                                                   entry.getValue());
                }
            }

            /**
             * Initialize MultiRowOptions if -start and -end are specified
             * for a field.
             */
            MultiRowOptions mro = null;
            if (rgStart != null || rgEnd != null) {
                mro = CommandUtils.createMultiRowOptions(tableImpl,
                    table, key, null, null, frFieldName, rgStart, rgEnd);
            }

            /* Perform aggregation operation. */
            execAgg(tableImpl, key, mro, fields, aggResult);
        }

        /**
         * It iterates the matched rows, get value of specified fields with
         * numeric types and tally them.
         */
        private void execAgg(final TableAPI tableImpl,
                             final RecordValue key,
                             final MultiRowOptions mro,
                             final List<String> fields,
                             final AggResult aggResult)
            throws ShellException {

            new RunTableAPIOperation() {

                @Override
                void doOperation() throws ShellException {
                    TableIterator<?> iter = null;
                    try {
                        if (fields == null) {
                            if (key.isPrimaryKey()) {
                                iter = tableImpl.tableKeysIterator(
                                    key.asPrimaryKey(), mro, null);
                            } else {
                                iter = tableImpl.tableKeysIterator(
                                    key.asIndexKey(), mro, null);
                            }
                        } else {
                            if (key.isPrimaryKey()) {
                                iter = tableImpl.tableIterator(
                                    key.asPrimaryKey(), mro, null);
                            } else {
                                iter = tableImpl.tableIterator(
                                    key.asIndexKey(), mro, null);
                            }
                        }
                        while (iter.hasNext()) {
                            if (fields != null) {
                                Row row = (Row)iter.next();
                                for (String field: fields) {
                                    tallyFieldValue(aggResult, row, field);
                                }
                            } else {
                                iter.next();
                            }
                            aggResult.tallyCount();
                        }
                    } catch (StoreIteratorException sie) {
                        Throwable t = sie.getCause();
                        if (t != null && t instanceof FaultException) {
                            throw (FaultException)t;
                        }
                        throw new ShellException(
                            t != null ? t.getMessage() : sie.getMessage());
                    } finally {
                        if (iter != null) {
                            iter.close();
                        }
                    }
                }
            }.run();
        }

        private void tallyFieldValue(AggResult aggResult,
                                     Row row, String field) {
            FieldValue fv = row.get(field);
            if (fv.isNull()) {
                return;
            }
            if (fv.isInteger()) {
                aggResult.tallyInt(field, fv.asInteger().get());
            } else if (fv.isLong()) {
                aggResult.tallyLong(field, fv.asLong().get());
            } else if (fv.isFloat()) {
                aggResult.tallyFloat(field, fv.asFloat().get());
            } else if (fv.isDouble()) {
                aggResult.tallyDouble(field, fv.asDouble().get());
            }
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }
    }
}
