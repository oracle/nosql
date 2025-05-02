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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import oracle.kv.Direction;
import oracle.kv.FaultException;
import oracle.kv.KVStore;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.KeyValueVersion;
import oracle.kv.ParallelScanIterator;
import oracle.kv.StoreIteratorConfig;
import oracle.kv.StoreIteratorException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.admin.client.CommandShell;
import oracle.kv.impl.admin.client.TableSizeCommand;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.util.FileUtils;
import oracle.kv.shell.CommandUtils.RunTableAPIOperation;
import oracle.kv.table.Index;
import oracle.kv.table.IndexKey;
import oracle.kv.table.KeyPair;
import oracle.kv.table.MultiRowOptions;
import oracle.kv.table.PrimaryKey;
import oracle.kv.table.RecordValue;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableAPI;
import oracle.kv.table.TableIterator;
import oracle.kv.table.TableUtils;
import oracle.kv.util.shell.CommandWithSubs;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellException;

public class GetCommand extends CommandWithSubs {
    final static String FILE_FLAG = "-file";
    final static String FILE_FLAG_DESC = FILE_FLAG + " <output>";
    final static String START_FLAG = "-start";
    final static String END_FLAG = "-end";
    final static String JSON_FLAG = "-json";
    final static String KEY_ONLY_FLAG = "-keyonly";
    final static String KEY_ONLY_FLAG_DESC = KEY_ONLY_FLAG;

    final static String COMMAND_OVERVIEW =
        "The get command encapsulates commands that get key/value" + eol +
        "pairs from a store or get rows from a table.";

    private static final
        List<? extends SubCommand> subs =
            Arrays.asList(new GetKVCommand(),
                          new GetTableCommand());

    public GetCommand() {
        super(subs, "get", 3, 1);
        overrideJsonFlag = true;
    }

    @Override
    protected String getCommandOverview() {
        return COMMAND_OVERVIEW;
    }

    static class GetKVCommand extends SubCommand {
        final static String COMMAND_NAME = "kv";
        final static String KEY_FLAG = "-key";
        final static String KEY_FLAG_DESC = KEY_FLAG + " <key>";
        final static String VALUE_ONLY_FLAG = "-valueonly";
        final static String VALUE_ONLY_FLAG_DESC = VALUE_ONLY_FLAG;
        final static String END_FLAG_DESC = END_FLAG + " <prefixString>";
        final static String START_FLAG_DESC = START_FLAG + " <prefixString>";
        final static String MULTI_FLAG = "-all";
        final static String MULTI_FLAG_DESC = MULTI_FLAG;

        final static String COMMAND_SYNTAX =
            "get " + COMMAND_NAME + " " + KEY_FLAG_DESC +
            " [" + FILE_FLAG_DESC + "] " +
            "[" + MULTI_FLAG_DESC + "] [" + KEY_ONLY_FLAG_DESC +"] " +
            eolt + "[" + VALUE_ONLY_FLAG_DESC + "] " +
            "[" + START_FLAG_DESC + "] [" + END_FLAG_DESC + "]";

        final static String COMMAND_DESCRIPTION =
            "Performs a simple get operation on the key in the store." + eolt +
            KEY_FLAG + " indicates the key (prefix) to use.  Optional with " +
            MULTI_FLAG + "." + eolt +
            MULTI_FLAG + " is specified for iteration starting at the key, " +
            "or with" + eolt + "an empty key to iterate the entire store." +
            eolt + START_FLAG + " and " + END_FLAG + " flags can be used " +
            "for restricting the range used" + eolt + "for iteration." + eolt +
            KEY_ONLY_FLAG + " works with " + MULTI_FLAG + " and restricts " +
            "information to keys." + eolt +
            VALUE_ONLY_FLAG + " works with " + MULTI_FLAG + " and restricts " +
            "information to values." + eolt +
            FILE_FLAG + " is used to specify an output file, which is " +
            "truncated.";

        public GetKVCommand() {
            super(COMMAND_NAME, 2);
            /*
             * If there's a move to Jackson 2.x use the line below
             * instead of the one above:
             * ObjectWriter writer = mapper.writer().withDefaultPrettyPrinter();
             */
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            boolean iterate = false;
            Key key = null;
            String keyString = null;
            String outFile = null;
            String rangeStart = null;
            String rangeEnd = null;
            boolean keyOnly = false;
            boolean valueOnly = false;
            KVStore store = ((CommandShell)shell).getStore();

            for (int i = 1; i < args.length; i++) {
                String arg = args[i];
                if (KEY_FLAG.equals(arg)) {
                    keyString = Shell.nextArg(args, i++, this);
                    try {
                        key = CommandUtils.createKeyFromURI(keyString);
                    } catch (IllegalArgumentException iae) {
                        invalidArgument(iae.getMessage());
                    }
                } else if (MULTI_FLAG.equals(arg)) {
                    iterate = true;
                } else if (KEY_ONLY_FLAG.equals(arg)) {
                    keyOnly = true;
                } else if (VALUE_ONLY_FLAG.equals(arg)) {
                    valueOnly = true;
                } else if (FILE_FLAG.equals(arg)) {
                    outFile = Shell.nextArg(args, i++, this);
                } else if (START_FLAG.equals(arg)) {
                    rangeStart = Shell.nextArg(args, i++, this);
                } else if (END_FLAG.equals(arg)) {
                    rangeEnd = Shell.nextArg(args, i++, this);
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            String retString = null;
            if (!iterate) {
                if (key == null) {
                    shell.requiredArg(KEY_FLAG, this);
                }
                ValueVersion valueVersion = null;
                try {
                    valueVersion = store.get(key);
                    if (valueVersion == null) {
                        return "Key not found in store: " + keyString;
                    }
                } catch (Exception e) {
                    throw new ShellException(
                        "Exception from NoSQL DB in get:" +
                            eolt + e.getMessage(), e);
                }
                Value value = valueVersion.getValue();
                if (value.getFormat() == Value.Format.NONE ||
                    value.getFormat() == Value.Format.TABLE ||
                    value.getFormat() == Value.Format.TABLE_V1) {
                    if (outFile == null) {
                        retString = printableString(value.getValue());
                    }
                } else {
                    // report unsupported error
                    throw new ShellException("Unsupported Format " +
                                             value.getFormat());
                }
                if (outFile != null) {
                    try {
                        if (retString == null) {
                            FileUtils.writeBytesToFile(new File(outFile),
                                                       value.getValue());
                        } else {
                            FileUtils.writeStringToFile(new File(outFile),
                                                        retString);
                        }
                        retString = "Wrote value to file " + outFile + ".";
                    } catch (IOException ioe) {
                        throw new ShellException("Could not write to file " +
                                                 outFile, ioe);
                    }
                }
                return retString;
            }

            /* Initialize KeyRange */
            KeyRange kr = null;
            if (rangeStart != null || rangeEnd != null) {
                try {
                    kr = new KeyRange(rangeStart, true, rangeEnd, true);
                } catch (IllegalArgumentException iae) {
                    invalidArgument(iae.getMessage());
                }
            }

            /* Initialize RecordOutput */
            Writer fwriter = null;
            ResultOutput output = null;
            if (outFile != null) {
                try {
                    File file = new File(outFile);
                    fwriter = new BufferedWriter(new FileWriter(file));
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Could not open the output file " + outFile, ioe);
                }
                output = new ResultOutput(fwriter);
            } else {
                output = new ResultOutput(shell);
            }

            /* Perform iteration */
            try {
                if (keyOnly) {
                    retString = iterateKeys(store, key, kr, output);
                } else {
                    retString = iterateValues(store, key, kr,
                                              valueOnly, output);
                }
            } catch (ShellException se) {
                throw se;
            } finally {
                if (fwriter != null) {
                    try {
                        fwriter.flush();
                        fwriter.close();
                    } catch (IOException ioe) {
                        throw new ShellException(
                            "Could not flush to file " + outFile, ioe);
                    }
                    retString += eol + "Wrote value to file " + outFile;
                }
            }
            return retString;
        }

        private String iterateKeys(KVStore store, Key key, KeyRange kr,
                                   ResultOutput output)
            throws ShellException {

            Iterator<Key> it = null;
            try {
                if (key != null && key.getMinorPath() != null &&
                    key.getMinorPath().size() > 0) {
                    /* There's a minor key path, use it to advantage */
                    it = store.multiGetKeysIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                } else {
                    /* A generic store iteration */
                    it = store.storeKeysIterator(Direction.UNORDERED,
                                                 100, key, kr, null, null,
                                                 0, null, getIteratorConfig());
                    if (!it.hasNext() && key != null) {
                        closeIterator(it);
                        /*
                         * a complete major path won't work with store iterator
                         * and we can't distinguish between a complete major
                         * path or not, so if store iterator fails entire,
                         * try the key as a complete major path.
                         */
                        it = store.multiGetKeysIterator(Direction.FORWARD,
                                                        100, key, kr, null);
                    }
                }

                long totalNumKeys = 0;
                StringBuilder sb = new StringBuilder();
                while (it.hasNext()) {
                    if (!output.writeRecord(
                            CommandUtils.createURI(it.next()))) {
                        break;
                    }
                    totalNumKeys++;
                }
                output.flushWriting();
                sb.append(eol);
                sb.append(totalNumKeys);
                sb.append(((totalNumKeys > 1) ? " Keys" : " Key"));
                sb.append(" returned");
                return sb.toString();
            } catch (Exception e) {
                throw new ShellException("Failed to iterate keys :" +
                        eolt + e.getMessage(), e);
            } finally {
                if (it != null) {
                    closeIterator(it);
                }
            }
        }

        private String iterateValues(KVStore store, Key key, KeyRange kr,
                                     boolean valueOnly, ResultOutput output)
            throws ShellException {

            Iterator<KeyValueVersion> it = null;
            try {
                if (key != null && key.getMinorPath() != null &&
                    key.getMinorPath().size() > 0) {
                    /* There's a minor key path, use it to advantage */
                    it = store.multiGetIterator(Direction.FORWARD,
                                                100, key, kr, null);
                } else {
                    /* A generic store iteration */
                    it = store.storeIterator(Direction.UNORDERED,
                                             100, key, kr, null, null,
                                             0, null, getIteratorConfig());
                    if (!it.hasNext() && key != null) {
                        closeIterator(it);
                        /*
                         * a complete major path won't work with store iterator
                         * and we can't distinguish between a complete major
                         * path or not, so if store iterator fails entire,
                         * try the key as a complete major path.
                         */
                        it = store.multiGetIterator(Direction.FORWARD,
                                                    100, key, kr, null);
                    }
                }

                long totalNumRecords = 0;
                StringBuilder sb  = new StringBuilder();
                while (it.hasNext()) {
                    /* Generate string for key/values. */
                    KeyValueVersion kvv = it.next();
                    Value value = kvv.getValue();
                    if (value == null) {
                        continue;
                    }
                    String record = "";
                    if (!valueOnly) {
                        record += CommandUtils.createURI(kvv.getKey()) + eol;
                    }

                    if (value.getFormat() == Value.Format.NONE ||
                        value.getFormat() == Value.Format.TABLE ||
                        value.getFormat() == Value.Format.TABLE_V1 ||
                        value.getFormat() == Value.Format.MULTI_REGION_TABLE) {
                        record += printableString(value.getValue());
                    } else {
                        // report unsupported error
                        throw new ShellException("Unsupported Format " +
                                                 value.getFormat());
                    }
                    if (!output.writeRecord(record)) {
                        break;
                    }
                    totalNumRecords++;
                }
                output.flushWriting();
                sb.append(eol);
                sb.append(totalNumRecords);
                sb.append(((totalNumRecords > 1) ?
                          " Records returned" : " Record returned"));
                return sb.toString();

            } catch (Exception e) {
                throw new ShellException("Failed to iterate records :" +
                    eolt + e.getMessage(), e);
            } finally {
                if (it != null) {
                    closeIterator(it);
                }
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

        private StoreIteratorConfig getIteratorConfig() {
            /**
             * Setting it to 0 lets the KV Client determine the number of
             * threads based on topology information.
             */
            return new StoreIteratorConfig().setMaxConcurrentRequests(0);
        }

        private void closeIterator(Iterator<?> iterator) {
            if (iterator instanceof ParallelScanIterator) {
                ((ParallelScanIterator<?>)iterator).close();
            }
        }

        /* Encoded with base64 if it is not display-able */
        private String printableString(byte[] buf)
            throws ShellException {

            if (isAsciiPrintable(buf)) {
                return new String(buf);
            }
            return CommandUtils.encodeBase64(buf) + " [Base64]";
        }

        private boolean isAsciiPrintable(byte[] buf) {
            if (buf == null) {
                return true;
            }
            for (byte element : buf) {
                if ((element < 32) || (element > 126)) {
                    return false;
                }
            }
            return true;
        }
    }

    static class GetTableCommand extends SubCommand {
        final static String COMMAND_NAME = "table";
        final static String TABLE_FLAG = "-name";
        final static String TABLE_FLAG_DESC = TABLE_FLAG + " <name>";
        final static String FIELD_FLAG = "-field";
        final static String FIELD_FLAG_DESC = FIELD_FLAG + " <name>";
        final static String VALUE_FLAG = "-value";
        final static String VALUE_FLAG_DESC = VALUE_FLAG + " <value>";
        final static String NULL_VALUE_FLAG = "-null-value";
        final static String NULL_VALUE_FLAG_DESC = "-null-value";
        final static String ANCESTOR_FLAG = "-ancestor";
        final static String ANCESTOR_FLAG_DESC = ANCESTOR_FLAG + " <name>";
        final static String CHILD_FLAG = "-child";
        final static String CHILD_FLAG_DESC = CHILD_FLAG + " <name>";
        final static String INDEX_FLAG = "-index";
        final static String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
        final static String JSON_FLAG_DESC = JSON_FLAG + " <string>";
        final static String END_FLAG_DESC = END_FLAG + " <value>";
        final static String START_FLAG_DESC = START_FLAG + " <value>";
        final static String REPORT_SIZE_FLAG = "-report-size";
        final static String REPORT_SIZE_FLAG_DESC = REPORT_SIZE_FLAG;
        final static String PRETTY_FLAG = "-pretty";
        final static String PRETTY_FLAG_DESC = PRETTY_FLAG;

        final static String COMMAND_SYNTAX =
            "get " + COMMAND_NAME + " " + TABLE_FLAG_DESC +
            " [" + INDEX_FLAG_DESC + "]" + eolt +
            "[" + FIELD_FLAG_DESC + " [" +
            VALUE_FLAG_DESC + " | " + NULL_VALUE_FLAG_DESC + "]]+" + eolt +
            "[" + FIELD_FLAG_DESC + " [" + START_FLAG_DESC + "] [" +
            END_FLAG_DESC + "]]" + eolt +
            "[" + ANCESTOR_FLAG_DESC + "]+ [" + CHILD_FLAG_DESC + "]+" + eolt +
            "[" + JSON_FLAG_DESC + "] [" + FILE_FLAG_DESC + "] " +
            "[" + KEY_ONLY_FLAG + "]" + eolt +
            "[" + PRETTY_FLAG_DESC + "] [" + REPORT_SIZE_FLAG_DESC + "]" ;

        final static String COMMAND_DESCRIPTION =
            "Performs a get operation to retrieve one or more rows " +
            "from a named " + eolt +
            "table.  The table name is an optionally namespace qualified " +
            eolt + "dot-separated name with the format " +
            "[ns:]tableName[.childTableName]+." + eolt + FIELD_FLAG + " and " +
            VALUE_FLAG + " pairs are used to " +
            "specify fields of the" + eolt + "primary key or " +
            "index key used for the operation.  If no fields are" + eolt +
            "specified an iteration of the entire table or index is " +
            "performed" + eolt +
            FIELD_FLAG + "," + START_FLAG + " and " + END_FLAG + " flags " +
            "can be used to define a value range for" + eolt +
            "the last field specified." +  eolt +
            ANCESTOR_FLAG + " and " + CHILD_FLAG + " flags can be " +
            "used to return results from" + eolt +
            "specified ancestor and/or descendant tables as well as " +
            "the target" + eolt + "table." + eolt +
            JSON_FLAG + " indicates that the key field values are in " +
            "JSON format." + eolt +
            FILE_FLAG + " is used to specify an output file, " +
            "which is truncated." + eolt +
            KEY_ONLY_FLAG + " is used to restrict information to " +
            "keys only." + eolt +
            PRETTY_FLAG + " is used for a nicely formatted JSON string " +
            "with indentation" + eolt + "and carriage returns." + eolt +
            REPORT_SIZE_FLAG + " is used to show key and data size " +
            "information for primary" + eolt + "keys, data values, and " +
            "index keys for matching records.  When" + eolt +
            "-report-size is specified no data is displayed.";

        public GetTableCommand() {
            super("table", 3);
        }

        @Override
        public String execute(String[] args, Shell shell)
            throws ShellException {

            Shell.checkHelp(args, this);

            String tableName = null;
            String namespace = null;
            String indexName = null;
            String rgStart = null;
            String rgEnd = null;
            String jsonString = null;
            String outFile = null;
            String frFieldName = null;
            boolean pretty = false;
            boolean reportSize = false;
            boolean keyOnly = false;
            List<String> lstAncestor = new ArrayList<String>();
            List<String> lstChild = new ArrayList<String>();
            HashMap<String, String> mapVals = new HashMap<String, String>();
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
                        } else if (NULL_VALUE_FLAG.equals(arg)) {
                            mapVals.put(fname, null);
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
                } else if (ANCESTOR_FLAG.equals(arg)) {
                    lstAncestor.add(Shell.nextArg(args, i++, this));
                } else if (CHILD_FLAG.equals(arg)) {
                    lstChild.add(Shell.nextArg(args, i++, this));
                } else if (FILE_FLAG.equals(arg)) {
                    outFile = Shell.nextArg(args, i++, this);
                } else if (JSON_FLAG.equals(arg)) {
                    jsonString = Shell.nextArg(args, i++, this);
                } else if (PRETTY_FLAG.equals(arg)) {
                    pretty = true;
                } else if (REPORT_SIZE_FLAG.equals(arg)) {
                    reportSize = true;
                } else if (KEY_ONLY_FLAG.equals(arg)) {
                    keyOnly = true;
                } else {
                    shell.unknownArgument(arg, this);
                }
            }

            if (tableName == null) {
                shell.requiredArg(TABLE_FLAG, this);
            }

            String retString = null;
            CommandShell cmdShell = (CommandShell) shell;
            final TableAPI tableImpl = cmdShell.getStore().getTableAPI();
            namespace = NameUtils.getNamespaceFromQualifiedName(tableName);
            final String fullName =
                NameUtils.getFullNameFromQualifiedName(tableName);
            if (namespace == null) {
                namespace = cmdShell.getNamespace();
            }
            final Table table = CommandUtils.findTable(tableImpl,
                                                       namespace,
                                                       fullName);

            /* Create key. */
            RecordValue key = null;
            if (jsonString == null) {
                if (indexName == null) {
                    key = table.createPrimaryKey();
                } else {
                    key = CommandUtils.findIndex(table, indexName)
                          .createIndexKey();
                }
                /* Set fieldValues to key. */
                for (Map.Entry<String, String> entry: mapVals.entrySet()) {
                    CommandUtils.putIndexKeyValues(key, entry.getKey(),
                                                   entry.getValue());
                }
            } else {
                key = CommandUtils.createKeyFromJson(table, indexName,
                                                     jsonString);
            }

            /* Initialize MultiRowOptions. */
            MultiRowOptions mro = null;
            if (rgStart != null || rgEnd != null ||
                !lstAncestor.isEmpty() || !lstChild.isEmpty()) {
                mro = CommandUtils.createMultiRowOptions(tableImpl,
                        table, key, lstAncestor, lstChild,
                        frFieldName, rgStart, rgEnd);
            }

            /* Initialize output file. */
            ResultOutput output = null;
            Writer fwriter = null;
            if (outFile != null) {
                try {
                    File file = new File(outFile);
                    fwriter = new BufferedWriter(new FileWriter(file));
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Could not open the output file " + outFile, ioe);
                }
                output = new ResultOutput(fwriter);
            } else {
                output = new ResultOutput(shell);
            }

            /* Get rows */
            try {
                retString = doGetOperation(tableImpl, key, mro,
                                           keyOnly, output, pretty, reportSize);
            } catch (ShellException se) {
                throw se;
            } finally {
                if (fwriter != null) {
                    try {
                        fwriter.flush();
                        fwriter.close();
                    } catch (IOException ioe) {
                        throw new ShellException(
                            "Could not flush to file " + outFile, ioe);
                    }
                    if (retString != null && retString.length() > 0) {
                        retString += eol;
                    }
                    retString += "Wrote result to file " + outFile;
                }
            }
            return (retString == null || retString.length() == 0) ?
                    null : retString;
        }

        /**
         * Class used to tally size information:
         *  type: PRIMARY_KEY, DATA, INDEX_KEY
         *  name: name of index if type is INDEX_KEY, null for other types.
         *  min: minimal value of sizes.
         *  max: maximal value of sizes.
         *  sum: sum of all sizes.
         *  count: num of sizes.
         */
        private static final class SizeInfo {
            static enum Type {PRIMARY_KEY, DATA, INDEX_KEY}
            private final Type type;
            private final String name;
            private int min;
            private int max;
            private double sum;
            private int count;

            SizeInfo(Type type) {
                this(type, null);
            }

            SizeInfo(Type type, String name) {
                this.name = name;
                this.type = type;
                min = 0;
                max = 0;
                sum = 0.0;
                count = 0;
            }

            void tally(int size) {
                if (min == 0 || size < min) {
                    min = size;
                }
                if (size > max) {
                    max = size;
                }
                sum += size;
                count++;
            }

            String getName() {
                return name;
            }

            Type getType() {
                return type;
            }

            int getMin() {
                return min;
            }

            int getMax() {
                return max;
            }

            double getAvg() {
                if (count == 0) {
                    return 0.0;
                }
                return sum/count;
            }

            int getCount() {
                return count;
            }
        }

        private String doGetOperation(final TableAPI tableImpl,
                                      final RecordValue key,
                                      final MultiRowOptions mro,
                                      final boolean keyOnly,
                                      final ResultOutput output,
                                      final boolean pretty,
                                      final boolean reportSize)
            throws ShellException {

            final StringBuilder sb = new StringBuilder();
            new RunTableAPIOperation() {
                @Override
                void doOperation() throws ShellException {

                    if (key.isPrimaryKey()) {
                        PrimaryKey pKey = key.asPrimaryKey();
                        if (mro == null &&
                            CommandUtils.matchFullPrimaryKey(pKey)) {
                            Row row = tableImpl.get(pKey, null);
                            if (row != null) {
                                output.writeRecord(formatReturnInfo(row));
                            } else {
                                sb.append("Key not found in store: ");
                                sb.append(key.toJsonString(false));
                            }
                        } else {
                            Iterator<?> itr = null;
                            try {
                                if (CommandUtils.matchFullMajorKey(pKey)) {
                                    if (keyOnly) {
                                        itr = tableImpl.multiGetKeys(pKey,
                                            mro, null).iterator();
                                    } else {
                                        itr = tableImpl.multiGet(pKey,
                                            mro, null).iterator();
                                    }
                                } else {
                                    if (keyOnly) {
                                        itr = tableImpl.tableKeysIterator(pKey,
                                            mro, null);
                                    } else {
                                        itr = tableImpl.tableIterator(pKey,
                                            mro, null);
                                    }
                                }
                                SizeInfo[] stInfo = null;
                                if (reportSize) {
                                    stInfo = initSizeInfos(pKey.getTable(),
                                        false);
                                }
                                doIteration(itr, stInfo,
                                    pKey.getTable().getFullName());
                            } finally {
                                if (itr instanceof TableIterator) {
                                    ((TableIterator<?>)itr).close();
                                }
                            }
                        }
                    } else {
                        TableIterator<?> itr = null;
                        IndexKey idxKey = (IndexKey)key;
                        try {
                            if (keyOnly) {
                                itr = tableImpl.tableKeysIterator(idxKey,
                                    mro, null);
                            } else {
                                itr = tableImpl.tableIterator(idxKey,
                                    mro, null);
                            }
                            SizeInfo[] stInfo = null;
                            if (reportSize) {
                                stInfo = initSizeInfos(
                                    ((IndexKey)key).getIndex().getTable(),
                                    true);
                            }
                            doIteration(itr, stInfo,
                                idxKey.getIndex().getTable().getFullName());
                        } finally {
                            if (itr != null) {
                                itr.close();
                            }
                        }
                    }
                    output.flushWriting();
                }

                /* Format return information for a single row. */
                private String formatReturnInfo(Row row) {
                    if (reportSize) {
                        return getSizeTitle(SizeInfo.Type.PRIMARY_KEY) +
                            " size: " + getKeySize(row) + eol +
                            getSizeTitle(SizeInfo.Type.DATA) + " size: " +
                            (keyOnly ? "Not available" : getDataSize(row));
                    }
                    return (keyOnly) ?
                        row.createPrimaryKey().toJsonString(pretty) :
                        row.toJsonString(pretty);
                }

                private SizeInfo[] initSizeInfos(Table table,
                                                 boolean indexScan) {
                    SizeInfo[] sizeInfos = null;
                    if (keyOnly) {
                        if (indexScan) {
                            sizeInfos = new SizeInfo[2];
                            sizeInfos[0] =
                                new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                            sizeInfos[1] =
                                new SizeInfo(SizeInfo.Type.INDEX_KEY);
                        } else {
                            sizeInfos = new SizeInfo[1];
                            sizeInfos[0] =
                                new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                        }
                    } else {
                        int i = 0;
                        sizeInfos = new SizeInfo[2 + table.getIndexes().size()];
                        sizeInfos[i++] =
                            new SizeInfo(SizeInfo.Type.PRIMARY_KEY);
                        sizeInfos[i++] = new SizeInfo(SizeInfo.Type.DATA);
                        for (Entry<String, Index> entry :
                             table.getIndexes().entrySet()) {
                            sizeInfos[i++] =
                                new SizeInfo(SizeInfo.Type.INDEX_KEY,
                                    entry.getKey());
                        }
                    }
                    return sizeInfos;
                }

                private void doIteration(final Iterator<?> iterator,
                                         SizeInfo[] stInfo,
                                         String tableName)
                    throws ShellException {

                    long nRec = 0;
                    try {
                        while (iterator.hasNext()) {

                            Object obj = iterator.next();
                            if (stInfo != null) {
                                tallySize(obj, stInfo);
                            } else {
                                String jsonString = getJsonString(obj);
                                if (!output.writeRecord(jsonString)) {
                                    break;
                                }
                                if (pretty) {
                                    output.newLine();
                                }
                                nRec++;
                            }
                        }
                    } catch (StoreIteratorException sie) {
                        Throwable t = sie.getCause();
                        if (t != null && t instanceof FaultException) {
                            throw (FaultException)t;
                        }
                        throw new ShellException(
                            t != null ? t.getMessage() : sie.getMessage());
                    }
                    if (stInfo != null) {
                        output.writeRecord(formatOutputSizesInfo(stInfo));
                    } else {
                        sb.append(nRec);
                        sb.append((nRec > 1) ?
                            " rows returned" : " row returned");
                        if (output.IsOutputFile()) {
                            sb.append(" from ");
                            sb.append(tableName);
                            sb.append(" table");
                        }
                    }
                }

                private String getJsonString(Object obj) {
                    if (obj instanceof KeyPair) {
                        return ((KeyPair)obj).getPrimaryKey()
                            .toJsonString(pretty);
                    }
                    return ((Row)obj).toJsonString(pretty);
                }

                private void tallySize(Object obj, SizeInfo[] stInfo) {
                    for (SizeInfo info: stInfo) {
                        switch (info.getType()) {
                        case PRIMARY_KEY:
                            info.tally(getKeySize(obj));
                            break;
                        case DATA:
                            info.tally(getDataSize(obj));
                            break;
                        case INDEX_KEY:
                            info.tally(getIndexKeySize(obj, info.getName()));
                            break;
                        default:
                            break;
                        }
                    }
                }

                private int getKeySize(Object obj) {
                    if (obj instanceof KeyPair) {
                        return TableUtils.getKeySize(
                            ((KeyPair)obj).getPrimaryKey());
                    }
                    return TableUtils.getKeySize((Row)obj);
                }

                private int getDataSize(Object obj) {
                    if (!(obj instanceof Row)) {
                        return 0;
                    }
                    return TableUtils.getDataSize((Row)obj);
                }

                private int getIndexKeySize(Object obj, String indexName) {
                    if (obj instanceof PrimaryKey) {
                        return 0;
                    }

                    if (obj instanceof KeyPair) {
                        return TableUtils.getKeySize(
                            ((KeyPair)obj).getIndexKey());
                    }

                    if (indexName == null) {
                        return 0;
                    }
                    final Row row = (Row)obj;
                    final Index index = row.getTable().getIndex(indexName);
                    if (index == null ||
                        /* skip text index since it lives outside the table */
                        index.getType().equals(Index.IndexType.TEXT)) {
                        return 0;
                    }
                    return getIndexKeySize(index, row);
                }

                int getIndexKeySize(Index index, Row row) {
                    return TableSizeCommand.getIndexKeySize(index, row);
                }

                private String getSizeTitle(SizeInfo.Type type) {
                    switch (type) {
                    case PRIMARY_KEY:
                        return "Primary Key";
                    case DATA:
                        return "Data";
                    case INDEX_KEY:
                        return "Index Key";
                    default:
                        break;
                    }
                    return null;
                }

                private String formatOutputSizesInfo(SizeInfo[] stInfo) {
                    StringBuilder buf = new StringBuilder();
                    Formatter fmt = new Formatter(buf);
                    int nRec = stInfo[0].getCount();
                    fmt.format("Number of records: %d", nRec);
                    for (SizeInfo info: stInfo) {
                        if (info.getName() != null) {
                            fmt.format(eol + "%s sizes of %s:",
                                getSizeTitle(info.getType()), info.getName());
                        } else {
                            fmt.format(eol + "%s sizes:",
                                getSizeTitle(info.getType()));
                        }
                        if (nRec > 0) {
                            fmt.format(eolt + "Minimum size: %d",
                                info.getMin());
                            fmt.format(eolt + "Maximum size: %d",
                                info.getMax());
                            fmt.format(eolt + "Average size: %.1f",
                                info.getAvg());
                        } else {
                            fmt.format(" Not available");
                        }
                    }
                    if (stInfo.length == 1) {
                        fmt.format(eol + "%s sizes: Not available",
                            getSizeTitle(SizeInfo.Type.DATA));
                    }
                    fmt.close();
                    return buf.toString();
                }
            }.run();
            return sb.toString();
        }

        @Override
        protected String getCommandDescription() {
            return COMMAND_DESCRIPTION;
        }

        @Override
        protected String getCommandSyntax() {
            return COMMAND_SYNTAX;
        }
    }

    private static class ResultOutput {
        private final static int MAX_OUTFILE_BUFF_SIZE = 512 * 1024;

        private final boolean pagingEnabled;
        private final int pageHeight;
        private int pageLines;
        private int pageRecords;
        private int totalRecords;
        private final StringBuilder output;
        private Shell shell = null;
        private Writer writer = null;

        ResultOutput(Writer writer) {
            this.writer = writer;
            pagingEnabled = false;
            pageHeight = 0;
            totalRecords = 0;
            output = new StringBuilder();
        }

        ResultOutput(Shell shell) {
            this.shell = shell;
            pageLines = 0;
            totalRecords = 0;
            pageRecords = 0;
            output = new StringBuilder();

            final CommandShell cmdShell = (CommandShell)shell;
            pagingEnabled = isPagingEnabled();
            pageHeight = pagingEnabled ? cmdShell.getPageHeight() : 0;
        }

        public void flushWriting()
            throws ShellException {

            if (IsOutputFile()) {
                try {
                    writer.write(output.toString());
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Can not write to the output file", ioe);
                }
                output.setLength(0);
            } else {
                if (output.length() > 0) {
                    shell.println(output.toString());
                }
            }
        }

        public boolean writeRecord(String record)
            throws ShellException {

            if (IsOutputFile()) {
                writeToFile(record);
                return true;
            }
            return writeToTerm(record);
        }

        public void newLine() {
            output.append(eol);
            if (!IsOutputFile() && isPagingEnabled()) {
                pageLines++;
            }
        }

        private boolean isPagingEnabled() {
            return (shell.getInput() != null &&
                    ((CommandShell)shell).isPagingEnabled());
        }

        private void writeToFile(String record)
            throws ShellException {

            if (output.length() >= MAX_OUTFILE_BUFF_SIZE) {
                try {
                    writer.write(output.toString());
                } catch (IOException ioe) {
                    throw new ShellException(
                        "Can not write to the output file", ioe);
                }
                output.setLength(0);
            }
            if (record != null) {
                output.append(record);
                output.append(eol);
            }
        }

        private boolean writeToTerm(String record)
            throws ShellException {

            if (!pagingEnabled) {
                if (output.length() >= MAX_OUTFILE_BUFF_SIZE) {
                    shell.println(output.toString());
                }
                output.append(record);
                output.append(eol);
                return true;
            }

            if (pageLines >= pageHeight) {
                output.append("--More--(");
                output.append((totalRecords - pageRecords + 1));
                output.append("~");
                output.append(totalRecords);
                output.append(")");
                shell.println(output.toString());
                output.setLength(0);
                pageLines = 0;
                pageRecords = 0;
                try {
                    String ret = shell.getInput().readLine("");
                    /* Terminate the shell if user input Ctrl+C or Ctrl+D */
                    if (ret == null) {
                        shell.setTerminate();
                        return false;
                    }
                    if (ret.toLowerCase().startsWith("q")) {
                        return false;
                    }
                } catch (IOException e) {
                    throw new ShellException("Exception reading input");
                }
            }
            output.append(record);
            output.append(eol);
            pageRecords++;
            pageLines += countLines(record);
            totalRecords++;
            return true;
        }

        boolean IsOutputFile() {
            return (writer != null);
        }

        private int countLines(String str) {
            return str.split("\r\n|\r|\n").length;
        }
    }
}
