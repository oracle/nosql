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

package oracle.kv.impl.admin.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.rmi.RemoteException;
import java.text.NumberFormat;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import oracle.kv.impl.admin.CommandServiceAPI;
import oracle.kv.impl.api.table.IndexImpl;
import oracle.kv.impl.api.table.NameUtils;
import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.api.table.TableImpl;
import oracle.kv.impl.api.table.TableMetadata;
import oracle.kv.impl.metadata.Metadata.MetadataType;
import oracle.kv.impl.param.ParameterMap;
import oracle.kv.impl.param.ParameterState;
import oracle.kv.impl.param.ParameterUtils;
import oracle.kv.table.FieldValue;
import oracle.kv.table.Index;
import oracle.kv.table.Row;
import oracle.kv.table.Table;
import oracle.kv.table.TableUtils;
import oracle.kv.util.shell.Column;
import oracle.kv.util.shell.Shell;
import oracle.kv.util.shell.ShellCommand;
import oracle.kv.util.shell.ShellException;
import oracle.kv.util.shell.Column.Align;

import com.sleepycat.je.util.DbCacheSize;

/* Table size command */
public class TableSizeCommand extends ShellCommand {

    static final String COMMAND_NAME = "table-size";

    static final String TABLE_NAME_FLAG = "-name";
    static final String TABLE_NAME_FLAG_DESC = TABLE_NAME_FLAG + " <name>";
    static final String NROWS_FLAG = "-rows";
    static final String NROWS_FLAG_DESC = NROWS_FLAG + " <num>";
    static final String JSON_FLAG = "-json";
    static final String JSON_FLAG_DESC = JSON_FLAG + " <string>";
    static final String PRIMARYKEY_FLAG = "-primarykey";
    static final String PRIMARYKEY_FLAG_DESC = PRIMARYKEY_FLAG;
    static final String INDEX_FLAG = "-index";
    static final String INDEX_FLAG_DESC = INDEX_FLAG + " <name>";
    static final String KEY_PREFIX_FLAG = "-keyprefix";
    static final String KEY_PREFIX_FLAG_DESC = KEY_PREFIX_FLAG + " <size>";

    static final String COMMAND_SYNTAX =
        COMMAND_NAME + " " + TABLE_NAME_FLAG_DESC +
        " " + JSON_FLAG_DESC + eolt + " [" + NROWS_FLAG_DESC +
        " [[" + PRIMARYKEY_FLAG_DESC + " | " + INDEX_FLAG_DESC  + "] " +
        KEY_PREFIX_FLAG_DESC +  "]+]";

    static final String COMMAND_DESCRIPTION  =
        "Calculates key and data sizes for the specified table using the " +
        "row" + eolt + "input, optionally estimating the NoSQL DB cache " +
        "size required for a" + eolt + "specified number of rows of the " +
        "same format.  Running this command on" + eolt + "multiple sample" +
        " rows can help determine the necessary cache size for" + eolt +
        "desired store performance." + eolt +
        JSON_FLAG + " specifies a sample row used for the " +
        "calculation." + eolt +
        NROWS_FLAG + " specifies the number of rows to use for the cache " +
        "size calculation." + eolt +
        INDEX_FLAG + " or " + PRIMARYKEY_FLAG + " and " + KEY_PREFIX_FLAG +
        " are used to specify the expected" + eolt +
        "commonality of index keys in terms of number of bytes.";

    public TableSizeCommand() {
        super(COMMAND_NAME, 7);
        overrideJsonFlag = true;
    }

    @Override
    public String execute(String[] args, Shell shell)
        throws ShellException {

        Shell.checkHelp(args, this);
        String tableName = null;
        String jsonString = null;
        long nRows = 0;
        boolean calcCacheSize = false;
        Map<String, Integer> keyPrefixSizes =
            new HashMap<String, Integer>();
        int primarykeyPrefix = 0;
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            if (TABLE_NAME_FLAG.equals(arg)) {
                tableName = Shell.nextArg(args, i++, this);
            } else if (NROWS_FLAG.equals(arg)) {
                nRows = parseUnsignedLong(Shell.nextArg(args, i++, this));
            } else if (JSON_FLAG.equals(arg)) {
                jsonString = Shell.nextArg(args, i++, this);
            } else if (PRIMARYKEY_FLAG.equals(arg) ||
                       INDEX_FLAG.equals(arg)) {
                String idxName = null;
                if (INDEX_FLAG.equals(arg)) {
                    idxName = Shell.nextArg(args, i++, this);
                }
                if (++i < args.length) {
                    arg = args[i];
                    if (KEY_PREFIX_FLAG.equals(arg)) {
                        int size = parseUnsignedInt(
                            Shell.nextArg(args, i++, this));
                        if (idxName == null) {
                            primarykeyPrefix = size;
                        } else {
                            keyPrefixSizes.put(idxName, size);
                        }
                    } else {
                        invalidArgument(arg + ", " + KEY_PREFIX_FLAG +
                                        " is required");
                    }
                } else {
                    shell.requiredArg(KEY_PREFIX_FLAG, this);
                }
            } else {
                shell.unknownArgument(arg, this);
            }
        }

        if (tableName == null) {
            shell.requiredArg(TABLE_NAME_FLAG, this);
        }
        if (jsonString == null) {
            shell.requiredArg(JSON_FLAG, this);
        }

        if (nRows > 0) {
            calcCacheSize = true;
        }

        String namespace = NameUtils.getNamespaceFromQualifiedName(tableName);
        String fullName = NameUtils.getFullNameFromQualifiedName(tableName);

        if (namespace == null) {
            namespace = ((CommandShell)shell).getNamespace();
        }

        final Table table = findTable(shell, namespace, fullName);
        if (!keyPrefixSizes.isEmpty()) {
            for (Entry<String, Integer> entry: keyPrefixSizes.entrySet()) {
                String idxName = entry.getKey();
                if (table.getIndex(entry.getKey()) == null) {
                    throw new ShellException("Index does not exist: " +
                        idxName + " on table: " + tableName);
                }
            }
        }

        Row row = null;
        try {
            row = table.createRowFromJson(jsonString, false);
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
        /* Check if the primary key fields are not null. */
        validatePrimaryKeyFields(row);

        /* Calculate the primary key sizes, data size and index key sizes.*/
        int primaryKeySize = TableUtils.getKeySize(row);
        int dataSize = TableUtils.getDataSize(row);
        Map<String, Integer> indexKeySizes = calcIndexesKeySize(row);

        /* Calculate the DbCacheSize. */
        Map<String, Properties> cacheInfoMap = null;
        if (calcCacheSize) {
            String[] jeParams = getJEConfigParams();
            cacheInfoMap = calcDbCacheSize((TableImpl)table, nRows,
                                           primaryKeySize, dataSize,
                                           primarykeyPrefix, indexKeySizes,
                                           keyPrefixSizes, jeParams);
        }

        /* Generate the output information. */
        StringBuilder sb = new StringBuilder();
        sb.append(eol);
        sb.append(genKeySizesInfo(primaryKeySize, dataSize, indexKeySizes));
        if (cacheInfoMap != null) {
            sb.append(eol);
            sb.append(genCacheSizeInfo(cacheInfoMap));
        }
        return sb.toString();
    }

    private static Table findTable(Shell shell,
                                   String namespace,
                                   String tableName)
        throws ShellException {

        final CommandShell cmd = (CommandShell) shell;
        final CommandServiceAPI cs = cmd.getAdmin();
        TableMetadata meta = null;
        try {
            meta = cs.getMetadata(TableMetadata.class,
                                  MetadataType.TABLE);
            if (meta != null) {
                return meta.getTable(NameUtils.switchToInternalUse(namespace),
                    tableName, true);
            }
        } catch (RemoteException re) {
            cmd.noAdmin(re);
        } catch (IllegalStateException ise) {
            throw new ShellException(ise.getMessage(), ise);
        } catch (IllegalArgumentException iae) {
            throw new ShellException(iae.getMessage(), iae);
        }
        throw new ShellException("Table does not exist: " + tableName);
    }

    private void validatePrimaryKeyFields(Row row)
        throws ShellException {

        List<String> pkFields = row.createPrimaryKey().getFieldNames();
        for (String field: pkFields) {
            FieldValue fv = row.get(field);
            if (fv == null || fv.isNull()) {
                throw new ShellException("Primary key field cannot " +
                                         "be null: " + field);
            }
        }
    }

    private Map<String, Integer> calcIndexesKeySize(Row row) {

        Map<String, Index> indexes =
            row.getTable().getIndexes(Index.IndexType.SECONDARY);
        if (indexes == null || indexes.isEmpty()) {
            return null;
        }
        Map<String, Integer> sizeMap = new LinkedHashMap<String, Integer>();
        for (Entry<String, Index> entry: indexes.entrySet()) {
            int size = getIndexKeySize(entry.getValue(), row);
            sizeMap.put(entry.getKey(), size);
        }
        return sizeMap;
    }

    /* this is public for access by GetCommand */
    public static int getIndexKeySize(Index idx, Row row) {

        final IndexImpl index = (IndexImpl)idx;

        if (!index.isMultiKey()) {
            byte[] key = index.extractIndexKey((RowImpl)row);
            return key.length;
        }

        int maxKeys = Integer.parseInt(
            ParameterState.RN_MAX_INDEX_KEYS_PER_ROW_DEFAULT);

        List<byte[]> keys = index.extractIndexKeys((RowImpl)row, maxKeys);
        int sum = 0;

        if (keys != null) {
            for (byte[] key : keys) {
                sum += key.length;
            }
        }

        return sum;
    }

    private Map<String, Properties> calcDbCacheSize(TableImpl table,
                              long nRows, int primaryKeySize,
                              int dataSize, int primaryKeyPrefix,
                              Map<String, Integer> indexKeySizes,
                              Map<String, Integer> indexKeyPrefixSizes,
                              String[] jeParams)
        throws ShellException {

        Map<String, Properties> map =
            new LinkedHashMap<String, Properties>();

        /**
         * Calculate the DbCacheSize for primary db:
         * Primary db key prefix = length of table id string + 1.
         *  - table id: the common key prefix of a table.
         *  - Additional 1 byte: key component delimiter that system added.
         */
        primaryKeyPrefix += table.getIdString().length() + 1;
        Properties props = runDbCacheSize(nRows, primaryKeySize, dataSize,
                                          primaryKeyPrefix, jeParams, null);
        map.put("Table", props);
        if (indexKeySizes == null) {
            return map;
        }

        /* Calculate the DbCacheSize for indexes. */
        for (Entry<String, Index> entry:
            table.getIndexes(Index.IndexType.SECONDARY).entrySet()) {

            final String indexName = entry.getKey();
            final int keySize = indexKeySizes.get(indexName);
            int keyPrefixSize = 0;
            if (indexKeyPrefixSizes != null &&
                indexKeyPrefixSizes.containsKey(indexName)) {
                keyPrefixSize = indexKeyPrefixSizes.get(indexName);
            }
            props = runDbCacheSize(nRows, keySize, primaryKeySize,
                                   keyPrefixSize, jeParams,
                                   new String[] {"-duplicates"});
            map.put(indexName, props);
        }
        return map;
    }

    private Properties runDbCacheSize(long nRows, int keySize,
                                      int dataSize, int keyPrefixSize,
                                      String[] jeParams,
                                      String[] otherParams)
        throws ShellException {

        String[] args = getDbCacheSizeArgs(keySize, dataSize,
                                           keyPrefixSize, nRows,
                                           jeParams, otherParams);
        /**
         * Currently, just call DbCacheSize.main() to perform
         * calculation on required cache size, then parse the output
         * PrintStream to get the result.
         */
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream old = System.out;
        System.setOut(ps);
        try {
            DbCacheSize.main(args);
        } catch (Throwable e) {
            throw new ShellException(e.getMessage());
        }
        String ret = baos.toString();
        System.out.flush();
        System.setOut(old);
        Properties props = null;
        if (ret != null) {
            props = new Properties();
            try {
                props.load(new StringReader(ret));
            } catch (IOException e) {
                throw new ShellException(e.getMessage(), e);
            }
        }
        return props;
    }

    /* Get the arguments for DbCacheSize. */
    private String[] getDbCacheSizeArgs(int keySize, int dataSize,
                                        int keyPerfixsize, long nRows,
                                        String[] jeParams,
                                        String[] otherParams) {

        String[] basicArgs = new String[] {
            "-records", String.valueOf(nRows),
            "-key", String.valueOf(keySize),
            "-data", String.valueOf(dataSize),
            "-keyprefix", String.valueOf(keyPerfixsize),
            "-outputproperties",
            "-replicated"
        };
        int nArgs = basicArgs.length;
        if (jeParams != null) {
            nArgs += jeParams.length;
        }
        if (otherParams != null) {
            nArgs += otherParams.length;
        }

        String[] args = null;
        if (nArgs > basicArgs.length) {
            args = new String[nArgs];
            int i = 0;
            System.arraycopy(basicArgs, 0, args, i, basicArgs.length);
            i += basicArgs.length;
            if (jeParams != null) {
                System.arraycopy(jeParams, 0, args, i, jeParams.length);
                i += jeParams.length;
            }
            if (otherParams != null) {
                System.arraycopy(otherParams, 0, args, i,
                                 otherParams.length);
            }
        } else {
            args = basicArgs;
        }
        return args;
    }

    /* Get all JE configuration parameters used in kvstore. */
    private String[] getJEConfigParams() {
        ParameterUtils pu = new ParameterUtils(new ParameterMap());
        Properties props = pu.getRNRepEnvConfig().getProps();
        if (props == null || props.isEmpty()) {
            return null;
        }
        String[] jeParams = new String[props.size() * 2];
        Enumeration<?> e = props.propertyNames();
        int i = 0;
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            jeParams[i++] = "-" + key;
            jeParams[i++] = props.getProperty(key);
        }
        return jeParams;
    }

    /* Generate the output of key average sizes information. */
    private String genKeySizesInfo(int keySize, int dataSize,
                                   Map<String, Integer> indexSizes) {

        String title = "Key and Data Size";
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        map.put("Primary Key", keySize);
        map.put("Data", dataSize);
        if (indexSizes != null) {
            for (Entry<String, Integer> entry: indexSizes.entrySet()) {
                map.put("Index Key of " + entry.getKey(), entry.getValue());
            }
        }
        return formatKeySizesInfo(title, map);
    }

    /* Format the output for sizes information. */
    private String formatKeySizesInfo(String title,
                                      Map<String, Integer> sizes) {

        StringBuilder sb = new StringBuilder();
        sb.append(formatTableTitle(title));
        sb.append(formatKeySizesTable(sizes));
        return sb.toString();
    }

    private String formatKeySizesTable(Map<String, Integer> sizes) {
        Column colName = new Column("Name", Align.LEFT, false, 0, 0);
        Column colSize = new Column("Number of Bytes", Align.CENTER,
                                    false, 0, 0);
        for (Entry<String, Integer> entry: sizes.entrySet()) {
            /* Object name */
            colName.appendData(entry.getKey());
            /* Number of bytes */
            colSize.appendData(entry.getValue().toString());
        }
        return formatColumnsOutput(new Column[]{colName, colSize}, "  ");
    }

    /* Generate the output of DbCacheSize information. */
    private String genCacheSizeInfo(Map<String, Properties> cacheSizesMap)
        throws ShellException {

        final String titleCacheOverhead = "Environment Cache Overhead";
        final String titleCacheSize = "Database Cache Sizes";
        final String footCacheSize =
            "For further information see the DbCacheSize javadoc.";
        final String propNames[] = new String[] {
            "internalNodes",
            "internalNodesAndVersions",
            "allNodes"
        };
        final String descriptions[] = new String [] {
            "Internal nodes only",
            "Internal nodes and record versions",
            "Internal nodes and leaf nodes"
        };
        final String columnNames[] = new String[] {
            "Name", "Number of Bytes", "Description"
        };
        final String propOverhead = "overhead";
        final String totalName = "Total";
        final NumberFormat INT_FORMAT = NumberFormat.getIntegerInstance();

        int nValues = propNames.length;
        long sum[] = new long[nValues];
        long cacheOverhead = 0;
        Map<String, LinkedHashMap<String, String>> result =
            new LinkedHashMap<String, LinkedHashMap<String, String>>();
        for (Entry<String, Properties> info: cacheSizesMap.entrySet()) {
            Properties props = info.getValue();
            if (cacheOverhead == 0) {
                cacheOverhead = getPropLongValue(props, propOverhead);
            }
            LinkedHashMap<String, String> map =
                new LinkedHashMap<String, String>(nValues);
            long value[] = new long[nValues];
            for (int i = 0; i < value.length; i++) {
                value[i] = getPropLongValue(props, propNames[i]);
                sum[i] += value[i];
                map.put(descriptions[i], INT_FORMAT.format(value[i]));
            }
            result.put(info.getKey(), map);
        }
        /* Added sum information to result. */
        LinkedHashMap<String, String> map =
            new LinkedHashMap<String, String>(nValues);
        for (int i = 0; i < nValues; i++) {
            map.put(descriptions[i], INT_FORMAT.format(sum[i]));
        }
        result.put(totalName, map);

        /* Generate cache overhead information */
        String output = formatCacheOverHeadInfo(titleCacheOverhead,
            String.format("%s minimum bytes",
                          INT_FORMAT.format(cacheOverhead)));

        /* Generate cache size information */
        output += formatCacheSizeInfo(titleCacheSize, columnNames,
                                      result, footCacheSize);
        return output;
    }

    private long getPropLongValue(Properties props, String propName)
        throws ShellException {

        String value = props.getProperty(propName);
        if (value == null) {
            return 0;
        }
        try {
            return Long.parseLong(props.getProperty(propName));
        } catch (NumberFormatException nfe) {
            throw new ShellException("Value of property '" + propName +
                "' is not long value: " + value);
        }
    }

    /* Format cache overhead information. */
    private String formatCacheOverHeadInfo(String title, String subHead) {

        StringBuilder sb = new StringBuilder();
        sb.append(formatTableTitle(title));
        if (subHead != null) {
            sb.append(subHead);
            sb.append(eol);
            sb.append(eol);
        }
        return sb.toString();
    }

    /* Format cache sizes information. */
    private String formatCacheSizeInfo(String title, String columnNames[],
            Map<String, LinkedHashMap<String, String>> result,
            String foot) {

        StringBuilder sb = new StringBuilder();
        sb.append(formatTableTitle(title));
        if (result != null) {
            sb.append(formatCacheSizeTable(columnNames, result));
        }
        if (foot != null) {
            sb.append(eol);
            sb.append(foot);
            sb.append(eol);
        }
        return sb.toString();
    }

    private String formatCacheSizeTable(String[] headers,
            Map<String, LinkedHashMap<String, String>> result) {

        Column[] columns = new Column[headers.length];
        for (int i = 0; i < columns.length ; i++) {
            columns[i] = new Column(headers[i], Align.LEFT, false, 0, 0);
        }

        int i = 0;
        for (Entry<String, LinkedHashMap<String, String>> entry:
             result.entrySet()) {

            String key = entry.getKey();
            HashMap<String, String> map = entry.getValue();
            boolean appendDelimiter = false;
            if (++i < result.size()) {
                appendDelimiter = true;
            }

            /* Object name */
            Column column = columns[0];
            for (int j = 0; j < map.size(); j++) {
                if (j == map.size()/2) {
                    column.appendData(key);
                } else {
                    column.appendData("");
                }
            }
            if (appendDelimiter) {
                column.appendSeparatorLine();
            }

            /* Number of bytes */
            column = columns[1];
            for (String text: map.values()) {
                column.appendData(text, Column.Align.RIGHT);
            }
            if (appendDelimiter) {
                column.appendSeparatorLine();
            }

            /* Description */
            column = columns[2];
            for (String text: map.keySet()) {
                column.appendData(text);
            }
            if (appendDelimiter) {
                column.appendSeparatorLine();
            }
        }
        return formatColumnsOutput(columns, "  ");
    }

    private String formatTableTitle(String title) {
        final String appending = "===";
        return String.format("%s %s %s" + eol + eol,
                             appending, title, appending);
    }

    private String formatColumnsOutput(Column columns[],
                                       final String separator) {
        StringBuilder sb = new StringBuilder();
        int nRows = columns[0].getHeight();
        for (int i = 0; i < nRows; i++) {
            for (Column col: columns) {
                sb.append(col.getFormattedText(i));
                sb.append(separator);
            }
            sb.append(eol);
        }
        return sb.toString();
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
