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
package oracle.kv.impl.query.shell.output;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import oracle.kv.impl.api.table.JsonCollectionRowImpl;
import oracle.kv.impl.query.shell.output.ResultOutputFactory.ResultOutput;
import oracle.kv.table.ArrayValue;
import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.MapValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.util.shell.Column;
import oracle.kv.util.shell.Column.Align;
import oracle.kv.util.shell.Shell;

/**
 * The ColumnOutput class is used to display records in the tabular format.
 *
 * For table that contains fields with simple types, its output is like below.
 * e.g. Table users contains 4 fields:
 *  1. id INTEGER
 *  2. name STRING
 *  3. age INTEGER
 *  4. email STRING
 *
 * {@literal
 * onql-> select * from users;
 * +-----+------------+-----+--------------------+
 * | id  |    name    | age |      email         |
 * +-----+------------+-----+--------------------+
 * |  1  | Jack Smith |  20 | Jack.Smith@ABC.com |
 * |  2  | Tom White  |  30 | Tom.White@ABC.com  |
 * | ... | ...        | ... |      ...           |
 * | ... | ...        | ... |      ...           |
 * +-----+------------+-----+--------------------+
 * }
 *
 * If table contains complex field like map or record, then the complex value
 * are displayed hierarchically like below:
 *
 * e.g. Table employees contains 3 fields:
 *  1. id INTEGER
 *  2. name RECORD(firstName STRING, lastName STRING)
 *  3. address RECORD(street STRING,
 *                    city STRING,
 *                    state STRING,
 *                    phone ARRAY(RECORD(type ENUM(work, home),
 *                                       areacode INTEGER,
 *                                       number INTEGER)
 *                    )
 *     )
 *
 * {@literal
 * onql-> select * from employees;
 * +----+--------------=-------+-----------------------------------+
 * | id |         name         |              address              |
 * +----+----------------------+-----------------------------------+
 * |  1 | firstName | Nathan   | street       | 187 Aspen Drive    |
 * |    | lastName  | Dawson   | city         | Middleburg         |
 * |    |                      | state        | FL                 |
 * |    |                      | phone                             |
 * |    |                      |     type     | work               |
 * |    |                      |     areacode | 305                |
 * |    |                      |     number   | 1234079            |
 * |    |                      |                                   |
 * |    |                      |     type     | home               |
 * |    |                      |     areacode | 305                |
 * |    |                      |     number   | 2066401            |
 * +----+----------------------+-----------------------------------+
 * |  2 | firstName | Alyssa   | street       | 36 Mulberry Street |
 * |    | lastName  | Guerrero | city         | Leominster         |
 * |    |                      | state        | MA                 |
 * |    |                      | phone                             |
 * |    |                      |     type     | work               |
 * |    |                      |     areacode | 339                |
 * |    |                      |     number   | 4120211            |
 * |    |                      |                                   |
 * |    |                      |     type     | work               |
 * |    |                      |     areacode | 339                |
 * |    |                      |     number   | 8694021            |
 * +----+----------------------+-----------------------------------+
 * |  ...     ...      ...      ...        ...         ...         |
 * +----+----------------------+-----------------------------------+
 * }
 *
 */
public class ColumnOutput extends ResultOutput {

    /* The columns of the output table. */
    Column[] columns;

    /* The output table format related information */
    private final TableFormat format;

    /* Whether the i-th column is hidden */
    private final boolean[] isColumnHiddenAtPos;

    public ColumnOutput(Shell shell,
                        PrintStream output,
                        RecordDef recordDef,
                        Iterator<RecordValue> iterator,
                        boolean pagingEnabled,
                        int pageHeight) {
        this(shell, output, recordDef, iterator, pagingEnabled,
             pageHeight, new TableFormat());
    }

    public ColumnOutput(Shell shell,
                        PrintStream output,
                        RecordDef recordDef,
                        Iterator<RecordValue> iterator,
                        boolean pagingEnabled,
                        int pageHeight,
                        int[] hiddenColumns) {
        this(shell, output, recordDef, iterator, pagingEnabled,
             pageHeight, new TableFormat(), hiddenColumns);
    }

    public ColumnOutput(Shell shell,
                        PrintStream output,
                        RecordDef recordDef,
                        Iterator<RecordValue> iterator,
                        boolean pagingEnabled,
                        int pageHeight,
                        TableFormat tableFormat) {
        this(shell, output, recordDef, iterator, pagingEnabled,
             pageHeight, tableFormat, null);
    }

    public ColumnOutput(Shell shell,
                        PrintStream output,
                        RecordDef recordDef,
                        Iterator<RecordValue> iterator,
                        boolean pagingEnabled,
                        int pageHeight,
                        TableFormat tableFormat,
                        int[] hiddenColumns) {
        super(shell, output, recordDef, iterator, pagingEnabled, pageHeight);
        format = tableFormat;
        columns = null;
        if (hiddenColumns != null) {
            isColumnHiddenAtPos = new boolean[recordDef.getNumFields()];
            for (int icol : hiddenColumns) {
                isColumnHiddenAtPos[icol] = true;
            }
        } else {
            isColumnHiddenAtPos = null;
        }
    }

    /**
     * Initialize the columns.
     *
     * If the field is one of simple field types or a array of simple field
     * type, then use Column object to display the values. If the field contains
     * complex types like map, record types, then use CompositeColumn object to
     * display the field values.
     */
    void initColumns() {
        final int nColumns = recordDef.getNumFields();
        final char dataDelimiter = format.getDataDelimiter();
        columns = new Column[nColumns];
        for (int i = 0; i < nColumns; i++) {
            final String title = recordDef.getFieldName(i);
            final FieldDef def = recordDef.getFieldDef(i);
            final Column col;

            if (useCompositeColumn(def)) {
                col = new CompositeColumn(title, dataDelimiter);
            } else {
                col = new Column(title, getDefaultColumnAlign(def));
            }

            if (def.isComplex()) {
                if (!format.hasRowSeparator()) {
                    format.enableRowSeparator(true);
                }
            }
            columns[i] = col;
        }
    }

    /**
     * Returns default alignment for the given FieldDef object.
     */
    private Align getDefaultColumnAlign(FieldDef def) {
        if (def.isNumeric()) {
            return Align.RIGHT;
        }
        return Align.LEFT;
    }

    /**
     * Returns true if CompositeColumn should be used to display the value for
     * the given FieldDef object, otherwise return false.
     */
    boolean useCompositeColumn(FieldDef fdef) {
        if (fdef.isArray()) {
            fdef = fdef.asArray().getElement();
        }
        return fdef.isComplex();
    }

    @Override
    long outputRecords(long maxLines, boolean pagingEnabled) {

        long nRows = 0;
        if (columns == null) {
            initColumns();
        }

        while (resultIterator.hasNext()) {
            RecordValue record = resultIterator.next();
            if (record instanceof JsonCollectionRowImpl) {
                throw new IllegalArgumentException(
                    "A columnar output mode (line, csv) cannot be used " +
                    "to display results of a select * query on a JSON " +
                    "collection table");
            }
            appendRecord(nRows++, record);
            if (columns[0].getHeight() >= maxLines) {
                break;
            }
        }
        if (nRows > 0 && hasPageSeparator(pagingEnabled)) {
            appendSeparator();
        }
        final String ret = getFormatedRowText();
        output(ret);
        resetColumns(pagingEnabled);
        return nRows;
    }

    /**
     * Resets the columns.
     */
    void resetColumns(boolean pagingEnabled) {
        for (Column col : columns) {
            col.reset(pagingEnabled, !pagingEnabled);
        }
    }

    /**
     * Appends a row to the output table.
     */
    void appendRecord(@SuppressWarnings("unused") long rowIndex,
                      RecordValue recordValue) {

        final int nColumns = columns.length;
        int height = 0;
        for (int i = 0; i < nColumns; i++) {
            final Column col = columns[i];
            final FieldValue val = recordValue.get(i);
            appendValue(col, val);
            int colHeight = col.getHeight();
            if (height < colHeight) {
                height = col.getHeight();
            }
        }
        finishingRow(height);
    }

    /**
     * Do some work after a row is appended to the columns:
     *  1. Append empty cell(s) to those column with smaller height.
     *  2. Append row separator line if needed.
     */
    void finishingRow(int height) {
        for (Column col : columns) {
            final int diff = height - col.getHeight();
            for (int i = 0; i < diff; i ++) {
                col.appendEmptyData();
            }
        }
        if (format.hasRowSeparator()) {
            appendSeparator();
        }
    }

    /**
     * Returns true if need append a page separator line.
     */
    private boolean hasPageSeparator(final boolean pagingEnabled) {
        if (!format.hasLeftRightBorder() || format.hasRowSeparator()) {
            return false;
        }
        return pagingEnabled ? true : !resultIterator.hasNext();
    }

    /**
     * Appends a value to column.
     */
    void appendValue(final Column col, final FieldValue fv) {

        if (fv.isComplex()) {
            appendNestedValue(col, null, fv, 0);
        } else {
            final String value = getStringValue(fv);
            col.appendData(value);
        }
    }

    /**
     * Appends nested values of complex field to the column, the values are
     * displayed hierarchically that uses indentation to indicate nesting
     * level.
     */
    private void appendNestedValue(final Column col,
                                   final String field,
                                   final FieldValue fval,
                                   final int currentlevel) {

        int level = currentlevel;

        if (!fval.isComplex()) {
            final String value = getStringValue(fval);
            col.appendData(level, field, value);
            return;
        }

        if (field != null) {
            col.appendData(level, field, "");
            level++;
        }
        if (fval.isRecord()) {
            final RecordValue rval = fval.asRecord();
            final List<String> fields = rval.getFieldNames();
            for (String fname : fields) {
                final FieldValue val = rval.get(fname);
                appendNestedValue(col, fname, val, level);
            }
        } else if (fval.isMap()) {
            final MapValue mval = fval.asMap();
            final Set<String> keys = mval.getFields().keySet();
            if (keys.size() > 0) {
                for (String key : keys) {
                    final FieldValue val = mval.get(key);
                    appendNestedValue(col, key, val, level);
                }
            } else {
                col.appendEmptyData();
            }
        } else if (fval.isArray()) {
            final ArrayValue aval = fval.asArray();
            if (aval.size() > 0) {
                for (int i = 0; i < aval.size(); i++) {
                    final FieldValue val = aval.get(i);
                    if (i > 0 && val.isComplex()) {
                        col.appendEmptyLine();
                    }
                    appendNestedValue(col, null, val, level);
                }
            } else {
                col.appendEmptyData();
            }
        }
    }

    /**
     * Appends separator line
     */
    private void appendSeparator() {
        for (Column col : columns) {
            col.appendSeparatorLine();
        }
    }

    /**
     * Formats the columns into a string for output.
     */
    private String getFormatedRowText() {
        final StringBuilder sb = new StringBuilder();
        final int nRows = columns[0].getHeight();
        final int nCols = columns.length;
        for (int i = 0; i < nRows; i++) {
            final char delimiter = getDelimiter(columns[0], i);
            setLeftBorder(sb, delimiter);
            boolean hasLeftColumn = false;
            for (int iCol = 0; iCol < nCols; iCol++) {
                final Column col = columns[iCol];
                if (isColumnHidden(iCol)) {
                    continue;
                }
                if (hasLeftColumn) {
                    setCellBorder(sb, delimiter);
                } else {
                    hasLeftColumn = true;
                }
                sb.append(col.getFormattedText(i));
            }
            setRightBorder(sb, delimiter);
            sb.append(Shell.eol);
        }
        return sb.toString();
    }

    private boolean isColumnHidden(int index) {
        if (isColumnHiddenAtPos != null) {
            return isColumnHiddenAtPos[index];
        }
        return false;
    }

    /**
     * Returns the delimiter character according to column type.
     */
    private char getDelimiter(final Column column, final int index) {
        if (column.isHeader(index)) {
            return format.getHeaderDelimiter();
        } else if (column.isSeparatorLine(index) &&
                  !column.isSeparatorEmptyLine(index)) {
            return format.getSeparatorLineDelimter();
        }
        return format.getDataDelimiter();
    }

    /**
     * Appends left border to the output StringBuilder if needed.
     */
    private void setLeftBorder(final StringBuilder sb,
                               final char delimiter) {
        if (format.hasLeftRightBorder()) {
            setCellBorder(sb, " ", delimiter);
        }
    }

    /**
     * Appends right border to the output StringBuilder if needed.
     */
    private void setRightBorder(final StringBuilder sb,
                                final char delimiter) {
        if (format.hasLeftRightBorder()) {
            setCellBorder(sb, delimiter);
        }
    }

    private void setCellBorder(final StringBuilder sb,
                               final char delimiter) {
        setCellBorder(sb, null, delimiter);
    }

    private void setCellBorder(final StringBuilder sb,
                               final String leftPadding,
                               final char delimiter) {
        if (!format.isNoDelimiter(delimiter)) {
            if (leftPadding != null) {
                sb.append(leftPadding);
            }
            sb.append(delimiter);
        }
    }

    /**
     * A class that represents the format information of the output table.
     */
    static class TableFormat {

        /* The default delimiter character used in header columns */
        final static char HEADER_DELIMITER = '|';

        /* The default delimiter character used in data columns */
        final static char DATA_DELIMITER = '|';

        /* The default delimiter character used in separator lines */
        final static char SEPARATOR_LINE_DELIMITER = '+';

        /* A character indicates no delimiter */
        final static char NO_DELIMITER = '\0';

        private boolean hasRowSeparator;
        private final boolean hasLeftRightBorder;
        private final boolean hasHeaderDelimiter;
        private final char dataDelimiter;
        private final char headerDelimiter;
        private final char separatorLineDelimter;

        TableFormat() {
            this(true, true, false);
        }

        TableFormat(boolean hasLeftRightBorder,
                    boolean hasHeaderDelimiter,
                    boolean hasRowSeparator) {
            this(hasLeftRightBorder, hasHeaderDelimiter,
                 hasRowSeparator, DATA_DELIMITER);
        }

        TableFormat(boolean hasLeftRightBorder,
                    boolean hasHeaderDelimiter,
                    boolean hasRowSeparator,
                    char dataDelimiter) {
            this(hasLeftRightBorder, hasHeaderDelimiter, hasRowSeparator,
                 HEADER_DELIMITER, dataDelimiter, SEPARATOR_LINE_DELIMITER);
        }

        TableFormat(boolean hasLeftRightBorder,
                    boolean hasHeaderDelimiter,
                    boolean hasRowSeparator,
                    char headerDelimiter,
                    char dataDelimiter,
                    char separatorLineDelimter) {

            this.hasRowSeparator = hasRowSeparator;
            this.hasLeftRightBorder = hasLeftRightBorder;
            this.hasHeaderDelimiter = hasHeaderDelimiter;
            this.dataDelimiter = dataDelimiter;
            this.headerDelimiter = headerDelimiter;
            this.separatorLineDelimter = separatorLineDelimter;
        }

        boolean hasRowSeparator() {
            return hasRowSeparator;
        }

        boolean hasLeftRightBorder() {
            return hasLeftRightBorder;
        }

        boolean hasHeaderDelimiter() {
            return hasHeaderDelimiter;
        }

        char getDataDelimiter() {
            return dataDelimiter;
        }

        char getHeaderDelimiter() {
            return hasHeaderDelimiter ? headerDelimiter : NO_DELIMITER;
        }

        char getSeparatorLineDelimter() {
            return separatorLineDelimter;
        }

        void enableRowSeparator(boolean value) {
            hasRowSeparator = value;
        }

        boolean isNoDelimiter(char delimiter) {
            return delimiter == NO_DELIMITER;
        }
    }
}
