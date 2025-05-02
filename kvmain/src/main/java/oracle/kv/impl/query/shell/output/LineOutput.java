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

import oracle.kv.table.FieldDef;
import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.util.shell.Column;
import oracle.kv.util.shell.Shell;

/**
 * The class is used to display records in line mode, one value per line.
 *
 * e.g. Table users contains 3 fields:
 *  - id INTEGER
 *  - name STRING
 *  - age INTEGER
 *
 * {@literal
 * onql-> select * from users;
 * > Row 1
 * +------+------------+
 * | id   | 1          |
 * | name | Jack Smith |
 * | age  | 20         |
 * +------+------------+
 *
 * > Row 2
 * +------+------------+
 * | id   | 2          |
 * | name | Tom White  |
 * | age  | 30         |
 * +------+------------+
 *
 * > Row #
 * ...
 * }
 */

public class LineOutput extends ColumnOutput {

    /* The column to store field names */
    private Column colLabel;

    /* The column to store field values */
    private Column colData;

    LineOutput(final Shell shell,
               final PrintStream output,
               final RecordDef recordDef,
               final Iterator<RecordValue> iterator,
               final boolean pagingEnabled,
               final int pageHeight) {
        super(shell, output, recordDef, iterator, pagingEnabled, pageHeight,
              new TableFormat(true  /* hasLeftRightBorder */,
                              false /* hasHeaderDelimiter */,
                              true  /* hasRowSeparator */)
            );
    }

    /**
     * Initializes the label column(field names) and data column(field values).
     */
    @Override
    void initColumns() {
        columns = new Column[2];
        boolean isCompositeColumn = false;
        for(String fieldName : recordDef.getFieldNames()) {
            final FieldDef fdef = recordDef.getFieldDef(fieldName);
            if (useCompositeColumn(fdef)) {
                isCompositeColumn = true;
                break;
            }
        }

        colLabel = new Column();
        if (isCompositeColumn) {
            colData = new CompositeColumn();
        } else {
            colData = new Column();
        }
        columns[0] = colLabel;
        columns[1] = colData;
    }

    @Override
    void appendRecord(long index, RecordValue recordValue) {

        final int nColumnCount = recordDef.getFieldNames().size();
        final long iRow = getNumRecords() + index;
        appendRowLabel(iRow);
        for (int i = 0; i < nColumnCount; i++) {
            final String label = recordDef.getFieldName(i);
            final FieldValue val = recordValue.get(i);
            colLabel.appendData(label);
            appendValue(colData, val);
            finishingRow(Math.max(colLabel.getHeight(), colData.getHeight()));
        }
    }

    private void appendRowLabel(long i) {
        colLabel.appendTitle("\n > Row " + i);
        colData.appendTitle("");
    }

    @Override
    void resetColumns(boolean pagingEnabled) {
        for (Column column : columns) {
            column.reset(false, false);
        }
    }
}
