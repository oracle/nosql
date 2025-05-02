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
import oracle.kv.util.shell.Column.Align;
import oracle.kv.util.shell.Shell;

/**
 * Display records in comma separated values format.
 */
public class CSVOutput extends ColumnOutput {

    private final static char COMMA_DELIMITER = ',';

    public CSVOutput(final Shell shell,
                     final PrintStream output,
                     final RecordDef recordDef,
                     final Iterator<RecordValue> iterator,
                     final boolean pagingEnabled,
                     final int pageHeight) {
        super(shell, output, recordDef, iterator, pagingEnabled, pageHeight,
              new TableFormat(false /* hasLeftRightBorder */,
                              false /* hasHeaderDelimiter */,
                              false /* hasRowSeparator */,
                              COMMA_DELIMITER /* dataDelimiter */));
    }

    @Override
    void initColumns() {
        final int nColumns = recordDef.getFieldNames().size();
        columns = new Column[nColumns];
        for (int i = 0; i < nColumns; i++) {
            final FieldDef fdef = recordDef.getFieldDef(i);
            if (fdef.isComplex()) {
                throw new IllegalArgumentException("The type of field \"" +
                    recordDef.getFieldName(i) + "\" in the result set is " +
                    fdef.getType() + " that can not be displayed in csv " +
                    "format.");
            }
            final Column col = new Column(null, Align.UNALIGNED);
            columns[i] = col;
        }
    }

    @Override
    void appendRecord(long rowIndex, RecordValue recordValue) {

        final int nColumns = columns.length;
        for (int i = 0; i < nColumns; i++) {
            final Column col = columns[i];
            final FieldValue val = recordValue.get(i);
            final String value = getStringValue(val);
            col.appendData(value);
        }
    }

    @Override
    String getStringValue(final FieldValue fval) {
        String value = super.getStringValue(fval);
        if (!fval.isNull() && (fval.isString() || fval.isEnum())) {
            value = "\"" + value + "\"";
        }
        return value;
    }

    @Override
    String getNullString() {
        return "";
    }
}
