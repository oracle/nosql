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

import oracle.kv.impl.query.shell.output.ResultOutputFactory.ResultOutput;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.util.shell.Shell;

/**
 * Display records in JSON format that can be single line or multiple lines.
 */
public class JSONOutput extends ResultOutput {

    private final boolean pretty;

    JSONOutput(final Shell shell,
               final PrintStream output,
               final RecordDef recordDef,
               final Iterator<RecordValue> iterator,
               final boolean pagingEnabled,
               final int pageHeight) {
        this(shell, output, recordDef, iterator, pagingEnabled,
             pageHeight, false);
    }

    JSONOutput(final Shell shell,
               final PrintStream output,
               final RecordDef recordDef,
               final Iterator<RecordValue> iterator,
               final boolean pagingEnabled,
               final int pageHeight,
               final boolean pretty) {
        super(shell, output, recordDef, iterator, pagingEnabled, pageHeight);
        this.pretty = pretty;
    }

    @Override
    long outputRecords(long maxLines, boolean enablePaging) {
        long nRows = 0;
        long nLines = 0;
        final StringBuilder sb = new StringBuilder();
        while (resultIterator.hasNext()) {
            final RecordValue recordValue = resultIterator.next();
            final String jsonString = getJsonString(recordValue, pretty);
            sb.append(jsonString);
            sb.append(Shell.eol);
            if (pretty) {
                nLines += countLines(jsonString) + 1;
            } else {
                nLines++;
            }
            nRows++;
            if (nLines >= maxLines) {
                break;
            }
        }
        output(sb.toString());
        return nRows;
    }

    private String getJsonString(final RecordValue recordValue,
                                 final boolean isPretty) {
        String str = recordValue.toJsonString(isPretty);
        if (isPretty) {
            return str + Shell.eol;
        }
        return str;
    }

    private static int countLines(String str){
        String[] lines = str.split("\r\n|\r|\n");
        return lines.length;
    }
}
