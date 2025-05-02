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

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;

import oracle.kv.table.FieldValue;
import oracle.kv.table.RecordDef;
import oracle.kv.table.RecordValue;
import oracle.kv.util.shell.Shell;

/**
 * Returns a ResultOutput object for a specific OutputMode
 */
public class ResultOutputFactory {

    public static enum OutputMode {COLUMN, LINE, JSON, JSON_PRETTY, CSV}

    public static ResultOutput getOutput(final OutputMode mode,
                                         final Shell shell,
                                         final PrintStream output,
                                         final RecordDef recordDef,
                                         final Iterator<RecordValue> iterator,
                                         final boolean pagingEnabled,
                                         final int pageHeight) {
        return getOutput(mode, shell, output, recordDef, iterator,
                         pagingEnabled, pageHeight, null);
    }

    /*
     * TODO: hiding columns is only supported in Column mode, we may support
     * it in other mode if need.
     */
    public static ResultOutput getOutput(final OutputMode mode,
                                         final Shell shell,
                                         final PrintStream output,
                                         final RecordDef recordDef,
                                         final Iterator<RecordValue> iterator,
                                         final boolean pagingEnabled,
                                         final int pageHeight,
                                         final int[] hiddenColumns) {
        switch (mode) {
            case COLUMN:
                return new ColumnOutput(shell, output, recordDef, iterator,
                                        pagingEnabled, pageHeight,
                                        hiddenColumns);
            case LINE:
                return new LineOutput(shell, output, recordDef, iterator,
                                      pagingEnabled, pageHeight);
            case JSON:
                return new JSONOutput(shell, output, recordDef, iterator,
                                      pagingEnabled, pageHeight);
            case JSON_PRETTY:
                return new JSONOutput(shell, output, recordDef, iterator,
                                      pagingEnabled, pageHeight, true);
            case CSV:
                return new CSVOutput(shell, output, recordDef, iterator,
                                     pagingEnabled, pageHeight);
            default:
                break;
        }
        return null;
    }

    /**
     * An abstract ResultOutput class.
     */
    public static abstract class ResultOutput {
        public final static String NULL_STRING = "NULL";
        private final static int MAX_LINES = 100;

        /* The Shell instance used to get the input from console */
        private final Shell shell;

        /* The output PrintStream */
        private final PrintStream output;

        /* The flag indicates if the paging is enabled or not */
        private final boolean pagingEnabled;

        /* The max lines per batch for output */
        private final int batchLines;

        /* The total number of records displayed */
        private long numRecords;

        /* The recodeDef of record in result */
        final RecordDef recordDef;

        /* The TableIterator over the records in this result */
        Iterator<RecordValue> resultIterator;

        ResultOutput(final Shell shell,
                     final PrintStream output,
                     final RecordDef recordDef,
                     final Iterator<RecordValue> iterator,
                     final boolean pagingEnabled,
                     final int pageHeight) {

            this.shell = shell;
            this.recordDef = recordDef;
            this.resultIterator = iterator;
            this.output = output;
            this.pagingEnabled = pagingEnabled;
            batchLines = pagingEnabled ? pageHeight : MAX_LINES;
            numRecords = 0;
        }

        /**
         * Abstract method to output records.
         */
        abstract long outputRecords(long maxLines, boolean paging);

        /**
         * Displays the result set.
         */
        public long outputResultSet()
            throws IOException {

            final String fmt = "--More--(%d~%d)\n";
            final int maxLines = (batchLines > 3) ? batchLines - 3 : batchLines;
            boolean hasMore = resultIterator.hasNext();
            while (hasMore) {
                final long nRows = outputRecords(maxLines, pagingEnabled);
                final long prev = numRecords;
                numRecords += nRows;
                hasMore = resultIterator.hasNext();
                if (!hasMore) {
                    break;
                }
                if (!pagingEnabled) {
                    continue;
                }
                final String msg = String.format(fmt, (prev + 1), numRecords);
                output(msg);
                final String in = shell.getInput().readLine("");
                /* Terminate the shell if user input Ctrl+C or Ctrl+D */
                if (in == null) {
                    shell.setTerminate();
                    break;
                }
                if (in.toLowerCase().startsWith("q")) {
                    break;
                }
            }
            return numRecords;
        }

        /**
         * Outputs string
         */
        void output(String string) {
            output.print(string);
            output.flush();
        }

        /**
         * Returns the total records displayed
         */
        long getNumRecords() {
            return numRecords;
        }

        /**
         * Returns the string to display for null value
         */
        String getNullString() {
            return NULL_STRING;
        }

        /**
         * Returns the string value of each field value.
         */
        String getStringValue(final FieldValue val) {

            if (val.isNull()) {
                return getNullString();
            }
            if (!val.isComplex()) {
                return val.toString();
            }

            throw new IllegalArgumentException("The type is not supported by " +
                "this function : " + val.getDefinition().getType().name());
        }
    }
}
