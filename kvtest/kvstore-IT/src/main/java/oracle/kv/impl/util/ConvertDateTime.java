/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.text.ParseException;
import java.util.Date;

/**
 * Utility program for converting between our standard "YYYY-MM-DD HH:MM:SS.mmm
 * ZZZ" date/time format and the standard Java milliseconds time format.
 */
public class ConvertDateTime {

    /**
     * Convert between our standard date/time format and the standard
     * Java milliseconds format.
     *
     * <p>Usage: java ConvertDateTime [-m] [-v] [<i>input</i>]
     * <ul>
     * <li>-m - Convert standard date format to milliseconds.  If not
     * specified, converts milliseconds to the standard date format.
     * <li>-v - Include the input value in the output
     * <li><i>input</i> - The input value to parse, else use the current time
     * </ul>
     */
    public static void main(String[] args) throws ParseException {
        boolean toMillis = false;
        boolean verbose = false;
        int i;
        for (i = 0; i < args.length; i++) {
            final String arg = args[i];
            boolean done = false;
            switch (arg) {
            case "-m":
                toMillis = true;
                break;
            case "-v":
                verbose = true;
                break;
            default:
                done = true;
                break;
            }
            if (done) {
                break;
            }
        }
        if (toMillis) {
            final String input = (i < args.length) ?
                args[i] :
                FormatUtils.formatTimeMillis(System.currentTimeMillis());
            final Date date =
                FormatUtils.parseDateTimeMillis(input);
            if (verbose) {
                System.out.print(input + " => ");
            }
            System.out.println(date.getTime());
        } else {
            final long timestamp = (i < args.length) ?
                Long.parseLong(args[i]) :
                System.currentTimeMillis();
            if (verbose) {
                System.out.print(timestamp + " => ");
            }
            System.out.println(FormatUtils.formatTimeMillis(timestamp));
        }
    }
}
