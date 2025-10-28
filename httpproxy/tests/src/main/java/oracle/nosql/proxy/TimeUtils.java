package oracle.nosql.proxy;

import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import oracle.nosql.util.fault.ErrorCode;
import oracle.nosql.util.fault.RequestFaultException;

/**
 * A collection of static utility methods for converting between timestamps
 * represented as long and as String.
 */
class TimeUtils {
    /**
     * When specifying timestamps in the API, these formats are accepted.
     */
    private static SimpleDateFormat[] dateFormats = {
        new SimpleDateFormat("MM-dd-yy'T'HHmmss.SSS"),
        new SimpleDateFormat("MM-dd-yy'T'HHmmss"),
        new SimpleDateFormat("MM-dd-yy'T'HHmm"),
        new SimpleDateFormat("MM-dd-yy"),
        new SimpleDateFormat("HHmmss"),
        new SimpleDateFormat("HHmm")
    };

    static {
        final TimeZone tz = TimeZone.getTimeZone("UTC");
        for (SimpleDateFormat sdf : dateFormats) {
            sdf.setTimeZone(tz);
            sdf.setLenient(false);
        }
    }

    private static String getDateFormatsUsage() {
        String usage =
            "Timestamps can be given in the following formats," +
            "which are interpreted in the UTC time zone:";

        for (SimpleDateFormat sdf : dateFormats) {
            usage += " " + sdf.toPattern();
        }

        return usage;
    }

    /**
     * Apply the above formats in sequence until one of them matches.
     * Synchronize on the class object to serialize use of the SDF instances.
     */
    public static synchronized long parseTimestamp(String s, String label) {

        if (s == null) {
            return 0L;
        }

        /*
         * Accept long as string as well
         */
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException nfe) {
            /* not an error, continue */
        }

        Date r = null;
        for (SimpleDateFormat sdf : dateFormats) {
            try {
                r = sdf.parse(s);
                break;
            } catch (ParseException pe) /* CHECKSTYLE:OFF */ {
            } /* CHECKSTYLE:ON */
        }

        if (r == null) {
            throw new RequestFaultException
                ("The " + label + " parameter could not be parsed. " +
                 getDateFormatsUsage(),
                 ErrorCode.ILLEGAL_ARGUMENT);
        }

        /*
         * If the date parsed is in the distant past (i.e., in January 1970)
         * then the string lacked a year/month/day.  We'll be friendly and
         * interpret the time as being in the recent past, that is, today.
         */

        final Calendar rcal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        rcal.setTime(r);

        if (rcal.get(Calendar.YEAR) == 1970) {
            final Calendar nowCal = Calendar.getInstance();
            nowCal.setTime(new Date());

            rcal.set(nowCal.get(Calendar.YEAR),
                     nowCal.get(Calendar.MONTH),
                     nowCal.get(Calendar.DAY_OF_MONTH));

            /* If the resulting time is in the future, subtract one day. */

            if (rcal.after(nowCal)) {
                rcal.add(Calendar.DAY_OF_MONTH, -1);
            }
            r = rcal.getTime();
        }
        return r.getTime();
    }

    public synchronized static String getTimeStr(long timestamp) {
        final DateFormat f = dateFormats[0];

        return f.format(new Date(timestamp),
                        new StringBuffer(),
                        new FieldPosition(0)).toString();
    }
}
