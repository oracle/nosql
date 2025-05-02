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

package oracle.kv.impl.util;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.ParamConstant;
import oracle.kv.impl.param.Parameter;
import oracle.kv.impl.param.ParameterMap;

import com.sleepycat.je.utilint.PropUtil;

/**
 * Utility class for different external data sources.
 */
public final class ExternalDataSourceUtils {

    /* Check that all parameters in the arguments map are known. */
    public static void checkParams(final ParameterMap params) {
        final Map<String, ParamConstant> allParams =
            ParamConstant.getAllParams();
        for (Parameter p : params) {
            if (!allParams.containsKey(p.getName())) {
                throw new IllegalArgumentException
                    (p.getName() + " is not a recognized parameter.");
            }
        }
    }

    /*
     * Convert a Consistency specification into an oracle.kv.Consistency
     * instance.
     */
    @SuppressWarnings("deprecation")
    public static Consistency parseConsistency(final String consistencyString) {
        Consistency consistency = null;
        if ("ABSOLUTE".equalsIgnoreCase(consistencyString)) {
            consistency = Consistency.ABSOLUTE;
        } else if ("NONE_REQUIRED_NO_MASTER".equalsIgnoreCase(
            consistencyString)) {
            consistency = Consistency.NONE_REQUIRED_NO_MASTER;
        } else if ("NONE_REQUIRED".equalsIgnoreCase(consistencyString)) {
            consistency = Consistency.NONE_REQUIRED;
        } else if ("TIME".regionMatches
                   (true, 0, consistencyString, 0, 4)) {
            final String consistencyParamName =
                ParamConstant.CONSISTENCY.getName();
            final int firstParenIdx = consistencyString.indexOf("(");
            final int lastParenIdx = consistencyString.indexOf(")");
            if (firstParenIdx < 0 || lastParenIdx < 0 ||
                lastParenIdx < firstParenIdx) {
                throw new ExternalDataSourceException
                    (consistencyParamName + " value of " +
                     consistencyString + " is formatted incorrectly.");
            }
            final String timeParam =
                consistencyString.substring(firstParenIdx + 1, lastParenIdx);
            final String[] lagAndTimeout = timeParam.split(",");
            if (lagAndTimeout.length != 2) {
                throw new ExternalDataSourceException
                    (consistencyParamName + " value of " +
                     consistencyString + " is formatted incorrectly.");
            }

            try {
            final long permissibleLagMSecs =
                PropUtil.parseDuration(lagAndTimeout[0].trim());
            final long timeoutMSecs =
                PropUtil.parseDuration(lagAndTimeout[1].trim());
            consistency =
                new Consistency.Time(permissibleLagMSecs,
                                     TimeUnit.MILLISECONDS,
                                     timeoutMSecs,
                                     TimeUnit.MILLISECONDS);
            } catch (IllegalArgumentException IAE) {
                throw new ExternalDataSourceException
                    (consistencyParamName + " value of " +
                     consistencyString + " is formatted incorrectly.");
            }
        } else {
            throw new ExternalDataSourceException
                ("Unknown consistency specified: " + consistencyString);
        }

        return consistency;
    }

    /* Convert a time + optional timeunit to milliseconds. */
    public static int parseTimeout(final String timeoutValue) {
        return PropUtil.parseDuration(timeoutValue);
    }
}
