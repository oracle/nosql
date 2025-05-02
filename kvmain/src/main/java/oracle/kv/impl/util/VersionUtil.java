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

import oracle.kv.KVVersion;

/**
 * Version utility methods.
 *
 * @see oracle.kv.KVVersion
 */
public class VersionUtil {

    private static final String JAVA_VERSION_KEY = "java.version";

    /* Prevent construction */
    private VersionUtil() {}

    /**
     * Returns a KVVersion instance representing the minor version of the
     * specified version.
     */
    public static KVVersion getMinorVersion(KVVersion v) {
        /* Zero out the patch # */
        return new KVVersion(v.getOracleMajor(),
                             v.getOracleMinor(),
                             v.getMajor(),
                             v.getMinor(),
                             0, null);
    }

    /**
     * Compares the two specified version, ignoring the patch number.
     *
     * Returns -1 if v1 is an earlier minor version than v2.
     * Returns 0 if v1 is the same minor version as v2.
     * Returns 1 if v1 is a later minor version than v2..
     *
     * In the comparison all numeric fields except patch are compared.
     */
    public static int compareMinorVersion(KVVersion v1, KVVersion v2) {
        return getMinorVersion(v1).compareTo(getMinorVersion(v2));
    }

    /**
     * Checks whether the specified software version can be upgraded to the
     * current version and throws IllegalStateException if it cannot. The
     * previous version must be greater than or equal to the prerequisite and
     * must not be a newer minor version.
     *
     * @param previousVersion the previous software version to check
     * @param type the type of the previous version
     *
     * @throws IllegalStateException if the check fails
     */
    public static void checkUpgrade(KVVersion previousVersion, String type) {
        checkUpgrade(previousVersion,
                     KVVersion.PREREQUISITE_VERSION,
                     KVVersion.CURRENT_VERSION,
                     type);
    }

    /**
     * Checks whether the specified software version can be upgraded to the
     * specified target version and throws IllegalStateException if it cannot.
     * the previous version must be greater than or equal to the target's
     * prerequisite and must not be a newer minor version.
     *
     * @param previousVersion the previous software version to check
     * @param prerequisiteVersion the target's prerequisite
     * @param targetVersion the target version
     * @param type the type of the previous version
     */
    private static void checkUpgrade(KVVersion previousVersion,
                                     KVVersion prerequisiteVersion,
                                     KVVersion targetVersion,
                                     String type) {

        if (previousVersion.compareTo(prerequisiteVersion) < 0) {
            throw new IllegalStateException(
                    "The " + type + " software version " +
                    previousVersion.getNumericVersionString() +
                    " does not satisfy the prerequisite for " +
                    targetVersion.getNumericVersionString() +
                    " which requires version " +
                    prerequisiteVersion.getNumericVersionString() +
                    " or later.");
        }

        /*
         * Candidate is at least the prerequisite, check that it is not
         * a newer minor version.
         */
        if (compareMinorVersion(previousVersion, targetVersion) > 0) {
            throw new IllegalStateException(
                    "The " + type + " software version " +
                    previousVersion.getNumericVersionString() +
                    " is a newer minor version than " +
                    targetVersion.getNumericVersionString());
        }
    }

    /**
     * Returns true if the specified version is Enterprise Edition.
     */
    public static boolean isEnterpriseEdition(KVVersion version) {
        final String edition = version.getReleaseEdition();
        return (edition == null) ? false : "Enterprise".equals(edition);
    }

    /**
     * Returns the major version number of the current Java VM as an integer,
     * returning -1 if the version cannot be determined.  Handles four version
     * styles:
     * <ul>
     * <li> 1.8.0_151 returns 8 (for versions 1.8 and earlier)
     * <li> 9.0.4 returns 9 (for release versions 9 and later)
     * <li> 10-ea returns 10 (for early access versions)
     * <li> 17 returns 17 (for newer versions that seem to skip minor and patch
     *      components)
     * </ul>
     */
    public static int getJavaMajorVersion() {
        return getJavaMajorVersion(System.getProperty(JAVA_VERSION_KEY));
    }

    /** Provide version as an argument, for testing. */
    static int getJavaMajorVersion(String version) {
        if (version != null) {
            if (version.endsWith("-ea")) {

                /* <version>-ea */
                version = version.substring(0, version.length() - 3);
            }

            final int dot1 = version.indexOf('.');
            if (dot1 > 0) {
                final String v1 = version.substring(0, dot1);
                if ("1".equals(v1)) {

                    /* 1.major.xxx */
                    final int dot2 = version.indexOf('.', dot1 + 1);
                    if (dot2 > 0) {
                        final String v2 =
                            version.substring(dot1 + 1, dot2);
                        try {
                            return Integer.parseInt(v2);
                        } catch (NumberFormatException e) {
                        }
                    }
                } else {

                    /* major.xxx */
                    try {
                        return Integer.parseInt(v1);
                    } catch (NumberFormatException e) {
                    }
                }
            } else {
                /* major */
                try {
                    return Integer.parseInt(version);
                } catch (NumberFormatException e) {
                }
            }
        }

        /* Bad Java version */
        return -1;
    }
}
