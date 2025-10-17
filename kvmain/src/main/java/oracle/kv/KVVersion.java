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

package oracle.kv;

import java.io.InputStream;
import java.io.IOException;
import java.io.Serializable;

import java.util.Properties;

/**
 * Oracle NoSQL DB version information.  Versions consist of major, minor and
 * patch numbers.
 *
 * <p>There is one KVVersion object per running JVM and it may be accessed
 * using the static field {@link #CURRENT_VERSION}.
 */
public class KVVersion implements Comparable<KVVersion>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The highest Oracle major version number where Oracle major and minor
     * numbers are different from the regular ones.  Starting in 2018, all
     * Oracle products switched to a release numbering convention where the
     * major version is the last two digits of the year, and the minor number
     * represents the release within that year, in our case as a sequential
     * number incremented for each release.  Going forward, the Oracle and
     * regular major and minor numbers should be the same.
     */
    private static final int HIGHEST_INDEPENDENT_ORACLE_MAJOR = 12;

    /*
     * Create a hidden field for each major and minor release, marking
     * unsupported ones deprecated. Update the patch version of the value for
     * each build of the initial major or minor release, but don't include the
     * patch number in the field name, so that they can be used for
     * prerequisite checks in the code. Don't change the field values for patch
     * releases.
     */

    /* Unsupported versions */

    /** @hidden */
    @Deprecated
    public static final KVVersion R1_2_123 =
        new KVVersion(11, 2, 1, 2, 123, null);  /* R1.2.123 12/2011 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R2_0_23 =
        new KVVersion(11, 2, 2, 0, 23, null);   /* R2.0 10/2012 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R2_1_0 =
        new KVVersion(12, 1, 2, 1, 0, null);    /* Initial R2.1 version */
    /** @hidden */
    @Deprecated
    public static final KVVersion R2_1 =
        new KVVersion(12, 1, 2, 1, 8, null);    /* R2.1 7/2013 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_0 =
        new KVVersion(12, 1, 3, 0, 5, null);    /* R3 4/2013 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_1 =
        new KVVersion(12, 1, 3, 1, 0, null);    /* R3.1 9/2014 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_2 =
        new KVVersion(12, 1, 3, 2, 0, null);    /* R3.2 11/2014 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_3 =
        new KVVersion(12, 1, 3, 3, 4, null);    /* R3.3 3/2015 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_4 =
        new KVVersion(12, 1, 3, 4, 7, null);    /* R3.4 7/2015 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R3_5 =
        new KVVersion(12, 1, 3, 5, 2, null);    /* R3.5 11/2015 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_0 =
        new KVVersion(12, 1, 4, 0, 9, null);    /*
                                                 * R4.0 3/2016,
                                                 * prerequisite: 3.0
                                                 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_1 =
        new KVVersion(12, 1, 4, 1, 7, null);    /* R4.1 8/2016 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_2 =
        new KVVersion(12, 1, 4, 2, 14, null);   /* R4.2 12/2016 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_3 =
        new KVVersion(12, 1, 4, 3, 11, null);   /* R4.3 2/2017 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_4 =
        new KVVersion(12, 2, 4, 4, 6, null);    /* R4.4 4/2017 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R4_5 =
        new KVVersion(12, 2, 4, 5, 8, null);    /* R4.5 8/2017 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R18_1 =
        new KVVersion(18, 1, 5, null);   /* R18.1 3/2018 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R18_2 =
        new KVVersion(18, 2, 0, null);   /* R18.2 ?/2018 (Cloud-only) */
    /** @hidden */
    @Deprecated
    public static final KVVersion R18_3 =
        new KVVersion(18, 3, 9, null);   /* R18.3 11/2018 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R19_1 =
        new KVVersion(19, 1, 8, null);   /* R19.1 4/2019, prerequisite: 4.0 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R19_2 =
        new KVVersion(19, 2, 2, null);   /* R19.2 ?/2019 (Cloud-only) */
    /** @hidden */
    @Deprecated
    public static final KVVersion R19_3 =
        new KVVersion(19, 3, 2, null);   /* R19.3 8/2019 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R19_5 =
        new KVVersion(19, 5, 13, null);   /* R19.5 12/2019 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R20_1 =
        new KVVersion(20, 1, 12, null);   /* R20.1 4/2020, prerequisite: 18.1 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R20_2 =
        new KVVersion(20, 2, 15, null);   /* R20.2 8/2020 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R20_3 =
        new KVVersion(20, 3, 11, null);   /* R20.3 11/2020 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R21_1 =
        new KVVersion(21, 1, 8, null);   /* R21.1 3/2021, prerequisite: 19.1 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R21_2 =
        new KVVersion(21, 2, 15, null);   /* R21.2 8/2021 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R21_3 =
        new KVVersion(21, 3, 8, null);   /* R21.3 (not released) */
    /** @hidden */
    @Deprecated
    public static final KVVersion R22_1 =
        new KVVersion(22, 1, 12, null);   /* R22.1 4/2022, prerequisite: 20.1 */
    /** @hidden */
    @Deprecated
    public static final KVVersion R22_2 =
        new KVVersion(22, 2, 10, null);   /* R22.2 8/2022 */

    /* Supported versions */

    /** @hidden */
    public static final KVVersion R22_3 =
        new KVVersion(22, 3, 11, null);   /* R22.3 11/2022 */
    /** @hidden */
    public static final KVVersion R22_4 =
        new KVVersion(22, 4, 6, null);   /* R22.4 (for cloud MR tables) */
    /** @hidden */
    public static final KVVersion R23_1 =
        new KVVersion(23, 1, 18, null);   /* R23.1 4/2023 */
    /** @hidden */
    public static final KVVersion R23_3 =
        new KVVersion(23, 3, 26, null);   /* R23.3 11/2023 */
    /** @hidden */
    public static final KVVersion R24_1 =
        new KVVersion(24, 1, 12, null);   /* R24.1 4/2024 */
    /** @hidden */
    public static final KVVersion R24_2 =
        new KVVersion(24, 2, 0, null);   /* R24.2 (for cloud) */
    /** @hidden */
    public static final KVVersion R24_3 =
        new KVVersion(24, 3, 5, null);   /* R24.3 8/2024 */
    /** @hidden */
    public static final KVVersion R24_4 =
        new KVVersion(24, 4, 4, null);   /* R24.4 11/2024 */
    /** @hidden */
    public static final KVVersion R25_1 =
        new KVVersion(25, 1, 10, null);   /* R25.1 4/2025, prerequisite: 22.3 */
    /** @hidden */
    public static final KVVersion R25_2 =
        new KVVersion(25, 2, 0, null);   /* R25.2 (for cloud) prerequisite: 22.3 */
    /** @hidden */
    public static final KVVersion R25_3 =
        new KVVersion(25, 3, 21, null);   /* R25.3 8/2025 */

    /**
     * The current software version.
     */
    public static final KVVersion CURRENT_VERSION =
        /*
         * WHEN YOU BUMP THIS VERSION, BE SURE TO BUMP THE VERSIONS IN
         * misc/rpm/*.spec.
         */
        R25_3;

   /**
    * The current prerequisite version.  Nodes can only join the cluster if
    * they are running at least this version of the software.
    *
    * NOTE: As of 25.1 the prerequisite *should* be 23.1 but in order to make
    * cloud upgrades to 25.x the prerequisite is 22.3 (the last public release
    * before 22.4 which is the cloud version as of Jan 2025). This is a
    * temporary departure from the convention that the prerequisite version is
    * a "XX.1" version.
    *
    * See note in SerialVersion.java about how to manage prerequisite version,
    * compatibility, and dead code that occurs when the prerequisite moves.
    */
    public static final KVVersion PREREQUISITE_VERSION = R22_3;

    private final int oracleMajor;
    private final int oracleMinor;
    private final int majorNum;
    private final int minorNum;
    private final int patchNum;
    private String releaseId = null;
    private String releaseDate = null;
    private String releaseEdition = null;
    private final String name;
    private Properties versionProps;

    public static void main(String argv[]) {
        System.out.println(CURRENT_VERSION);
    }

    public KVVersion(int majorNum,
                     int minorNum,
                     int patchNum,
                     String name) {
        this(majorNum, minorNum, majorNum, minorNum, patchNum, name);
    }

    public KVVersion(int oracleMajor,
                     int oracleMinor,
                     int majorNum,
                     int minorNum,
                     int patchNum,
                     String name) {
        this.oracleMajor = oracleMajor;
        this.oracleMinor = oracleMinor;
        this.majorNum = majorNum;
        this.minorNum = minorNum;
        this.patchNum = patchNum;
        this.name = name;
    }

    @Override
    public String toString() {
        return getVersionString();
    }

    /**
     * Oracle Major number of the release version.
     *
     * @return The Oracle major number of the release version.
     */
    public int getOracleMajor() {
        return oracleMajor;
    }

    /**
     * Oracle Minor number of the release version.
     *
     * @return The Oracle minor number of the release version.
     */
    public int getOracleMinor() {
        return oracleMinor;
    }

    /**
     * Major number of the release version.
     *
     * @return The major number of the release version.
     */
    public int getMajor() {
        return majorNum;
    }

    /**
     * Minor number of the release version.
     *
     * @return The minor number of the release version.
     */
    public int getMinor() {
        return minorNum;
    }

    /**
     * Patch number of the release version.
     *
     * @return The patch number of the release version.
     */
    public int getPatch() {
        return patchNum;
    }

    /**
     * Returns the internal release ID for the release version, or null if not
     * known.
     *
     * @return the release ID or null
     */
    public String getReleaseId() {
        initVersionProps();
        return releaseId;
    }

    /**
     * Returns the release date for the release version, or null if not
     * known.
     *
     * @return the release date or null
     */
    public String getReleaseDate() {
        initVersionProps();
        return releaseDate;
    }

    /**
     * Returns the name of the edition of the release version, or null if not
     * known.
     *
     * @return the release edition or null
     */
    public String getReleaseEdition() {
        initVersionProps();
        return releaseEdition;
    }

    private synchronized void initVersionProps() {
        if (versionProps != null) {
            return;
        }

        final InputStream releaseProps =
            KVVersion.class.getResourceAsStream("/version/build.properties");
        if (releaseProps == null) {
            return;
        }

        versionProps = new Properties();
        try {
            versionProps.load(releaseProps);
        } catch (IOException IOE) {
            throw new IllegalStateException(IOE);
        }
        releaseId = versionProps.getProperty("release.id");
        releaseDate = versionProps.getProperty("release.date");
        releaseEdition = versionProps.getProperty("release.edition");
    }

    /**
     * The numeric version string, without the patch tag.
     *
     * @return The release version
     */
    public String getNumericVersionString() {
        StringBuilder version = new StringBuilder();
        if (oracleMajor <= HIGHEST_INDEPENDENT_ORACLE_MAJOR) {
            version.append(oracleMajor).append(".");
            version.append(oracleMinor).append(".");
        }
        version.append(majorNum).append(".");
        version.append(minorNum).append(".");
        version.append(patchNum);
        return version.toString();
    }

    /**
     * Release version, suitable for display.
     *
     * @return The release version, suitable for display.
     */
    public String getVersionString() {
        initVersionProps();
        StringBuilder version = new StringBuilder();
        if (oracleMajor <= HIGHEST_INDEPENDENT_ORACLE_MAJOR) {
            version.append(oracleMajor);
            version.append((oracleMajor == 12 ? "cR" : "gR"));
            version.append(oracleMinor).append(".");
        }
        version.append(majorNum).append(".");
        version.append(minorNum).append(".");
        version.append(patchNum);
        if (name != null) {
            version.append(" (");
            version.append(name);
            version.append(")");
        }
        if (releaseId != null) {
            version.append(" ").append(releaseDate).append(" ");
            version.append(" Build id: ").append(releaseId);
        }
        if (releaseEdition != null) {
            version.append(" Edition: ").append(releaseEdition);
        }
        return version.toString();
    }

    /**
     * Returns a KVVersion object representing the specified version string
     * without the release ID, release date,and name parts filled in. This
     * method is basically the inverse of getNumericVersionString(). This
     * method will also parse a full version string (returned from toString())
     * but only the numeric version portion of the string.
     *
     * @param versionString version string to parse
     * @return a KVVersion object
     */
    public static KVVersion parseVersion(String versionString) {

        /*
         * The full verion string will have spaces after the numeric portion of
         * version.
         */
        final String[] tokens = versionString.split(" ");

        /*
         * Full versions before 18.1 will have "cR" or "gR" in the numeric
         * portion of the string. So we convert it to a numeric version
         * (replace the "cR"/"gR" with ".").
         */
        final String numericString = tokens[0].replaceAll("[cg]R", ".");

        final String[] numericTokens = numericString.split("\\.");

        try {
            if (numericTokens.length > 0) {
                final int major = Integer.parseInt(numericTokens[0]);
                if (major <= HIGHEST_INDEPENDENT_ORACLE_MAJOR) {
                    if (numericTokens.length == 5) {
                        return new KVVersion(
                            major,
                            Integer.parseInt(numericTokens[1]),
                            Integer.parseInt(numericTokens[2]),
                            Integer.parseInt(numericTokens[3]),
                            Integer.parseInt(numericTokens[4]),
                            null);
                    }
                } else if (numericTokens.length == 3) {
                    final int minor = Integer.parseInt(numericTokens[1]);
                    return new KVVersion(major, minor, major, minor,
                                         Integer.parseInt(numericTokens[2]),
                                         null);
                }
            }
            throw new IllegalArgumentException
                ("Invalid version string: " + versionString);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException
                ("Invalid version string: " + versionString, nfe);
        }
    }

    /*
     * For testing.
     *
     * @hidden For internal use only
     */
    public void setReleaseId(String releaseId) {
        initVersionProps();
        this.releaseId = releaseId;
    }

    /*
     * Return -1 if the current version is earlier than the comparedVersion.
     * Return 0 if the current version is the same as the comparedVersion.
     * Return 1 if the current version is later than the comparedVersion.
     */
    @Override
    public int compareTo(KVVersion comparedVersion) {
        int result = 0;

        if (oracleMajor == comparedVersion.getOracleMajor()) {
            if (oracleMinor == comparedVersion.getOracleMinor()) {
                if (majorNum == comparedVersion.getMajor()) {
                    if (minorNum == comparedVersion.getMinor()) {
                        if (patchNum > comparedVersion.getPatch()) {
                            result = 1;
                        } else if (patchNum < comparedVersion.getPatch()) {
                            result = -1;
                        }
                    } else if (minorNum > comparedVersion.getMinor()) {
                        result = 1;
                    } else {
                        result = -1;
                    }
                } else if (majorNum > comparedVersion.getMajor()) {
                    result = 1;
                } else {
                    result = -1;
                }
            } else if (oracleMinor > comparedVersion.getOracleMinor()) {
                result = 1;
            } else {
                result = -1;
            }
        } else if (oracleMajor > comparedVersion.getOracleMajor()) {
            result = 1;
        } else {
            result = -1;
        }

        return result;
    }

    /*
     * If its type is KVVersion, and the version numbers are the same,
     * then we consider these two versions equal.
     */
    @Override
    public boolean equals(Object o) {
        return (o instanceof KVVersion) && (compareTo((KVVersion) o) == 0);
    }

    /* Produce a unique hash code for KVVersion. */
    @Override
    public int hashCode() {
        return majorNum * 1000 * 1000 + minorNum * 1000 + patchNum;
    }
}
