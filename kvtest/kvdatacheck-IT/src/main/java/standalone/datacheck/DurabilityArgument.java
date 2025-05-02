/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities to parse and print durability values for command line tests. <p>
 *
 * Supported values:
 * <ul>
 * <li> {@code COMMIT_NO_SYNC} - for {@link Durability#COMMIT_NO_SYNC}
 * <li> {@code COMMIT_SYNC} - for {@link Durability#COMMIT_SYNC}
 * <li> {@code COMMIT_WRITE_NO_SYNC} - for {@link
 *      Durability#COMMIT_WRITE_NO_SYNC}
 * <li> {@code
 *      masterSync=<i>sync</i>,replicaSync=<i>sync</i>,replicaAck=<i>ack</i>} -
 *      for an instance of {@link Durability} with the specified {@code
 *      masterSync}, {@code replicaSync}, and {@code replicaAck} values.  The
 *      <i>sync</i> values should be one of {@code NO_SYNC}, {@code SYNC}, or
 *      {@code WRITE_NO_SYNC}.  The <i>ack</i> values should be one of
 *      {@code ALL}, {@code NONE}, or {@code SIMPLE_MAJORITY}.
 * </ul> <p>
 *
 * Parsing is case insensitive.
 */
public final class DurabilityArgument {

    /** Regular expression pattern for specifying Durability. */
    private static final Pattern DURABILITY_PATTERN =
        Pattern.compile("masterSync=([a-z_]+)" +
                        ",replicaSync=([a-z_]+)" +
                        ",replicaAck=([a-z_]+)",
                        Pattern.CASE_INSENSITIVE);

    /** This class should not be instantiated. */
    private DurabilityArgument() { }

    /**
     * Parses a durability value.
     *
     * @param string the input string
     * @return the durability value
     * @throws IllegalArgumentException if the input string is not recognized
     */
    public static Durability parseDurability(String string) {
        if (string == null) {
            throw new IllegalArgumentException("The argument must not be null");
        } else if (string.equalsIgnoreCase("COMMIT_NO_SYNC")) {
            return Durability.COMMIT_NO_SYNC;
        } else if (string.equalsIgnoreCase("COMMIT_SYNC")) {
            return Durability.COMMIT_SYNC;
        } else if (string.equalsIgnoreCase("COMMIT_WRITE_NO_SYNC")) {
            return Durability.COMMIT_WRITE_NO_SYNC;
        }
        Matcher matcher = DURABILITY_PATTERN.matcher(string);
        if (!matcher.matches()) {
            throw new IllegalArgumentException
                ("Unrecognized durability: " + string);
        }
        SyncPolicy masterSync =
            SyncPolicy.valueOf(matcher.group(1).toUpperCase());
        SyncPolicy replicaSync =
            SyncPolicy.valueOf(matcher.group(2).toUpperCase());
        ReplicaAckPolicy replicaAck =
            ReplicaAckPolicy.valueOf(matcher.group(3).toUpperCase());
        return new Durability(masterSync, replicaSync, replicaAck);
    }

    /**
     * Returns a string for a durability value in a format understood by {@link
     * #parseDurability}.
     *
     * @param durability the durability value
     * @return the string
     */
    public static String toString(Durability durability) {
        if (durability == null) {
            throw new IllegalArgumentException("The argument must not be null");
        } else if (durability == Durability.COMMIT_NO_SYNC) {
            return "COMMIT_NO_SYNC";
        } else if (durability == Durability.COMMIT_SYNC) {
            return "COMMIT_SYNC";
        } else if (durability == Durability.COMMIT_WRITE_NO_SYNC) {
            return "COMMIT_WRITE_NO_SYNC";
        } else {
            return "masterSync=" + durability.getMasterSync() +
                ",replicaSync=" + durability.getReplicaSync() +
                ",replicaAck=" + durability.getReplicaAck();
        }
    }
}
