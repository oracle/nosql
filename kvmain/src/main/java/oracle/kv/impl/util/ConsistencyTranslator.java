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

import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Consistency.Time;
import oracle.kv.Consistency.Version;

import com.sleepycat.je.CommitToken;
import com.sleepycat.je.ReplicaConsistencyPolicy;
import com.sleepycat.je.rep.AbsoluteConsistencyPolicy;
import com.sleepycat.je.rep.CommitPointConsistencyPolicy;
import com.sleepycat.je.rep.NoConsistencyRequiredPolicy;
import com.sleepycat.je.rep.TimeConsistencyPolicy;

/**
 * Translates between HA and KV consistency.
 */
public final class ConsistencyTranslator {

    /**
     * Private constructor to satisfy CheckStyle and to prevent
     * instantiation of this utility class.
     */
    private ConsistencyTranslator() {
        throw new AssertionError("Instantiated utility class " +
            ConsistencyTranslator.class);
    }

    /**
     * Maps a KV consistency to a HA consistency, without restricting
     * timeouts. The inverse of the method below. Used in tests only, so
     * always run with auth master enabled.
     */
    public static ReplicaConsistencyPolicy translate(
        Consistency consistency) {

        return translate(false, consistency, Integer.MAX_VALUE);
    }

    /**
     * Maps a KV consistency to a HA consistency and supplies a maximum timeout
     * to apply as part of the translation.
     */
    @SuppressWarnings("deprecation")
    public static ReplicaConsistencyPolicy translate(boolean disableAuthMaster,
                                                     Consistency consistency,
                                                     long maxTimeoutMs) {

        if (Consistency.ABSOLUTE.equals(consistency)) {
            return disableAuthMaster ?
                NoConsistencyRequiredPolicy.NO_CONSISTENCY :
                new AbsoluteConsistencyPolicy(maxTimeoutMs,
                                              TimeUnit.MILLISECONDS);
        } else if (Consistency.NONE_REQUIRED.equals(consistency) ||
            Consistency.NONE_REQUIRED_NO_MASTER.equals(consistency)) {

            return NoConsistencyRequiredPolicy.NO_CONSISTENCY;
        } else if (consistency instanceof Time) {

            final Time c = (Time) consistency;
            return new TimeConsistencyPolicy(
                c.getPermissibleLag(TimeUnit.MICROSECONDS),
                TimeUnit.MICROSECONDS,
                Math.min(c.getTimeout(TimeUnit.MILLISECONDS), maxTimeoutMs),
                TimeUnit.MILLISECONDS);
        } else if (consistency instanceof Consistency.Version) {
            final Consistency.Version c = (Version) consistency;
            final CommitToken commitToken =
                new CommitToken(c.getVersion().getRepGroupUUID(),
                                c.getVersion().getVLSN());
            return new CommitPointConsistencyPolicy(
                commitToken,
                Math.min(c.getTimeout(TimeUnit.MILLISECONDS), maxTimeoutMs),
                TimeUnit.MILLISECONDS);
        } else {
            throw new UnsupportedOperationException("unknown consistency: " +
                                                    consistency);
        }
    }

    /**
     * Maps a HA consistency to a KV consistency. The inverse of the
     * above method.
     */
    public static Consistency translate(ReplicaConsistencyPolicy consistency) {
        return translate(consistency, null);
    }

    /**
     * Returns the <code>oracle.kv.Consistency</code> that corresponds
     * to the given
     * <code>com.sleepycat.je.ReplicaConsistencyPolicy</code>. Note
     * that for some values of <code>ReplicaConsistencyPolicy</code>,
     * there is more than one corresponding KV
     * <code>Consistency</code>. For those cases, the caller must
     * supply the KV <code>Consistency</code> to return; where if
     * <code>null</code> is supplied, a default mapping is used to
     * determine the value to return.
     *
     * @param consistency the HA consistency from which the KV
     * consistency should be determined.
     *
     * @param kvConsistency KV <code>Consistency</code> value that
     * should be supplied when the HA consistency that is input has
     * more than one corresponding KV consistency value. If the HA
     * consistency value that is input corresponds to only one KV
     * consistency, then this parameter will be ignored. Alternatively,
     * if the HA consistency value that is input corresponds to more
     * than one KV consistency, then the following criteria is used to
     * determine the return value:
     *
     * <ul>
     * <li>If <code>null</code> is input for this parameter, a default
     * mapping is used to determine the return value.</li>
     * <li>If the value input for this parameter does not map to the given HA
     * consistency, then an <code>UnsupportedOperationException</code>
     * is thrown.</li>
     * <li>In all other cases, the value input for this parameter is
     * returned.</li>
     * </ul>
     *
     * @return the instance of <code>oracle.kv.Consistency</code> that
     * corresponds to the value of the <code>consistency</code>
     * parameter; or the value input for the
     * <code>kvConsistency</code> parameter if the criteria described
     * above is satisfied.
     */
    @SuppressWarnings("deprecation")
    public static Consistency translate(ReplicaConsistencyPolicy consistency,
                                        Consistency kvConsistency) {

        if (NoConsistencyRequiredPolicy.NO_CONSISTENCY.equals(consistency)) {
            if (kvConsistency == null) {
                return Consistency.NONE_REQUIRED;
            } else if (!(Consistency.NONE_REQUIRED).equals(kvConsistency) &&
                !(Consistency.NONE_REQUIRED_NO_MASTER).equals(kvConsistency)) {
                    throw new UnsupportedOperationException
                        ("invalid consistency [KV consistency=" +
                         kvConsistency + ", HA consistency=" +
                         consistency + "]");
            }
            return kvConsistency;
        } else if (consistency instanceof TimeConsistencyPolicy) {
            final TimeConsistencyPolicy c =
                (TimeConsistencyPolicy) consistency;
            return new Consistency.Time
                (c.getPermissibleLag(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS,
                 c.getTimeout(TimeUnit.MICROSECONDS),
                 TimeUnit.MICROSECONDS);
        } else if (consistency instanceof CommitPointConsistencyPolicy) {
            final CommitPointConsistencyPolicy c =
                (CommitPointConsistencyPolicy) consistency;
            final CommitToken commitToken = c.getCommitToken();
            final oracle.kv.Version version =
                new oracle.kv.Version(commitToken.getRepenvUUID(),
                                             commitToken.getVLSN());
            return new Version
                       (version,
                        c.getTimeout(TimeUnit.MICROSECONDS),
                        TimeUnit.MICROSECONDS);
        } else {
            throw new UnsupportedOperationException
                ("unknown consistency: " + consistency);
        }
    }
}
