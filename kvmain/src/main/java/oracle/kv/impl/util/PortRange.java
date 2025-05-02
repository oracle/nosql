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

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Port ranges are expressed as a String property. Currently the format is
 * as follows:
 *
 *    firstPortNum,lastPortNum
 *
 * where the available ports are all those from {@literal first->last},
 * inclusive. Over time, we may want to make this more sophisticated, so that
 * one can express something other than a consecutive range.
 *
 * Services can share ports if they appear in the same JVM and their server
 * sockets are configured compatibly.  For non-secure server sockets, the only
 * configuration parameter is the backlog.  For secure sockets, there are
 * additional security parameters.  In particular, the trusted login service
 * uses SSL client authentication, which is incompatible with other secure
 * services.
 *
 * Ports are allocated in the following groups, with sharing only supported
 * within a group.  Within each group, ports can be shared if they have
 * compatible backlog and security configurations, unless otherwise indicated:
 *
 * StorageNode:
 * 1) The storage node admin service
 * 2) The storage node monitor service
 * 3) The storage node trusted login service if security is enabled.  This
 *    service always uses its own port if it is present.
 * 4) The bootstrap admin service.  This service uses its own port
 *    if security is enabled because it has different security parameters.
 *
 * RepNode:
 * 1) The request handler service
 * 2) The rep node admin service
 * 3) The rep node monitor service
 * 4) The rep node login service if security is enabled
 *
 * Management;
 * 1) The JMX or SNMP server port, if enabled
 *
 * Admin:
 * 1) The command service
 * 2) The login service if security is enabled
 * 3) Tracker listeners.  All three listeners share the same port, and do not
 *    share with other services if security is enabled.
 *
 * For the actual port computation, see
 * BootstrapParams.validateSufficientPorts.
 */
public class PortRange {

    /**
     * Denotes an unconstrained use of ports
     */
    public static final String UNCONSTRAINED = "0";

    /**
     * Ensure that the HA portRange string is correct.
     */
    public static void validateHA(String portRange)
        throws IllegalArgumentException {

        String problem = portRange +
            " is not valid; format should be [firstPort,secondPort]";
        StringTokenizer tokenizer = new StringTokenizer(portRange, ",");
        if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException(problem);
        }

        try {
            int firstPort = Integer.parseInt(tokenizer.nextToken());
            int secondPort = Integer.parseInt(tokenizer.nextToken());

            if ((firstPort < 1) || (secondPort < 1)) {
                throw new IllegalArgumentException(problem +
                    ", ports must be > 0");
            }

            if (firstPort > secondPort) {
                throw new IllegalArgumentException(problem +
                ", firstPort must be <= secondPort");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(problem +
                                               ", not a valid number " +
                                               e.getMessage());
        }
    }

    /**
     * Validate the free range associated with
     * @param portRange
     * @throws IllegalArgumentException
     */
    public static void validateService(String portRange)
        throws IllegalArgumentException {

        if (isUnconstrained(portRange)) {
            return;
        }

        validateHA(portRange);
    }

    /**
     * Predicate to determine whether the port range is constrained
     */
    public static boolean isUnconstrained(String portRange) {
        return UNCONSTRAINED.equals(portRange);
    }

    /**
     * Ensures that <code>portRange</code> does not overlap
     * <code>otherRange</code>
     */
    public static void validateDisjoint(String portRange,
                                        String otherRange) {

        if (isUnconstrained(portRange) || isUnconstrained(otherRange)) {
            return;
        }

        final List<Integer> r1 = getRange(portRange);
        final List<Integer> r2 = getRange(otherRange);

        if ((r1.get(0) < r2.get(0)) && (r1.get(1) < r2.get(0))) {
            return;
        }

        if ((r2.get(0) < r1.get(0)) && (r2.get(1) < r1.get(0))) {
            return;
        }

        throw new IllegalArgumentException("Overlapping port ranges: " +
                                           portRange + " " + otherRange);
    }

    /**
     * Return the number of port numbers in the specified range, inclusive of
     * the range endpoints.
     *
     * @param portRange a comma-separated pair of point numbers or the
     * canonical {@link PortRange#UNCONSTRAINED} value.
     *
     * @return the size of the range
     *
     * @throws IllegalArgumentException if the port range string is not either
     * the canonical {@link PortRange#UNCONSTRAINED} value or a comma-separated
     * pair of tokens.
     * @throws NumberFormatException if either tokens in the range are not in
     * valid integer format
     */
    public static int rangeSize(String portRange) {
        if (isUnconstrained(portRange)) {
            return Integer.MAX_VALUE;
        }

        final List<Integer> r = getRange(portRange);
        return r.get(1) - r.get(0) + 1;
    }

    /**
     * Returns an integer list doublet representing the end points of the
     * port range.
     *
     * @throws IllegalArgumentException if the port range string is not a
     *  comma separated pair of tokens
     * @throws NumberFormatException if either tokens in the range are not
     *  in valid integer format
     */
    public static List<Integer> getRange(String portRange) {

        if (isUnconstrained(portRange)) {
            throw new IllegalArgumentException("Unconstrained port range");
        }

        final StringTokenizer tokenizer = new StringTokenizer(portRange, ",");

        if (tokenizer.countTokens() != 2) {
            throw new IllegalArgumentException
                (portRange + " is not a valid port range expression");
        }

        return Arrays.asList(Integer.parseInt(tokenizer.nextToken()),
                             Integer.parseInt(tokenizer.nextToken()));
    }

    /**
     * @param portRange
     * @param port
     * @return true if port is within the portRange.
     */
    public static boolean contains(String portRange, int port) {
        List<Integer> range = getRange(portRange);
        return (range.get(0) <= port) && (range.get(1) >= port);
    }
}

