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

package oracle.kv.pubsub;

/**
 * Object to represent a subscriber id in a subscription group. When user
 * creates a subscription group with multiple subscribers, each covering a
 * disjoint subset of shards, user need to specify the NoSQLSubscriberId
 * for each subscriber in that group.
 *
 * <p> In addition to the checkpoint table name, the index consists of two
 * parts, 1) the total number of subscribers in the group, and 2) the index
 * of the given subscriber. The index must be fall in the range between 0
 * inclusively, and the total number of subscribers exclusively. For example,
 * assume a subscription group with 3 subscribers, each covering a portion of
 * the shards in source store, the index of the 3 subscribers should be 0, 1
 * and 2, respectively.
 */
public class NoSQLSubscriberId {

    /* total number of subscribers in the group */
    private final int total;

    /* 0-based index of the subscriber in group in [0, total-1] inclusively  */
    private final int index;

    /* true if subscriber replaces a dead subscriber */
    private boolean replacement;

    /**
     * Constructs a NoSQLSubscriber Id, which identifies a subscriber in the
     * context of a subscription that is uniquely identified by the checkpoint
     * table.
     *
     * @param total       total number of members in the group
     * @param index       index of subscriber
     *
     * @throws IllegalArgumentException if total is than less or equal to 0, or
     * index is not in range between 0 inclusively and the total number of
     * subscribers exclusively, or the groupId is null or empty.
     */
    public NoSQLSubscriberId(int total, int index)
        throws IllegalArgumentException {

        if (total <= 0) {
            throw new IllegalArgumentException("Invalid total number (" +
                                               total + ") of subscribers in " +
                                               "group.");
        }

        if (index < 0 || index >= total) {
            throw new IllegalArgumentException("Out of bound index (" + index +
                                               ") in a group with " + total +
                                               " members.");
        }

        this.total = total;
        this.index = index;
        replacement = false;
    }

   /**
     * Gets total number of subscribers in the group.
     *
     * @return total number of subscribers in the group
     */
    public int getTotal() {
        return total;
    }

    /**
     * Gets subscriber index in the group.
     *
     * @return subscriber index in the group
     */
    public int getIndex() {
        return index;
    }

    /**
     * @hidden
     *
     * Used in test only, mark the subscriber a replacement.
     */
    public void setReplacement() {
        replacement = true;
    }

    /**
     * @hidden
     *
     * @return true if single agent group
     */
    public boolean isSingleAgentGroup() {
        return total == 1;
    }

    @Override
    public String toString() {
        return total + "_" + index + (replacement ? "_R" : "");
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + total;
        result = 31 * result + index;

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NoSQLSubscriberId)) {
            return false;
        }
        final NoSQLSubscriberId o = (NoSQLSubscriberId)obj;
        return getIndex() == o.getIndex() &&
               getTotal() == o.getTotal();
    }
}
