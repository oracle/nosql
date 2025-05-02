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

package oracle.kv.hadoop.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Direction;
import oracle.kv.PasswordCredentials;
import oracle.kv.impl.topo.RepGroupId;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * Concrete implementation of the InputSplit interface required to perform
 * Hadoop MapReduce. A RecordReader will take instances of this class, where
 * each such instance corresponds to data stored in an Oracle NoSQL Database
 * store, and use those instances to retrieve that data when performing the
 * MapReduce job.
 *
 * @since 3.1
 */
public class TableInputSplit extends InputSplit implements Writable {

    /*
     * Use int instead of enum for query type; because of concerns about
     * how hadoop (split) serialization/deserialization handles enums.
     */
    public static final int QUERY_BY_PRIMARY_ALL_PARTITIONS = 0;
    public static final int QUERY_BY_PRIMARY_SINGLE_PARTITION = 1;
    public static final int QUERY_BY_INDEX = 2;
    public static final int QUERY_BY_ONQL_ALL_PARTITIONS = 3;
    public static final int QUERY_BY_ONQL_SINGLE_PARTITION = 4;
    public static final int QUERY_BY_ONQL_SHARDS = 5;

    public static final String EMPTY_STR = "";

    private int queryBy = QUERY_BY_PRIMARY_ALL_PARTITIONS;
    private String onqlWhereClause = EMPTY_STR;

    private String[] locations = new String[0];

    private String kvStore;
    private String[] kvHelperHosts;
    private String tableName;

    private String primaryKeyProperty;

    /* For MultiRowOptions */
    private String fieldRangeProperty;

    /* For TableIteratorOptions */
    private Direction direction;
    private Consistency consistency;
    private long timeout;
    private TimeUnit timeoutUnit;
    private int maxRequests;
    private int batchSize;
    private int maxBatches;

    private String loginFlnm = null;
    private PasswordCredentials pwdCredentials = null;
    private String trustFlnm = null;

    private List<Set<Integer>> partitionSets;
    private Set<RepGroupId> shardSet;

    /**
     * No-arg constructor required by Hadoop semantics.
     */
    public TableInputSplit() {
    }

    /**
     * Get the size of the split, so that the input splits can be sorted by
     * size.
     *
     * @return the number of bytes in the split
     */
    @Override
    public long getLength() {

        if (partitionSets == null) {
            return 0L;
        }
        return partitionSets.size();
    }

    /**
     * Get the list of nodes by name where the data for the split would be
     * local.  The locations do not need to be serialized.
     *
     * @return a new array of the node nodes.
     */
    @Override
    public String[] getLocations() {

        return locations;
    }

    TableInputSplit setLocations(String[] newLocations) {
        this.locations = newLocations;
        return this;
    }

    TableInputSplit setKVHelperHosts(String[] newHelperHosts) {
        this.kvHelperHosts = newHelperHosts;
        return this;
    }

    /**
     * Returns a String array with elements of the form,
     * '&lt;hostname:port&gt;'; where each elment specifies the connection
     * information for each of store's helper hosts.
     */
    public String[] getKVHelperHosts() {
        return kvHelperHosts;
    }

    TableInputSplit setKVStoreName(String newStoreName) {
        this.kvStore = newStoreName;
        return this;
    }

    /**
     * Returns the name of the store containing the records associated with
     * this split.
     */
    public String getKVStoreName() {
        return kvStore;
    }

    public String getTableName() {
        return tableName;
    }

    TableInputSplit setTableName(String newTableName) {
        this.tableName = newTableName;
        return this;
    }

    TableInputSplit setPrimaryKeyProperty(String newProperty) {
        this.primaryKeyProperty = newProperty;
        return this;
    }

    String getPrimaryKeyProperty() {
        return primaryKeyProperty;
    }

    /* For MultiRowOptions */

    TableInputSplit setFieldRangeProperty(String newProperty) {
        this.fieldRangeProperty = newProperty;
        return this;
    }

    String getFieldRangeProperty() {
        return fieldRangeProperty;
    }

    /* For TableIteratorOptions */

    TableInputSplit setDirection(Direction newDirection) {
        this.direction = newDirection;
        return this;
    }

    Direction getDirection() {
        return direction;
    }

    TableInputSplit setConsistency(Consistency newConsistency) {
        this.consistency = newConsistency;
        return this;
    }

    Consistency getConsistency() {
        return consistency;
    }

    TableInputSplit setTimeout(long newTimeout) {
        this.timeout = newTimeout;
        return this;
    }

    long getTimeout() {
        return timeout;
    }

    TableInputSplit setTimeoutUnit(TimeUnit newTimeoutUnit) {
        this.timeoutUnit = newTimeoutUnit;
        return this;
    }

    TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    TableInputSplit setMaxRequests(int newMaxRequests) {
        this.maxRequests = newMaxRequests;
        return this;
    }

    int getMaxRequests() {
        return maxRequests;
    }

    TableInputSplit setBatchSize(int newBatchSize) {
        this.batchSize = newBatchSize;
        return this;
    }

    int getBatchSize() {
        return batchSize;
    }

    TableInputSplit setMaxBatches(int newMaxBatches) {
        this.maxBatches = newMaxBatches;
        return this;
    }

    int getMaxBatches() {
        return maxBatches;
    }

    TableInputSplit setPartitionSets(List<Set<Integer>> newPartitionSets) {
        this.partitionSets = newPartitionSets;
        return this;
    }

    /**
     * Returns a <code>List</code> whose elements are <code>Set</code>s of
     * partitions; whose union is the set of all partitions in the store.
     */
    public List<Set<Integer>> getPartitionSets() {
        return partitionSets;
    }

    public TableInputSplit setQueryInfo(int newQueryBy,
                                        String whereClause) {
        this.queryBy = newQueryBy;
        this.onqlWhereClause = whereClause;
        return this;
    }

    public int getQueryBy() {
        return queryBy;
    }

    public String getWhereClause() {
        return onqlWhereClause;
    }

    TableInputSplit setShardSet(Set<RepGroupId> newShardSet) {
        this.shardSet = newShardSet;
        return this;
    }

    public Set<RepGroupId> getShardSet() {
        return shardSet;
    }

    TableInputSplit setKVStoreSecurity(
                        final String loginFile,
                        final PasswordCredentials passwordCredentials,
                        final String trustFile) {

        this.loginFlnm = loginFile;
        this.pwdCredentials = passwordCredentials;
        this.trustFlnm = trustFile;
        return this;
    }

    public String getSecurityLogin() {
        return loginFlnm;
    }

    public PasswordCredentials getSecurityCredentials() {
        return pwdCredentials;
    }

    public String getSecurityTrust() {
        return trustFlnm;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void write(DataOutput out)
        throws IOException {

        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; i++) {
            Text.writeString(out, locations[i]);
        }

        out.writeInt(kvHelperHosts.length);
        for (int i = 0; i < kvHelperHosts.length; i++) {
            Text.writeString(out, kvHelperHosts[i]);
        }
        Text.writeString(out, kvStore);
        Text.writeString(out, tableName);

        Text.writeString(out,
            (primaryKeyProperty == null ? EMPTY_STR : primaryKeyProperty));

        /* For MultiRowOptions */
        Text.writeString(out,
            (fieldRangeProperty == null ? EMPTY_STR : fieldRangeProperty));

        /* For TableIteratorOptions */
        Text.writeString(out, (direction == null ?
                               EMPTY_STR : direction.name()));
        writeBytes(out, (consistency == null ?
                         null :
                         consistency.toByteArray()));
        out.writeLong(timeout);
        Text.writeString(out, (timeoutUnit == null ?
                               EMPTY_STR : timeoutUnit.name()));
        out.writeInt(maxRequests);
        out.writeInt(batchSize);
        out.writeInt(maxBatches);

        out.writeInt(partitionSets.size());
        for (Set<Integer> partitions : partitionSets) {
            out.writeInt(partitions.size());
            for (Integer p : partitions) {
                out.writeInt(p);
            }
        }

        out.writeInt(queryBy);

        out.writeInt(shardSet.size());
        for (RepGroupId repGroupId : shardSet) {
            out.writeInt(repGroupId.getGroupId());
        }

        /* Serialize empty string rather than null to avoid NPE from Hadoop. */
        Text.writeString(out,
            (onqlWhereClause == null ? EMPTY_STR : onqlWhereClause));

        /*
         * Write the name of the login file, the user credentials (username
         * and password), and the name of the trust file. If name of the
         * login file is null, leave the others null too.
         */
        if (loginFlnm == null) {
            Text.writeString(out, EMPTY_STR); /* login file name */
            Text.writeString(out, EMPTY_STR); /* username */
            Text.writeString(out, EMPTY_STR); /* password */
            Text.writeString(out, EMPTY_STR); /* trust file name */

        } else {
            Text.writeString(out, loginFlnm);
            Text.writeString(out, pwdCredentials.getUsername());
            Text.writeString(
                out, String.copyValueOf(pwdCredentials.getPassword()));
            Text.writeString(out, trustFlnm);
        }
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     *
     * <p>For efficiency, implementations should attempt to re-use storage in
     * the existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void readFields(DataInput in)
        throws IOException {

        final int len = in.readInt();
        locations = new String[len];
        for (int i = 0; i < len; i++) {
            locations[i] = textReadString(in);
        }

        final int nHelperHosts = in.readInt();
        kvHelperHosts = new String[nHelperHosts];
        for (int i = 0; i < nHelperHosts; i++) {
            kvHelperHosts[i] = textReadString(in);
        }
        kvStore = textReadString(in);
        tableName = textReadString(in);

        primaryKeyProperty = textReadString(in);
        if (EMPTY_STR.equals(primaryKeyProperty)) {
            primaryKeyProperty = null;
        }

        /* For MultiRowOptions */

        fieldRangeProperty = textReadString(in);
        if (EMPTY_STR.equals(fieldRangeProperty)) {
            fieldRangeProperty = null;
        }

        /* For TableIteratorOptions */

        final String dirStr = textReadString(in);
        if (dirStr == null || EMPTY_STR.equals(dirStr)) {
            direction = Direction.UNORDERED;
        } else {
            direction = Direction.valueOf(dirStr);
        }

        /*
         * NOTE: in certain mocked test cases, the byte[] array returned
         *       by readBytes() can have a length of 1 with single value
         *       equal to 0. When this occurs, Consistency.fromByteArray
         *       will throw a NullPointerException wrapped in a FaultException.
         *       To deal with this situation, those unusual cases are now
         *       treated the same as when null is returned by readBytes().
         */
        final byte[] consBytes = readBytes(in);
        if (consBytes == null || consBytes.length == 0 ||
            (consBytes.length == 1 && consBytes[0] == 0)) {
            consistency = null;
        } else {
            consistency = Consistency.fromByteArray(consBytes);
        }

        timeout = in.readLong();

        final String tuStr = textReadString(in);
        if (tuStr == null || EMPTY_STR.equals(tuStr)) {
            timeoutUnit = null;
        } else {
            timeoutUnit = TimeUnit.valueOf(tuStr);
        }

        maxRequests = in.readInt();
        batchSize = in.readInt();
        maxBatches = in.readInt();

        int nSets = in.readInt();
        partitionSets = new ArrayList<Set<Integer>>(nSets);
        while (nSets > 0) {
            int nPartitions = in.readInt();
            final Set<Integer> partitions = new HashSet<Integer>(nPartitions);
            partitionSets.add(partitions);

            while (nPartitions > 0) {
                partitions.add(in.readInt());
                nPartitions--;
            }
            nSets--;
        }

        queryBy = in.readInt();

        final int nShards = in.readInt();
        shardSet = new HashSet<RepGroupId>(nShards);
        for (int i = 0; i < nShards; i++) {
            shardSet.add(new RepGroupId(in.readInt()));
        }

        /* Null where clause was serialized as empty string to avoid NPE. */
        onqlWhereClause = textReadString(in);
        if (EMPTY_STR.equals(onqlWhereClause)) {
            onqlWhereClause = null;
        }

        /*
         * Read the name of the login file, the user credentials (username
         * and password, and the name of the trust file.
         */
        loginFlnm = textReadString(in);
        if (EMPTY_STR.equals(loginFlnm)) {
            loginFlnm = null;
        }

        String userName = textReadString(in);
        if (EMPTY_STR.equals(userName)) {
            userName = null;
        }

        char[] userPassword = null;
        final String userPasswordStr = textReadString(in);
        if (userPasswordStr != null && !(EMPTY_STR.equals(userPasswordStr)) &&
            userName != null) {
            userPassword = userPasswordStr.toCharArray();
            pwdCredentials = new PasswordCredentials(userName, userPassword);
        }

        trustFlnm = textReadString(in);
        if (EMPTY_STR.equals(trustFlnm)) {
            trustFlnm = null;
        }
    }

    private void writeBytes(DataOutput out, byte[] bytes)
        throws IOException {

        if (bytes == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private byte[] readBytes(DataInput in)
        throws IOException {

        final int len = in.readInt();
        if (len == 0) {
            return null;
        }

        final byte[] ret = new byte[len];
        in.readFully(ret);
        return ret;
    }

    /*
     * A well-defined equals, hashCode, and toString method should be provided
     * so that instances of this class can be compared, uniquely identified,
     * and stored in collections.
     */

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TableInputSplit)) {
            return false;
        }

        if (obj == this) {
            return true;
        }

        final TableInputSplit obj1 = this;
        final TableInputSplit obj2 = (TableInputSplit) obj;

        final String obj2KvStoreName = obj2.getKVStoreName();
        if (obj1.kvStore == null || obj2KvStoreName == null) {
            return false;
        }

        if (!obj1.kvStore.equals(obj2KvStoreName)) {
            return false;
        }

        if (obj1.locations == null || obj2.getLocations() == null) {
            return false;
        }

        final List<String> obj1Locations = Arrays.asList(obj1.locations);
        final List<String> obj2Locations = Arrays.asList(obj2.getLocations());

        if (obj1Locations.size() != obj2Locations.size()) {
            return false;
        }

        if (!obj1Locations.containsAll(obj2Locations)) {
            return false;
        }

        if (obj1.kvHelperHosts == null || obj2.getKVHelperHosts() == null) {
            return false;
        }

        final List<String> obj1KvHelperHosts =
            Arrays.asList(obj1.kvHelperHosts);
        final List<String> obj2KvHelperHosts =
            Arrays.asList(obj2.getKVHelperHosts());

        if (obj1KvHelperHosts.size() != obj2KvHelperHosts.size()) {
            return false;
        }

        if (!obj1KvHelperHosts.containsAll(obj2KvHelperHosts)) {
            return false;
        }

        if (obj1.tableName != null) {
            if (!obj1.tableName.equals(obj2.getTableName())) {
                return false;
            }
        }

        if (obj1.primaryKeyProperty != null) {
            if (!obj1.primaryKeyProperty.equals(
                                             obj2.getPrimaryKeyProperty())) {
                return false;
            }
        }

        if (obj1.fieldRangeProperty != null) {
            if (!obj1.fieldRangeProperty.equals(
                                             obj2.getFieldRangeProperty())) {
                return false;
            }
        }

        if (obj1.queryBy != obj2.queryBy) {
            return false;
        }

        /* Compare shard sets for all iteration types. */
        if (obj1.shardSet != null) {
            if (!obj1.shardSet.containsAll(obj2.shardSet)) {
                return false;
            }
        } else {
            if (obj2.shardSet != null) {
                return false;
            }
        }

        /* Compare partition sets for all iteration types. */
        if (obj1.partitionSets != null) {
            if (!obj1.partitionSets.containsAll(obj2.partitionSets)) {
                return false;
            }
        } else {
            if (obj2.partitionSets != null) {
                return false;
            }
        }

        /* Compare predicates only if iteration is native based. */
        if (queryBy != QUERY_BY_PRIMARY_ALL_PARTITIONS &&
            queryBy != QUERY_BY_PRIMARY_SINGLE_PARTITION &&
            queryBy != QUERY_BY_INDEX) {

            if (obj1.onqlWhereClause != null) {
                if (!obj1.onqlWhereClause.equals(obj2.onqlWhereClause)) {
                    return false;
                }
            } else {
                if (obj2.onqlWhereClause != null) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public int hashCode() {

        final int pm = 37;
        int hc = 11;
        int hcSum = 0;

        if (kvStore != null) {
            hcSum = hcSum + kvStore.hashCode();
        }

        hcSum = hcSum + Arrays.hashCode(locations);

        hcSum = hcSum + Arrays.hashCode(kvHelperHosts);

        if (tableName != null) {
            hcSum = hcSum + tableName.hashCode();
        }

        if (primaryKeyProperty != null) {
            hcSum = hcSum + primaryKeyProperty.hashCode();
        }

        if (fieldRangeProperty != null) {
            hcSum = hcSum + fieldRangeProperty.hashCode();
        }

        hcSum += queryBy;

        if (partitionSets != null) {
            hcSum += partitionSets.hashCode();
        }

        if (shardSet != null) {
            hcSum += shardSet.hashCode();
        }

        /* Predicate contributes to hash only if iteration is native based. */
        if (queryBy != QUERY_BY_PRIMARY_ALL_PARTITIONS &&
            queryBy != QUERY_BY_PRIMARY_SINGLE_PARTITION &&
            queryBy != QUERY_BY_INDEX) {

            if (onqlWhereClause != null) {
                hcSum += onqlWhereClause.hashCode();
            }
        }

        hc = (pm * hc) + hcSum;
        return hc;
    }

    @Override
    public String toString() {
        final StringBuilder buf =
            new StringBuilder(this.getClass().getSimpleName());
        buf.append(": [store=");
        buf.append(kvStore);

        buf.append(", dataNodeLocations=");
        if (locations != null) {
            buf.append(Arrays.asList(locations));
        } else {
            buf.append("null");
        }

        buf.append(", kvStoreHosts=");
        if (kvHelperHosts != null) {
            buf.append(Arrays.asList(kvHelperHosts));
        } else {
            buf.append("null");
        }

        if (tableName == null) {
            buf.append(", tableName=NOT_SET_YET");
        } else {
            buf.append(", tableName=");
            buf.append(tableName);
        }

        if (primaryKeyProperty != null) {
            buf.append(", primaryKeyProperty=");
            buf.append(primaryKeyProperty);
        }

        if (fieldRangeProperty != null) {
            buf.append(", fieldRangeProperty=");
            buf.append(fieldRangeProperty);
        }

        if (queryBy == QUERY_BY_INDEX) {
            buf.append(", IndexKeyIteration");
        } else if (queryBy == QUERY_BY_PRIMARY_ALL_PARTITIONS) {
            buf.append(", PrimaryKeyIterationAllPartitions");
        } else if (queryBy == QUERY_BY_PRIMARY_SINGLE_PARTITION) {
            buf.append(", PrimaryKeyIterationSinglePartition");
        } else {
            if (queryBy == QUERY_BY_ONQL_ALL_PARTITIONS) {
                buf.append(", NativeIterationAllPartitions");
            } else if (queryBy == QUERY_BY_ONQL_SINGLE_PARTITION) {
                buf.append(", NativeIterationSinglePartition");
            } else if (queryBy == QUERY_BY_ONQL_SHARDS) {
                buf.append(", NativeIterationByShards");
            } else {
                buf.append(", NativeIterationDefault");
            }
            buf.append(" [predicate=" + onqlWhereClause + "]");
        }

        if (shardSet == null) {
            buf.append(", shardSet=null");
        } else {
            buf.append(", shardSet=");
            buf.append(shardSet);
        }

        if (partitionSets == null) {
            buf.append(", partitionSets=null");
        } else {
            /*
             * The list of sets of partition ids can get quite large. Only
             * display the whole list if its size is reasonably small;
             * otherwise, elide most of the elements.
             */
            final int maxListSize = 5;
            final int maxSetSize = 3;
            buf.append(", partitionSets=[");
            /* Display only the 1st maxListSize sets in the list. */
            int i = 0;
            for (Set<Integer> partitionSet : partitionSets) {
                if (i >= maxListSize) {
                    buf.append(", ...");
                    break;
                }
                if (i > 0) {
                    buf.append(", [");
                } else {
                    buf.append("[");
                }
                i++;

                bufPartitionIdSets(partitionSet, maxSetSize, buf);
                buf.append("]");
            }
            buf.append("]");
        }
        buf.append("]");

        return buf.toString();
    }

    /*
     * Convenience method that encapsulates functionality employed in toString
     * above. This method will append the partition ids contained in the given
     * partitionSet to the given StringBuilder buffer in a way appropriate for
     * logging or display. But if the number of elements in the given set
     * exceed the given maxSetSize, then only the first maxSetSize elements
     * will be appended to the buffer.
     */
    private void bufPartitionIdSets(Set<Integer> partitionSet, int maxSetSize,
                                    StringBuilder buf) {

        /* Display only 1st maxSetSize elements of the set. */
        int i = 0;
        for (Integer partitionId : partitionSet) {
            if (i >= maxSetSize) {
                buf.append(", ...");
                break;
            }
            if (i > 0) {
                buf.append(", " + partitionId);
            } else {
                buf.append(partitionId);
            }
            i++;
        }
    }

    /*
     * Convenience method that calls the readString method on the Hadoop
     * Text class, and returns the result with all whitespace removed.
     *
     * Whitespace is trimmed from the value returned by this method because
     * in certain test cases in which the DataInput is simulated/mocked,
     * Text.readString can return a string containing the single UTF-16
     * value corresponding to the byte value of 0, rather than an empty
     * string. For this case, if the whitespace that is returned by
     * Text.readString is not removed, then when that returned string is
     * compared to the EMPTY_STR defined in this method, the equals method
     * returns false; which is incorrect behavior. Thus, the trim() is
     * called to remove all whitespace from the value returned by
     * Text.readString.
     */
    private String textReadString(final DataInput in) throws IOException {
        return (in == null ? null : (Text.readString(in)).trim());
    }
}
