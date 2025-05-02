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

package oracle.kv.impl.rep.subscription.partreader;

import java.io.Serializable;
import java.util.Date;

import oracle.kv.impl.topo.PartitionId;

/**
 * Object for reporting status of a partition reader.
 */
public class PartitionReaderStatus implements Serializable {
    private static final long serialVersionUID = 1L;

    private final PartitionId partitionId;
    private PartitionRepState state;

    /* start and end timing of transferring the partition */
    private long startTime;
    private long endTime;
    /* number of bytes read from source */
    private long numBytesRead;
    /* number of operations read */
    private long numOps;
    /* number of committed TXNs */
    private long numCommittedTXNs;
    /* number of aborted TXNs */
    private long numAborttedTXNs;
    /* number of items filtered and skipped */
    private long numFilteredOps;
    /* number of errors during transfer */
    private int errors;

    PartitionReaderStatus(PartitionId pid) {
        state = PartitionRepState.IDLE;
        partitionId = pid;
        startTime = 0;
        endTime = 0;
        numBytesRead = 0;
        numOps = 0;
        numCommittedTXNs = 0;
        numAborttedTXNs = 0;
        numFilteredOps = 0;
        errors = 0;
    }

    /**
     * Gets the partition id for this reader
     *
     * @return id of the partition
     */
    public PartitionId getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the partition reader state
     *
     * @param s state of partition reader
     */
    public void setState(PartitionRepState s) {
        state = s;
    }

    /**
     * Gets the state of partition reader
     *
     * @return state of partition reader
     */
    public PartitionRepState getState() {
        return state;
    }

    /**
     * Sets the starting time as current system time
     */
    public void setStartTime() {
        startTime = System.currentTimeMillis();
    }

    /**
     * Sets the end time of transferring a partition as current system time
     */
    public void setEndTime() {
        endTime = System.currentTimeMillis();
    }

    /**
     * Gets start time the transferring starts
     *
     * @return time to start transfer
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Gets the time transferring completes
     *
     * @return time partition reader finishes reading all data for that
     * partition
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * Gets the amount of data in bytes read for that partition
     *
     * @return bytes read by the reader
     */
    public long getNumBytesRead() {
        return numBytesRead;
    }

    /**
     * Gets the number of operations read for that partition
     *
     * @return number of operations read by the reader
     */
    public long getNumOps() {
        return numOps;
    }

    /**
     * Gets the number of committed transactions
     *
     * @return number of committed transactions
     */
    public long getNumCommittedTXNs() {
        return numCommittedTXNs;
    }
    
    /**
     * Gets the number of aborted transactions
     *
     * @return the number of aborted transactions
     */
    public long getNumAborttedTXNs() {
        return numAborttedTXNs;        
    }
    
    /**
     * Gets the number of operations filtered by reader
     *
     * @return number of filtered ops
     */
    public long getNumFilteredOps() {
        return numFilteredOps;
    }

    /**
     * Gets the number of errors occurred during transfer
     *
     * @return number of errors
     */
    public int getErrors() {
        return errors;
    }

    /**
     * Increments errors during transfer
     */
    public synchronized void incrErrors() {
        errors++;
    }

    /**
     * Increments bytes read by the reader
     *
     * @param bytes  bytes consumed by reader
     */
    public synchronized void incrBytesRead(int bytes) {
        numBytesRead += bytes;
    }

    /**
     * Increments number of operations receive by reader
     */
    public synchronized void incrOpsRead() {
        numOps++;
    }

    /**
     * Increments number of committed transactions
     */
    public synchronized void incrCommittedTXNs() {
        numCommittedTXNs++;
    }
    
   /**
    * Increments number of aborted transactions
    */
    public synchronized void incrAbortedTXNs() {
        numAborttedTXNs++;
    }
    
    /**
     * Increments number of filtered operations
     */
    public synchronized void incrNumFilteredOps() {
        numFilteredOps++;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("status of reader (partition id: ")
          .append(partitionId.toString())
          .append(" replication state: ")
          .append(state.toString());

        switch (state) {
            case IDLE:
                break;
            case REPLICATING:
                sb.append("\n");
                sb.append("start time: ").append(toDate(startTime))
                        .append(", ");
                sb.append("bytes received: ").append(getNumBytesRead())
                        .append(", ");
                sb.append("committed txn received: ")
                  .append(getNumCommittedTXNs()).append(", ");
                sb.append("aborted txn received: ")
                  .append(getNumAborttedTXNs()).append(", ");
                sb.append("ops received: ").append(getNumOps())
                        .append(", ");
                sb.append("ops filtered: ")
                        .append(getNumFilteredOps());
                break;
            case DONE:
                sb.append("\n");
                sb.append("start time: ").append(toDate(startTime))
                        .append(", ");
                sb.append("end time: ").append(toDate(endTime))
                  .append(", ");
                sb.append("bytes received: ").append(getNumBytesRead())
                  .append(", ");
                sb.append("committed txn received: ")
                  .append(getNumCommittedTXNs()).append(", ");
                sb.append("aborted txn received: ")
                  .append(getNumAborttedTXNs()).append(", ");
                sb.append("ops received: ").append(getNumOps())
                        .append(", ");
                sb.append("ops filtered: ")
                        .append(getNumFilteredOps());
                break;
            case ERROR:
            case CANCELLED:
                sb.append("\n");
                sb.append("start time: ").append(toDate(startTime))
                        .append(", ");
                sb.append("stopped time: ").append(toDate(endTime))
                        .append(", ");
                sb.append("errors: ").append(errors).append(", ");
                sb.append("bytes received: ").append(getNumBytesRead())
                  .append(", ");
                sb.append("committed txn received: ")
                  .append(getNumCommittedTXNs()).append(", ");
                sb.append("aborted txn received: ")
                  .append(getNumAborttedTXNs()).append(", ");
                sb.append("ops received: ").append(getNumOps())
                        .append(", ");
                sb.append("ops filtered: ")
                        .append(getNumFilteredOps());
                break;
            default:
                sb.append("\n");
        }

        return sb.toString();
    }

    private String toDate(long t) {
        return (t == 0) ? "N/A" : new Date(t).toString();
    }

    public enum PartitionRepState {
        /* not yet started */
        IDLE,

        /* replication in progress */
        REPLICATING,

        /* replication is done without error */
        DONE,

        /* replication stopped due to error */
        ERROR,

        /* replication cancelled */
        CANCELLED;

        @Override
        public String toString() {
            return name().toUpperCase();
        }
    }
}
