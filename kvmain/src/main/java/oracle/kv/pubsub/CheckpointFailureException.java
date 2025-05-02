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
 * Exception that is raised when a subscriber fails to make a checkpoint.
 */
public class CheckpointFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /* id of the failed subscriber */
    private final NoSQLSubscriberId sid;
    /* checkpoint table name */
    private final String ckptTbl;

    /**
     * @hidden
     *
     * Constructor of a checkpoint failure with cause
     *
     * @param sid     id of failed subscriber
     * @param ckptTbl checkpoint table name
     * @param msg     exception message
     * @param cause   exception being wrapped
     */
    public CheckpointFailureException(NoSQLSubscriberId sid,
                                      String ckptTbl,
                                      String msg,
                                      Throwable cause) {
        super(msg, cause);
        this.sid = sid;
        this.ckptTbl = ckptTbl;
    }

    /**
     * Gets the checkpoint table name
     *
     * @return the checkpoint table name
     */
    public String getCheckpointTableName() {
        return ckptTbl;
    }

    /**
     * Gets id of subscriber that fails to checkpoint
     *
     * @return id of subscriber that fails to checkpoint
     */
    public NoSQLSubscriberId getSubscriberId() {
        return sid;
    }
}
