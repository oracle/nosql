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


package oracle.kv.impl.xregion.agent.pitr;

import java.util.logging.Logger;

import oracle.kv.impl.api.table.RowImpl;
import oracle.kv.impl.xregion.agent.BaseRegionAgentSubscriber;
import oracle.kv.impl.xregion.agent.RegionAgentThread;
import oracle.kv.pubsub.NoSQLSubscriptionConfig;
import oracle.kv.pubsub.StreamOperation;
import oracle.kv.pubsub.StreamPosition;
import oracle.kv.table.Row;

/**
 * Subscriber of region agent for Point-In-Time-Recovery (PITR)
 */
public class PITRSubscriber extends BaseRegionAgentSubscriber {

    public PITRSubscriber(RegionAgentThread agent,
                          NoSQLSubscriptionConfig config,
                          Logger logger) {
        super(agent, config, logger);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void onNext(StreamOperation sp) {
        try {
            /* get source row */
            final Row srcRow;
            if (sp.getType().equals(StreamOperation.Type.PUT)) {
                srcRow = sp.asPut().getRow();
            } else {
                srcRow = sp.asDelete().getPrimaryKey();
            }

            final String tbName = srcRow.getTable().getFullNamespaceName();
            /* stats update */
            long sz = ((RowImpl) srcRow).getDataSize() +
                      ((RowImpl) srcRow).getKeySize();
            getMetrics().getTableMetrics(tbName).incrStreamBytes(sz);
            if (sp.getType().equals(StreamOperation.Type.PUT)) {
                getMetrics().getTableMetrics(tbName).incrPuts(1);
            } else {
                getMetrics().getTableMetrics(tbName).incrDels(1);
            }
        } catch (Exception exp) {
            logger.warning(lm("Cannot process operation, error=" + exp));
            throw exp;
        }
    }

    @Override
    public void onError(Throwable t) {
    }

    @Override
    public void onWarn(Throwable t) {
    }

    @Override
    public void onCheckpointComplete(StreamPosition sp, Throwable exp) {
    }

    /* private functions */
    private String lm(String msg) {
        return "[PITRSubscriber-" + config.getSubscriberId() + "] " + msg;
    }

    /*
     * Stat reference may change if interval stat is collected, thus get
     * the reference from parent instead of keeping a constant reference
     */
    private PITRAgentMetrics getMetrics() {
        return (PITRAgentMetrics) parent.getMetrics();
    }
}
