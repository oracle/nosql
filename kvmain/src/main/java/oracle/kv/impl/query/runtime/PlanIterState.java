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

package oracle.kv.impl.query.runtime;

import oracle.kv.impl.query.QueryStateException;

public class PlanIterState {

    public static enum StateEnum {
        OPEN,
        RUNNING,
        DONE,
        CLOSED
    }

    private StateEnum theState;

    public PlanIterState() {
        theState = StateEnum.OPEN;
    }

    public boolean isOpen() {
        return theState == StateEnum.OPEN;
    }

    boolean isClosed() {
        return theState == StateEnum.CLOSED;
    }

    public boolean isDone() {
        return theState == StateEnum.DONE;
    }

    @SuppressWarnings("unused")
    public void reset(PlanIter iter) {
        setState(StateEnum.OPEN);
    }

    public void close() {
        setState(StateEnum.CLOSED);
    }

    public void done() {
        setState(StateEnum.DONE);
    }

    public void setState(StateEnum v) {
        switch (theState) {
        case RUNNING:
            if (v == StateEnum.RUNNING ||
                v == StateEnum.DONE ||
                v == StateEnum.CLOSED ||
                v == StateEnum.OPEN) {
                theState = v;
                return;
            }
            break;
        case DONE:
            if (v == StateEnum.OPEN || v == StateEnum.CLOSED) {
                theState = v;
                return;
            }
            break;
        case OPEN:
            /*
             * OPEN --> DONE transition is allowed for iterators that are "done"
             * on the 1st next() call after an open() or reset() call. In this
             * case, rather than setting the state to RUNNING on entrance to the
             * next() call and then setting the state again to DONE before
             * returning from the same next() call, we allow a direct transition
             * from OPEN to DONE.
             */
            if (v == StateEnum.OPEN ||
                v == StateEnum.RUNNING ||
                v == StateEnum.CLOSED ||
                v == StateEnum.DONE) {
                theState = v;
                return;
            }
            break;
        case CLOSED:
            break;
        }

        throw new QueryStateException(
            "Wrong state transition for iterator " + getClass() +
            ". Current state: " + theState + " New state: " + v);
    }
}
