/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package standalone.datacheck;

/**
 * Represents the various types of update operations.  Given that half of the
 * entries they encounter are absent, each of the operations either has no net
 * effect on the number of entries or has a 1/2 chance of either adding or
 * removing an entry. <p>
 */

interface UpdateOpType<K, V, O> {


    /* Methods */

    /**
     * Returns the associated operation to be performed by an exercise thread.
     *
     * @param dc the data check instance
     * @param index the operation index
     * @param first whether being called from the first of the pair of exercise
     *        threads
     * @param requestPrevValue whether to request previous value in the call
     * @return the operation
     */
    UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc, long index,
                                     boolean first, boolean requestPrevValue);

    /**
     * Returns the result of performing the associated operation with the new
     * value given the specified current value.
     *
     * @param newValue the new value being applied by the operation
     * @param previousValue the previous value in the store
     */
    V getResult(V newValue, V previousValue);

    /**
     * Returns whether this is a versioned operation.  Versioned operations
     * will have no effect if the previous version has not propagated before
     * they are called.
     */
    default boolean versioned() { return false; }

    abstract static class AbstractUpdateOpType<K, V, O>
            implements UpdateOpType<K, V, O> {

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }
    /* Classes */

    static class Put<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
        @Override
        public V getResult(V newValue, V previousValue) {
            return newValue;
        }
    }

    static class PutExecute<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean first,
                                boolean requestPrevValue) {
            return new UpdateOp.PutExecuteOp<K, V, O>(
                dc, index, first, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return newValue;
        }
    }

    static class PutIfAbsent<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfAbsentOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue != null) ? previousValue : newValue;
        }
    }

    static class PutIfAbsentExecute<K, V, O>
            extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfAbsentExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue != null) ? previousValue : newValue;
        }
    }

    static class PutIfPresent<K, V, O> extends AbstractUpdateOpType<K, V, O>  {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfPresentOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue == null) ? null : newValue;
        }
    }

    static class PutIfPresentExecute<K, V, O>
            extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfPresentExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue == null) ? null : newValue;
        }
    }

    static class PutIfVersion<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfVersionOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public boolean versioned() { return true; }

        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue == null) ? null : newValue;
        }
    }

    static class PutIfVersionExecute<K, V, O>
            extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.PutIfVersionExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public boolean versioned() { return true; }

        @Override
        public V getResult(V newValue, V previousValue) {
            return (previousValue == null) ? null : newValue;
        }
    }

    static class TwoPutExecute<K, V, O> extends PutExecute<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.TwoPutExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
    }

    static class BulkPut<K, V, O> extends PutExecute<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                       long index,
                                       boolean firstThread,
                                       boolean requestPrevValue) {
            /* Bulk put is expensive, so ask datacheck when to use it */
            if (!dc.chooseBulkPut()) {
                return new UpdateOp.PutOp<>(
                    dc, index, firstThread, requestPrevValue);
            }
            return new UpdateOp.BulkPutOp<>(dc, index, firstThread);
        }
    }

    static class Delete<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.DeleteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
        @Override
        public V getResult(V newValue, V previousValue) {
            return null;
        }
    }

    static class DeleteExecute<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.DeleteExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return null;
        }
    }

    static class DeleteIfVersion<K, V, O> extends Delete<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.DeleteIfVersionOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
        @Override
        public boolean versioned() { return true; }
    }

    static class DeleteIfVersionExecute<K, V, O>
        extends DeleteIfVersion<K, V, O> {

        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O> dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.DeleteIfVersionExecuteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }
    }

    static class MultiDelete<K, V, O> extends AbstractUpdateOpType<K, V, O> {
        @Override
        public UpdateOp<K, V, O> getOp(DataCheck<K, V, O>  dc,
                                long index,
                                boolean firstThread,
                                boolean requestPrevValue) {
            return new UpdateOp.MultiDeleteOp<K, V, O>(
                dc, index, firstThread, requestPrevValue);
        }

        @Override
        public V getResult(V newValue, V previousValue) {
            return null;
        }
    }
}
