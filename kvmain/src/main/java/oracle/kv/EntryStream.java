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

package oracle.kv;

/**
 * The stream interface that supplies the data (Row or Key/Value pair) to be
 * batched and loaded into the store.
 *
 * Any exceptions throw by methods of this interface are handled by the method
 * {@link #catchException}.
 *
 * @param <E> Must be a Row, or a KeyValue. The term entry is used to cover both
 * cases, in the API and the following javadoc.
 *
 * @since 4.0
 */
public interface EntryStream<E> {

    /**
     * Returns a name to associate with the stream. It's used to identify
     * a specific stream in logs and exception messages.
     *
     * @return the name of the stream. It must not be null.
     */
    String name();

    /**
     * Returns the next entry in the stream. This method is invoked
     * sequentially so that each getNext() operation is allowed to complete
     * before the next call is issues by the loader.
     * <p>
     * The order of entries in the stream impacts duplicate processing.
     * Assuming the key is not already present in the store, at the time the
     * load operation is initiated, the first entry will result in the key
     * being inserted and the second and subsequent duplicate entries will
     * result in the keyExists method being invoked.
     * </p>
     * <p>
     * The bulk put operation will generate an interrupt if it encounters some
     * issue which forces it to terminate the bulk put operation before it can
     * be completed. The getNext() method must make provisions to handle
     * InterruptedException by cleaning up and returning null.
     * </p>
     *
     * @return the next entry in the stream. It returns null if at the end of
     * the stream or if the operation was interrupted.
     */
    E getNext();

    /**
     * Invoked by the loader to indicate that all the entries supplied by the
     * stream have been processed. The callback happens sometime after the
     * <code>getNext()</code> method returns null and all entries supplied by
     * the stream have been written to the store.
     * <p>
     * Applications may choose to use this callback to checkpoint progress
     * during a bulk operation. In case there is a failure, the operation can
     * be resumed by skipping streams that have been completely processed, but
     * reloading all streams that have been partially processed or have not
     * been processed at all. Reloading a partially processed stream will
     * result in {#keyExists} being invoked for entries that were already
     * loaded.
     * </p>
     * The method should not block. It must do minimal processing, delegating
     * any blocking or time-consuming operations to a separate thread and
     * return back to the caller.
     */
    void completed();

    /**
     * The method that's invoked when an entry with an existing primary key is
     * found in the store.
     * <p>
     * This method must be re-entrant and should not block. It must do minimal
     * processing, delegating any blocking or time-consuming operations to a
     * separate thread of control and return back to the caller.
     * </p>
     * @param entry the entry associated with the key that was already present
     */
    void keyExists(E entry);

    /**
     * The method that is invoked when an exception (such as Durability
     * and RequestTimeout) is encountered while trying to add an entry to
     * the store.
     * <p>
     * The method must be re-entrant, since the bulk load operation is run
     * in parallel and multiple concurrent exceptions can be encountered
     * simultaneously.
     * </p>
     * <p>
     * If the method returns normally, the entry is ignored and the bulk
     * load operation continues with the loading of other entries. If the
     * methods throws an exception, the entire bulk load operation is
     * terminated and the exception is rethrown from the <code>put</code>
     * operation that initiated the bulk load.
     * </p>
     * @param exception the exception that was encountered
     *
     * @param entry the entry(Row or KeyValue) associated with the exception
     */
    void catchException(RuntimeException exception, E entry);
}
