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

package oracle.kv.impl.api.lob;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.FaultException;
import oracle.kv.Key;
import oracle.kv.KeyRange;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.test.TestHookExecute;
import oracle.kv.impl.util.UserDataControl;

/**
 * Implements the LOB delete operation
 */
public class DeleteOperation extends WriteOperation {

    /**
     * The batch size used by the store iterator when deleting partial LOBs.
     */
    private static final int DELETE_KEYS_BATCH_SIZE = 1000;

    /**
     * Test hook to help create partially deleted objects
     */
    private static TestHook<Integer> deleteTestHook;

    public DeleteOperation(KVStoreImpl kvsImpl,
                           Key appLobKey,
                           Durability durability,
                           long chunkTimeout,
                           TimeUnit timeoutUnit) {
        super(kvsImpl, appLobKey, null, durability, chunkTimeout, timeoutUnit);
    }

    public static void setDeleteTestHook(TestHook<Integer> deleteTestHook) {
        DeleteOperation.deleteTestHook = deleteTestHook;
    }

    /**
     * Deletes a complete or partial LOB. The delete proceeds as follows:
     *
     * 1) Update the metadata associated with the LOB to indicate that it's a
     * partially deleted LOB.
     *
     * 2) Delete all the chunks associated with the LOB.
     *
     * 3) Delete the LOB metadata.
     *
     * 4) Delete the appLOBKey/value pair.
     *
     * If there is a failure between steps 3) and 4) we are unable to
     * distinguish between a partially inserted (or appended LOB) and partially
     * deleted LOB. In this case we treat it like a partially inserted LOB.
     *
     * @return true if the delete is successful, or false if no existing value
     * is present. Note that the method will return true if a partial LOB was
     * deleted.
     */
    public boolean execute(boolean retainAppKV) {

        final ValueVersion appLobValueVersion =
            kvsImpl.get(appLOBKey, Consistency.ABSOLUTE,
                        chunkTimeoutMs, TimeUnit.MILLISECONDS);

        if (appLobValueVersion == null) {
            return false;
        }

        internalLOBKey = valueToILK(appLobValueVersion.getValue());

        /* Now get the metadata itself. */
        final ValueVersion metadataVV =
            kvsImpl.get(internalLOBKey, Consistency.ABSOLUTE, chunkTimeoutMs,
                        TimeUnit.MILLISECONDS);

        if (metadataVV != null) {
            initMetadata(metadataVV.getValue());

            /* Step 1: update metadata */
            lobProps.markDeleted();
            updateMetadata(metadataVV.getVersion());

            /*
             * Step 2: delete individual chunks. If the object is complete, use
             * the number of chunks associated with the object. If it's
             * incomplete then use the storewide iterator to be safe.
             */
            if (lobProps.getLastSuperChunkId() != null) {
                /*
                 * If the superchunk id is available (it should almost always
                 * be available), since it's always a part of the metadata, use
                 * range deletes to delete each superchunk in one go.
                 */
                deleteSuperChunks(lobProps.getLastSuperChunkId());
            } else {
                /* Fall back to less efficient mechanisms. */
                final Iterator<Key> chunkKeys =
                    (lobProps.getNumChunks() != null) ?
                     getChunkKeysNumChunksIterator(lobProps.getNumChunks()) :
                     kvsImpl.storeKeysIterator(Direction.UNORDERED,
                                               DELETE_KEYS_BATCH_SIZE,
                                               internalLOBKey,
                                               new KeyRange("", true, null,
                                                            false),
                                               Depth.DESCENDANTS_ONLY,
                                               Consistency.ABSOLUTE,
                                               chunkTimeoutMs,
                                               TimeUnit.MILLISECONDS);

                deleteChunks(chunkKeys);
            }
        }

        /* Step 3 */
        final boolean metadataDeleted = kvsImpl.delete(internalLOBKey);
        if (!metadataDeleted) {
            if (metadataVV != null) {
                final String msg = "Internal Lob key: " + internalLOBKey +
                    " deleted while LOB delete was in progress.";
                throw new ConcurrentModificationException(msg);
            }
            /* Metadata did not exist to start with. */
        } else {
            /* Metadata was deleted */
            if (metadataVV == null) {
                final String msg = "Internal Lob key: " + internalLOBKey +
                    " appeared while LOB delete was in progress.";
                throw new ConcurrentModificationException(msg);
            }
        }

        /* Step 4 Finally delete the app key */
        if (retainAppKV) {
            return true;
        }

        final boolean deleted =
            kvsImpl.deleteIfVersion(appLOBKey,
                                    appLobValueVersion.getVersion());
        if (!deleted) {
            final String msg = "LOB: " +
                UserDataControl.displayKey(appLOBKey) +
                " modified while delete was in progress.";
            throw new ConcurrentModificationException(msg);
        }
        return true;
    }

    /**
     * The (default) efficient implementation of delete that deletes all the
     * chunks under each super chunk by using a multiDelete.
     */
    private void deleteSuperChunks(long lastScid) throws FaultException {
        /*
         * The + 1 below for the upper bound is to allow for cases where the
         * super chunk id was not yet updated in the metadata. See the
         * putChunks() method for details.
         */
        for (long scid = 1; scid <= (lastScid + 1); scid++) {

            assert TestHookExecute.doHookIfSet(deleteTestHook, null);

            final Key scKey =
                chunkKeyFactory.createSuperChunkKey(internalLOBKey, scid);
            kvsImpl.multiDelete(scKey,
                                new KeyRange("", true, null, false),
                                Depth.DESCENDANTS_ONLY,
                                lobDurability,
                                chunkTimeoutMs,
                                TimeUnit.MILLISECONDS);
        }
    }

    /**
     * The fallback delete implementation, it's less efficient and deletes
     * the chunks one by one as provided by the iterator.
     */
    private void deleteChunks(Iterator<Key> chunkKeys)
        throws FaultException,
        ConcurrentModificationException {
        while (chunkKeys.hasNext()) {
            assert TestHookExecute.doHookIfSet(deleteTestHook, null);

            final Key chunkKey = chunkKeys.next();

            /*
             * Ignore the result since we could be retrying a delete and
             * may have already deleted some of the chunks.
             */
            @SuppressWarnings("unused")
            boolean deleted = kvsImpl.delete(chunkKey);
        }
    }

    @Override
    protected Version putChunks(long startByte,
                                byte chunkPrefix[],
                                Version metadataVersion) {

        throw new UnsupportedOperationException("Delete does not support "
            + "this operation.");
    }
}
