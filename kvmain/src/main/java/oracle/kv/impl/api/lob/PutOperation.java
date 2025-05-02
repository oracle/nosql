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

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.Key;
import oracle.kv.KeyValueVersion;
import oracle.kv.ReturnValueVersion;
import oracle.kv.ReturnValueVersion.Choice;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.lob.KVLargeObject.LOBState;
import oracle.kv.lob.PartialLOBException;

/**
 * Implements the basic put operation for a LOB. The operation will create a
 * new LOB, or resume an existing LOB.
 */
public class PutOperation extends WriteOperation {

    PutOperation(KVStoreImpl kvsImpl,
                 Key appLobKey,
                 InputStream lobStream,
                 Durability durability,
                 long chunkTimeout,
                 TimeUnit timeoutUnit) {

        super(kvsImpl, appLobKey, lobStream, durability,
              chunkTimeout, timeoutUnit);
    }

    /**
     * Insert the LOB representation. This involves the following steps:
     *
     * 1) Insert the application lobKey with the internalLOBKey as its value.
     * The internalLOBKey has the format: //lob/KVSUUID.
     *
     * 2) Insert the internalLobKey with the LOB metadata as its value.
     *
     * 3) Read the stream and populate the chunks. Each chunk is associated
     * with a key of the form:
     * //lob/KVSUUID/SUPERCHUNKID/-/CHUNKID.
     *
     * where SUPERCHUNKID is the ID associated with the SUPERCHUNK and
     * CHUNKID is the super-chunk relative minor sequence number associated
     * with the chunk.
     *
     * Update the LOB metadata after the first chunk is created in each
     * superchunk to capture the highest superchunk ID.
     *
     * 4) Update the LOB metadata to contain the LOB size and the max chunk id.
     *
     * Any exceptions after the first step result in the creation of a partial
     * LOB that can be deleted, or the insertion resumed.
     */
    Version execute(boolean requirePresent,
                    boolean requireAbsent)
        throws IOException {

        Version metadataVV = null;

        /* Step 1. Create the app KV pair */
        KeyValueVersion appLobKVV;
        try {
            appLobKVV = createAppLobKVV(requirePresent, requireAbsent);
            if (appLobKVV == null) {
                return null;
            }

            initMetadata();
            lobProps.startPut();

            /* Step 2. Create and persist initial metadata if not resuming */
            metadataVV = persistMetadata();
        } catch (ResumePutException e) {
            appLobKVV = e.appLobKVV;
            metadataVV  = e.metadataVersion;
            setupForResume(e.metadataVersion);
        }

        /* Step 3. Insert the chunks */
        metadataVV = putChunks(lobSize, null, metadataVV);

        /* Step 4. Update the metadata. */
        lobProps.endPut();
        updateMetadata(metadataVV);

        return appLobKVV.getVersion();
    }

    /**
     * Create the app visible key/value pair representing the LOB. The value
     * is simply a serialized version of the internal key whose value in turn
     * is the actual LOB representation in the hidden LOB keyspace.
     *
     * @param requireAbsent the LOB must be absent or partial
     * @param requirePresent the LOB must be present or partial
     *
     * @return the KeyValueVersion associated with the LOB or null if the
     * putIfPresent/Absent preconditions are not satisfied.
     *
     * @throws ResumePutException if the ALK pair was found and the put must
     * be resumed
     */
    private KeyValueVersion createAppLobKVV(boolean requirePresent,
                                            boolean requireAbsent)
        throws ResumePutException {

        internalLOBKey = createInternalLobKey();
        final Value appLobValue = ilkToValue(internalLOBKey);
        final ValueVersion prevVV;
        if (requirePresent) {
            /* Inspect non destructively, must not create a new version. */
            prevVV = kvsImpl.get(appLOBKey,Consistency.ABSOLUTE,
                                 chunkTimeoutMs, TimeUnit.MILLISECONDS);
            if (prevVV == null) {
                return null;
            }
            /* full or partial LOB */
        } else {
            prevVV = new ReturnValueVersion(Choice.ALL);
            final Version appLobVersion =
                kvsImpl.putIfAbsent(appLOBKey,
                                    appLobValue,
                                    (ReturnValueVersion)prevVV,
                                    CHUNK_DURABILITY,
                                    chunkTimeoutMs, TimeUnit.MILLISECONDS);
            /* No LOB or partial LOB */
            if (appLobVersion != null) {
                return new KeyValueVersion(appLOBKey, appLobValue,
                                           appLobVersion);
            }
        }

        /*
         * Three possible alternatives for an existing key/value pair:
         *
         * 1) Partially deleted LOB: delete the LOB and start new insert
         * 2) Complete LOB: delete the LOB and start new insert
         * 3) Partially inserted LOB: resume insert
         */
        final Key extantInternalLobKey = valueToILK(prevVV.getValue());
        final Value extantAppLobValue = ilkToValue(extantInternalLobKey);
        final Version extantAppLobVersion = prevVV.getVersion();

        final ValueVersion metadataVV =
            kvsImpl.get(extantInternalLobKey,
                        Consistency.ABSOLUTE,
                        chunkTimeoutMs, TimeUnit.MILLISECONDS);
        if (metadataVV == null) {
            /* Can resume with initial metadata creation. */
            internalLOBKey = extantInternalLobKey;
            return new KeyValueVersion(appLOBKey,
                                       extantAppLobValue,
                                       extantAppLobVersion);
        }

        /* Have old metadata */
        initMetadata(metadataVV.getValue());

        if (!lobProps.isPartiallyDeleted()) {
            if (!lobProps.isComplete()) {
                /*
                 * A partially inserted object, resume the put operation using
                 * the old metadata.
                 */
                if (lobProps.isPartiallyAppended()) {
                    throw new PartialLOBException("Partially appended LOB. Key: " +
                        UserDataControl.displayKey(appLOBKey) +
                        " Internal key: " + internalLOBKey,
                        LOBState.PARTIAL_APPEND,
                        false);
                }

                internalLOBKey = extantInternalLobKey;
                final KeyValueVersion kvv =
                    new KeyValueVersion(appLOBKey,
                                        extantAppLobValue,
                                        extantAppLobVersion);
                throw new ResumePutException(kvv, metadataVV.getVersion());

            } else if (requireAbsent) {
                /* Complete LOB present. */
                return null;
            }
        }

        /* Deleted or complete existing LOB, we are going to start a new. */
        new DeleteOperation(kvsImpl, appLOBKey, lobDurability,
                            chunkTimeoutMs,
                            TimeUnit.MILLISECONDS).execute(true);

        /*
         * App key is still present, replace it atomically, transitioning from
         * the old partial LOB without any rep in the internal lob space to the
         * new partial LOB which will be filled in by the rest of the put
         * operation.
         */
        final Version replaceAppLobVersion =
            kvsImpl.putIfVersion(appLOBKey,
                                 appLobValue,
                                 prevVV.getVersion(),
                                 null,
                                 CHUNK_DURABILITY,
                                 chunkTimeoutMs, TimeUnit.MILLISECONDS);
        if (replaceAppLobVersion == null) {
            final String msg = "Concurrent LOB put detected " +
                "for key: " + UserDataControl.displayKey(appLOBKey);
            throw new ConcurrentModificationException(msg);

        }
        return new KeyValueVersion(appLOBKey, appLobValue,
                                   replaceAppLobVersion);
    }

    /**
     * Persists the initial metadata associated with the internalLobKey
     *
     * @return the version associated with the metadata
     */
    private Version persistMetadata() {

        final Value propsArray = lobProps.serialize();

        /* Insert the new LOB */
        final Version storedVersion =
            kvsImpl.putIfAbsent(internalLOBKey, propsArray,
                                null,
                                lobDurability,
                                chunkTimeoutMs, TimeUnit.MILLISECONDS);

        if (storedVersion == null) {
            final String msg = "Metadata for internal key: " + internalLOBKey +
                " already exists for key: " + appLOBKey;
            throw new ConcurrentModificationException(msg);
        }

        return storedVersion;
    }

    private static Key createInternalLobKey() {

        /*
         * TODO: SR21635 create based upon internal KVS unique sequence number.
         * The use of UUID below works but produces a largish key bloating
         * the IN node size when it cannot be compressed away.
         */
        return Key.createKey(Arrays.asList(INTERNAL_KEY_SPACE,
                                           ILK_PREFIX_COMPONENT,
                                           UUID.randomUUID().toString()));
    }

    /**
     * Used to indicate that a put operation needs to be resumed.
     */
    private class ResumePutException extends Exception {

        private static final long serialVersionUID = 1L;

        private final Version metadataVersion;
        private final KeyValueVersion appLobKVV;

        public ResumePutException(KeyValueVersion applobKVV,
                                  Version metadataVersion) {
            this.appLobKVV = applobKVV;
            this.metadataVersion = metadataVersion;
        }
    }
}
