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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static oracle.kv.lob.KVLargeObject.LOBState.COMPLETE;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_APPEND;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_DELETE;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_PUT;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Key;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Value;
import oracle.kv.ValueVersion;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.util.UserDataControl;
import oracle.kv.lob.KVLargeObject.LOBState;
import oracle.kv.lob.PartialLOBException;

/**
 * The superclass for all LOB operations
 */
public abstract class Operation {

    protected static final String INTERNAL_KEY_SPACE = "";

    /* The second major key component for an ILK */
    protected static final String ILK_PREFIX_COMPONENT = "lob";

    /* The key prefix string for an ILK */
    protected static final String ILK_PREFIX =
        Key.createKey(Arrays.asList(INTERNAL_KEY_SPACE,
                                    ILK_PREFIX_COMPONENT)).toString() + "/";

    /**
     * The charset used to encode the Internal LOB Key as a byte array, when
     * it's stored as a value.
     */
    protected static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    /**
     * The replacement string used when a character cannot be translated into
     * UTF8.
     */
    private static final String UTF8_REPLACEMENT =
        UTF8_CHARSET.newDecoder().replacement();

    /** The KVS handle that owns this component. */
    protected final KVStoreImpl kvsImpl;
    protected final Key appLOBKey;
    protected Key internalLOBKey;

    protected long chunkTimeoutMs;

    /**
     * The chunk level layout in effect for this LOB object. These are
     * obtained from lobProps and are cached here for convenience. They are
     * constant through the lifetime of a LOB.
     */
    protected int chunkSize;
    protected int chunksPerPartition;

    /* Cached properties from lobProps, or MIN_VALUE if not initialized. */
    protected long lobSize = Long.MIN_VALUE;
    protected long numChunks = Long.MIN_VALUE;

    /**
     * Initialized at the start of each operation by the initMetadata methods.
     * For an existing LOB it's the serialized value associated with the
     * Internal LOB Key (ILK).
     */
    protected LOBProps lobProps;

    /**
     * The factory that generates chunk keys depending upon the metadata
     * version.
     */
    protected ChunkKeyFactory chunkKeyFactory;

    /**
     * Used to set explicit metadata versions for unit tests.
     */
    static int testMetadataVersion = 0;

    Operation(KVStoreImpl kvsImpl,
              Key appLobKey,
              long chunkTimeout,
              TimeUnit timeoutUnit) {

        super();
        this.kvsImpl = kvsImpl;
        this.appLOBKey = checkLOBKey(appLobKey);

        chunkTimeoutMs = (chunkTimeout > 0) ?
           MILLISECONDS.convert(chunkTimeout, timeoutUnit) :
           kvsImpl.getDefaultLOBTimeout();
    }

    /**
     * For test use only to facilitate creation of LOBs with a specific
     * metadata version.
     */
    public static void setTestMetadataVersion(int testMetadataVersion) {
        Operation.testMetadataVersion = testMetadataVersion;
    }

    /**
     * Reads in the metadata for an operation and verifies that the metadata
     * denotes a LOB that is either complete or of an allowed partial form. The
     * metadata is stored in lobProps.
     *
     * @param readConsistency the read consistency to use when fetching the
     * metadata.
     *
     * @param allowState the state that is allowed
     *
     * @return the ValueVersion associated with the metadata
     */
    protected ValueVersion initMetadata(final Consistency readConsistency,
                                        final LOBState allowState)
        throws PartialLOBException {

        ValueVersion metadataVV;
        try {
            /*
             * First try with ABSOLUTE and then fall back to a weaker
             * consistency if no master is available.
             */
            metadataVV = kvsImpl.get(internalLOBKey, Consistency.ABSOLUTE,
                        chunkTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (RequestTimeoutException rte) {
            /* No master available in timeout period. */
            if (Consistency.ABSOLUTE.equals(readConsistency)) {
                throw rte;
            }
            /*
             * Fallback to a looser consistency. We are exceeding the timeout
             * but it's for a worthy cause :-)
             */
            metadataVV = kvsImpl.get(internalLOBKey, readConsistency,
                                     chunkTimeoutMs, TimeUnit.MILLISECONDS);
        }

        /*
         * Application LOB Key (ALK)exists, but ILK associated with the
         * metadata does not.
         */
        if (metadataVV == null) {
            final String msg =
                "Partially put LOB (missing metadata). Key: " +
                    UserDataControl.displayKey(appLOBKey) +
                    " Internal key: " + internalLOBKey;
            throw new PartialLOBException(msg, PARTIAL_PUT, false);
        }

        initMetadata(metadataVV.getValue());

        if (lobProps.isPartiallyDeleted() &&
            (PARTIAL_DELETE != allowState)) {

            throw new PartialLOBException("Partially deleted LOB. Key: " +
                UserDataControl.displayKey(appLOBKey) +
                " Internal key: " + internalLOBKey,
                PARTIAL_DELETE,
                false);
        } else if (lobProps.isPartiallyAppended() &&
            (PARTIAL_APPEND != allowState)) {

            throw new PartialLOBException("Partially appended LOB. Key: " +
                UserDataControl.displayKey(appLOBKey) +
                " Internal key: " + internalLOBKey,
                PARTIAL_APPEND,
                false);
        } else if (lobProps.isPartiallyPut() &&
            (PARTIAL_PUT != allowState)) {

            throw new PartialLOBException("Partially put LOB. Key: " +
                UserDataControl.displayKey(appLOBKey) +
                " Internal key: " + internalLOBKey,
                PARTIAL_PUT,
                false);
        } else if ((allowState == COMPLETE) && ! lobProps.isComplete()) {

            throw new IllegalStateException("Expected complete LOB. " +
                "Key: " +  UserDataControl.displayKey(appLOBKey) +
                " Internal key: " + internalLOBKey);
        }

        return metadataVV;
    }

    /**
     * Used to initialize metadata when the we know it exists and the caller is
     * prepared to deal with the LOB in a partial state and continue with the
     * operation.
     *
     * @param propMapValue the serialized non-null metadata value
     */
    protected void initMetadata(final Value propMapValue) {
        lobProps = new LOBProps(propMapValue);
        final int mdVersion = lobProps.getMetadataVersion();
        if (mdVersion > getCurrentVersion()) {
            throw new IllegalStateException("Unknown metadata version:" +
                                            mdVersion);
        }
        chunkKeyFactory = new ChunkKeyFactory(mdVersion);
    }

    /**
     * Creates metadata for a brand new LOB.
     */
    protected void initMetadata() {
        lobProps = new LOBProps();
        final int mdVersion = lobProps.getMetadataVersion();
        chunkKeyFactory = new ChunkKeyFactory(mdVersion);
    }

    private int getCurrentVersion() {
        final int mdVersion = testMetadataVersion > 0 ?
            testMetadataVersion : LOBMetadataKeys.CURRENT_VERSION;
        return mdVersion;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public LOBProps getLOBProps() {
        return lobProps;
    }

    public KVStoreImpl getKvsImpl() {
        return kvsImpl;
    }

    public Key getInternalLOBKey() {
        return internalLOBKey;
    }

    public long getChunkTimeoutMs() {
        return chunkTimeoutMs;
    }

    /**
     * Returns a key iterator that spans all the chunks covered by the byte
     * range.
     *
     * @param startByteIndex zero based start index
     *
     * @param endByteIndex zero based end index
     *
     * @return the iterator
     */
    protected ChunkKeysIterator
        getChunkKeysByteRangeIterator(long startByteIndex,
                                      long endByteIndex) {

        return new ChunkKeysIterator(internalLOBKey,
                                     startByteIndex,
                                     endByteIndex,
                                     chunkSize,
                                     chunksPerPartition,
                                     chunkKeyFactory);
    }

    /**
     * Returns a key iterator that returns the first numChunks keys.
     *
     * @param nChunks the number of leading chunks
     */
    protected ChunkKeysIterator
        getChunkKeysNumChunksIterator(long nChunks) {

        return new ChunkKeysIterator(internalLOBKey,
                                     0,
                                     chunkSize * nChunks,
                                     chunkSize,
                                     chunksPerPartition,
                                     chunkKeyFactory);
    }

    /**
     * Returns the factory used to create chunk keys.
     */
    public ChunkKeyFactory getChunkKeyFactory() {
        return chunkKeyFactory;
    }

    /**
     * Performs LOB key validity checks. Throw IAE if the checks fail.
     */
    private Key checkLOBKey(Key key) {
        if (key == null) {
            throw new IllegalArgumentException("LOB key must not be null");
        }

        final List<String> fullPath = key.getFullPath();
        final String lastComponent = fullPath.get(fullPath.size() - 1);
        final String lobSuffix = kvsImpl.getDefaultLOBSuffix();
        if ((lobSuffix != null) &&
            !lastComponent.endsWith(lobSuffix)) {
            throw new IllegalArgumentException("LOB key: " + key +
                                                " must end with the suffix: " +
                                                lobSuffix);
        }
        return key;
    }

    /**
     * Returns the deserialized form of the ILK. The three methods below are
     * static to facilitate testing.
     */
   public static Key valueToILK(Value serializedKeyValue) {
        try {
            return valueToILKInternal(serializedKeyValue, UTF8_CHARSET);
        } catch (IllegalArgumentException iae) {
            try {
                /*
                 * Try with the default character set. This is a compatibility
                 * hack to deal with pre 2.1.55 LOB implementations which did
                 * not specify an explicit encoding.
                 */
                return valueToILKInternal(serializedKeyValue, null);
            } catch (IllegalArgumentException iae2) {
                throw iae;
            }
        }
    }

    /**
     * Deserialize the string into a key. The sequence of checks to determine
     * whether a non-utf8 charset was used for the encoding is not foolproof
     * but should catch all but the more esoteric cases.
     */
   public static Key valueToILKInternal(Value serializedKeyValue,
                                        Charset charSet)
        throws IllegalStateException {

        final String ilkString =
            (charSet == null) ?
            new String(serializedKeyValue.getValue()) :
            new String(serializedKeyValue.getValue(), charSet);

        if (!ilkString.startsWith(ILK_PREFIX)) {
            throw new IllegalArgumentException("Invalid ILK:" + ilkString);
        }

        if (ilkString.contains(UTF8_REPLACEMENT)) {
            throw new IllegalArgumentException("Invalid ILK:" + ilkString);
        }

        final Key ilk = Key.fromString(ilkString);

        if (ilk.getFullPath().size() != 3) {
            throw new IllegalStateException("Invalid ILK:" + ilkString);
        }

        return ilk;
    }

    public static Value ilkToValue(Key key) {
        return Value.createValue(key.toString().getBytes(UTF8_CHARSET));
    }

    /**
     * Skip bytes on an input stream, making repeated calls if individual skip
     * calls skip a smaller number of bytes than requested.  Gives up if a skip
     * call says that no bytes were skipped.
     *
     * @param in the input stream
     * @param nBytes the number of bytes to skip
     * @return the number of bytes skipped
     * @throws IOException if an I/O error occurs
     */
    static long skipInput(final InputStream in, final long nBytes)
        throws IOException {
        long totalBytesSkipped = 0;
        while (totalBytesSkipped < nBytes) {
            final long thisSkip = in.skip(nBytes - totalBytesSkipped);

            /*
             * We could read the bytes ourselves here, but it seems strange
             * that a correctly implemented stream would refuse to skip any
             * bytes at all, so don't bother until we have evidence of this
             * problem in practice.
             */
            if (thisSkip == 0) {
                break;
            }
            totalBytesSkipped += thisSkip;
        }
        return totalBytesSkipped;
    }

    /**
     * Encapsulates the LOB properties that define the LOB layout and
     * persistent state. LOBMetadataKeys describes the individual keys and
     * their purpose in detail.
     */
    class LOBProps implements LOBMetadataKeys {

        /* The property map. */
        private final Map<String, Object> propMap;

        /**
         * Default constructor.
         */
        LOBProps() {
            propMap = new HashMap<String, Object>();
            propMap.put(METADATA_VERSION,  getCurrentVersion());
        }

        /**
         * Constructor for existing LOBs that are being read, deleted, or
         * resumed.
         */
        public LOBProps(Value propMapValue) {
            Object rawObject = null;
            Exception exception = null;

            try {
                final ByteArrayInputStream bis =
                    new ByteArrayInputStream(propMapValue.getValue());
                final ObjectInputStream ois = new ObjectInputStream(bis);
                rawObject = ois.readObject();
                @SuppressWarnings("unchecked")
                final Map<String, Object> coercePropMap =
                    (Map<String, Object>) rawObject;
                propMap = coercePropMap;

                /* Initialize cached/initial values. */
                chunkSize = (Integer)propMap.get(CHUNK_SIZE);
                chunksPerPartition =
                    (Integer)propMap.get(CHUNKS_PER_PARTITION);
                if (getLOBSize() != null) {
                    /* Initialize it, if it exists. */
                    lobSize = getLOBSize();
                }
                if (getNumChunks() != null) {
                    numChunks = getNumChunks();
                }
                return;
            } catch (IOException e) {
                exception = e;
            } catch (ClassNotFoundException e) {
                exception = e;
            } catch (ClassCastException e) {
                exception = e;
            }

            final String msg = "Unexpected exception. + " +
                " The value: " + rawObject +
                " associated with the key: " +
                UserDataControl.displayKey(appLOBKey) +
                " does not represent a LOB value.";

            throw new IllegalArgumentException(msg, exception);
        }

        /**
         * Return the serialized representation of the LOB properties. It's
         * simply the java serialization of the hash map.
         *
         * The callers must arrange to delete keys as appropriate before
         * calling this method.
         */
        Value serialize() {

            final int mdVersion = getMetadataVersion();

            Map<String, Object> serialMap = propMap;
            if (mdVersion == 1) {
                /*
                 * Store LAST_SUPER_CHUNK_ID and NUM_CHUNKS as ints for
                 * backwards compatibility.
                 */
                serialMap = new HashMap<String, Object>(propMap);
                Long lscid = getLong(LAST_SUPER_CHUNK_ID) ;
                if (lscid != null) {
                    if (lscid > Integer.MAX_VALUE) {
                        throw new IllegalStateException
                            ("LAST_SUPER_CHUNK_ID:" + lscid +
                             " exceeds Integer.MAX_VALUE");
                    }
                    serialMap.put(LAST_SUPER_CHUNK_ID, lscid.intValue());
                }

                Long nchunks = getLong(NUM_CHUNKS) ;

                if (nchunks != null) {
                    if (nchunks > Integer.MAX_VALUE) {
                        throw new IllegalStateException
                            ("NUM_CHUNKS:" + nchunks +
                             " exceeds Integer.MAX_VALUE");
                    }
                    serialMap.put(NUM_CHUNKS, nchunks.intValue());
                }
            }

            final ByteArrayOutputStream bos = new ByteArrayOutputStream();

            try {
                final ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(serialMap);
                oos.close();
            } catch (IOException ioe) {
                throw new IllegalStateException("Unexpected exception", ioe);
            }

            return Value.createValue(bos.toByteArray());
        }

        int getVersion() {
            return (Integer) propMap.get(METADATA_VERSION);
        }

        /**
         * Utility method to account for the fact that some values were stored
         * as ints in version 1 of the metadata format.
         */
        private Long getLong(String key) {

            final Object value = propMap.get(key);

            return (value instanceof Integer) ?
                Long.valueOf(((Integer) value).longValue()) :
                (Long) value;
        }

        boolean isComplete() {
            return propMap.containsKey(NUM_CHUNKS) ;
        }

        /**
         * Returns true if the LOB was partially inserted.
         */
        public boolean isPartiallyPut() {
            return propMap.get(NUM_CHUNKS) == null &&
                !isPartiallyDeleted() &&
                !isPartiallyAppended();
        }

        /**
         * Returns true if the LOB was partially appended.
         */
        public boolean isPartiallyAppended() {
            return propMap.get(APPEND_LOB_SIZE) != null;
        }

        /**
         * Returns true if the LOB was deleted.
         */
        boolean isPartiallyDeleted() {
            return propMap.get(DELETED) != null;
        }

        public void markDeleted() {
            propMap.put(DELETED, new Date().toString());
        }

        public Object remove(String key) {
            return propMap.remove(key);
        }

        public Long getNumChunks() {
            return getLong(NUM_CHUNKS);
        }

        public void setNumChunks(long numChunks) {
            propMap.put(NUM_CHUNKS, numChunks);
        }

        public Long getLastSuperChunkId() {
            return getLong(LAST_SUPER_CHUNK_ID);
        }

        public void setLastSuperChunkId(long superChunkId) {
            propMap.put(LAST_SUPER_CHUNK_ID, superChunkId);
        }

        public Long getAppendLobSize() {
            return (Long)propMap.get(APPEND_LOB_SIZE);
        }


        public void setAppendLobSize(long appendLobSize) {
            propMap.put(APPEND_LOB_SIZE, appendLobSize);
        }

        public Long getLOBSize() {
            return getLong(LOB_SIZE);
        }

        /**
         * Returns the  lob size in bytes or null
         */
        void setLOBSize(long lobSize) {
            propMap.put(LOB_SIZE, lobSize);
        }

        public int getMetadataVersion() {
            return (Integer)propMap.get(METADATA_VERSION);
        }

        /**
         * Update the metadata to indicate that an append operation is being
         * initiated. Every complete append operation has one startAppend() and
         * endAppend() operation associated with it.
         */
        void startAppend() {
            /* Set APPEND_LOB_SIZE  to denote onging append operation*/
            setAppendLobSize(lobSize);

            /*
             * Remove keys whose values are about to change. These values are
             * not updated on a chunk by chunk basis, only at super chunk
             * transitions, to minimize write overheads. They are recomputed,
             * from the super chunk id should the operation need to be resumed.
             */
            remove(LOB_SIZE);
            remove(NUM_CHUNKS);
        }

        /**
         * Update the metadata to indicate the end of the append operation. It
         * removes the transient keys that were in place while the append
         * was in progress.
         */
        void endAppend() {
            remove(APPEND_LOB_SIZE);
            /* restore keys that were removed. */
            endPut();
        }

        /**
         * Create the initial metadata for a new LOB. Every complete put
         * operation has one startPut() and endPut() operation associated with
         * it.
         */
        void startPut() {
            chunkSize = kvsImpl.getDefaultChunkSize();
            chunksPerPartition = kvsImpl.getDefaultChunksPerPartition();

            lobSize = 0;
            numChunks = 0;

            propMap.put(CHUNK_SIZE, chunkSize);
            propMap.put(CHUNKS_PER_PARTITION, chunksPerPartition);
            propMap.put(APP_KEY, appLOBKey.toString());
            propMap.put(LAST_SUPER_CHUNK_ID, 1l);
        }

        /**
         * Update the metadata to indicate that a put (or more generally a
         * write) operation has been completed. At the end of the operation we
         * know the total size of the LOB and can store it.
         */
        void endPut() {

            if (((lobSize == 0) && (numChunks != 0)) ||
                ((lobSize != 0) &&
                  (((lobSize - 1) / chunkSize) + 1) != numChunks)) {

                final String msg = "LOBsize:" + lobSize +
                    " is inconsistent with the chunk count:" + numChunks +
                    " for the chunk size:" + chunkSize;
                throw new IllegalStateException(msg);
            }

            if (((numChunks == 0) && (getLastSuperChunkId() != 1)) ||
                ((numChunks > 0) &&
                 (getLastSuperChunkId() !=
                  (((numChunks - 1) / chunksPerPartition) + 1)))) {
                final String msg = "Inconsistent super chunk id:" +
                    getLastSuperChunkId() + " for num chunks:" + numChunks;
                throw new IllegalStateException(msg);
            }

            /* Written out all chunks. Update the metadata. */
            setLOBSize(lobSize);
            setNumChunks(numChunks);
            final Long lastScid = getLastSuperChunkId();
            if ((lastScid == null) || (lastScid < 0)) {
               throw new IllegalStateException("LOBProps:" + lobProps);
           }
        }

        @Override
        public String toString() {
           return "< LOBProps: " +
                  " Internal LOB key:" + internalLOBKey +
                  " LOB size:" + getLOBSize() +
                  " Chunk size:" + getChunkSize() +
                  " Num chunks: " + getNumChunks() +
                  " Last SC id:" + getLastSuperChunkId() +
                  " >";
        }
    }
}
