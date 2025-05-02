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

package oracle.kv.lob;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Durability;
import oracle.kv.KVStoreConfig;
import oracle.kv.Key;
import oracle.kv.Version;
import oracle.kv.impl.util.FastExternalizable;

/**
 * The KVLargeObject interface defines the operations used to read and write
 * Large Objects (LOBs) such as audio and video files. As a general rule, any
 * object larger than 1 MB is a good candidate for representation as a LOB. The
 * LOB API permits access to large values, without having to materialize the
 * value in its entirety by providing streaming APIs for reading and writing
 * these objects.
 * <p>
 * A LOB is stored as a sequence of chunks whose sizes are optimized for the
 * underlying storage system. The chunks constituting a LOB may not all be the
 * same size. Individual chunk sizes are chosen automatically by the system
 * based upon its knowledge of the underlying storage architecture and
 * hardware. Splitting a LOB into chunks permits low latency operations across
 * mixed work loads with values of varying sizes. The stream based APIs serve
 * to insulate the application from the actual representation of the LOB in the
 * underlying storage system.
 * <p>
 * The methods used to read and write LOBs are not atomic.
 * Relaxing the atomicity requirement permits distribution of chunks across the
 * entire store. It's the application's responsibility to coordinate operations
 * on a LOB. The implementation will make a good faith effort to detect
 * concurrent modification of an LOB and throw
 * <code>ConcurrentModificationException</code> when it detects such
 * concurrency conflicts but does not guarantee that it will detect all such
 * conflicts. The safe course of action upon encountering this exception in the
 * context of conflicting write operations is to delete the LOB and replace it
 * with a new value after fixing the application level coordination issue that
 * provoked the exception.
 * <p>
 * Failures during a LOB write operation result in the creation of a
 * <code>partial</code> LOB. The LOB value of a <code>partial</code> LOB is in
 * some intermediate state, where it cannot be read by the application;
 * attempts to <code>getLOB</code> on a partial LOB will result in a
 * <code>PartialLOBException</code>. A partial LOB resulting from an incomplete
 * <code>putLOB</code>, <code>deleteLOB</code> or <code>appendLOB</code>
 * operation can be repaired by retrying the corresponding failed
 * <code>putLOB</code>, <code>deleteLOB</code> or <code>appendLOB</code>
 * operation. Or it can be deleted and a new key/value pair can be created in
 * its place. The documentation associated with individual LOB methods
 * describes their behavior when invoked on partial LOBs in greater detail.
 * <p>
 * LOBs, due to their representation as a sequence of chunks, must be accessed
 * exclusively via the LOB APIs defined in this interface. The family of
 * <code>KVStore.get</code> methods when applied to a LOB key will be presented
 * with a value that is internal to the KVS implementation and cannot be used
 * directly by the application.
 * <p>
 * Keys associated with LOBs must have a trailing suffix string (as defined by
 * {@link KVStoreConfig#getLOBSuffix}) at the end of their final Key component.
 * This requirement permits non-LOB methods to check for inadvertent
 * modifications to LOB objects.
 * <p>
 * All methods in this class verify that the key used to access LOBs meets this
 * trailing suffix requirement and throw <code>IllegalArgumentException</code>
 * if the verification fails. The use of the name <code>lobKey</code> for the
 * key argument in the method signatures below emphasizes this requirement.
 * <p>
 * Here is a summary of LOB related key checks performed across all methods:
 * <ul>
 * <li>
 * All non-LOB write operations check for the absence of the LOB suffix as part
 * of the other key validity checks. If the check fails it will result in a
 * IllegalArgumentException.</li>
 * <li>
 * All non-LOB read operations return the associated opaque value used
 * internally to construct a LOB stream.</li>
 * <li>
 * All LOB write and read operations in this interface check for the presence
 * of the LOB suffix. If the check fails it will result in an
 * IllegalArgumentException.</li>
 * </ul>
 * <p>
 * Example:
 * <p>
 * The following simplified code fragment loads an mp3 file named "f1.mp3" into
 * the store associating it with the key "f1.lob". Note that this interface is
 * a superinterface for {@link oracle.kv.KVStore}, so to access the LOB methods
 * you simply create and use a KVStore handle.
 * <pre>
 * File file = new File(&quot;f1.mp3&quot;);
 *
 * FileInputStream fis = new FileInputStream(file);
 * Version version = store.putLOB(Key.createKey(&quot;f1.lob&quot;),
 *                                fis,
 *                                Durability.COMMIT_WRITE_NO_SYNC,
 *                                5, TimeUnit.SECONDS);
 * </pre>
 * <p>
 * The following simplified code fragment retrieves the LOB that was loaded
 * above and computes its size:
 * <pre>
 * InputStreamVersion istreamVersion =
 *     store.getLOB(Key.createKey(&quot;f1.lob&quot;),
 *                  Consistency.NONE_REQUIRED,
 *                  5, TimeUnit.SECONDS);
 *
 * InputStream stream = istreamVersion.getInputStream();
 * int byteCount = 0;
 * while (stream.read() != -1) {
 *     byteCount++;
 * }
 * </pre>
 *
 * @since 2.0
 */
public interface KVLargeObject {

    /**
     * The enumeration defines the states associated with a LOB.
     *
     * @since 2.1.55
     * @hidden.see {@link #writeFastExternal FastExternalizable format}
     */
    public static enum LOBState implements FastExternalizable {
        /**
         * Denotes a LOB resulting from an incomplete <code>put</code>
         * operation. It can be completed by retrying the put operation, thus
         * proceeding from where the <code>put</code> failed or it can be
         * deleted and the <code>put</code> can be retried afresh.
         */
        PARTIAL_PUT(0),

        /**
         * Denotes a LOB resulting from an incomplete <code>append</code>
         * operation. It can be completed by retrying the append operation,
         * thus proceeding from where the <code>append</code> failed or it can
         * be deleted and the LOB can be recreated in its entirety.
         */
        PARTIAL_APPEND(1),

        /**
         * Denotes a LOB resulting from an incomplete <code>delete</code>
         * operation. It can be deleted or replaced by a new <code>put</code>
         * operation.
         */
        PARTIAL_DELETE(2),

        /**
         * Denotes a LOB that has been completed. The LOB can be read, deleted,
         * or can be modified by initiating a new <code>append</code>
         * operation.
         */
        COMPLETE(3);

        private static final LOBState[] VALUES = values();

        private LOBState(int ordinal) {
            if (ordinal != ordinal()) {
                throw new IllegalArgumentException("Wrong ordinal");
            }
        }

        static LOBState readFastExternal(
            DataInput in, @SuppressWarnings("unused") short serialVersion)
            throws IOException {

            final int ordinal = in.readUnsignedByte();
            try {
                return VALUES[ordinal];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                    "unknown LOBState: " + ordinal);
            }
        }

        /**
         * Writes this object to the output stream.  Format:
         * <ol>
         * <li> ({@code byte}) <i>ordinal</i> // {@link #PARTIAL_PUT}=0, {@link
         *      #PARTIAL_APPEND}=1, {@link #PARTIAL_DELETE}=2, {@link
         *      #COMPLETE}=3
         * </ol>
         *
         * @hidden For internal use only
         */
        @Override
        public void writeFastExternal(DataOutput out, short serialVersion)
            throws IOException {

            out.writeByte(ordinal());
        }
    }

    /**
     * Put a key/LOB value pair, inserting new value or overwriting an existing
     * pair as appropriate. If a new key/LOB value pair was successfully
     * inserted or updated, the (non-null) version of the new KV pair is
     * returned. Failures result in one of the exceptions listed below being
     * thrown.
     * <p>
     * The value associated with the large object (LOB) is obtained by reading
     * the <code>InputStream</code> associated with the <code>lobStream</code>
     * parameter. The stream must be positioned at the first byte of the LOB
     * value and must return -1 after the last byte of the LOB has been
     * fetched. For best performance the stream should support an efficient
     * implementation of {@link InputStream#read(byte[], int, int)}. The stream
     * implementation is not required to support the {@link InputStream#mark}
     * and {@link InputStream#reset} methods, that is,
     * {@link InputStream#markSupported} may return false. If the methods are
     * supported, they may be used during internal retry operations. Such retry
     * operations are otherwise transparent to the application.
     * <p>
     * This method, like all LOB methods, is not atomic. Failures (like the
     * loss of network connectivity) while an insert operation is in progress
     * may result in a <i>partially inserted</i> LOB.
     * <p>
     * If the method detects a <i>partially inserted</i> LOB, it will skip
     * reading the bytes that were already loaded and resume insertion of the
     * LOB after ensuring that the trailing bytes in the partial LOB match the
     * ones supplied by the <code>lobStream</code>. The number of bytes that
     * are matched is determined by the configuration parameter {@link
     * KVStoreConfig#getLOBVerificationBytes()}. If the trailing bytes do not
     * match, or the stream does not skip to the requested location, it throws
     * an <code>IllegalArgumentException</code>.
     * <p>
     * A partially deleted LOB is deleted in its entirety and replaced with the
     * new key/value pair.
     * </p>
     *
     * @param lobKey the key associated with the LOB.
     *
     * @param lobStream the stream of bytes representing the LOB as described
     * earlier.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param lobTimeout is an upper bound on the time taken for storing each
     * chunk. A best effort is made not to exceed the specified limit. If zero,
     * the {@link KVStoreConfig#getLOBTimeout} value is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter and may be null
     * only if timeout is zero.
     *
     * @return the Version associated with the newly inserted LOB
     *
     * @throws PartialLOBException if it is invoked on a <i>partially
     * updated</i> LOB.
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the insertion was in progress.
     *
     * @throws IOException if one is generated by the <code>lobStream</code>.
     *
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     */
    public Version putLOB(Key lobKey,
                          InputStream lobStream,
                          Durability durability,
                          long lobTimeout,
                          TimeUnit timeoutUnit)
        throws PartialLOBException,
               ConcurrentModificationException,
               IOException;

    /**
     * Put a key/LOB value pair, but only if the key either has no value or has
     * a partially inserted or deleted LOB value present. Returns null if the
     * lobKey is associated with a complete LOB, and throws PartialLOBException
     * if the LOB has been partially appended. Its behavior is otherwise
     * identical to {@link #putLOB putLOB}. Like the {@link #putLOB putLOB}
     * operation, it will resume the insertion if it encounters a <i>partially
     * inserted</i> LOB.
     *
     * @param lobKey the key associated with the LOB.
     *
     * @param lobStream the stream of bytes representing the LOB as described
     * earlier.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param lobTimeout is an upper bound on the time taken for storing each
     * chunk. A best effort is made not to exceed the specified limit. If zero,
     * the {@link KVStoreConfig#getLOBTimeout} value is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value, or null if an existing value is
     * present and the put is unsuccessful.
     *
     * @throws PartialLOBException if it is invoked on a <i>partially
     * appended</i> LOB.
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the insertion was in progress.
     *
     * @throws IOException if one is generated by the <code>lobStream</code>
     *
     * @see #putLOB
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     */
    public Version putLOBIfAbsent(Key lobKey,
                                  InputStream lobStream,
                                  Durability durability,
                                  long lobTimeout,
                                  TimeUnit timeoutUnit)
        throws PartialLOBException,
               ConcurrentModificationException,
               IOException;

    /**
     * Put a key/LOB value pair, but only if a complete value for the given key
     * is present. If the <code>lobKey</code> is absent it returns null.
     * Its behavior is otherwise identical to {@link #putLOB putLOB}.
     * Like the {@link #putLOB putLOB} operation, it will resume
     * the insertion if it encounters a <i>partially inserted</i> LOB.
     *
     * @param lobKey the key associated with the LOB.
     *
     * @param lobStream the stream of bytes representing the LOB as described
     * earlier.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param lobTimeout is an upper bound on the time taken for storing each
     * chunk. A best effort is made not to exceed the specified limit. If zero,
     * the {@link KVStoreConfig#getLOBTimeout} value is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter and may be null
     * only if timeout is zero.
     *
     * @return the version of the new value, or null if no existing value is
     * present and the put is unsuccessful.
     *
     * @throws PartialLOBException if it is invoked on a <code>partially
     * appended</code> LOB.
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the insertion was in progress.
     *
     * @throws IOException if one is generated by the <code>lobStream</code>
     *
     * @see #putLOB
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     */
    public Version putLOBIfPresent(Key lobKey,
                                   InputStream lobStream,
                                   Durability durability,
                                   long lobTimeout,
                                   TimeUnit timeoutUnit)
        throws PartialLOBException,
               ConcurrentModificationException,
               IOException;


    /**
     * Returns an InputStream representing the underlying LOB value associated
     * with the key.
     * <p>
     * An attempt to access a partial LOB will result in a
     * <code>PartialLOBException</code> being thrown.
     * <p>
     * The returned input stream can be read to obtain the value associated
     * with the LOB. The application can use the InputStream method
     * {@link InputStream#skip(long)} in conjunction with
     * {@link InputStream#mark} and {@link InputStream#reset} to read random
     * byte ranges within the LOB.
     * <p>
     * Reading the input stream can result in various exceptions like
     * <code>ConsistencyException</code>,
     * <code>RequestTimeoutException</code>,
     * <code>ConcurrentModificationException</code> or
     * <code>FaultException</code>, etc.
     * All such exceptions are wrapped in <code>IOException</code>. Specialized
     * stream readers can use {@link Exception#getCause()} to examine the
     * underlying cause and take appropriate action. The application must
     * ensure that the KVStore handle is not closed before the contents of the
     * returned InputStream have been read. Such a premature close will result
     * in an IOException when the stream is subsequently read.
     * </p>
     *
     * @param lobKey the key used to lookup the key/value pair.
     *
     * @param consistency determines the consistency associated with the read
     * used to lookup and read the value via the returned stream object. If
     * null, the {@link KVStoreConfig#getConsistency default consistency} is
     * used. Note that Consistency.Version cannot be used to access a LOB
     * that's striped across multiple partitions; it will result in an
     * IllegalArgumentException being thrown.
     *
     * @param lobTimeout is an upper bound on the time interval for
     * retrieving a chunk or its associated metadata. A best effort is made not
     * to exceed the specified limit. If zero, the
     * {@link KVStoreConfig#getLOBTimeout} value is used.
     * Note that this timeout
     * also applies to read operations performed on the returned stream object.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return the input stream and version associated with the key, or null
     * if there is no LOB associated with the key.
     *
     * @throws PartialLOBException if it is invoked on a partial LOB
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the operation was in progress
     *
     * @see <a href="../KVStore.html#readExceptions">Read exceptions</a>
     */
    public InputStreamVersion getLOB(Key lobKey,
                                     Consistency consistency,
                                     long lobTimeout,
                                     TimeUnit timeoutUnit)
        throws PartialLOBException,
               ConcurrentModificationException;


    /**
     * Deletes the LOB associated with the key. This method can be used to
     * delete partial LOBs.
     *
     * @param lobKey the key associated with the LOB.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param lobTimeout is an upper bound on the time taken for deleting each
     * chunk. A best effort is made not to exceed the specified limit. If zero,
     * the {@link KVStoreConfig#getLOBTimeout} value is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter, and may be null
     * only if timeout is zero.
     *
     * @return true if the delete is successful, or false if no existing value
     * is present. Note that the method will return true if a partial LOB was
     * deleted.
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the operation was in progress.
     *
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     */
    public boolean deleteLOB(Key lobKey,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws ConcurrentModificationException;

    /**
     * Appends to a value of an existing LOB key/value pair. If the append was
     * successful, the (non-null) version of the modified KV pair is returned.
     * This method is most efficient for large granularity append operations
     * where the value being appended is 128K bytes or greater in size.
     *
     * <p>
     * The value to be appended to the large object (LOB) is obtained by
     * reading the <code>InputStream</code> associated with the
     * <code>lobAppendStream</code> parameter. The stream must be positioned at
     * the first byte of the value to be appended and must return -1 after the
     * last byte of the LOB has been fetched. For best performance the stream
     * should support an efficient implementation of
     * {@link InputStream#read(byte[], int, int)}. The stream implementation is
     * not required to support the {@link InputStream#mark} and
     * {@link InputStream#reset} methods, that is,
     * {@link InputStream#markSupported} may return false. If the methods are
     * supported, they may be used during internal retry operations. Such retry
     * operations are otherwise transparent to the application.
     * <p>
     * This method, like all LOB methods, is not atomic. Failures (like the
     * loss of network connectivity) while an append operation is in progress
     * may result in a <i>partially appended</i> LOB. The append operation can
     * be resumed by repeating the <code>appendLOB</code> operation.
     * <p>
     * If the method detects a <i>partially appended</i> LOB, it will skip
     * reading the bytes that were already appended and resume appending to the
     * LOB after ensuring that the trailing bytes in the partial LOB match the
     * ones supplied by the <code>lobAppendStream</code>. The number of bytes
     * that are matched is determined by the configuration parameter
     * {@link KVStoreConfig#getLOBVerificationBytes()}. If the trailing bytes
     * do not match, or the stream does not skip to the requested location,
     * it throws an <code>IllegalArgumentException</code>.
     * </p>
     *
     * @param lobKey the key associated with the existing LOB.
     *
     * @param lobAppendStream the stream of bytes representing just the value
     * to be appended.
     *
     * @param durability the durability associated with the operation. If null,
     * the {@link KVStoreConfig#getDurability default durability} is used.
     *
     * @param lobTimeout is an upper bound on the time taken for storing each
     * chunk. A best effort is made not to exceed the specified limit. If zero,
     * the {@link KVStoreConfig#getLOBTimeout} value is used.
     *
     * @param timeoutUnit is the unit of the timeout parameter and may be null
     * only if timeout is zero.
     *
     * @return the Version associated with the updated LOB
     *
     * @throws PartialLOBException if it is invoked on a <i>partially
     * inserted</i>LOB or <i>partially deleted</i> LOB.
     *
     * @throws ConcurrentModificationException if it detects that an attempt
     * was made to modify the object while the insertion was in progress.
     *
     * @throws IOException if one is generated by the <code>lobStream</code>.
     *
     * @see <a href="../KVStore.html#writeExceptions">Write exceptions</a>
     *
     * @since 2.1.55
     */
    public Version appendLOB(Key lobKey,
                             InputStream lobAppendStream,
                             Durability durability,
                             long lobTimeout,
                             TimeUnit timeoutUnit)
        throws PartialLOBException,
               ConcurrentModificationException,
               IOException;

}
