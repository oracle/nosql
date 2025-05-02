/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.impl.api.ops;

import static oracle.kv.Durability.COMMIT_WRITE_NO_SYNC;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_APPEND;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_DELETE;
import static oracle.kv.lob.KVLargeObject.LOBState.PARTIAL_PUT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import oracle.kv.Consistency;
import oracle.kv.Depth;
import oracle.kv.Direction;
import oracle.kv.Durability;
import oracle.kv.Durability.ReplicaAckPolicy;
import oracle.kv.Durability.SyncPolicy;
import oracle.kv.Key;
import oracle.kv.Operation;
import oracle.kv.OperationFactory;
import oracle.kv.RequestTimeoutException;
import oracle.kv.Value;
import oracle.kv.Version;
import oracle.kv.impl.api.KVStoreImpl;
import oracle.kv.impl.api.lob.DeleteOperation;
import oracle.kv.impl.api.lob.WriteOperation;
import oracle.kv.impl.fault.OperationFaultException;
import oracle.kv.impl.test.TestHook;
import oracle.kv.impl.util.FilterableParameterized;
import oracle.kv.lob.InputStreamVersion;
import oracle.kv.lob.KVLargeObject.LOBState;
import oracle.kv.lob.PartialLOBException;

import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;

// TODO:
// 1) repeated failures when resuming puts and appends
// 2) Vary LOB verification bytes
// 3) Simple put with retained LOBS to verify no unintended interaction

@RunWith(FilterableParameterized.class)
public class LOBOperationTest extends ClientTestBase {

    /**
     * Use this as the first major key component for all test keys so we can
     * distinguish them from other entries that might be created automatically,
     * like key distribution stats.
     */
    private static final String KEY_PREFIX = "lobtest";

    private static final Key KEY_PREFIX_KEY = Key.createKey(KEY_PREFIX);

    /**
     * Number of chunks per partition. Scaled down for the test to a minimal
     * number: two edge cases and a middle.
     */
    private static final int CHUNKS_PER_PARTITION = 3;

    /**
     * The chunk size used for these tests. Minimal sized chunk to speed up
     * unit tests.
     */
    private static final int TEST_CHUNK_SIZE = 4;

    /** Provide random values for use by all test methods. */
    private static final Random random = new Random();

    public LOBOperationTest(boolean secure) {
        super(secure);
    }

    @Override
    public void setUp() throws Exception {
        /*
         *  This test does not create any tables, so no need to run
         *  multi-region table mode.
         */
        Assume.assumeFalse("Test should not run in MR table mode", mrTableMode);
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {

        oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(0);
        super.tearDown();
    }

    @Override
    void customizeConfig() {
        config.setLOBChunksPerPartition(CHUNKS_PER_PARTITION);
        config.setLOBChunkSize(TEST_CHUNK_SIZE);
        config.setLOBVerificationBytes(TEST_CHUNK_SIZE/2);
    }

    private void lobConfigExceptionsTest() {
        try {
            config.setLOBSuffix(null);
            fail("expected exception");
        } catch (IllegalArgumentException IAE) {
            /* Expected. */
        }

        try {
            config.setLOBSuffix("");
            fail("expected exception");
        } catch (IllegalArgumentException IAE) {
            /* Expected. */
        }

        try {
            config.setLOBVerificationBytes(-1);
            fail("expected exception");
        } catch (IllegalArgumentException IAE) {
            /* Expected. */
        }
    }

    /**
     * Demonstrates use of a File for uploading a LOB
     */
    private void docExampleTest() throws IOException {

        /* Create the file */
        final int lobSize = 1024 + 13;
        final File file = File.createTempFile("f01", "mp3");
        initializeFile(lobSize, file);

        final Key lobKey = createKey("f1.lob");
        while (true) {

            final FileInputStream fis = new FileInputStream(file);
            try {
                /* Upload into store. */
                store.putLOB(lobKey,
                             fis,
                             Durability.COMMIT_WRITE_NO_SYNC,
                             5, TimeUnit.SECONDS);
                break;
            } catch (RequestTimeoutException rte) {
                continue;
            } finally {
                fis.close();
            }
        }

        /* Retrieve stream handle to LOB. */
        final InputStream lis =
            store.getLOB(lobKey,
                         Consistency.NONE_REQUIRED,
                         5, TimeUnit.SECONDS).getInputStream();

        {
            /* Verify LOB contents */
            final FileInputStream fis = new FileInputStream(file);
            while (true) {
                final int b1 = fis.read();
                final int b2 = lis.read();

                assertEquals(b1, b2);

                if (b1 == -1) {
                    break;
                }
            }
            fis.close();
        }


        /* Illustrate use of appendLOB API, no error handling. */

        Version result = null;
        int RETRY_MAX = 10;
        for (int i=0; i < RETRY_MAX ; i++) {
            final FileInputStream fis = new FileInputStream(file);
            try {
                result = store.appendLOB(lobKey,
                                        fis,
                                        COMMIT_WRITE_NO_SYNC,
                                        5, TimeUnit.SECONDS);
                break;
            } catch (PartialLOBException pe) {
                /*
                 * A LOB that was the result of a partial put operation.
                 * retry the put operation.
                 */
                // throw new IllegalStateException("some application level bug");
                fail("some application level bug");
            } catch (OperationFaultException ofe) {
                /*
                 * the append LOB operation failed retry the append operation.
                 * On the subsequent invocation, the appendLOB method will skip
                 * any bytes that were already written by the previous failed
                 * attempts at invoking appendLOB, by invoking fis.skip(long)
                 */
                continue;
            } finally {

                fis.close();
            }
        }

        assertNotNull(result);

        store.deleteLOB(lobKey, COMMIT_WRITE_NO_SYNC, 5, TimeUnit.SECONDS);

        file.delete();
    }

    /**
     * Create the specified key with KEY_PREFIX as an added initial major
     * component.
     */
    private static Key createKey(String major) {
        return Key.createKey(Arrays.asList(KEY_PREFIX, major));
    }

    private void initializeFile(final int lobSize, final File file)
        throws FileNotFoundException,
        IOException {
        final TestInputStream tis = new TestInputStream(lobSize);
        file.deleteOnExit();
        final FileOutputStream fos = new FileOutputStream(file);

        for (int b = tis.read(); b != -1; b = tis.read()) {
            fos.write(b);
        }

        tis.close();
        fos.close();
        assertEquals(lobSize, file.length());
    }

    /**
     * Test cumulative appends.
     */
    private void cumulativeAppendTests()
        throws IOException {

        final Key lobKey = createKey("l" + config.getLOBSuffix());

        /* Create zero byte LOB */
        final InputStream putStream = new TestInputStream(0);
        store.putLOB(lobKey,
                     putStream,
                     Durability.COMMIT_WRITE_NO_SYNC,
                     5, TimeUnit.SECONDS);
        putStream.close();

        /* Repeated appends to build up a large LOB */
        final int appends[] = new int[(2 * TEST_CHUNK_SIZE) + 1];

        for (int i=0; i <= 2 * TEST_CHUNK_SIZE; i++) {
            appends[i] = i;
            final InputStream appendStream = new TestInputStream(i);
            Version v = store.appendLOB(lobKey, appendStream,
                                        COMMIT_WRITE_NO_SYNC,
                                        1, TimeUnit.MINUTES);
            assertNotNull(v);
            verifyLOB(lobKey, Arrays.copyOf(appends, i+1));
        }

        assertTrue(store.deleteLOB(lobKey, COMMIT_WRITE_NO_SYNC,
                                   1, TimeUnit.MINUTES));
        verifyEmptyStore();
    }

    /**
     * Test sunny day appends with varying initial and append sizes. Test zero
     * initial and append sizes in particular as boundary conditions.
     */
    private void basicPutAndAppendTests()
        throws IOException {

        final Key lobKey = createKey("l" + config.getLOBSuffix());

        /* Try append with varying initial and append sizes */
        final int limitSize = (CHUNKS_PER_PARTITION * TEST_CHUNK_SIZE) + 1;
        for (int lobSize = 0; lobSize <= limitSize; lobSize++) {
            for (int appendSize=0;
                appendSize <= (TEST_CHUNK_SIZE + 1);
                appendSize++) {

                final InputStream putStream = new TestInputStream(lobSize);
                Version putVersion = store.putLOB(lobKey,
                                                  putStream,
                                                  COMMIT_WRITE_NO_SYNC,
                                                  5, TimeUnit.SECONDS);
                putStream.close();

                /* Verify put is correct. */
                verifyLOB(lobKey, lobSize);

                final InputStream appendStream =
                    new TestInputStream(appendSize);
                final Version appendVersion =
                    store.appendLOB(lobKey, appendStream,
                                    COMMIT_WRITE_NO_SYNC,
                                    1, TimeUnit.MINUTES);
                appendStream.close();

                assertNotNull(appendVersion);
                assertTrue(!putVersion.equals(appendVersion));
                verifyLOB(lobKey, lobSize, appendSize);
            }
        }
        assertTrue(store.deleteLOB(lobKey, COMMIT_WRITE_NO_SYNC,
                                   1, TimeUnit.MINUTES));
        verifyEmptyStore();
    }

    /**
     * Test put resume with varying full sizes that are continued from varying
     * partial sizes. Both full and partial sizes are designed to cover (scaled
     * down sizes) all the way to 2 super chunks. Verify behavior of partially
     * inserted LOBs:
     *
     * 1) Throw exceptions on an attempt to read
     * 2) Verify that puts can be resumed.
     * 3) Verify that they can be deleted with no detritus.
     *
     * TODO: special case for 0,0
     */
    private void putResumeTests()
        throws IOException {

        final Key lobKey = createKey("l" + config.getLOBSuffix());

        /* Try append with varying initial and append sizes */
        final int limitSize = (CHUNKS_PER_PARTITION * TEST_CHUNK_SIZE) + 1;
        for (int lobSize=1; lobSize <= limitSize; lobSize++) {
            for (int errorByteIndex=1;
                errorByteIndex < lobSize; errorByteIndex++) {

                createPartiallyInsertedLOB(lobKey, lobSize, errorByteIndex);

                verifyUnreadablePartialLOB(lobKey, PARTIAL_PUT);

                /*
                 * Test resuming a put with a too-short input stream.  When
                 * more than a chunk-sized amount of data was written, the
                 * underlying implementation will attempt to skip, and won't be
                 * able to skip far enough.
                 */
                if (errorByteIndex > TEST_CHUNK_SIZE) {
                    final TestInputStream lobStream = new TestInputStream(1);
                    try {
                        store.putLOB(lobKey, lobStream, COMMIT_WRITE_NO_SYNC,
                                     1, TimeUnit.SECONDS);
                        fail("Expected IllegalArgumentException");
                    } catch (IllegalArgumentException e) {
                        final String expected =
                            "The LOB input stream did not skip the" +
                            " requested number of bytes";
                        final String msg = e.getMessage();
                        assertTrue("Message: " + msg,
                                   (msg != null) && msg.contains(expected));
                    }
                }

                final TestInputStream lobStream = new TestInputStream(lobSize);
                /* resume put */
                Version v = store.putLOB(lobKey, lobStream,
                                         COMMIT_WRITE_NO_SYNC,
                                         1, TimeUnit.MINUTES);
                checkVerifyBytes(0, errorByteIndex,
                                 config.getLOBVerificationBytes(),
                                 lobStream);
                assertNotNull(v);
                verifyLOB(lobKey, lobSize);
                assertTrue(store.deleteLOB(lobKey, COMMIT_WRITE_NO_SYNC,
                                           1, TimeUnit.MINUTES));
            }
        }

        verifyEmptyStore();
    }

    /**
     * Verify:
     *  1) Append operations can be resumed with varying initial and
     * appended LOB sizes
     *  2) That append partial LOBs cannot be read
     *  3) The configured  number of bytes are verified upon resumption.
     */
    private void appendResumeTests()
        throws IOException {

        final Key lobKey = createKey("l" + config.getLOBSuffix());

        /* Try append with varying initial and append sizes */
        final int limitSize = (2 * CHUNKS_PER_PARTITION * TEST_CHUNK_SIZE) + 1;

        for (int putSize=0; putSize <= limitSize; putSize++) {

            for (int appendSize=0;
                appendSize <= (TEST_CHUNK_SIZE + 2);
                appendSize++) {

                for (int errorIndex=0; errorIndex < appendSize;
                    errorIndex++) {

                    createPartiallyAppendedLOB(lobKey, putSize, appendSize,
                                               errorIndex);

                    verifyUnreadablePartialLOB(lobKey, PARTIAL_APPEND);

                    /*
                     * Test resuming an append with a too-short input stream.
                     * When more than a chunk-sized amount of data was written
                     * by the append and it started on a chunk boundary, the
                     * underlying implementation will attempt to skip, and
                     * won't be able to skip far enough.
                     */
                    if (((putSize % TEST_CHUNK_SIZE) == 0) &&
                        errorIndex > TEST_CHUNK_SIZE) {
                        final TestInputStream lobStream =
                            new TestInputStream(1);
                        try {
                            store.appendLOB(lobKey, lobStream,
                                            COMMIT_WRITE_NO_SYNC,
                                            1, TimeUnit.SECONDS);
                            fail("Expected IllegalArgumentException");
                        } catch (IllegalArgumentException e) {
                            final String expected =
                                "The LOB input stream did not skip the" +
                                " requested number of bytes";
                            final String msg = e.getMessage();
                            assertTrue("Message: " + msg,
                                       (msg != null) &&
                                       msg.contains(expected));
                        }
                    }

                    /* Verify varying number of bytes. */
                    for (int verifyBytes = 0;
                        verifyBytes <= (putSize + errorIndex);
                        verifyBytes++) {
                        /*
                        System.err.println("put size:" + putSize +
                                           " append size:" + appendSize +
                                           " error index:" + errorIndex +
                                           " verify bytes:" + verifyBytes);
                         */
                        WriteOperation.setTestVerificationByteCount(verifyBytes);
                        final TestInputStream appendStream =
                            new TestInputExceptionStream(appendSize, errorIndex);
                        try {
                            store.appendLOB(lobKey, appendStream,
                                            COMMIT_WRITE_NO_SYNC,
                                            1, TimeUnit.MINUTES);
                            fail("IOE expected. append stream size:" + appendSize +
                                 " IOE byte index:" + errorIndex);
                        } catch (IOException  ioe) {
                            /* Expected. */
                        }
                        checkVerifyBytes(putSize, errorIndex,
                                         verifyBytes,
                                         appendStream);
                    }
                    WriteOperation.revertTestVerificationByteCount();
                    /* retry append. */
                    final TestInputStream appendStream =
                        new TestInputStream(appendSize);
                    Version v =
                        store.appendLOB(lobKey, appendStream,
                                        COMMIT_WRITE_NO_SYNC,
                                        1, TimeUnit.MINUTES);
                    checkVerifyBytes(putSize, errorIndex,
                                     config.getLOBVerificationBytes(),
                                     appendStream);
                    appendStream.close();
                    assertNotNull(v);
                    verifyLOB(lobKey, putSize, appendSize);
                    assertTrue(store.deleteLOB(lobKey,
                                               COMMIT_WRITE_NO_SYNC,
                                               1, TimeUnit.MINUTES));
                }
            }
        }

        WriteOperation.revertTestVerificationByteCount();
        verifyEmptyStore();
    }

    /**
     * Checks that the number of verified bytes actually verified, matches
     * number of bytes expected to be verified for a resumed operation.
     */
    private void checkVerifyBytes(long putSize,
                                  long errorIndex,
                                  long configVerifyBytes,
                                  TestInputStream lobStream) {

        long maxPersistBytes = putSize + errorIndex;
        long maxChunkAlignedSize =
            maxPersistBytes - (maxPersistBytes % TEST_CHUNK_SIZE);

        final long lastPersistByte =
            (putSize > maxChunkAlignedSize) ? putSize : maxChunkAlignedSize;

        long verificationStart = lastPersistByte - configVerifyBytes;

        if (verificationStart < putSize) {
            assertEquals(0, lobStream.skipBytes);
            return;
        }

        long skipBytes = verificationStart - putSize;
        assertEquals(skipBytes, lobStream.skipBytes);
    }

    /**
     * Create a partially appended LOB. It can only be resumed by an append
     * operation.
     */
    private void createPartiallyAppendedLOB(Key lobKey,
                                            int putSize,
                                            int appendSize,
                                            int errorByteIndex)
        throws IOException {

        final InputStream putStream = new TestInputStream(putSize);

        store.putLOB(lobKey,
                     putStream,
                     COMMIT_WRITE_NO_SYNC,
                     5, TimeUnit.SECONDS);
        putStream.close();

        final InputStream appendStream =
            new TestInputExceptionStream(appendSize, errorByteIndex);
        try {
            store.appendLOB(lobKey, appendStream,
                            COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail("IOE expected. append stream size:" + appendSize +
                 " IOE byte index:" + errorByteIndex);
        } catch (IOException  ioe) {
            /* Expected. */
        }
    }

    /**
     * A single test method is used to avoid the overhead of service setup and
     * teardown for every test.
     */
    @Test
    public void testAll()
        throws Exception {
        utfCompatibilityTest();
        lobCompatibilityTests();

        lobConfigExceptionsTest();
        nonLobKeyWriteExceptionTests();
        lobKeyExceptionTests();

        basicTests();

        concurrentDeleteTests();

        putResumeTests();
        partialPutLOBResumeVerificationTests();

        basicPutAndAppendTests();
        appendResumeTests();
        cumulativeAppendTests();

        readDeleteConflictTests();

        partialLOBExceptionTests();

        streamTests();
        docExampleTest();

        close();
    }


    private void lobCompatibilityTests()
        throws IOException {
       /*
        * Verify that a LOB written in MD version 1 can be read and deleted
        * by version 2
        */
        oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(1);
        final Key key = createKey("k1.lob");

        for (int lobSize=1;
             lobSize <= TEST_CHUNK_SIZE*CHUNKS_PER_PARTITION * 4;
             lobSize += TEST_CHUNK_SIZE) {

            oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(1);

            final InputStream lobStream = new TestInputStream(lobSize);

            Version v = store.putLOB(key, lobStream,
                                     Durability.COMMIT_WRITE_NO_SYNC,
                                     1, TimeUnit.MINUTES);
            assertNotNull(v);
            verifyLOB(key, lobSize);

            /* Switch to new version. */
            oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(0);

            /* Read and verify the lob. */
            verifyLOB(key, lobSize);

            /* Delete the LOB */
            store.deleteLOB(key, Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);

            verifyEmptyStore();
        }

        /*
         * Verify that a partially "put" meta data version 1 lob can be
         * resumed by the current version to yield a complete LOB.
         */
        oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(1);
        final int lobSize = 1000;
        createPartiallyInsertedLOB(key, lobSize, 100);

        oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(0);
        final TestInputStream lobStream = new TestInputStream(lobSize);
        /* resume put */
        store.putLOB(key, lobStream, COMMIT_WRITE_NO_SYNC,
                     1, TimeUnit.MINUTES);
        verifyLOB(key, lobSize);

        /*
         * Verify that a version 1 metadata LOB cannot be appended to.
         */
        try {
            store.appendLOB(key, new TestInputStream(1),
                            Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail("Expected UOE");
        } catch (UnsupportedOperationException uoe) {
            /* Expected */
        }

        store.deleteLOB(key, Durability.COMMIT_WRITE_NO_SYNC,
                        1, TimeUnit.MINUTES);

        oracle.kv.impl.api.lob.Operation.setTestMetadataVersion(0);

        verifyEmptyStore();
    }

    private void utfCompatibilityTest() {

        Charset.availableCharsets();
        final Key lobKey = Key.createKey(Arrays.asList("", "lob", "1234"));

        Value ilkValue = Value.
            createValue(lobKey.toString().getBytes());

        Key lobKey2 = oracle.kv.impl.api.lob.Operation.
            valueToILK(ilkValue);

        assertEquals(lobKey, lobKey2);

        try {
            final Key badKey =
                Key.createKey(Arrays.asList("", "lobX", "1234"));
            Value badValue = Value.
                createValue(badKey.toString().getBytes());

            oracle.kv.impl.api.lob.Operation.valueToILK(badValue);
            fail("expected IAE on bad prefix");
        } catch(IllegalArgumentException iae) {
            /* Expected. */
        }

        try {
            final Key badKey =
                Key.createKey(Arrays.asList("", "lobX", "1234", "111"));
            Value badValue = Value.
                createValue(badKey.toString().getBytes());

            oracle.kv.impl.api.lob.Operation.valueToILK(badValue);
            fail("expected IAE on incorrect prefix length");
        } catch(IllegalArgumentException iae) {
            /* Expected. */
        }
    }

    /**
     * Simulate a concurrent delete, which deletes LOB metadata while a
     * delete operation is in progress.
     */
    private void concurrentDeleteTests()
        throws IOException {

        final int lobSize = 10 * TEST_CHUNK_SIZE;

        final Key lobKey = createKey("k.lob");
        final InputStream lobStream = new TestInputStream(lobSize);

        Version v = store.putLOB(lobKey, lobStream,
                                 COMMIT_WRITE_NO_SYNC,
                                 10, TimeUnit.SECONDS);
        assertNotNull(v);

        DeleteOperation.setDeleteTestHook(new TestHook<Integer>() {
            int chunkNum = 0;

            @Override
            public void doHook(Integer currentChunkNum) {
                if (chunkNum++ == 0) {
                    /* Nested ("concurrent" delete) */
                    assertTrue(store.deleteLOB(lobKey,
                                               Durability.COMMIT_WRITE_NO_SYNC,
                                               1, TimeUnit.MINUTES));
                }
            }
        });

        try {
            store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail ("CME expected");
        } catch (ConcurrentModificationException cme) {
            /* Expected. */
        }

        /* resume normal delete operation. */
        DeleteOperation.setDeleteTestHook(null);
    }

    /**
     * Verifies that operations on the stream returned by a getLOB operation.
     */
    private void streamTests()
        throws IOException {

        final Key lobKey = createKey("k.lob");
        streamExceptionTest(lobKey);

        final int lobSize = 5 * TEST_CHUNK_SIZE + (TEST_CHUNK_SIZE/2);
        streamTestsInternal(lobSize, lobKey);

        /* Special case LOB thats an integral number of chunks */
        streamTestsInternal(5 * TEST_CHUNK_SIZE, lobKey);
    }

    private void streamTestsInternal(final int lobSize, final Key lobKey)
        throws  IOException {
        InputStream lobStream = new TestInputStream(lobSize);

        final Version v = store.putLOB(lobKey, lobStream,
                                       Durability.COMMIT_WRITE_NO_SYNC,
                                       10, TimeUnit.SECONDS);
        assertNotNull(v);
        streamReadTest(lobSize, lobKey);
        streamMarkReset(lobSize, lobKey);
        streamSkipTest(lobSize, lobKey);
        streamEOSTest(lobSize, lobKey);

        store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                        1, TimeUnit.MINUTES);
    }

    /**
     * Test stream operations when positioned at EOS
     */
    private void streamEOSTest(int lobSize, Key lobKey)
        throws IOException {

        InputStream is =
            store.getLOB(lobKey, null, 0, null).getInputStream();

        is.mark(100);

        verifyStream(is, 0, lobSize);

        assertEquals(-1, is.read());

        assertEquals(-1, is.read(new byte[10], 0, 5));

        assertEquals(0, is.skip(10));

        is.reset();
        verifyStream(is, 0, lobSize);

        is.mark(100); /* mark while at EOS */

        is.reset(); /* stay at EOS */
        assertEquals(-1, is.read());
    }

    /**
     * Verify the skip() operation
     */
    private void streamSkipTest(int lobSize, Key lobKey)
        throws IOException {

        {
            InputStream is1 =
                store.getLOB(lobKey, null, 0, null).getInputStream();

            /* Special cases. */
            assertEquals(0, is1.skip(-1));
            assertEquals(0, is1.skip(0));
            assertEquals(lobSize, is1.skip(lobSize));
            assertEquals(-1, is1.read());
            assertEquals(0, is1.skip(lobSize));

            is1.close();

            is1 = store.getLOB(lobKey, null, 0, null).getInputStream();
            /* Skip to chunk/superchunk boundaries */
            for (int i=0; i < lobSize/TEST_CHUNK_SIZE; i++) {
                verifyByte(i * TEST_CHUNK_SIZE, is1.read());
                assertEquals(TEST_CHUNK_SIZE - 1,
                             is1.skip(TEST_CHUNK_SIZE - 1 /* 1 byte read */));
            }

            /* consume what's left. */
            assertEquals(lobSize % TEST_CHUNK_SIZE, is1.skip(lobSize+1));

            is1.close();
        }

        /* Exercise random reads + skips */
        for (int i=0; i < lobSize; i++) {
            final InputStream is =
                store.getLOB(lobKey, null, 0, null).getInputStream();
            /* read i bytes into the stream. */
            for (int j=0; j < i; j++) {
                is.read();
            }

            /* Advance the stream  by a random skip */
            final int skipLength = random.nextInt(lobSize - i);
            is.skip(skipLength);
            final int trailBytes = (lobSize - i - skipLength);
            verifyStream(is, i + skipLength, trailBytes);
            is.close();
        }
    }

    private void streamExceptionTest(Key lobKey)
        throws IOException {

        InputStream lobStream = new TestInputStream(10);

        final Version v = store.putLOB(lobKey, lobStream,
                                       Durability.COMMIT_WRITE_NO_SYNC,
                                       10, TimeUnit.SECONDS);

        assertNotNull(v);

        final InputStream is =
            store.getLOB(lobKey, null, 0, null).getInputStream();

        new ExpectIOBException() {

            @Override
            void op() throws IOException {
                byte b[] = new byte[1];
               is.read(b, 0, 5 /* too many bytes for buffer */);
            }
        }.exec();

        new ExpectIOBException() {

            @Override
            void op() throws IOException {
                byte b[] = new byte[1];
               is.read(b, -1, 1);
            }
        }.exec();

        new ExpectIOBException() {

            @Override
            void op() throws IOException {
                byte b[] = new byte[1];
               is.read(b, 0, -1);
            }
        }.exec();

        is.close();

        /* Now check for IOE resulting from ops on a closed stream. */
        new ExpectIOException() {

            @Override
            void op() throws IOException {
               is.read();
            }
        }.exec();

        new ExpectIOException() {

            @Override
            void op() throws IOException {
                byte b[] = new byte[1];
               is.read(b, 0, 1);
            }
        }.exec();

        new ExpectIOException() {

            @Override
            void op() throws IOException {
               is.skip(0);
            }
        }.exec();

        new ExpectIOException() {

            @Override
            void op() throws IOException {
               is.reset();
            }
        }.exec();

        store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                        1, TimeUnit.MINUTES);
    }

    /**
     * Read in varying sizes and verify the result.
     */
    private void streamReadTest(int lobSize, Key lobKey)
        throws IOException {

        for (int readLen=1; readLen < lobSize; readLen++) {
            InputStream is =
                store.getLOB(lobKey, null, 0, null).getInputStream();

            final byte b[] = new byte[lobSize];
            int off = 0;

            /* read the stream in varying size chunks. */
            while (lobSize != off) {
                int len = Math.min((lobSize - off), readLen);
                int bytes = is.read(b, off, len);
                if (bytes == -1) {
                    assertEquals(off, lobSize);
                    break;
                }
                off += bytes;
            }
            verifyBytes(b);
        }
    }

    /**
     * Verifies mark/reset operations on a LOB stream
     */
    private void streamMarkReset(final int lobSize, final Key lobKey)
        throws IOException {
        {
            final InputStream fis =
                store.getLOB(lobKey, null, 0, null).getInputStream();
            assertTrue(fis.markSupported());

            new ExpectIOException() {

                @Override
                void op() throws IOException {
                    fis.reset(); /* reset without a preceding mark */
                }
            }.exec();

            InputStream is =
                store.getLOB(lobKey, null, 0, null).getInputStream();
            is.mark(100); is.reset(); /* Explicit mark */
            verifyStream(is, 0, lobSize);
        }

        for (int i=0; i < lobSize; i++) {
            final InputStream is =
                store.getLOB(lobKey, null, 0, null).getInputStream();
            /* Advance the stream */
            is.skip(i);
            is.mark(1);

            final int skipLength = random.nextInt(lobSize - i);
            is.skip(skipLength);
            is.reset();
            verifyStream(is, i, (lobSize - i));
        }
    }

    /**
     * Verify that an IAE is generated when an attempt is made to resume a
     * LOB with a stream value different from the original partial LOB.
     */
    private void partialPutLOBResumeVerificationTests()
        throws IOException {

        final int lobSize = 10 * TEST_CHUNK_SIZE;
        final Key lobKey = createKey("k.lob");

        for (int errorByteIndex :
            Arrays.asList(/* Check edge case, error at start, no chunks so
                             no verification. */
                          (int)(config.getLOBVerificationBytes() - 1),

                          /* Check error in middle, calls for verification */
                          TEST_CHUNK_SIZE * 3 + 5)) {
            InputStream lobErrorStream =
                new TestInputExceptionStream(lobSize, errorByteIndex);

            try {
                store.putLOB(lobKey, lobErrorStream,
                             Durability.COMMIT_WRITE_NO_SYNC,
                             1, TimeUnit.MINUTES);
                if (errorByteIndex > TEST_CHUNK_SIZE) {
                    fail("IOE expected. LOB size:" + lobSize +
                         " IOE byte index:" + errorByteIndex);
                }
            } catch (IOException  ioe) {
                /* Expected. */
            }

            /* A mismatched stream. */
            final InputStream lobStream = new TestInputStream(lobSize, 1);

            try {
                store.putLOB(lobKey, lobStream,
                             Durability.COMMIT_WRITE_NO_SYNC,
                             1, TimeUnit.MINUTES);
                if (errorByteIndex > TEST_CHUNK_SIZE) {
                    fail("IAE expected. LOB size:" + lobSize +
                         " IAE byte index:" + errorByteIndex);
                }
            } catch (IllegalArgumentException ise) {
                /* Expected. */
            }

            assertTrue(store.deleteLOB(lobKey, null, 10, TimeUnit.SECONDS));
        }
    }

    /**
     * Test for IOException in a simulated concurrent read/delete conflict
     * scenario.
     *
     * 1) Create a LOB
     * 2) Open a stream and read halfway into it.
     * 3) Delete the LOB
     * 4) Try read the rest of the LOB, expecting an IOE
     */
    private void readDeleteConflictTests()
        throws IOException {

        final int lobSize = 10 * TEST_CHUNK_SIZE;

        final Key lobKey = createKey("k.lob");

        final InputStream lobStream = new TestInputStream(lobSize);

        Version v = store.putLOB(lobKey, lobStream,
                                 Durability.COMMIT_WRITE_NO_SYNC,
                                 10, TimeUnit.SECONDS);
        assertNotNull(v);

        InputStream stream =
            store.getLOB(lobKey, null, 0, null).getInputStream();

        /* read so we are positioned in the middle of the LOB. */
        for (int i=0; i < (lobSize/2); i++ ) {
            assertTrue(stream.read() != -1);
        }

        final Durability allAck =
            new Durability(SyncPolicy.SYNC,
                           SyncPolicy.NO_SYNC,
                           ReplicaAckPolicy.ALL);
        assertTrue(store.deleteLOB(lobKey, allAck, 10, TimeUnit.SECONDS));

        try {
            for (int i=0; i < (lobSize/2); i++ ) {
                assertTrue(stream.read() != -1);
            }
            fail("IOE expected");
        } catch (IOException ioe) {
            assertTrue(ioe.getCause() instanceof
                       ConcurrentModificationException);
        }
    }

    /**
     * Verify that access to partial LOBs produce the expected exceptions:
     *
     * 1) Partially put lobs produce PLE when accessed for append
     * 2) Partially appended lobs produce PLE when accessed by put operations
     * 3) Partially deleted lobs produce PLEs when used for a read or an
     * append operation.
     * 4) All partial lobs produce PLEs when a read is attempted on them.
     */
    private void partialLOBExceptionTests()
        throws IOException {

        final Key lobKey = createKey("k.lob");
        /* Partially put LOBs */
        for (int lobSize : new int[]{0, 10 * TEST_CHUNK_SIZE} ) {
            createPartiallyInsertedLOB(lobKey, lobSize, lobSize/2);

            try {
                store.appendLOB(lobKey, new TestInputStream(lobSize),
                                Durability.COMMIT_WRITE_NO_SYNC,
                                1, TimeUnit.MINUTES);
                fail("Expected exception");
            } catch (PartialLOBException e) {
                assertEquals(e.getPartialState(), LOBState.PARTIAL_PUT);
            }

            verifyUnreadablePartialLOB(lobKey, PARTIAL_PUT);
            store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            verifyEmptyStore();
        }

        /* Partially appended LOBS. */
        for (int lobSize : new int[]{0, 10 * TEST_CHUNK_SIZE} ) {
            createPartiallyAppendedLOB(lobKey, lobSize, lobSize, lobSize/2);

            try {
                store.putLOB(lobKey, new TestInputStream(lobSize),
                             Durability.COMMIT_WRITE_NO_SYNC,
                             1, TimeUnit.MINUTES);
                fail("Expected exception");
            } catch (PartialLOBException e) {
                assertEquals(e.getPartialState(), LOBState.PARTIAL_APPEND);
            }

            verifyUnreadablePartialLOB(lobKey, PARTIAL_APPEND);

            store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            verifyEmptyStore();
        }

        /* partially deleted LOBs */
        partialLOBDeleteExceptionTests();
    }

    /**
     * Test correction operation of delete on partially deleted LOBS.
     */
    private void partialLOBDeleteExceptionTests()
        throws IOException {

        int lobSize = 4 * TEST_CHUNK_SIZE;
        final Key lobKey = createKey("l" + config.getLOBSuffix());

        createPartiallyDeletedLOB(lobSize, lobKey, 0);

        /* Have a partial object. Should not be able to read it.  */
        try {
            store.getLOB(lobKey, null, 0, null).getInputStream();
            fail("PLE expected");
        } catch (PartialLOBException  ple) {
            /* Expected. */
            assertEquals("partial delete expected",
                         PARTIAL_DELETE,  ple.getPartialState());
        }

        try {
            store.appendLOB(lobKey, new TestInputStream(lobSize),
                            Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail(" PLE expected");
        } catch (PartialLOBException  ple) {
            /* Expected. */
            assertEquals("partial delete expected",
                         PARTIAL_DELETE, ple.getPartialState());
        }

        assertTrue(store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                                   1, TimeUnit.MINUTES));

        verifyEmptyStore();
    }

    private void createPartiallyDeletedLOB(int lobSize,
                                           final Key lobKey,
                                           final int errorChunkIndex)
        throws IOException {

        final InputStream lobStream = new TestInputStream(lobSize);
        store.putLOB(lobKey, lobStream,
                     Durability.COMMIT_WRITE_NO_SYNC,
                     1, TimeUnit.MINUTES);

        DeleteOperation.setDeleteTestHook(new TestHook<Integer>() {
            int chunkNum = 0;

            @Override
            public void doHook(Integer currentChunkNum) {
                if (chunkNum++ == errorChunkIndex) {
                    throw new IllegalStateException("Test exception");
                }
            }
        });

        try {
            store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail ("chunk index:" + errorChunkIndex + " ISE expected");
        } catch (IllegalStateException ise) {
            /* Expected. */
        }

        /* resume normal delete operation. */
        DeleteOperation.setDeleteTestHook(null);
    }

    /**
     * Verify that non-LOB keys result in IAEs
     */
    private void lobKeyExceptionTests()
        throws IOException {

        /* Missing lob suffix */
        Key notLobKey = createKey("notlob");
        InputStream lobStream = new TestInputStream(100);

        try {
            store.putLOB(notLobKey, lobStream,
                            Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail("IAE expected");
        } catch (IllegalArgumentException  iae) {
            /* Expected. */
        }

        try {
            store.putLOBIfPresent(notLobKey, lobStream,
                                  Durability.COMMIT_WRITE_NO_SYNC,
                                  1, TimeUnit.MINUTES);
            fail("IAE expected");
        } catch (IllegalArgumentException  iae) {
            /* Expected. */
        }

        try {
            store.putLOBIfAbsent(notLobKey, lobStream,
                                  Durability.COMMIT_WRITE_NO_SYNC,
                                  1, TimeUnit.MINUTES);
            fail("IAE expected");
        } catch (IllegalArgumentException  iae) {
            /* Expected. */
        }

        try {
            store.deleteLOB(notLobKey,
                            Durability.COMMIT_WRITE_NO_SYNC,
                            1, TimeUnit.MINUTES);
            fail("IAE expected");
        } catch (IllegalArgumentException  iae) {
            /* Expected. */
        }

        try {
            store.getLOB(notLobKey, Consistency.ABSOLUTE,
                         1, TimeUnit.MINUTES);
            fail("IAE expected");
        } catch (IllegalArgumentException  iae) {
            /* Expected. */
        }

        final Key lobKey = createKey("doesNotExist.lob");
        try {
            store.appendLOB(lobKey,  new InputStream() {

                @Override
                public int read()
                    throws IOException {
                    /* Should never be called. */
                    fail();
                    return -1;
                }},
                Durability.COMMIT_WRITE_NO_SYNC,
                5, TimeUnit.SECONDS);

            fail("Expected IAE");
        } catch (IllegalArgumentException iae) {
            /* Success */
        }
    }

    /**
     * Verifies that attempts to modify LOBs via non-lob APIs result in
     */
    private void nonLobKeyWriteExceptionTests()
        throws IOException {

        final List<String> majorPath =
            Arrays.asList(KEY_PREFIX, "M1", "M2");

        final List<String> minorPath =
            Arrays.asList("m1", "m2" + config.getLOBSuffix());
        final Key majorKey = Key.createKey(majorPath);
        final Key lobKey = Key.createKey(majorPath, minorPath);

        final Value val = Value.createValue(new byte[0]);

        final Version version = new Version(new UUID(0, 0), 1);

        final OperationFactory opFactory = store.getOperationFactory();

        new ExpectFaultException() {
            @Override
            void op() { store.put(lobKey, val); }
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {return opFactory.createPut(lobKey, val); }
        }.execList();

        new ExpectFaultException() {
            @Override
                void op() {store.putIfAbsent(lobKey, val);}
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {
                return opFactory.createPutIfAbsent(lobKey, val);
            }
        }.execList();

        new ExpectFaultException() {
            @Override
            void op() {store.putIfPresent(lobKey, val);}
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {
                return opFactory.createPutIfPresent(lobKey, val);
            }
        }.execList();

        new ExpectFaultException() {
            @Override
                void op() {store.putIfVersion(lobKey, val, version);}
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {
                return opFactory.createPutIfVersion(lobKey, val, version);
            }
        }.execList();

        new ExpectFaultException() {
            @Override
            void op() {store.delete(lobKey);}
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {
                return opFactory.createDelete(lobKey);
            }
        }.execList();

        new ExpectFaultException() {
            @Override
            void op() {store.deleteIfVersion(lobKey, version);}
        }.exec();

        new ExpectFaultException() {
            @Override
            Operation getOp() {
                return opFactory.createDeleteIfVersion(lobKey, version);
            }
        }.execList();

        new ExpectFaultException() {
            @Override
            List<Operation> getOps() {
                Key nlKey = createKey("a");
                return Arrays.asList(opFactory.createPut(nlKey, val),
                                     opFactory.
                                     createDeleteIfVersion(lobKey, version),
                                     opFactory.createDelete(nlKey));
            }
        }.execList();

        /* Create a LOB */

        /* The input stream used for the LOB insert. */
        InputStream lobStream = new TestInputStream(100);

        Version v = store.putLOB(lobKey, lobStream,
                                 Durability.COMMIT_WRITE_NO_SYNC,
                                 1, TimeUnit.MINUTES);
        assertNotNull(v);

        new ExpectFaultException() {
            @Override
            void op() {store.multiDelete(majorKey, null,
                                         Depth.DESCENDANTS_ONLY);}
        }.exec();

        /* Clean up for subsequent tests. */
        store.deleteLOB(lobKey, Durability.COMMIT_WRITE_NO_SYNC,
                        1, TimeUnit.MINUTES);
    }

    /**
     * Verifies that a partial LOB will result in a PLE
     */
    private void verifyUnreadablePartialLOB(Key lobKey,
                                            LOBState state) {
        try {
            store.getLOB(lobKey, null, 0, null).getInputStream();
            fail("PLE expected");
        } catch (PartialLOBException  ple) {
            /* Expected. */
            assertEquals(state, ple.getPartialState());
        }
    }


    private void createPartiallyInsertedLOB(Key lobKey,
                                            int lobSize,
                                            int errorByteIndex) {
        final InputStream lobErrorStream =
            new TestInputExceptionStream(lobSize, errorByteIndex);

        try {
            store.putLOB(lobKey, lobErrorStream,
                         Durability.COMMIT_WRITE_NO_SYNC,
                         1, TimeUnit.MINUTES);
            fail("IOE expected. LOB size:" + lobSize +
                 " IOE byte index:" + errorByteIndex);
        } catch (IOException  ioe) {
            /* Expected. */
        }
    }

    /**
     * Basic tests to exercise all correct sunny day operations
     */
    private void basicTests()
        throws IOException {

        for (int lobId = 1; lobId < 100; lobId++) {
            final String lobKeyString = "l" + lobId + config.getLOBSuffix();
            final Key key = createKey(lobKeyString);

            final int lobSize = random.nextInt(300);
            if (lobSize == 0) {
                continue;
            }

            /* The input stream used for the LOB insert. */
            InputStream lobStream = new TestInputStream(lobSize);

            Version v = store.putLOB(key, lobStream,
                                     Durability.COMMIT_WRITE_NO_SYNC,
                                     1, TimeUnit.MINUTES);
            assertNotNull(v);
            verifyLOB(key, lobSize);

            /* Check the version on InputStreamVersion */
            InputStreamVersion versionStream =
                store.getLOB(key, Consistency.ABSOLUTE, 0, null);
            Version isvVersion = versionStream.getVersion();
            assertEquals(isvVersion, v);

            /* Check InputStreamVersion.toString() */
            String isvAsString = versionStream.toString();
            String versionAsString = isvVersion.toString();
            assertTrue(isvAsString.contains(versionAsString));

            lobStream = new TestInputStream(lobSize);
            v = store.putLOBIfAbsent(key, lobStream,
                                     Durability.COMMIT_WRITE_NO_SYNC,
                                     1, TimeUnit.MINUTES);
            assertNull(v);

            lobStream = new TestInputStream(lobSize);
            v = store.putLOBIfPresent(key, lobStream,
                                      Durability.COMMIT_WRITE_NO_SYNC,
                                      1, TimeUnit.MINUTES);
            assertNotNull(v);

            verifyLOB(key, lobSize);

            assertTrue(store.deleteLOB(key, Durability.COMMIT_WRITE_NO_SYNC,
                                       1, TimeUnit.MINUTES));
            assertFalse(store.deleteLOB(key, Durability.COMMIT_WRITE_NO_SYNC,
                                        1, TimeUnit.MINUTES));
        }

        verifyEmptyStore();
    }

    private void verifyEmptyStore() {
        /* Verify that the store is empty. */
        final Iterator<Key> si =
            KVStoreImpl.makeInternalHandle(store).
                storeKeysIterator(Direction.UNORDERED, 1000);
        while (si.hasNext()) {
            final Key key = si.next();
            if (KEY_PREFIX_KEY.isPrefix(key)) {
                fail("Found key:" + key);
            }
        }
    }

    /**
     * Verify the contents of a lob that was created from one or more streams
     *
     * @param lobKey the key associated with the lob
     * @param streamSizes the sizes of the put + append* streams used to create
     * and extend the lob
     */
    private void verifyLOB(Key lobKey,
                           int ... streamSizes)
        throws IOException {

        InputStream streams[] = new InputStream[streamSizes.length];

        for (int i=0; i < streamSizes.length; i++) {
            streams[i] = new TestInputStream(streamSizes[i]);
        }

        verifyLOB(lobKey, streams);
    }

    private void verifyLOB(Key lobKey, InputStream ... lobStreams)
        throws IOException {

        /* Now read and verify the LOB */
        InputStream stream =
            store.getLOB(lobKey, Consistency.ABSOLUTE, 0, null).getInputStream();

        long byteCount = 0;
        for (InputStream lobStream : lobStreams) {
            for (int readByte; (readByte=lobStream.read()) != -1; byteCount++) {
                assertEquals("lob byte index: " + byteCount,
                             readByte, stream.read());
            }
        }
        assertEquals("EOF not found at byte:" + byteCount, -1, stream.read());
    }

    /**
     * Read expectedBytes from the stream and verify them.
     */
    private void verifyStream(InputStream lobStream,
                             int startByte,
                             int expectedBytes)
        throws IOException {

        int byteCount = startByte;

        for (int readByte; (readByte=lobStream.read()) != -1; byteCount++) {
            assertEquals("byte index: " + byteCount,
                         (byteCount % 255), readByte);
        }
        assertEquals("start byte=" + startByte,
                     expectedBytes, (byteCount - startByte));
    }

    /* Verify the byte at a specific index. */
    private void verifyByte(int index, int b) {
        assertEquals("byte index: " + index,
                     index % 255, 0xFF & b);
    }

    private void verifyBytes(byte streamBytes[]) {

         for (int i=0; i < streamBytes.length; i++) {
             verifyByte(i, streamBytes[i]);
         }
     }

    /**
     * Produces an easily verifiable test stream, verified using the
     * verifyStream method above.
     */
    public static class TestInputStream extends InputStream {
        private final int lobSize;
        protected int byteCount;
        protected int delta = 0;

        long skipBytes = 0;

        public TestInputStream(int lobSize) {
            super();
            this.lobSize = lobSize;
        }

        /**
         * A test input stream that returns a different "delta shifted" byte
         * stream.
         */
        public TestInputStream(int lobSize,
                               int delta) {
            this(lobSize);
            this.delta = delta;
        }

        @Override
        public int read()
            throws IOException {

            return byteCount >= lobSize ? -1 : ((byteCount++ + delta) % 255);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return super.read(b, off, len);
        }


        @Override
        public long skip(long n)
            throws IOException {

            /* Don't skip beyond the stream length */
            if (n + byteCount > lobSize) {
                n = lobSize - byteCount;
            }

            /* Skip less than requested, to test that callers retry */
            if (n > 2) {
                n = 1 + random.nextInt((int) n/2);
            }

            skipBytes += n;
            return super.skip(n);
        }
    }

    /**
     * A test stream that produces IO exceptions to create partially inserted
     * LOBs.
     */
    private class TestInputExceptionStream extends TestInputStream {
        final int errorIndex;

        TestInputExceptionStream(int lobSize, int errorIndex) {
            super(lobSize);
            this.errorIndex = errorIndex;
        }

        @Override
        public int read()
            throws IOException {

            if (byteCount == errorIndex) {
                throw new IOException("simulated I/O exception at:" +
                                      errorIndex);
            }
            return super.read();
        }
    }

    /* Utility class to facilitate exception testing. */
    private abstract class ExpectFaultException {
        void op() {
            fail("unexpected call to op()");
        }

        Operation getOp() {
            fail("unexpected call to getOp()");
            return null;
        }

        List<Operation> getOps() {
            return Arrays.asList(getOp());
        }

        void exec() {
            try {
                op();
                fail("expected exception");
            } catch (IllegalArgumentException iae) {
                /* Expected. */
            }
        }

        void execList() {
            try {
                store.execute(getOps());
                fail("expected exception");
            } catch (IllegalArgumentException iae) {
                /* Expected. */
            }  catch (Exception e) {
                fail("unexpected exception" + e);
            }
        }
    }

    private abstract class ExpectIOException {

        abstract void op()
            throws IOException;

        void exec() {
            try {
                op();
                fail("expected exception");
            } catch (IOException ioe) {
                /* Expected. */
            }
        }
    }

    private abstract class ExpectIOBException {

        abstract void op() throws IOException;

        void exec() throws IOException {
            try {
                op();
                fail("expected exception");
            } catch (IndexOutOfBoundsException ioe) {
                /* Expected. */
            }
        }
    }
}
