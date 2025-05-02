/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.lob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;

import oracle.kv.Key;
import oracle.kv.TestBase;

import org.junit.Test;

// TODO: add tests for byte constructor

/**
 * Verifies that LOB paths have the expected structure:
 *  //lob/ilk/000001/-/000001
 *
 *  and are distributed, as expected across partitions.
 */
public class ChunkKeysIteratorTest extends TestBase {

    final Key ilk = Key.createKey("//lob/ilk");
    final int chunksPerPartition = 3;
    final int chunkSize = 16;

    final ChunkKeyFactory keyFactory =
        new ChunkKeyFactory(LOBMetadataKeys.CURRENT_VERSION);

    @Test
    public void testBasic() {

        for (int lobSize = 0;
             lobSize <= (chunksPerPartition * 3 * chunkSize);
             lobSize++) {

            ChunkKeysIterator cki =
                new ChunkKeysIterator(ilk, 0, lobSize,
                                      chunkSize, chunksPerPartition,
                                      keyFactory);

            int count = 0;
            while (cki.hasNext()) {
                count++;
                Key k = cki.next();
                assertTrue(ilk.isPrefix(k));
                assertEquals(2, k.getMajorPath().size());
                /* Chunk Id must be the only minor path component. */
                assertEquals(1, k.getMinorPath().size());

                final int chunkId = keyFactory.getChunkId(k);
                assertEquals("count:" + count,
                             ((count - 1) % chunksPerPartition) + 1, chunkId);

                final String superChunkName =
                    k.getMajorPath().get(k.getMajorPath().size() - 1);
                final int superChunkId =
                    Integer.parseInt(superChunkName, ChunkKeyFactory.KEY_RADIX);

                assertEquals(((count-1) / chunksPerPartition) + 1,
                             superChunkId);
            }
            assertEquals(count,
                         (lobSize == 0) ? 0 : ((lobSize - 1) / chunkSize) + 1);
        }
    }

    @Test
    public void testMarkReset() {

        final ChunkKeysIterator cki1 =
            new ChunkKeysIterator(ilk, 0, 10 * chunkSize, chunkSize,
                                  chunksPerPartition,
                                  keyFactory);

        final ChunkKeysIterator cki2 =
            new ChunkKeysIterator(ilk, 0, 10 * chunkSize, chunkSize,
                                  chunksPerPartition,
                                  keyFactory);

        LinkedList<Key> ckeys = new LinkedList<Key>();

        while (cki1.hasNext()) {
            cki2.reset(cki1);
            final Key k1 = cki1.next();
            final Key k2 = cki2.next();

            assertEquals(k1, k2);
            ckeys.addFirst(k1);
        }

        assertFalse(cki2.hasNext());

        /* test reset at EOS. */
        cki2.reset(cki1);
        assertFalse(cki2.hasNext());

        /* Test backup. */
        for (Key k : ckeys) {
            cki2.backup();
            assertTrue(cki2.hasNext());
            assertEquals(k, cki2.next());
            cki2.backup();
        }
    }

    @Test
    public void testSkip() {

        final int limitChunks = 20;
        ChunkKeysIterator cki =
            new ChunkKeysIterator(ilk, 0, limitChunks * chunkSize,
                                   chunkSize, chunksPerPartition,
                                   keyFactory);

        assertEquals(0, cki.skip(0));

        /* Greater than LOB size */
        long n = cki.skip(limitChunks + 1);
        assertEquals(limitChunks, n);
        assertFalse(cki.hasNext());

        /* Nothing left to skip */
        assertEquals(0, cki.skip(1));

        /* Move exactly to end */
        cki = new ChunkKeysIterator(ilk, 0, limitChunks * chunkSize,
                                    chunkSize, chunksPerPartition,
                                    keyFactory);

        assertEquals(limitChunks, cki.skip(limitChunks));
        assertFalse(cki.hasNext());

        for (int skipIncrement = 1;
             skipIncrement <= chunksPerPartition * 2;
             skipIncrement++) {

            cki = new ChunkKeysIterator(ilk, 0, limitChunks * chunkSize,
                                       chunkSize, chunksPerPartition,
                                       keyFactory);

            for (int i = 1; i <= limitChunks/skipIncrement; i++) {
                n = cki.skip(skipIncrement);
                assertEquals("i:" + i +
                             " Skip increment:" + skipIncrement,
                             skipIncrement, n);
            }

            n = cki.skip(skipIncrement);
            /* At EOLOB */
            assertEquals("Skip increment:" + skipIncrement,
                         (limitChunks % skipIncrement), n);
        }
    }
}
