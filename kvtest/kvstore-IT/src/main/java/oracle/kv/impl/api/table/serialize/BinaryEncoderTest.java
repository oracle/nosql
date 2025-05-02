/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.api.table.serialize;

import static oracle.kv.impl.util.SerializationUtil.LOCAL_BUFFER_SIZE;
import static oracle.kv.util.TestUtils.checkException;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import oracle.kv.TestBase;

import org.junit.Test;

public class BinaryEncoderTest extends TestBase {

    /**
     * Test for problem encoding UTF32 characters that have a two character
     * code point. [#27794]
     */
    @Test
    public void testUTF32Char() throws IOException {

        /*
         * Single character string with the U+1F60B "smiling face licking lips"
         * character, which has a single code point with two chars
         */
        String string = new String(new int[] { 128523, 0x1f60b }, 0, 1);
        checkEncodeDecode(string);
    }

    private void checkEncodeDecode(String string) throws IOException {
        ByteArrayOutputStream baos = encode(string);
        InputStream in = new ByteArrayInputStream(baos.toByteArray());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
        String encodeDecodeString = decoder.readString();
        assertEquals(string, encodeDecodeString);
    }

    private ByteArrayOutputStream encode(String string) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(100);
        AvroEncoder encoder = new AvroEncoder(baos, null);
        encoder.writeString(string);
        encoder.flush();
        return baos;
    }

    /** Test that needing to use a larger buffer works properly */
    @Test
    public void testOverflow() throws IOException {

        /* Pick size so that string will appear to fit per-thread buffer */
        int size = (int)
            (LOCAL_BUFFER_SIZE /
             StandardCharsets.UTF_8.newEncoder().averageBytesPerChar());
        final char[] chars = new char[size];

        /* Fill with Euro characters, which require three bytes to encode */
        Arrays.fill(chars, '\u20AC');

        checkEncodeDecode(new String(chars));
    }

    @Test
    public void testMalformed() {

        /* Missing second character */
        checkException(() -> encode("\uD83D"),
                       MalformedInputException.class, null);

        /* Invalid second character */
        checkException(() -> encode("\uD83D\u0000"),
                       MalformedInputException.class, null);
    }
}

