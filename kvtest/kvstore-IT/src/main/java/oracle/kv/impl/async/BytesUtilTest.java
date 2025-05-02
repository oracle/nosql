/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.async;

import static org.junit.Assert.assertEquals;

import oracle.kv.TestBase;

import org.junit.Test;

/**
 * Test the {@link BytesUtil}.
 */
public class BytesUtilTest extends TestBase {

    @Test
    public void testByteArrayToString() {
        final byte[] array = new byte[] {
             0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15,
            16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
            32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
        };
        compareArrayString(
                array, 0, 1,
                "[offset=0, len=1 " +
                "0x00000000: " +
                "00                      " +
                "                        " +
                "    " +
                ".                ]");
        compareArrayString(
                array, 0, 8,
                "[offset=0, len=8 " +
                "0x00000000: " +
                "00 01 02 03 04 05 06 07 " +
                "                        " +
                "    " +
                "........         ]");
        compareArrayString(
                array, 0, 17,
                "[offset=0, len=17\n" +
                "0x00000000: " +
                "00 01 02 03 04 05 06 07 " +
                "08 09 0A 0B 0C 0D 0E 0F " +
                "    " +
                "................\n" +
                "0x00000010: " +
                "10                      " +
                "                        " +
                "    " +
                ".               \n" +
                "]");
        compareArrayString(
                array, 1, 17,
                "[offset=1, len=17\n" +
                "0x00000000: " +
                "   01 02 03 04 05 06 07 " +
                "08 09 0A 0B 0C 0D 0E 0F " +
                "    " +
                " ...............\n" +
                "0x00000010: " +
                "10 11                   " +
                "                        " +
                "    " +
                "..              \n" +
                "]");
        compareArrayString(
                array, 8, 8,
                "[offset=8, len=8 " +
                "0x00000000: " +
                "                        " +
                "08 09 0A 0B 0C 0D 0E 0F " +
                "    " +
                "        ........ ]");
        compareArrayString(
                array, 20, 4,
                "[offset=20, len=4 " +
                "0x00000010: " +
                "            14 15 16 17 " +
                "                        " +
                "    " +
                "    ....         ]");
        compareArrayString(
                array, 20, 18,
                "[offset=20, len=18\n" +
                "0x00000010: " +
                "            14 15 16 17 " +
                "18 19 1A 1B 1C 1D 1E 1F " +
                "    " +
                "    ............\n" +
                "0x00000020: " +
                "20 21 22 23 24 25       " +
                "                        " +
                "    " +
                " !\"#$%          \n" +
                "]");
    }

    private void compareArrayString(byte[] array,
                                    int offset,
                                    int len,
                                    String expected) {
        assertEquals(expected, BytesUtil.toString(array, offset, len));
    }
}

