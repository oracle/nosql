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

package oracle.kv.impl.util;

import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * This class is used to implement a sizeof() method on FieldValueImpl. The
 * code here is copied from je/dbi/MemoryBugdet.java. See there for details.
 */
public class SizeOf {

    public final static int OBJECT_REF_OVERHEAD_32 = 4;
    public final static int OBJECT_REF_OVERHEAD_64 = 8;
    public final static int OBJECT_REF_OVERHEAD_OOPS = 4;

    // 2
    public final static int OBJECT_OVERHEAD_32 = 8;
    public final static int OBJECT_OVERHEAD_64 = 16;
    public final static int OBJECT_OVERHEAD_OOPS = 16;

    // 8
    public final static int ARRAY_OVERHEAD_32 = 16;
    public final static int ARRAY_OVERHEAD_64 = 24;
    public final static int ARRAY_OVERHEAD_OOPS = 16;

    // see byteArraySize
    public final static int ARRAY_SIZE_INCLUDED_32 = 4;
    public final static int ARRAY_SIZE_INCLUDED_64 = 0;
    public final static int ARRAY_SIZE_INCLUDED_OOPS = 0;

    // 20
    public final static int HASHMAP_OVERHEAD_32 = 120;
    public final static int HASHMAP_OVERHEAD_64 = 219;
    public final static int HASHMAP_OVERHEAD_OOPS = 128;

    // 21 - OBJECT_OVERHEAD - HASHMAP_OVERHEAD
    // 32b: 21 is 152
    // 64b: 21 is max(280,...,287) on Linux/Solaris 1.5/1.6
    // Oops: 21 is 176
    public final static int HASHMAP_ENTRY_OVERHEAD_32 = 24;
    public final static int HASHMAP_ENTRY_OVERHEAD_64 = 52;
    public final static int HASHMAP_ENTRY_OVERHEAD_OOPS = 32;

    // 22
    public final static int HASHSET_OVERHEAD_32 = 136;
    public final static int HASHSET_OVERHEAD_64 = 240;
    public final static int HASHSET_OVERHEAD_OOPS = 144;

    // 23 - OBJECT_OVERHEAD - HASHSET_OVERHEAD
    // 32b: 23 is 168
    // 64b: 23 is max(304,...,311) on Linux/Solaris
    // Oops: 23 is 192
    public final static int HASHSET_ENTRY_OVERHEAD_32 = 24;
    public final static int HASHSET_ENTRY_OVERHEAD_64 = 55;
    public final static int HASHSET_ENTRY_OVERHEAD_OOPS = 32;

    // 34
    public final static int TREEMAP_OVERHEAD_32 = 48;
    public final static int TREEMAP_OVERHEAD_64 = 80;
    public final static int TREEMAP_OVERHEAD_OOPS = 48;

    // 35 - OBJECT_OVERHEAD - TREEMAP_OVERHEAD
    // 32b: 35 is 88
    // 64b: 35 is 160
    // Oops: 35 is 104
    public final static int TREEMAP_ENTRY_OVERHEAD_32 = 32;
    public final static int TREEMAP_ENTRY_OVERHEAD_64 = 64;
    public final static int TREEMAP_ENTRY_OVERHEAD_OOPS = 40;

    // 27 minus zero length Object array
    public final static int EMPTY_OBJ_ARRAY = (int)objectArraySize(0);
    public final static int ARRAYLIST_OVERHEAD_32 = 40 - EMPTY_OBJ_ARRAY;
    public final static int ARRAYLIST_OVERHEAD_64 = 64 - EMPTY_OBJ_ARRAY;
    public final static int ARRAYLIST_OVERHEAD_OOPS = 40 - EMPTY_OBJ_ARRAY;


    public final static int OBJECT_REF_OVERHEAD;

    public final static int OBJECT_OVERHEAD;

    public final static int ARRAY_OVERHEAD;
    public final static int ARRAY_SIZE_INCLUDED;

    public final static int HASHMAP_OVERHEAD;
    public final static int HASHMAP_ENTRY_OVERHEAD;
    public final static int HASHSET_OVERHEAD;
    public final static int HASHSET_ENTRY_OVERHEAD;

    public final static int TREEMAP_OVERHEAD;
    public final static int TREEMAP_ENTRY_OVERHEAD;

    public final static int ARRAYLIST_OVERHEAD;

    /* Primitive long array item size is the same on all platforms. */
    public final static int PRIMITIVE_LONG_ARRAY_ITEM_OVERHEAD = 8;

    private final static String JVM_ARCH_PROPERTY = "sun.arch.data.model";
    private final static String FORCE_JVM_ARCH = "je.forceJVMArch";

    private static boolean COMPRESSED_OOPS_REQUESTED = false;
    private static boolean COMPRESSED_OOPS_KNOWN = false;
    private static boolean COMPRESSED_OOPS_KNOWN_ON = false;

    static {
        boolean is64 = false;
        String overrideArch = System.getProperty(FORCE_JVM_ARCH);

        try {
            if (overrideArch == null) {
                String arch = System.getProperty(JVM_ARCH_PROPERTY);
                if (arch != null) {
                    is64 = Integer.parseInt(arch) == 64;
                }
            } else {
                is64 = Integer.parseInt(overrideArch) == 64;
            }
        } catch (NumberFormatException NFE) {
            NFE.printStackTrace(System.err);
        }

        final Boolean checkCompressedOops = true;
            //CompressedOopsDetector.isEnabled(); TODO

        if (checkCompressedOops != null) {
            COMPRESSED_OOPS_KNOWN = true;
            COMPRESSED_OOPS_KNOWN_ON = checkCompressedOops;
        }

        List<String> args =
            ManagementFactory.getRuntimeMXBean().getInputArguments();

        for (String arg : args) {
            if ("-XX:+UseCompressedOops".equals(arg)) {
                COMPRESSED_OOPS_REQUESTED = true;
                break;
            }
        }

        final boolean useCompressedOops = (COMPRESSED_OOPS_KNOWN ?
                                           COMPRESSED_OOPS_KNOWN_ON :
                                           COMPRESSED_OOPS_REQUESTED);

        if (useCompressedOops) {
            OBJECT_REF_OVERHEAD = OBJECT_REF_OVERHEAD_OOPS;
            OBJECT_OVERHEAD = OBJECT_OVERHEAD_OOPS;
            ARRAY_OVERHEAD = ARRAY_OVERHEAD_OOPS;
            ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_OOPS;
            HASHMAP_OVERHEAD = HASHMAP_OVERHEAD_OOPS;
            HASHMAP_ENTRY_OVERHEAD = HASHMAP_ENTRY_OVERHEAD_OOPS;
            HASHSET_OVERHEAD = HASHSET_OVERHEAD_OOPS;
            HASHSET_ENTRY_OVERHEAD = HASHSET_ENTRY_OVERHEAD_OOPS;
            TREEMAP_OVERHEAD = TREEMAP_OVERHEAD_OOPS;
            TREEMAP_ENTRY_OVERHEAD = TREEMAP_ENTRY_OVERHEAD_OOPS;
            ARRAYLIST_OVERHEAD = ARRAYLIST_OVERHEAD_OOPS;

        } else if (is64) {
            OBJECT_REF_OVERHEAD = OBJECT_REF_OVERHEAD_64;
            OBJECT_OVERHEAD = OBJECT_OVERHEAD_64;
            ARRAY_OVERHEAD = ARRAY_OVERHEAD_64;
            ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_64;
            HASHMAP_OVERHEAD = HASHMAP_OVERHEAD_64;
            HASHMAP_ENTRY_OVERHEAD = HASHMAP_ENTRY_OVERHEAD_64;
            HASHSET_OVERHEAD = HASHSET_OVERHEAD_64;
            HASHSET_ENTRY_OVERHEAD = HASHSET_ENTRY_OVERHEAD_64;
            TREEMAP_OVERHEAD = TREEMAP_OVERHEAD_64;
            TREEMAP_ENTRY_OVERHEAD = TREEMAP_ENTRY_OVERHEAD_64;
            ARRAYLIST_OVERHEAD = ARRAYLIST_OVERHEAD_64;

        } else {
            OBJECT_REF_OVERHEAD = OBJECT_REF_OVERHEAD_32;
            OBJECT_OVERHEAD = OBJECT_OVERHEAD_32;
            ARRAY_OVERHEAD = ARRAY_OVERHEAD_32;
            ARRAY_SIZE_INCLUDED = ARRAY_SIZE_INCLUDED_32;
            HASHMAP_OVERHEAD = HASHMAP_OVERHEAD_32;
            HASHMAP_ENTRY_OVERHEAD = HASHMAP_ENTRY_OVERHEAD_32;
            HASHSET_OVERHEAD = HASHSET_OVERHEAD_32;
            HASHSET_ENTRY_OVERHEAD = HASHSET_ENTRY_OVERHEAD_32;
            TREEMAP_OVERHEAD = TREEMAP_OVERHEAD_32;
            TREEMAP_ENTRY_OVERHEAD = TREEMAP_ENTRY_OVERHEAD_32;
            ARRAYLIST_OVERHEAD = ARRAYLIST_OVERHEAD_32;
        }
    }

    /**
     * Returns the memory size occupied by a byte array of a given length.  All
     * arrays (regardless of element type) have the same overhead for a zero
     * length array.  On 32b Java, there are 4 bytes included in that fixed
     * overhead that can be used for the first N elements -- however many fit
     * in 4 bytes.  On 64b Java, there is no extra space included.  In all
     * cases, space is allocated in 8 byte chunks.
     */
    public static long byteArraySize(int arrayLen) {

        /*
         * ARRAY_OVERHEAD accounts for N bytes of data, which is 4 bytes on 32b
         * Java and 0 bytes on 64b Java.  Data larger than N bytes is allocated
         * in 8 byte increments.
         */
        long size = ARRAY_OVERHEAD;
        if (arrayLen > ARRAY_SIZE_INCLUDED) {
            size += ((arrayLen - ARRAY_SIZE_INCLUDED + 7) / 8) * 8;
        }

        return size;
    }

    public static long shortArraySize(int arrayLen) {
        return byteArraySize(arrayLen * 2);
    }

    public static long intArraySize(int arrayLen) {
        return byteArraySize(arrayLen * 4);
    }

    public static long longArraySize(int arrayLen) {
        return byteArraySize(arrayLen * 8);
    }

    public static long objectArraySize(int arrayLen) {
        return byteArraySize(arrayLen * OBJECT_REF_OVERHEAD);
    }

    public static long stringSize(String str) {
        return (OBJECT_OVERHEAD +
                OBJECT_REF_OVERHEAD +
                byteArraySize(2 * str.length()));
    }
}
