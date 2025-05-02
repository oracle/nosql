/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */
package oracle.kv.util;

import java.util.ArrayList;
import java.util.List;

import oracle.kv.Key;

import com.sleepycat.je.tree.Key.DumpType;
import com.sleepycat.je.utilint.CmdUtil;

/**
 * A debugging utility to display a kvstore key in JE binary, hex or text to
 * make it easier to translate the key when using utilities that dump a JE
 * database. 
 * <ul>
 * <li>com.sleepycat.je.util.DbDump can be used to determine if a record is
 * alive. DbDump displays text, but the field and major/minor key delimiters
 * are encoded more economically
 * <li>com.sleepycat.je.util.DbPrintLog displays binary by default.
 * </ul>
 * For example, suppose you have datacheck test output like this:
 * <pre>
 * 2014-05-07 15:27:24.248 UTC [DataCheck] UNEXPECTED_RESULT:
 * Missing previous value: currentIndex=0x1bf500, otherThreadIndex=0x1bf6c0, 
 *        op=DeleteIfVersionExecute, currentThread=b, keynum=0x37bcee, 
 *        partition=2, 
 * key={"majorKey1":"kfeb414e8","majorKey2":"4b","minorKey":"ee"},
 * previousValue=not present, retrying=false, populateIndex=0x1bde77,
 * populateValue={"majorKey1":"kfeb414e8","majorKey2":"4b","minorKey":"ee",
 *            "operation":"POPULATE","firstThread":false,"index":1826423}
 * </pre>
 * java oracle.kv.impl.util.KVToJEKey -f kfeb414e8 -f 4b -minor -f ee
 * results in this output:
 * <pre>
 * KVStore key /kfeb414e8/4b/-/ee becomes JE key:
 *
 *      text:   kfeb414e8\004b\ffee
 *      binary: key=&lt;key v="107 102 101 98 52 49 52 101 56 0 52 98 255 101 101 "/&gt;
 *      hex:    key=&lt;key v="6b 66 65 62 34 31 34 65 38 0 34 62 ff 65 65 "/&gt;
 * </pre>
 * One caveat is that the tool is not particularly friendly when used with the
 * table api, because you cannot input typed field values. For example,if you
 * know that the key is composed of fields like (integer) X, (long) Y, you'd
 * like to be able to specify KVToJEKey -fint X -flong Y. That's not possible
 * in this standalone version, because the conversion of field values to
 * serialized byte array requires access to the table metadata. If we'd like
 * typed table data input in the future, we could change this tool to be able 
 * to read AdminDB metadata.
 */

public class KVToJEKey {
    public static void main(String[] argv) {

        List<String> major = new ArrayList<String>();
        List<String> minor = new ArrayList<String>();
        
        parseArgs(argv, major, minor);

        /* Display the arguments as a KVStore key */
        Key kvKey = Key.createKey(major, minor);
        byte[] bites = kvKey.toByteArray();
        System.out.println("KVStore key " + kvKey + " becomes JE key:");
        
        /* Display the key in JE binary, hex, and text format */
        com.sleepycat.je.tree.Key.DUMP_TYPE = DumpType.BINARY;
        String binary = com.sleepycat.je.tree.Key.getNoFormatString(bites);
        
        com.sleepycat.je.tree.Key.DUMP_TYPE = DumpType.TEXT;
        StringBuilder sb = new StringBuilder();
        CmdUtil.formatEntry(sb, bites, true);
        String text = sb.toString();
        
        com.sleepycat.je.tree.Key.DUMP_TYPE = DumpType.HEX;
        String hex = com.sleepycat.je.tree.Key.getNoFormatString(bites);
        
        System.out.println();
        System.out.println("\ttext:   " + text);
        System.out.println("\tbinary: " + binary);
        System.out.println("\thex:    " + hex);
    }

    private static void parseArgs(String[] argv,
                                  List<String> majorList,
                                  List<String> minorList) {
        if (argv.length == 0) {
            usage();
            System.exit(1);
        }

        List<String> listInUse = majorList;
        int whichArg = 0;

        /* Arguments come in -flag <val> pairs */
        while (whichArg < argv.length -1) {
            String flag = argv[whichArg];
            if (("-minor").equals(flag)) {
                listInUse = minorList;
            } else if (("-f").equals(flag)) {
                listInUse.add(argv[++whichArg]);
            } else {
                System.err.println(flag + " is not a valid flag (" + whichArg +
                                   ")");
                usage();
                System.exit(1);
            }
            whichArg++;
        }
    }

    private static void usage() {
        StringBuilder sb = new StringBuilder();
        sb.append("Specify key fields in order, separating major/minor key with the -minor flag");
        sb.append("KVToJEKey [-f <major field value>]").
            append(" // one or more major key fields\n");
        sb.append("           -minor                  // indicate start of minor key");
        sb.append("            -f <minor field value> // one or more major key fields");
        System.err.println(sb);
    }
} 