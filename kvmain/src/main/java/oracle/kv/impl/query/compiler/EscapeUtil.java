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

package oracle.kv.impl.query.compiler;

import oracle.kv.impl.query.QueryException;

import com.fasterxml.jackson.core.io.CharTypes;

/**
 * Utility for inlining escape chars.
 */
public class EscapeUtil {

    /**
     * Replaces escape codes with it's character including unicode chars.
     * Supports: \b, \t, \n, \f, \r, \\, \/, \" and \ uhhhh.
     * Note: For any other char after \ an exception is thrown.
     */
    public static String inlineEscapedChars(String in) {
        //return in.replace("\\\"", "\"");
        StringBuilder sb = null;
        int saved = -1;

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);

            if (c == '\\') {
                if (sb == null) {
                    sb = new StringBuilder();
                }
                sb.append(in, saved + 1, i);
                i += decodeEscapedChar(in, i + 1, sb);
                saved = i;
            }
        }

        if (sb != null) {
            sb.append(in, saved + 1, in.length());
            return sb.toString();
        }

        return in;
    }

    private static int decodeEscapedChar(String in, int index,
        StringBuilder sb) {

        if (in.length() - 1 < index) {
            throw new QueryException("Illegal escape sequence, expecting " +
                "escape char" +
                " after \\.");
        }

        char c = in.charAt(index);
        switch (c) {
        case 'b':
            sb.append("\b");
            return 1;
        case 't':
            sb.append("\t");
            return 1;
        case 'n':
            sb.append("\n");
            return 1;
        case 'f':
            sb.append("\f"); 
            return 1;
        case 'r':
            sb.append("\r");
            return 1;

        case '/':
        case '\\':  /* backslash escape */
        case '\"':  /* double quote escape */
        //case '\'':  /* single quote escape not supported */
            sb.append(c);
            return 1;

        case 'u':   /* \ uhhhh hex escape */
            break;

        default:
            throw new QueryException("Illegal escape sequence: '\\" + c + "'");
        }

        if (in.length() - 1 < index + 4) {
            throw new QueryException("Illegal escape sequence, unicode escape" +
                " requires four hex-digits.");
        }

        int value = 0;
        for (int i = 0; i < 4; ++i) {
            int ch = in.charAt(index + 1 + i);
            int digit = CharTypes.charToHex(ch);
            if (digit < 0) {
                throw new QueryException("Illegal escape sequence, expected a" +
                    " hex-digit for character escape sequence: " + ch);
            }
            value = (value << 4) | digit;
        }
        sb.append((char) value);
        return 5;
    }
}
