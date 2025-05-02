/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import java.io.IOException;
import java.util.Arrays;

import oracle.kv.impl.security.util.PasswordReader;

/**
 * This class provides an implementation of PasswordReader that is
 * programable to provide either a fixed response, or a fixed sequence
 * of responses.
 */
public class TestPasswordReader implements PasswordReader {
    /* The password(s) to return.  If null, no passwords are supplied */
    private String[] passwordSequence = null;

    /* The index into passwordSequence of the next password to return. */
    private int passwordIndex = -1;

    /**
     * Constructor with a single response.
     * @param response the password to return
     */
    public TestPasswordReader(String response) {
        setPassword(response);
    }

    /**
     * @see PasswordReader#readPassword
     */
    @Override
    public char[] readPassword(String prompt) throws IOException {
        if (passwordSequence == null) {
            throw new IOException("No passwords available");
        }

        final String pw = passwordSequence[passwordIndex];
        if (passwordIndex + 1 < passwordSequence.length) {
            passwordIndex++;
        }

        return pw.toCharArray();
    }

    /**
     * Set the password to be used as a response.
     * @param response the password to be used as a response for each
     *        call to readPassword
     */
    public void setPassword(String response) {
        if (response == null) {
            passwordSequence = null;
        } else {
            passwordSequence = new String[] { response };
            passwordIndex = 0;
        }
    }

    /**
     * Set the password sequence to be used as responses.
     * @param responses the passwords to be used as a response for each
     *        call to readPassword.  Each call to readPassword consumes the
     *        next response in the sequence.  If we get to the end of the
     *        response list, we stay pegged to the final response.
     */
    public void setPasswords(String[] responses) {
        if (responses == null || responses.length == 0) {
            passwordSequence = null;
        } else {
            passwordSequence = Arrays.copyOf(responses, responses.length);
            passwordIndex = 0;
        }
    }

    /**
     * Given the current password sequence, reset the response index back
     * to the start of the list.
     */
    public void reset() {
        if (passwordSequence != null) {
            passwordIndex = 0;
        }
    }
}

