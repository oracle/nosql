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
package oracle.kv.impl.security.util;

import java.io.IOException;

import oracle.kv.util.shell.ShellInputReader;

/**
 * Read password using shell's reader. JLine console reader will be used if
 * exists.
 */
public class ShellPasswordReader implements PasswordReader {
    private final ShellInputReader inputReader;

    public ShellPasswordReader() {
        inputReader = ShellInputReader.getReader(System.in, System.out);
    }

    @Override
    public char[] readPassword(String prompt) throws IOException {

        return inputReader.readPassword(prompt);
    }
}
