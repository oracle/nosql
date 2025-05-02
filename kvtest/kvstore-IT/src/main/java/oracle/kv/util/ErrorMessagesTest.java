/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.text.MessageFormat;

import junit.framework.TestCase;
import oracle.kv.NoSQLRuntimeException;

public class ErrorMessagesTest extends TestCase {

    @Override
    protected void setUp()
        throws Exception {

        super.setUp();
    }

    @Override
    protected void tearDown()
        throws Exception {

        super.tearDown();
    }

    /**
     * Make sure that the messages file can be loaded from classpath
     */
    public void testResourceLoader() {
        NoSQLRuntimeException ex =
	    new NoSQLRuntimeException(ErrorMessage.NOSQL_2001, "100",
				      "initialized");

	/* Read the message directly from the messages file. */
	ClassLoader loader = ClassLoader.getSystemClassLoader();
	InputStream stream =
            loader.getResourceAsStream(MessageFileProcessor.MESSAGES_FILE_NAME);
	assertTrue(stream != null);
        LineNumberReader messageFile = new LineNumberReader
            (new BufferedReader
             (new InputStreamReader(stream)));
	String msgFromFile = null;

	try {
	    msgFromFile = MessageFileProcessor.getMessageForKey("2001",
								messageFile);
	    assertTrue(msgFromFile != null);
	} catch (IOException ioe) {
	    fail(ioe.toString());
	}

	String formattedMessage =
            MessageFormat.format(msgFromFile, "100", "initialized");
        assertEquals(ex.getMessage(), formattedMessage);
    }
}
