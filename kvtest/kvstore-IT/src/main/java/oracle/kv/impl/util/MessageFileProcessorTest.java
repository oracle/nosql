/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;

import oracle.kv.TestBase;
import oracle.kv.util.MessageFileProcessor;

import org.junit.Test;

public class MessageFileProcessorTest extends TestBase {

    /*
     * Negative Tests
     */
    @Test
    public void testMissingSecondColInMsgFile() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "1234, This should fail";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testNonIntegerInFirstColInMsgFile() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "foo, This should fail";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testMissingThirdColInMsgFile() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "1234, 0";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testMissingStartQuoteInMsg() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "1234, 0, Check this out\"";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testMissingEndQuoteInMsg() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "1234, 0, \"Check this out";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testTooManyCommasInMsgFormat() {
        String badMessageFileFormat = "//  OK comment line\n" +
            ", 1234, 0,,, \"Check this out";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    @Test
    public void testZeroLengthMessage() {
        String badMessageFileFormat = "//  OK comment line\n" +
            "1234, 0,  ";
        ByteArrayInputStream bis =
            new ByteArrayInputStream(badMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            MessageFileProcessor.getMessageForKey("1234", ln);
            fail();
        } catch (Exception re) {
            assertTrue(re instanceof RuntimeException);
        }
    }

    /**
     * Positive tests
     */
    @Test
    public void testValidMsgFormat() {
        String commentLine =
            "//  OK comment line\n";
        String msgPart1 =
            "1234, 0,  \"";
        String msgPart2 =
            "This is what a message should {0} like";
        String goodMessageFileFormat =
            commentLine + msgPart1 + msgPart2 + "\"";

        ByteArrayInputStream bis =
            new ByteArrayInputStream(goodMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            String msg = MessageFileProcessor.getMessageForKey("1234", ln);
            assertTrue(msg.equals(msgPart2));
        } catch (Exception re) {
            fail();
        }

        bis = new ByteArrayInputStream(goodMessageFileFormat.getBytes());
        ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            ln.readLine();
            MessageFileProcessor.getKey(ln.readLine(), ln.getLineNumber());
        } catch (Exception re) {
            fail();
        }
    }

    @Test
    public void testCommasInMsg() {
        String commentLine =
            "//  OK comment line\n";
        String msgPart1 =
            "1234, 0,  \"";
        String msgPart2 =
            "Yea, this is what a message should {0} like, " +
            " but don't count on all messages being this clean";
        String goodMessageFileFormat =
            commentLine + msgPart1 + msgPart2 + "\"";

        ByteArrayInputStream bis =
            new ByteArrayInputStream(goodMessageFileFormat.getBytes());
        LineNumberReader ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            String msg = MessageFileProcessor.getMessageForKey("1234", ln);
            assertTrue(msg.equals(msgPart2));
        } catch (Exception re) {
            fail();
        }

        bis = new ByteArrayInputStream(goodMessageFileFormat.getBytes());
        ln = new LineNumberReader(new InputStreamReader(bis));
        try {
            ln.readLine();
            MessageFileProcessor.getKey(ln.readLine(), ln.getLineNumber());
        } catch (Exception re) {
            fail();
        }
    }
}
