/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import oracle.kv.TestBase;
import oracle.kv.impl.util.SnapshotFileUtils.SnapshotOp;
import oracle.kv.impl.util.SnapshotFileUtils.UpdateConfigType;

import org.junit.Test;

public class SnapshotFileUtilsTest extends TestBase {

    private static final String PROP_KEY = "TestSnapshotFileUtils";
    private static final String PROP_VALUE = "TestOnly";
    private static final String PROP_NEW_VALUE = PROP_VALUE + "_new";
    private static final String PROP_FILE_NAME = "test.props";
    private static final String SNAPSHOT_STATUS_FILE_NAME = "snapshot.stat";

    @Test
    public void testSnapshotConfig() throws Exception {
        final File testBase = TestUtils.getTestDir();
        final File testFile = new File(testBase, PROP_FILE_NAME);
        final File snapshotDir = FileNames.getSnapshotBaseDir(testBase);
        final Properties props = new Properties();
        props.setProperty(PROP_KEY, PROP_VALUE);
        final FileOutputStream outStream = new FileOutputStream(testFile);
        props.store(outStream, "comments");
        outStream.close();
        SnapshotFileUtils.snapshotConfig(testFile, snapshotDir);
        final File snapshotFile =
            new File(snapshotDir, PROP_FILE_NAME);
        assertTrue(snapshotFile.exists());
        final FileInputStream inStream = new FileInputStream(snapshotFile);
        props.load(inStream);
        assertEquals(props.getProperty(PROP_KEY), PROP_VALUE);
    }

    @Test
    public void testRestoreSnapshotConfig() throws Exception {
        final File testBase = TestUtils.getTestDir();
        final File snapshotDir = FileNames.getSnapshotBaseDir(testBase);
        final File snapshotFile =
            new File(snapshotDir, PROP_FILE_NAME);
        final Properties props = new Properties();
        props.setProperty(PROP_KEY, PROP_VALUE);
        snapshotDir.mkdirs();
        final FileOutputStream outStream = new FileOutputStream(snapshotFile);
        props.store(outStream, "comments");
        outStream.close();

        final File statusFile = new File(snapshotDir,
                                         SNAPSHOT_STATUS_FILE_NAME);
        final FileOutputStream statusStream = new FileOutputStream(statusFile);
        final Properties statusProps = new Properties();
        statusProps.setProperty("SNAPSHOT", "COMPLETED");
        statusProps.store(statusStream, "comments");
        statusStream.close();

        final File destFile = new File(testBase, PROP_FILE_NAME);
        final FileOutputStream outStream2 = new FileOutputStream(destFile);
        props.setProperty(PROP_KEY, PROP_NEW_VALUE);
        props.store(outStream2, "comments");
        outStream2.close();

        try {
            SnapshotFileUtils.snapshotOpStart(SnapshotOp.RESTORE, snapshotDir);
            SnapshotFileUtils.restoreSnapshotConfig(
                 destFile, snapshotDir, UpdateConfigType.UNKNOWN);
            fail("Expected throw exception to warn user");
        } catch (IllegalStateException expected) {}

        SnapshotFileUtils.snapshotOpStart(SnapshotOp.RESTORE, snapshotDir);
        SnapshotFileUtils.restoreSnapshotConfig(
            destFile, snapshotDir, UpdateConfigType.FALSE);
        final FileInputStream inStream = new FileInputStream(destFile);
        props.load(inStream);
        inStream.close();
        assertEquals(props.getProperty(PROP_KEY), PROP_NEW_VALUE);

        SnapshotFileUtils.restoreSnapshotConfig(
             destFile, snapshotDir, UpdateConfigType.TRUE);
        final FileInputStream inStream2 = new FileInputStream(destFile);
        props.load(inStream2);
        inStream2.close();
        assertEquals(props.getProperty(PROP_KEY), PROP_VALUE);
    }
}
