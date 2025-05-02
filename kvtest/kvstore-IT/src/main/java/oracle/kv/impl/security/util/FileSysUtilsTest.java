/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.kv.impl.security.util;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import oracle.kv.TestBase;
import oracle.kv.impl.security.util.FileSysUtils.JavaAPIOperations;
import oracle.kv.impl.security.util.FileSysUtils.Operations;
import oracle.kv.impl.security.util.FileSysUtils.TestWindowsCmdLineOperations;
import oracle.kv.impl.security.util.FileSysUtils.XNixCmdLineOperations;

import org.junit.Test;

public class FileSysUtilsTest extends TestBase {

    private static String ERROR_MSG =
        "operation not supported on the windows platform";

    /* cache default os name */
    private static String OS_NAME = System.getProperty("os.name");

    @Override
    public void tearDown() {
        /* reset os name back to default */
        System.setProperty("os.name", OS_NAME);
        FileSysUtils.USE_TEST_WIN_FILE_OPERATIONS = true;
    }

    @Test
    public void testWindowsCmdLineOperation()
        throws Exception {

        System.setProperty("os.name", "win");
        FileSysUtils.USE_TEST_WIN_FILE_OPERATIONS = false;
        Operations op = FileSysUtils.selectOsOperations();
        File file = File.createTempFile("test", "log");
        file.deleteOnExit();
        try {
            op.makeOwnerAccessOnly(file);
            fail("expect UnsupportedOperation");
        } catch (UnsupportedOperationException uoe) {
            assertThat("Unsupported op", uoe.getMessage(),
                       containsString(ERROR_MSG));
        }

        try {
            op.makeOwnerOnlyWriteAccess(file);
            fail("expect UnsupportedOperation");
        } catch (UnsupportedOperationException uoe) {
            assertThat("Unsupported op", uoe.getMessage(),
                       containsString(ERROR_MSG));
        }
    }

    @Test
    public void testTestWindowsCmdLineOperations()
        throws Exception {

        File file = File.createTempFile("test", "log");
        file.deleteOnExit();

        TestWindowsCmdLineOperations testWin =
            new TestWindowsCmdLineOperations();
        boolean result = testWin.makeOwnerAccessOnly(file);
        assertTrue(result);
        result = testWin.makeOwnerOnlyWriteAccess(file);
        assertTrue(result);
        result = testWin.makeReadAccessOnly(file);
        assertTrue(result);
    }

    @Test
    public void testJavaAPIOperations()
        throws Exception {

        if (OS_NAME.indexOf("Windows") >= 0) {
            return;
        }
        File file = File.createTempFile("test", "log");
        file.deleteOnExit();

        /* Grant read and write permission to owner, group and others */
        int oChmodResult = SecurityUtils.runCmd(
            new String[] { "chmod", "a+rw", file.getPath() });
        assertEquals(oChmodResult, 0);
        PosixFileAttributeView ownerAttributeView =
            Files.getFileAttributeView(file.toPath(),
                                       PosixFileAttributeView.class);
        PosixFileAttributes attrs = ownerAttributeView.readAttributes();
        Set<PosixFilePermission> permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Test if making owner access only work */
        Operations op = new JavaAPIOperations();
        assertTrue(op.makeOwnerAccessOnly(file));
        assertTrue(file.canRead());
        assertTrue(file.canWrite());
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertFalse(permissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Grant read and write permission to owner, group and others */
        oChmodResult = SecurityUtils.runCmd(
            new String[] { "chmod", "a+rw", file.getPath() });
        assertEquals(oChmodResult, 0);
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Test if making owner write only work */
        assertTrue(op.makeOwnerOnlyWriteAccess(file));
        assertTrue(file.canRead());
        assertTrue(file.canWrite());
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Grant read and write permission to owner, group and others */
        oChmodResult = SecurityUtils.runCmd(
            new String[] { "chmod", "a+rw", file.getPath() });
        assertEquals(oChmodResult, 0);
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Test if making read only work */
        assertTrue(op.makeReadAccessOnly(file));
        assertTrue(file.canRead());
        assertFalse(file.canWrite());
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OWNER_WRITE));
    }

    @Test
    public void XNixCmdLineOperations()
        throws Exception {

        if (OS_NAME.indexOf("nix") < 0 && OS_NAME.indexOf("nux") < 0) {
            return;
        }
        File file = File.createTempFile("test", "log");
        file.deleteOnExit();

        /* Grant read and write permission to owner, group and others */
        int oChmodResult = SecurityUtils.runCmd(
            new String[] { "chmod", "a+rw", file.getPath() });
        assertEquals(oChmodResult, 0);
        PosixFileAttributeView ownerAttributeView =
            Files.getFileAttributeView(file.toPath(),
                                       PosixFileAttributeView.class);
        PosixFileAttributes attrs = ownerAttributeView.readAttributes();
        Set<PosixFilePermission> permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Test if making owner access only work */
        Operations op = new XNixCmdLineOperations();
        assertTrue(op.makeOwnerAccessOnly(file));
        assertTrue(file.canRead());
        assertTrue(file.canWrite());
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertFalse(permissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Grant read and write permission to owner, group and others */
        oChmodResult = SecurityUtils.runCmd(
            new String[] { "chmod", "a+rw", file.getPath() });
        assertEquals(oChmodResult, 0);
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));

        /* Test if making owner write only work */
        assertTrue(op.makeOwnerOnlyWriteAccess(file));
        assertTrue(file.canRead());
        assertTrue(file.canWrite());
        attrs = ownerAttributeView.readAttributes();
        permissions = attrs.permissions();
        assertTrue(permissions.contains(PosixFilePermission.GROUP_READ));
        assertTrue(permissions.contains(PosixFilePermission.OTHERS_READ));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_READ));
        assertFalse(permissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(permissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertTrue(permissions.contains(PosixFilePermission.OWNER_WRITE));
    }
}
