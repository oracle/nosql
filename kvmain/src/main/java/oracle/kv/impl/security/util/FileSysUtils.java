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

import java.io.File;
import java.io.IOException;

/*
 * TODO: consider using the java.nio.file functionality present in Java 7+
 */
/**
 * A collection of File-system permission utility functions.
 */
public final class FileSysUtils {

    /* Use test windows file operations, for unit test running on Windows */
    public static boolean USE_TEST_WIN_FILE_OPERATIONS = false;

    /*
     * Not instantiable
     */
    private FileSysUtils() {
    }

    public static Operations selectOsOperations() {
        final String os = System.getProperty("os.name").toLowerCase();

        if (os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0 ||
            os.indexOf("aix") > 0  || os.indexOf("sunos") >= 0) {
            return new JavaAPIOperations();
        }

        if (os.indexOf("win") >= 0) {
            if (USE_TEST_WIN_FILE_OPERATIONS) {
                return new TestWindowsCmdLineOperations();
            }
            return new WindowsCmdLineOperations();
        }

        return new JavaAPIOperations();
    }

    public interface Operations {
        /**
         * Given an abstract file, attempt to change permissions so that it
         * is readable only by the owner of the file.
         * @param f a File referencing a file or directory, on which
         *  permissions are to be changed.
         * @return true if the permissions were successfully changed
         */
        boolean makeOwnerAccessOnly(File f)
            throws IOException;

        /**
         * Given an abstract file, attempt to change permissions so that it
         * is writable only by the owner of the file.
         * @param f a File referencing a file or directory, on which should
         *  permissions are to be changed.
         * @return true if the permissions were successfully changed
         */
        boolean makeOwnerOnlyWriteAccess(File f)
            throws IOException;

        /**
         * Given an abstract file, attempt to change permissions so that it
         * is not writable.
         * @param f a File referencing a file or directory, on which should
         * permissions are to be changed.
         * @return true if the permissions were successfully changed
         */
        boolean makeReadAccessOnly(File f)
            throws IOException;
    }

    /*
     * Implementation of Operations using Java API operations.
     * This approach fails on Windows 7.
     */
    static class JavaAPIOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            /* readable by nobody */
            boolean result = f.setReadable(false, false);

            /* writable by nobody */
            result = result && f.setWritable(false, false);

            /* add back readability for owner */
            result = result && f.setReadable(true, true);

            /* add back writability for owner */
            result = result && f.setWritable(true, true);

            return result;
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            /*throws IOException*/ {

            /* writable by nobody */
            final boolean result = f.setWritable(false, false);

            /* add back writability for owner */
            return f.setWritable(true, true) && result;
        }

        @Override
        public boolean makeReadAccessOnly(File f)
            /*throws IOException*/ {

            /* writable by nobody */
            boolean result = f.setWritable(false, false);

            return result;
        }
    }

    /*
     * Change access permissions using *nix shell tools
     */
    static class XNixCmdLineOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            final int oChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "o-rwx", f.getPath() });
            final int gChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "g-rwx", f.getPath() });

            return (oChmodResult == 0 && gChmodResult == 0);
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            throws IOException {

            final int oChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "o-w", f.getPath() });
            final int gChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "g-w", f.getPath() });

            return (oChmodResult == 0 && gChmodResult == 0);
        }

        @Override
        public boolean makeReadAccessOnly(File f)
            throws IOException {

            final int oChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "o-w", f.getPath() });
            final int gChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "g-w", f.getPath() });
            final int uChmodResult =
                SecurityUtils.runCmd(
                    new String[] { "chmod", "u-w", f.getPath() });
            return (oChmodResult == 0 &&
                    gChmodResult == 0 &&
                    uChmodResult == 0);
        }
    }

    static class WindowsCmdLineOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            throws IOException {

            throw new UnsupportedOperationException(
                "operation not supported on the windows platform");
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            /*throws IOException*/ {

            throw new UnsupportedOperationException(
                "operation not supported on the windows platform");
        }

        @Override
        public boolean makeReadAccessOnly(File f)
            /*throws IOException*/ {

            throw new UnsupportedOperationException(
                "operation not supported on the windows platform");
        }
    }

    static class TestWindowsCmdLineOperations implements Operations {
        @Override
        public boolean makeOwnerAccessOnly(File f)
            /*throws IOException*/  {

            /* Do nothing for unit test */
            return true;
        }

        @Override
        public boolean makeOwnerOnlyWriteAccess(File f)
            /*throws IOException*/ {

            /* Do nothing for unit test */
            return true;
        }

        @Override
        public boolean makeReadAccessOnly(File f)
            throws IOException {

            /* Do nothing for unit test */
            return true;
        }
    }
}
