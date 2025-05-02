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

import java.io.File;
import java.io.FileWriter;

import oracle.kv.KVVersion;

public class CreateRELEASEFile {
    public static void main(String argv[])
        throws Exception {

        /* Put the release version into a properties file. */
        if (argv.length < 3) {
            System.out.println
                ("Usage: CreateRELEASEFile <property file> " +
                 "<releaseid> <releasedate>");
            System.exit(-1);
        }

        FileWriter propFile = new FileWriter(new File(argv[0]));
        propFile.write("release.version.full=" +
                       KVVersion.CURRENT_VERSION + "\n");
        propFile.write("release.version=" +
                       KVVersion.CURRENT_VERSION.getMajor() + "." +
                       KVVersion.CURRENT_VERSION.getMinor() + "\n");
        propFile.write("release.numeric.version=" +
                       KVVersion.CURRENT_VERSION.getNumericVersionString() +
                       "\n");
        propFile.write("release.major=" +
                       KVVersion.CURRENT_VERSION.getMajor() + "\n");
        propFile.write("release.minor=" +
                       KVVersion.CURRENT_VERSION.getMinor() + "\n");
        propFile.write("release.patch=" +
                       KVVersion.CURRENT_VERSION.getPatch() + "\n");
        propFile.write("release.id=" + argv[1] + "\n");
        propFile.write("release.date=" + argv[2] + "\n");
        propFile.close();
    }
}
