/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 */

package oracle.nosql.common.qtf;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileUtils {
    public static String readFileToString(File file)
        throws IOException {

        BufferedReader bufferedReader =
            new BufferedReader(new FileReader(file));

        StringBuffer stringBuffer = new StringBuffer();
        String line = null;

        while ((line = bufferedReader.readLine())!=null) {

            stringBuffer.append(line).append("\n");
        }

        bufferedReader.close();
        return stringBuffer.toString();
    }

    public static String relativize(File base, File child) {
        // jdk7 return base.toPath().relativize(child.toPath()).toString();
        return base.toURI().relativize(child.toURI()).toString();
    }
}
