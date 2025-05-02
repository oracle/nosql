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

package oracle.kv.impl.diagnostic;

import java.io.File;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

/**
 * Store all info of log section file, the file contains several log sections.
 * Its format is as follows:
 * line 1     --
 * ...          | -- log section 1
 * line 500   --
 * line 501   --
 * ...          | -- log section 2
 * line 1000  --
 * ...            -- ...
 * line x     --
 * ...          | -- log section z
 * line y     --
 * ...
 *
 * The class store file name of log section file and the first log item of
 * all sections
 */
public class LogSectionFileInfo {
    private String fileName;
    private String filePath;

    /* Store the first log item of all sections */
    private Deque<LogInfo> beginLogInfoList = new LinkedList<LogInfo>();


    public LogSectionFileInfo(File file, List<String> timeStampList) {
        filePath = file.getAbsolutePath();
        fileName = file.getName();

        for (String timeStamp : timeStampList) {
            add(timeStamp);
        }
    }

    public String getFileName() {
        return fileName;
    }

    public String getFilePath() {
        return filePath;
    }

    public LogInfo getFirst() {
        if (beginLogInfoList.isEmpty())
            return null;

        return beginLogInfoList.getFirst();
    }

    public LogInfo pop() {
        if (beginLogInfoList.isEmpty())
            return null;

        return beginLogInfoList.pop();
    }

    private void add(String logInfo) {
        beginLogInfoList.add(new LogInfo(logInfo));
    }

    public boolean isEmpty() {
        return beginLogInfoList.isEmpty();
    }
}
