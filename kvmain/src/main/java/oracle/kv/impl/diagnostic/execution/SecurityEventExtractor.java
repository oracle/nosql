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

package oracle.kv.impl.diagnostic.execution;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import oracle.kv.impl.diagnostic.LogFileInfo;
import oracle.kv.impl.diagnostic.LogFileInfo.LogFileType;
import oracle.kv.impl.diagnostic.LogInfo;

/**
 * The class is to extract all security events in log files.
 *
 */
public class SecurityEventExtractor extends LogExtractor {
    private String TEMP_FILE_SUFFIX = "_securityevent.tmp";
    private String SECURITY_EVENT_PATTERN = "KVAuditInfo";

    /*
     * Add prefix name for the file name of generated file to ensure the
     * file name is unique
     */
    private String prefixName;

    private File resultFile = null;

    public SecurityEventExtractor(String prefixName) {
        /* It is possible that security events exist all log files */
        super(LogFileType.ALL);
        this.prefixName = prefixName;
    }

    /**
     * Get the result security event file
     * @return the security event file
     */
    public File getResultFile() {
        return resultFile;
    }

    @Override
    protected void extract(Map<String, List<LogFileInfo>> logFileInfoMap)
            throws Exception {
        /* return when no log files found */
        if (logFileInfoMap.isEmpty()) {
            resultFile = null;
            return;
        }

        List<LogInfo> securityEventList = new ArrayList<LogInfo>();
        for (Map.Entry<String, List<LogFileInfo>> entry :
                logFileInfoMap.entrySet()) {
            List<LogFileInfo> logFileInfoList = entry.getValue();
            if (!logFileInfoList.isEmpty()) {
                for (LogFileInfo logFileInfo : logFileInfoList) {
                    getSecurityEvent(new File(logFileInfo.getFilePath()),
                            securityEventList);
                }
            }
        }

        BufferedWriter bw = null;
        try {
            resultFile = new File(prefixName + TEMP_FILE_SUFFIX);
            bw = new BufferedWriter(new FileWriter(resultFile));

            /* Sort security event order by time stamp */
            Collections.sort(securityEventList,
                    new LogInfo.LogInfoComparator());

            /*
             * Write all security event into file and remove the duplicate
             * event
             */
            LogInfo currentLog = null;
            for (LogInfo log : securityEventList) {
                if (currentLog == null || !currentLog.equals(log)) {
                    currentLog = log;
                    bw.write(log.toString());
                    bw.newLine();
                }
            }
        } catch (Exception ex) {
            resultFile = null;
            throw ex;
        } finally {
            try {
                if (bw != null)
                    bw.close();
            } catch (Exception ex) {
                throw ex;
            }
        }
    }

    /**
     * Extract security event log from a specified log file
     */
    private void getSecurityEvent(File file, List<LogInfo> list) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file));
            String line;
            while ((line = br.readLine()) != null) {
                /*
                 * When a log contains "KVAuditInfo", it is a security event
                 * log
                 */
                if (line.contains(SECURITY_EVENT_PATTERN)) {
                    list.add(new LogInfo(line));
                }
            }
        } catch (Exception ex) {
        } finally {

            try {
                if (br != null)
                    br.close();
            } catch (Exception ex) {
            }
        }
    }
}
