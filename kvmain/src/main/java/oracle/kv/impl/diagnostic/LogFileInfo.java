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
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This class contain the info of a log file. The format of log file name is as
 * follows:
 * [LogFileType][NodeID]_[FileSequeueID].log
 *
 * For example:
 * sn3_5.log
 * admin1_2.log
 *
 * And the format of rn log file name is as follows:
 * rg[GroupID]_rn[NodeID]_[FileSequeueID].log
 *
 * For example:
 * rg1_rn1_0.log
 */
public class LogFileInfo {
    private String LOG_FILENAME_FORMAT = "([0-9]+)\\_([0-9]+)\\.log$";
    private String RN_LOG_FILENAME_FORMAT =
            "rg([0-9]+)\\-rn([0-9]+)\\_([0-9]+)\\.log$";

    private int fileSequenceID;
    private File file;
    private int nodeID;
    private LogFileType type;

    /* the log file of a rn  */
    private int groupID;

    public LogFileInfo(File file, LogFileType type) {
        this.file = file;
        this.type = type;
        fileSequenceID = -1;
        nodeID = -1;
        groupID = -1;
        parse();
    }

    /**
     * Get node ID and log file sequence ID from file name
     */
    private void parse() {
        if (type == LogFileType.RN) {
            Pattern p = Pattern.compile(RN_LOG_FILENAME_FORMAT);
            Matcher matcher = p.matcher(file.getName());
            if (matcher.find()) {
                /* Group 1 is nodeID of log file */
                groupID = Integer.parseInt(matcher.group(1));

                /* Group 2 is nodeID of log file */
                nodeID = Integer.parseInt(matcher.group(2));

                /* Group 3 is sequene ID of log file */
                fileSequenceID = Integer.parseInt(matcher.group(3));
            }
        } else if (type != LogFileType.ALL) {
            Pattern p = Pattern.compile(type + LOG_FILENAME_FORMAT);
            Matcher matcher = p.matcher(file.getName());
            if (matcher.find()) {
                /* Group 1 is nodeID of log file */
                nodeID = Integer.parseInt(matcher.group(1));

                /* Group 2 is sequene ID of log file */
                fileSequenceID = Integer.parseInt(matcher.group(2));
            }
        }
    }

    public int getFileSequenceID() {
        return fileSequenceID;
    }

    public String getFilePath() {
        return file.getAbsolutePath();
    }

    public int getNodeID() {
        return nodeID;
    }

    /**
     * Get node name of owner of the log file
     */
    public String getNodeName() {
        /* The name of rn node has the group name */
        if (type == LogFileType.RN) {
            return "rg" + groupID + "-" +type.toString() + nodeID;
        } else if (type == LogFileType.ALL) {
            return "all";
        }
        return type.toString() + nodeID;
    }

    /**
     * LogFileType: admin log file, sn log file and rn log file
     */
    public static enum LogFileType {
        ADMIN("admin"),
        SN("sn"),
        RN("rn"),
        ALL("")/*All log files*/;

        private String prefixName;

        private LogFileType(String prefixName) {
            this.prefixName = prefixName;
        }

        @Override
        public String toString() {
            return prefixName;
        }
    }
    /**
     * The comparator ensure the log file is ordered by File
     * Sequence ID descend
     */
    public static class LogFileInfoComparator implements
            Comparator<LogFileInfo> {

        @Override
        public int compare(LogFileInfo o1, LogFileInfo o2) {
            if (o1.getFileSequenceID() < o2.getFileSequenceID()) {
                return 1;
            } else if (o1.getFileSequenceID() > o2.getFileSequenceID()) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
