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

/**
 * This class is to save the local path and remote path of a configuration file
 * of SNA.
 */
public class RemoteFile {
    private File file;
    private SNAInfo snaInfo;

    public RemoteFile(File file, SNAInfo snaInfo) {
        this.file = file;
        this.snaInfo = snaInfo;
    }

    /**
     * get local file path of configuration file.
     * @return local file path of configuration file.
     */
    public File getLocalFile() {
        return file;
    }

    public SNAInfo getSNAInfo() {
        return snaInfo;
    }

    @Override
    public String toString() {
        return snaInfo.getIP().getHostName() + ":" +
                ((snaInfo.getRemoteRootdir() == null) ? "" :
                    snaInfo.getRemoteRootdir()) + ":" +
                file.getName();
    }

    private String getRemotePath() {
        return snaInfo.getRemoteRootdir() + ":" + file.getName();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RemoteFile)) {
            return false;
        }
        RemoteFile remoteFile = (RemoteFile)obj;
        if (snaInfo.getIP().equals(remoteFile.snaInfo.getIP()) &&
                getRemotePath().equals(remoteFile.getRemotePath())) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result +
                ((getRemotePath() == null) ? 0 : getRemotePath().hashCode());
        result = prime * result +
                ((snaInfo.getIP() == null) ? 0 : snaInfo.getIP().hashCode());
        result = prime * result +
                ((file.getName() == null) ? 0 : file.getName().hashCode());
        return result;
    }
}
